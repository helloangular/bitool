(ns bitool.ops.queue
  (:require [bitool.db :as db]
            [bitool.control-plane :as control-plane]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- db-opts
  [conn]
  (jdbc/with-options conn {:builder-fn rs/as-unqualified-lower-maps}))

(defonce ^:private queue-tables-ready? (atom false))

(def ^:private max-bulk-size 100)

(defn- hours-from-range
  [time-range]
  (cond
    (number? time-range) (long time-range)
    (string? time-range)
    (if-let [[_ amount unit] (re-matches #"(?i)(\d+)(h|d)?" time-range)]
      (let [n (Long/parseLong amount)]
        (if (= "d" (some-> unit clojure.string/lower-case))
          (* 24 n)
          n))
      24)
    :else 24))

;; ---------------------------------------------------------------------------
;; 1. ensure-queue-tables!
;; ---------------------------------------------------------------------------

(defn ensure-queue-tables!
  "Create the ops_worker_drain table if it does not already exist.
   Uses an atom guard so the DDL is executed at most once per JVM lifetime."
  []
  (when-not @queue-tables-ready?
    (locking queue-tables-ready?
      (when-not @queue-tables-ready?
        (jdbc/execute!
         db/ds
         ["CREATE TABLE IF NOT EXISTS ops_worker_drain (
             worker_id        TEXT PRIMARY KEY,
             drain_requested  BOOLEAN NOT NULL DEFAULT true,
             requested_by     TEXT NOT NULL,
             requested_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
           )"])
        (reset! queue-tables-ready? true))))
  nil)

;; ---------------------------------------------------------------------------
;; 2. status-counts
;; ---------------------------------------------------------------------------

(defn status-counts
  "Return a map of {status count} for execution_request rows, optionally
   filtered by workspace-key and limited to the last `time-range` hours."
  [{:keys [workspace-key time-range]
    :or   {time-range 24}}]
  (let [hours (hours-from-range time-range)
        rows (jdbc/execute!
              (db-opts db/ds)
              ["SELECT status, count(*) AS cnt
                FROM execution_request
                WHERE (workspace_key = ? OR ? IS NULL)
                  AND requested_at_utc > now() - (? || ' hours')::interval
                GROUP BY status"
               workspace-key
               workspace-key
               (str hours)])
        counts (into {} (map (juxt :status :cnt)) rows)
        dlq-cnt (or (:cnt (jdbc/execute-one!
                           (db-opts db/ds)
                           ["SELECT COUNT(*) AS cnt
                             FROM execution_dlq d
                             JOIN execution_request r ON r.request_id = d.request_id
                             WHERE (r.workspace_key = ? OR ? IS NULL)
                               AND d.created_at_utc > now() - (? || ' hours')::interval"
                            workspace-key
                            workspace-key
                            (str hours)]))
                   0)]
    {:queued (long (or (get counts "queued") 0))
     :leased (long (or (get counts "leased") 0))
     :running (long (or (get counts "running") 0))
     :retrying (long (or (get counts "retrying") 0))
     :failed (long (or (get counts "failed") 0))
     :dlq (long dlq-cnt)}))

;; ---------------------------------------------------------------------------
;; 3. list-requests
;; ---------------------------------------------------------------------------

(defn list-requests
  "List execution_request rows with optional workspace-key and status filters.
   Returns results ordered by requested_at_utc DESC with LIMIT/OFFSET."
  [{:keys [workspace-key status limit offset]
    :or   {limit 50 offset 0}}]
  (let [clauses (cond-> []
                  workspace-key (conj "workspace_key = ?")
                  status        (conj "status = ?"))
        params  (cond-> []
                  workspace-key (conj workspace-key)
                  status        (conj status))
        sql-str (str "SELECT * FROM execution_request"
                     (when (seq clauses)
                       (str " WHERE " (clojure.string/join " AND " clauses)))
                     " ORDER BY requested_at_utc DESC"
                     " LIMIT ? OFFSET ?")]
    (mapv (fn [row]
            {:request_id (:request_id row)
             :source (str (or (:source_system row) "") "::" (or (:endpoint_name row) ""))
             :status (:status row)
             :worker_id (:worker_id row)
             :requested_at (:requested_at_utc row)
             :attempts (:retry_count row)
             :last_error (:error_message row)})
          (jdbc/execute!
           (db-opts db/ds)
           (into [sql-str] (conj params (long limit) (long offset)))))))

;; ---------------------------------------------------------------------------
;; 4. bulk-retry!
;; ---------------------------------------------------------------------------

(defn bulk-retry!
  "Retry up to 100 execution requests that are in `expected-status`.
   Atomically sets their status back to 'queued' using advisory lock + FOR
   UPDATE SKIP LOCKED.  Returns {:requested N :updated M :skipped (- N M)}."
  [{:keys [request-ids expected-status operator]
    :or   {expected-status "failed"}}]
  (let [ids      (vec (take max-bulk-size request-ids))
        n        (count ids)
        _        (when (zero? n)
                   (throw (ex-info "request-ids must not be empty" {:status 400})))
        updated  (jdbc/with-transaction [tx (db-opts db/ds)]
                   (jdbc/execute! tx ["SELECT pg_advisory_xact_lock(hashtext('ops-bulk-queue'))"])
                   (jdbc/execute! tx
                     ["WITH targets AS (
                         SELECT request_id FROM execution_request
                         WHERE request_id = ANY(?)
                           AND status = ?
                         FOR UPDATE SKIP LOCKED
                       )
                       UPDATE execution_request r
                       SET status = 'queued', updated_at_utc = now()
                       FROM targets t
                       WHERE r.request_id = t.request_id
                       RETURNING r.request_id"
                      (into-array java.util.UUID (map #(java.util.UUID/fromString (str %)) ids))
                      expected-status]))
        m        (count updated)
        result   {:requested n :updated m :skipped (- n m)}]
    (control-plane/record-audit-event!
     {:event_type "ops.queue.bulk_retry"
      :actor      operator
      :details    (merge result {:expected_status expected-status
                                 :request_ids     (mapv str ids)})})
    (log/info "bulk-retry!" result)
    result))

;; ---------------------------------------------------------------------------
;; 5. bulk-cancel!
;; ---------------------------------------------------------------------------

(defn bulk-cancel!
  "Cancel up to 100 execution requests (any active status).
   Sets status='cancelled'. Returns {:requested N :updated M :skipped (- N M)}."
  [{:keys [request-ids operator]}]
  (let [ids      (vec (take max-bulk-size request-ids))
        n        (count ids)
        _        (when (zero? n)
                   (throw (ex-info "request-ids must not be empty" {:status 400})))
        updated  (jdbc/with-transaction [tx (db-opts db/ds)]
                   (jdbc/execute! tx ["SELECT pg_advisory_xact_lock(hashtext('ops-bulk-queue'))"])
                   (jdbc/execute! tx
                     ["WITH targets AS (
                         SELECT request_id FROM execution_request
                         WHERE request_id = ANY(?)
                           AND status IN ('queued','leased','running','recovering_orphan')
                         FOR UPDATE SKIP LOCKED
                       )
                       UPDATE execution_request r
                       SET status = 'cancelled', finished_at_utc = now()
                       FROM targets t
                       WHERE r.request_id = t.request_id
                       RETURNING r.request_id"
                      (into-array java.util.UUID (map #(java.util.UUID/fromString (str %)) ids))]))
        m        (count updated)
        result   {:requested n :updated m :skipped (- n m)}]
    (control-plane/record-audit-event!
     {:event_type "ops.queue.bulk_cancel"
      :actor      operator
      :details    (merge result {:request_ids (mapv str ids)})})
    (log/info "bulk-cancel!" result)
    result))

;; ---------------------------------------------------------------------------
;; 6. bulk-requeue!
;; ---------------------------------------------------------------------------

(defn bulk-requeue!
  "Re-queue up to 100 dead-letter items back into 'queued' status.
   Sets status='queued' on items currently in the DLQ. Returns
   {:requested N :updated M :skipped (- N M)}."
  [{:keys [request-ids operator]}]
  (let [ids      (vec (take max-bulk-size request-ids))
        n        (count ids)
        _        (when (zero? n)
                   (throw (ex-info "request-ids must not be empty" {:status 400})))
        updated  (jdbc/with-transaction [tx (db-opts db/ds)]
                   (jdbc/execute! tx ["SELECT pg_advisory_xact_lock(hashtext('ops-bulk-queue'))"])
                   (jdbc/execute! tx
                     ["WITH targets AS (
                         SELECT r.request_id
                         FROM execution_request r
                         LEFT JOIN execution_dlq d ON d.request_id = r.request_id
                         WHERE r.request_id = ANY(?)
                           AND (r.status = 'failed' OR d.request_id IS NOT NULL)
                         FOR UPDATE SKIP LOCKED
                       )
                       UPDATE execution_request r
                       SET status = 'queued', updated_at_utc = now(),
                           worker_id = NULL, lease_expires_at_utc = NULL,
                           error_message = NULL, failure_class = NULL
                       FROM targets t
                       WHERE r.request_id = t.request_id
                       RETURNING r.request_id"
                      (into-array java.util.UUID (map #(java.util.UUID/fromString (str %)) ids))]))
        m        (count updated)
        result   {:requested n :updated m :skipped (- n m)}]
    (control-plane/record-audit-event!
     {:event_type "ops.queue.bulk_requeue"
      :actor      operator
      :details    (merge result {:request_ids (mapv str ids)})})
    (log/info "bulk-requeue!" result)
    result))

;; ---------------------------------------------------------------------------
;; 7. list-workers
;; ---------------------------------------------------------------------------

(defn list-workers
  "List distinct active workers (status IN leased/running) with their most
   recent heartbeat age and drain status."
  [{:keys [workspace-key]}]
  (ensure-queue-tables!)
  (let [params  (cond-> []
                  workspace-key (conj workspace-key workspace-key))
        sql-str (str
                 "WITH active_workers AS (
                    SELECT r.worker_id,
                           COUNT(*) AS active_requests,
                           MAX(r.last_heartbeat_at_utc) AS last_heartbeat_at_utc,
                           MAX(r.request_id::text) AS current_request
                    FROM execution_request r
                    WHERE r.worker_id IS NOT NULL
                      AND r.status IN ('leased','running')"
                 (when workspace-key
                   " AND r.workspace_key = ?")
                 "    GROUP BY r.worker_id
                  ),
                  worker_union AS (
                    SELECT worker_id FROM active_workers
                    UNION
                    SELECT worker_id FROM ops_worker_drain
                  )
                  SELECT w.worker_id,
                         COALESCE(a.active_requests, 0) AS active_requests,
                         a.current_request,
                         a.last_heartbeat_at_utc,
                         CASE
                           WHEN COALESCE(d.drain_requested, false) AND COALESCE(a.active_requests, 0) > 0 THEN 'draining'
                           WHEN COALESCE(d.drain_requested, false) THEN 'drained'
                           WHEN COALESCE(a.active_requests, 0) > 0 THEN 'running'
                           ELSE 'idle'
                         END AS status,
                         CASE
                           WHEN a.last_heartbeat_at_utc IS NULL THEN NULL
                           ELSE EXTRACT(EPOCH FROM (now() - a.last_heartbeat_at_utc))::INTEGER
                         END AS heartbeat_age_seconds,
                         COALESCE(d.drain_requested, false) AS drain_requested,
                         d.requested_by AS drain_requested_by,
                         d.requested_at_utc AS drain_requested_at_utc,
                         COALESCE(stats.jobs_completed, 0) AS jobs_completed
                  FROM worker_union w
                  LEFT JOIN active_workers a ON a.worker_id = w.worker_id
                  LEFT JOIN ops_worker_drain d ON d.worker_id = w.worker_id
                  LEFT JOIN (
                    SELECT worker_id, COUNT(*) AS jobs_completed
                    FROM execution_request
                    WHERE worker_id IS NOT NULL
                      AND status = 'succeeded'"
                 (when workspace-key
                   " AND workspace_key = ?")
                 "    GROUP BY worker_id
                  ) stats ON stats.worker_id = w.worker_id
                  ORDER BY active_requests DESC, w.worker_id ASC")]
    (mapv (fn [row]
            {:worker_id (:worker_id row)
             :status (:status row)
             :current_request (:current_request row)
             :last_heartbeat (:last_heartbeat_at_utc row)
             :heartbeat_stale (when-let [age (:heartbeat_age_seconds row)]
                                (> (long age) 120))
             :uptime (when-let [age (:heartbeat_age_seconds row)]
                       (str (long age) "s heartbeat age"))
             :jobs_completed (long (or (:jobs_completed row) 0))
             :drain_requested (boolean (:drain_requested row))})
          (jdbc/execute!
           (db-opts db/ds)
           (into [sql-str] params)))))

;; ---------------------------------------------------------------------------
;; 8. drain-worker!
;; ---------------------------------------------------------------------------

(defn drain-worker!
  "Mark a worker as draining. New work will not be assigned to it."
  [{:keys [worker-id operator]}]
  (ensure-queue-tables!)
  (when-not worker-id
    (throw (ex-info "worker-id is required" {:status 400})))
  (jdbc/execute!
   db/ds
   ["INSERT INTO ops_worker_drain (worker_id, drain_requested, requested_by, requested_at_utc)
     VALUES (?, true, ?, now())
     ON CONFLICT (worker_id) DO UPDATE
     SET drain_requested = true,
         requested_by    = EXCLUDED.requested_by,
         requested_at_utc = now()"
    worker-id
    (or operator "system")])
  (control-plane/record-audit-event!
   {:event_type "ops.worker.drain"
    :actor      operator
    :details    {:worker_id worker-id}})
  (log/info "drain-worker!" {:worker-id worker-id :operator operator})
  {:worker_id worker-id :drain_requested true})

;; ---------------------------------------------------------------------------
;; 9. undrain-worker!
;; ---------------------------------------------------------------------------

(defn undrain-worker!
  "Remove the drain flag for a worker, allowing it to accept new work again."
  [{:keys [worker-id operator]}]
  (ensure-queue-tables!)
  (when-not worker-id
    (throw (ex-info "worker-id is required" {:status 400})))
  (jdbc/execute!
   db/ds
   ["DELETE FROM ops_worker_drain WHERE worker_id = ?"
    worker-id])
  (control-plane/record-audit-event!
   {:event_type "ops.worker.undrain"
    :actor      operator
    :details    {:worker_id worker-id}})
  (log/info "undrain-worker!" {:worker-id worker-id :operator operator})
  {:worker_id worker-id :drain_requested false})

;; ---------------------------------------------------------------------------
;; 10. force-release!
;; ---------------------------------------------------------------------------

(defn force-release!
  "Force-release all requests held by a worker. Sets status='recovering_orphan'
   and clears worker_id so the requests can be re-claimed."
  [{:keys [worker-id operator]}]
  (when-not worker-id
    (throw (ex-info "worker-id is required" {:status 400})))
  (let [result (jdbc/execute!
                db/ds
                ["UPDATE execution_request
                  SET status = 'recovering_orphan',
                      worker_id = NULL,
                      lease_expires_at_utc = NULL
                  WHERE worker_id = ?
                    AND status IN ('leased','running')
                  RETURNING request_id"
                 worker-id])
        cnt    (count result)]
    (control-plane/record-audit-event!
     {:event_type "ops.worker.force_release"
      :actor      operator
      :details    {:worker_id       worker-id
                   :released_count  cnt
                   :request_ids     (mapv (comp str :request_id) result)}})
    (log/info "force-release!" {:worker-id worker-id :released cnt})
    {:worker_id worker-id :released cnt}))

;; ---------------------------------------------------------------------------
;; 11. list-dlq
;; ---------------------------------------------------------------------------

(defn list-dlq
  "List dead-letter queue items from execution_dlq, ordered by created_at_utc
   DESC, with LIMIT/OFFSET."
  [{:keys [workspace-key limit offset]
    :or   {limit 50 offset 0}}]
  (let [clauses (cond-> []
                  workspace-key (conj "r.workspace_key = ?"))
        params  (cond-> []
                  workspace-key (conj workspace-key))
        sql-str (str "SELECT d.*, r.workspace_key
                      FROM execution_dlq d
                      JOIN execution_request r ON r.request_id = d.request_id"
                     (when (seq clauses)
                       (str " WHERE " (clojure.string/join " AND " clauses)))
                     " ORDER BY d.created_at_utc DESC"
                     " LIMIT ? OFFSET ?")]
    (jdbc/execute!
     (db-opts db/ds)
     (into [sql-str] (conj params (long limit) (long offset))))))
