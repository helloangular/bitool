(ns bitool.ops.alerts
  (:require [bitool.db :as db]
            [bitool.control-plane :as control-plane]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [cheshire.core :as json]))

;; ---------------------------------------------------------------------------
;; Internals
;; ---------------------------------------------------------------------------

(def ^:private alert-table "ops_alert")
(def ^:private freshness-table "operations_endpoint_freshness_status")

(defonce ^:private alert-ready? (atom false))

(defn- db-opts
  [conn]
  (jdbc/with-options conn {:builder-fn rs/as-unqualified-lower-maps}))

(defn- now-utc []
  (java.time.Instant/now))

;; ---------------------------------------------------------------------------
;; Table bootstrap
;; ---------------------------------------------------------------------------

(defn ensure-alert-tables!
  "Create the ops_alert table and indexes if they do not already exist.
   Uses a double-check locking pattern so that concurrent callers only run
   the DDL once per JVM lifetime."
  []
  (when-not @alert-ready?
    (locking alert-ready?
      (when-not @alert-ready?
        (jdbc/execute!
         db/ds
         [(str "CREATE TABLE IF NOT EXISTS " alert-table " ("
               "alert_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(), "
               "alert_type     TEXT NOT NULL, "
               "severity       TEXT NOT NULL, "
               "source_key     TEXT NOT NULL, "
               "title          TEXT NOT NULL, "
               "detail_json    JSONB, "
               "state          TEXT NOT NULL DEFAULT 'fired', "
               "fired_at_utc   TIMESTAMPTZ NOT NULL DEFAULT now(), "
               "acked_by       TEXT, "
               "acked_at_utc   TIMESTAMPTZ, "
               "silenced_until TIMESTAMPTZ, "
               "resolved_at_utc TIMESTAMPTZ, "
               "resolved_by    TEXT, "
               "workspace_key  TEXT NOT NULL)")])
        (jdbc/execute!
         db/ds
         [(str "CREATE INDEX IF NOT EXISTS idx_ops_alert_active "
               "ON " alert-table "(workspace_key, state) "
               "WHERE state IN ('fired', 'acknowledged', 'silenced')")])
        (jdbc/execute!
         db/ds
         [(str "CREATE UNIQUE INDEX IF NOT EXISTS idx_ops_alert_dedup "
               "ON " alert-table "(workspace_key, source_key) "
               "WHERE state IN ('fired', 'acknowledged', 'silenced')")])
        (reset! alert-ready? true))))
  nil)

;; ---------------------------------------------------------------------------
;; Notification channels (pluggable dispatch)
;; ---------------------------------------------------------------------------

(defonce notification-channels (atom []))

(defn register-notification-channel!
  "Register a notification channel function.  `ch` must be a single-arity
   function that accepts an alert map and performs a side effect (e.g. send
   an email, post to Slack, enqueue to PagerDuty)."
  [ch]
  (when (fn? ch)
    (swap! notification-channels conj ch))
  nil)

(defn dispatch-notifications!
  "Invoke every registered notification channel with `alert`.
   Exceptions from individual channels are logged but do not propagate."
  [alert]
  (doseq [ch @notification-channels]
    (try
      (ch alert)
      (catch Exception e
        (log/warn e "Notification channel failed" {:alert_id (:alert_id alert)})))))

;; ---------------------------------------------------------------------------
;; fire!
;; ---------------------------------------------------------------------------

(defn fire!
  "Fire a new alert.  If an active alert for the same (workspace-key,
   source-key) already exists, the INSERT is silently skipped (dedup index).
   Returns the inserted row or nil when deduplicated."
  [{:keys [alert-type severity source-key title detail workspace-key]}]
  (ensure-alert-tables!)
  (let [detail-json (when detail (json/generate-string detail))
        row (jdbc/execute-one!
             (db-opts db/ds)
             [(str "INSERT INTO " alert-table "
                    (alert_type, severity, source_key, title, detail_json, state, fired_at_utc, workspace_key)
                    VALUES (?, ?, ?, ?, ?::jsonb, 'fired', now(), ?)
                    ON CONFLICT (workspace_key, source_key)
                      WHERE state IN ('fired', 'acknowledged', 'silenced')
                    DO NOTHING
                    RETURNING *")
              alert-type
              severity
              source-key
              title
              detail-json
              workspace-key])]
    (when row
      (dispatch-notifications! row))
    row))

;; ---------------------------------------------------------------------------
;; list-alerts
;; ---------------------------------------------------------------------------

(defn list-alerts
  "List active alerts with optional filters.  All parameters are optional.
   Results are ordered by fired_at_utc DESC."
  [{:keys [workspace-key state severity alert-type limit offset]
    :or   {limit 200 offset 0}}]
  (ensure-alert-tables!)
  (let [states  (->> (cond
                       (sequential? state) state
                       (string? state) (string/split state #",")
                       :else [])
                     (map string/trim)
                     (remove string/blank?)
                     vec)
        clauses (cond-> []
                  workspace-key (conj "workspace_key = ?")
                  (seq states)  (conj "state = ANY(?)")
                  severity      (conj "severity = ?")
                  alert-type    (conj "alert_type = ?"))
        params  (cond-> []
                  workspace-key (conj workspace-key)
                  (seq states)  (conj (into-array String states))
                  severity      (conj severity)
                  alert-type    (conj alert-type))
        sql-str (str "SELECT * FROM " alert-table
                     (when (seq clauses)
                       (str " WHERE " (string/join " AND " clauses)))
                     " ORDER BY fired_at_utc DESC"
                     " LIMIT ? OFFSET ?")]
    (jdbc/execute!
     (db-opts db/ds)
     (into [sql-str] (conj params (max 1 (long limit)) (max 0 (long offset)))))))

;; ---------------------------------------------------------------------------
;; alert-history
;; ---------------------------------------------------------------------------

(defn alert-history
  "Return resolved alerts, ordered by resolved_at_utc DESC."
  [{:keys [workspace-key limit offset]
    :or   {limit 200 offset 0}}]
  (ensure-alert-tables!)
  (let [clauses (cond-> ["state = 'resolved'"]
                  workspace-key (conj "workspace_key = ?"))
        params  (cond-> []
                  workspace-key (conj workspace-key))
        sql-str (str "SELECT * FROM " alert-table
                     " WHERE " (string/join " AND " clauses)
                     " ORDER BY resolved_at_utc DESC"
                     " LIMIT ? OFFSET ?")]
    (jdbc/execute!
     (db-opts db/ds)
     (into [sql-str] (conj params (max 1 (long limit)) (max 0 (long offset)))))))

;; ---------------------------------------------------------------------------
;; acknowledge!
;; ---------------------------------------------------------------------------

(defn acknowledge!
  "Acknowledge a fired alert.  Only transitions from state='fired'.
   Records an audit event and returns the updated row, or nil if the
   transition was not applicable."
  [alert-id user]
  (ensure-alert-tables!)
  (let [row (jdbc/execute-one!
             (db-opts db/ds)
             [(str "UPDATE " alert-table "
                    SET state = 'acknowledged',
                        acked_by = ?,
                        acked_at_utc = now()
                    WHERE alert_id = ?
                      AND state = 'fired'
                    RETURNING *")
              user
              alert-id])]
    (when row
      (control-plane/record-audit-event!
       {:event_type "ops.alert.acknowledged"
        :actor      user
        :details    {:alert_id   (str (:alert_id row))
                     :source_key (:source_key row)
                     :severity   (:severity row)}}))
    row))

;; ---------------------------------------------------------------------------
;; silence!
;; ---------------------------------------------------------------------------

(defn silence!
  "Silence a fired or acknowledged alert until `silence-until` (a
   java.time.Instant or compatible).  Records an audit event."
  [alert-id user silence-until]
  (ensure-alert-tables!)
  (let [row (jdbc/execute-one!
             (db-opts db/ds)
             [(str "UPDATE " alert-table "
                    SET state = 'silenced',
                        silenced_until = ?
                    WHERE alert_id = ?
                      AND state IN ('fired', 'acknowledged')
                    RETURNING *")
              (java.sql.Timestamp/from silence-until)
              alert-id])]
    (when row
      (control-plane/record-audit-event!
       {:event_type "ops.alert.silenced"
        :actor      user
        :details    {:alert_id       (str (:alert_id row))
                     :source_key     (:source_key row)
                     :silenced_until (str silence-until)}}))
    row))

;; ---------------------------------------------------------------------------
;; resolve!
;; ---------------------------------------------------------------------------

(defn resolve!
  "Resolve an active alert (fired, acknowledged, or silenced).
   Records an audit event."
  [alert-id user]
  (ensure-alert-tables!)
  (let [row (jdbc/execute-one!
             (db-opts db/ds)
             [(str "UPDATE " alert-table "
                    SET state = 'resolved',
                        resolved_by = ?,
                        resolved_at_utc = now()
                    WHERE alert_id = ?
                      AND state IN ('fired', 'acknowledged', 'silenced')
                    RETURNING *")
              user
              alert-id])]
    (when row
      (control-plane/record-audit-event!
       {:event_type "ops.alert.resolved"
        :actor      user
        :details    {:alert_id   (str (:alert_id row))
                     :source_key (:source_key row)
                     :severity   (:severity row)}}))
    row))

;; ---------------------------------------------------------------------------
;; condition-cleared? (heuristic)
;; ---------------------------------------------------------------------------

(defn- condition-cleared?
  "Return true when the source identified by `source-key` appears healthy in
   the endpoint freshness table.  The heuristic: if the source has a row
   whose `last_success_at_utc` is within its `freshness_sla_seconds`, the
   condition is considered cleared.

   Returns false when:
   - the freshness table does not exist
   - no matching row is found
   - any query error occurs"
  [source-key]
  (try
    (let [row (jdbc/execute-one!
               (db-opts db/ds)
               [(str "SELECT 1 AS healthy
                      FROM " freshness-table "
                      WHERE source_system || '::' || endpoint_name = ?
                        AND last_success_at_utc IS NOT NULL
                        AND EXTRACT(EPOCH FROM (now() - last_success_at_utc)) <= freshness_sla_seconds
                      LIMIT 1")
                source-key])]
      (boolean row))
    (catch Exception e
      (log/debug e "condition-cleared? check failed; treating as not cleared"
                 {:source_key source-key})
      false)))

;; ---------------------------------------------------------------------------
;; unsilence-expired-alerts!
;; ---------------------------------------------------------------------------

(defn unsilence-expired-alerts!
  "Process silenced alerts whose silence window has elapsed.
   For each expired alert:
     - if the underlying condition is cleared, resolve the alert
     - otherwise, re-fire it (state='fired', fresh fired_at_utc, clear ack fields)"
  []
  (ensure-alert-tables!)
  (let [expired (jdbc/execute!
                 (db-opts db/ds)
                 [(str "SELECT * FROM " alert-table "
                        WHERE state = 'silenced'
                          AND silenced_until <= now()")])]
    (doseq [alert expired]
      (let [alert-id   (:alert_id alert)
            source-key (:source_key alert)]
        (if (condition-cleared? source-key)
          ;; Auto-resolve: condition went away while silenced
          (let [resolved (jdbc/execute-one!
                          (db-opts db/ds)
                          [(str "UPDATE " alert-table "
                                 SET state = 'resolved',
                                     resolved_by = 'system/unsilence',
                                     resolved_at_utc = now()
                                 WHERE alert_id = ?
                                   AND state = 'silenced'
                                 RETURNING *")
                           alert-id])]
            (when resolved
              (control-plane/record-audit-event!
               {:event_type "ops.alert.auto_resolved"
                :actor      "system/unsilence"
                :details    {:alert_id   (str alert-id)
                             :source_key source-key
                             :reason     "condition_cleared_during_silence"}})))
          ;; Re-fire: condition is still active
          (let [refired (jdbc/execute-one!
                         (db-opts db/ds)
                         [(str "UPDATE " alert-table "
                                SET state = 'fired',
                                    fired_at_utc = now(),
                                    acked_by = NULL,
                                    acked_at_utc = NULL,
                                    silenced_until = NULL
                                WHERE alert_id = ?
                                  AND state = 'silenced'
                                RETURNING *")
                          alert-id])]
            (when refired
              (control-plane/record-audit-event!
               {:event_type "ops.alert.unsilenced"
                :actor      "system/unsilence"
                :details    {:alert_id   (str alert-id)
                             :source_key source-key
                             :reason     "silence_expired_condition_active"}})
              (dispatch-notifications! refired))))))
    (count expired)))

;; ---------------------------------------------------------------------------
;; auto-resolve-stale-alerts!
;; ---------------------------------------------------------------------------

(defn auto-resolve-stale-alerts!
  "Scan fired/acknowledged alerts and auto-resolve any whose underlying
   condition has cleared (source_key is back within its freshness SLA).
   Returns the number of alerts resolved."
  []
  (ensure-alert-tables!)
  (let [active (jdbc/execute!
                (db-opts db/ds)
                [(str "SELECT * FROM " alert-table "
                       WHERE state IN ('fired', 'acknowledged')")])]
    (->> active
         (filter (fn [alert] (condition-cleared? (:source_key alert))))
         (map (fn [alert]
                (let [alert-id (:alert_id alert)
                      resolved (jdbc/execute-one!
                                (db-opts db/ds)
                                [(str "UPDATE " alert-table "
                                       SET state = 'resolved',
                                           resolved_by = 'system/auto_resolve',
                                           resolved_at_utc = now()
                                       WHERE alert_id = ?
                                         AND state IN ('fired', 'acknowledged')
                                       RETURNING *")
                                 alert-id])]
                  (when resolved
                    (control-plane/record-audit-event!
                     {:event_type "ops.alert.auto_resolved"
                      :actor      "system/auto_resolve"
                      :details    {:alert_id   (str alert-id)
                                   :source_key (:source_key alert)
                                   :reason     "condition_cleared"}})
                    resolved))))
         (remove nil?)
         count)))
