(ns bitool.ops.correlation
  (:require [bitool.db :as db]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

;; ---------------------------------------------------------------------------
;; Drill-down correlation engine
;;
;; Three typed entry points: from-request, from-run, from-batch.
;; Each walks the execution_request -> execution_run -> ingest_batch_artifact_store
;; hierarchy and returns a unified envelope:
;;
;;   {:entity_kind "request"|"run"|"batch"
;;    :request     {...}   ; execution_request row or nil
;;    :runs        [...]   ; execution_run rows
;;    :batches     [...]   ; ingest_batch_artifact_store rows
;;    :bad_records {:count N :sample [...]}}
;; ---------------------------------------------------------------------------

(def ^:private request-table "execution_request")
(def ^:private run-table "execution_run")
(def ^:private artifact-table "ingest_batch_artifact_store")

(defn- db-opts
  [conn]
  (jdbc/with-options conn {:builder-fn rs/as-unqualified-lower-maps}))

;; ---------------------------------------------------------------------------
;; Internal query helpers -- each returns nil on missing tables / errors
;; ---------------------------------------------------------------------------

(defn- fetch-request
  "Fetch a single execution_request row by request_id (UUID string)."
  [request-id]
  (try
    (jdbc/execute-one!
     (db-opts db/ds)
     [(str "SELECT * FROM " request-table " WHERE request_id = ?::uuid")
      (str request-id)])
    (catch Exception e
      (log/debug e "fetch-request failed" {:request_id request-id})
      nil)))

(defn- fetch-request-by-run
  "Fetch the execution_request row that owns a given run_id."
  [run-id]
  (try
    (jdbc/execute-one!
     (db-opts db/ds)
     [(str "SELECT r.* FROM " request-table " r
            JOIN " run-table " er ON er.request_id = r.request_id
            WHERE er.run_id = ?::uuid")
      (str run-id)])
    (catch Exception e
      (log/debug e "fetch-request-by-run failed" {:run_id run-id})
      nil)))

(defn- fetch-runs-by-request
  "Fetch all execution_run rows for a given request_id."
  [request-id]
  (try
    (jdbc/execute!
     (db-opts db/ds)
     [(str "SELECT * FROM " run-table "
            WHERE request_id = ?::uuid
            ORDER BY started_at_utc DESC NULLS LAST")
      (str request-id)])
    (catch Exception e
      (log/debug e "fetch-runs-by-request failed" {:request_id request-id})
      [])))

(defn- fetch-run
  "Fetch a single execution_run row by run_id."
  [run-id]
  (try
    (jdbc/execute-one!
     (db-opts db/ds)
     [(str "SELECT * FROM " run-table " WHERE run_id = ?::uuid")
      (str run-id)])
    (catch Exception e
      (log/debug e "fetch-run failed" {:run_id run-id})
      nil)))

(defn- fetch-batches-by-run
  "Fetch all artifact-store rows for a given run_id."
  [run-id]
  (try
    (jdbc/execute!
     (db-opts db/ds)
     [(str "SELECT * FROM " artifact-table "
            WHERE run_id = ?
            ORDER BY created_at_utc DESC")
      (str run-id)])
    (catch Exception e
      (log/debug e "fetch-batches-by-run failed" {:run_id run-id})
      [])))

(defn- fetch-batch
  "Fetch a single artifact-store row by artifact_id."
  [batch-id]
  (try
    (jdbc/execute-one!
     (db-opts db/ds)
     [(str "SELECT * FROM " artifact-table " WHERE artifact_id = ?")
      (long batch-id)])
    (catch Exception e
      (log/debug e "fetch-batch failed" {:batch_id batch-id})
      nil)))

(defn- aggregate-bad-records
  "Count bad-record artifacts and return a small sample across a set of run_ids."
  [run-ids]
  (if (empty? run-ids)
    {:count 0 :sample []}
    (try
      (let [placeholders (clojure.string/join ", " (repeat (count run-ids) "?"))
            count-row (jdbc/execute-one!
                       (db-opts db/ds)
                       (into [(str "SELECT COUNT(*) AS cnt FROM " artifact-table "
                                    WHERE run_id IN (" placeholders ")
                                      AND artifact_kind = 'bad_record'")]
                             (map str run-ids)))
            sample    (jdbc/execute!
                       (db-opts db/ds)
                       (into [(str "SELECT * FROM " artifact-table "
                                    WHERE run_id IN (" placeholders ")
                                      AND artifact_kind = 'bad_record'
                                    ORDER BY created_at_utc DESC
                                    LIMIT 10")]
                             (map str run-ids)))]
        {:count  (long (or (:cnt count-row) 0))
         :sample (vec sample)})
      (catch Exception e
        (log/debug e "aggregate-bad-records failed" {:run_ids run-ids})
        {:count 0 :sample []}))))

(defn- aggregate-batch-counts
  "Return total batch count and row count summaries for a set of run_ids."
  [run-ids]
  (if (empty? run-ids)
    {:batch_count 0}
    (try
      (let [placeholders (clojure.string/join ", " (repeat (count run-ids) "?"))
            row (jdbc/execute-one!
                 (db-opts db/ds)
                 (into [(str "SELECT COUNT(*) AS batch_count
                              FROM " artifact-table "
                              WHERE run_id IN (" placeholders ")
                                AND artifact_kind != 'bad_record'")]
                        (map str run-ids)))]
        {:batch_count (long (or (:batch_count row) 0))})
      (catch Exception e
        (log/debug e "aggregate-batch-counts failed" {:run_ids run-ids})
        {:batch_count 0}))))

;; ---------------------------------------------------------------------------
;; Public API
;; ---------------------------------------------------------------------------

(defn from-request
  "Drill down from an execution request.

   Fetches the request, its runs, batch/artifact data, and bad-record counts."
  [request-id]
  (try
    (let [request (fetch-request request-id)]
      (if-not request
        {:entity_kind "request"
         :request     nil
         :runs        []
         :batches     []
         :bad_records {:count 0 :sample []}}
        (let [runs      (fetch-runs-by-request request-id)
              run-ids   (mapv #(str (:run_id %)) runs)
              batches   (vec (mapcat fetch-batches-by-run run-ids))
              bad-recs  (aggregate-bad-records run-ids)]
          {:entity_kind "request"
           :request     request
           :runs        (vec runs)
           :batches     batches
           :bad_records bad-recs})))
    (catch Exception e
      (log/warn e "from-request correlation failed" {:request_id request-id})
      {:entity_kind "request"
       :request     nil
       :runs        []
       :batches     []
       :bad_records {:count 0 :sample []}})))

(defn from-run
  "Drill down from an execution run.

   Fetches the run, walks up to its request, and gathers batch/bad-record data."
  [run-id]
  (try
    (let [run (fetch-run run-id)]
      (if-not run
        {:entity_kind "run"
         :request     nil
         :runs        []
         :batches     []
         :bad_records {:count 0 :sample []}}
        (let [request   (fetch-request-by-run run-id)
              batches   (fetch-batches-by-run run-id)
              bad-recs  (aggregate-bad-records [(str (:run_id run))])]
          {:entity_kind "run"
           :request     request
           :runs        [run]
           :batches     (vec batches)
           :bad_records bad-recs})))
    (catch Exception e
      (log/warn e "from-run correlation failed" {:run_id run-id})
      {:entity_kind "run"
       :request     nil
       :runs        []
       :batches     []
       :bad_records {:count 0 :sample []}})))

(defn from-batch
  "Drill down from an artifact/batch row.

   Walks up from ingest_batch_artifact_store to execution_run to execution_request."
  [batch-id]
  (try
    (let [batch (fetch-batch batch-id)]
      (if-not batch
        {:entity_kind "batch"
         :request     nil
         :runs        []
         :batches     []
         :bad_records {:count 0 :sample []}}
        (let [run-id    (str (:run_id batch))
              run       (fetch-run run-id)
              request   (when run (fetch-request-by-run run-id))
              batches   (fetch-batches-by-run run-id)
              bad-recs  (aggregate-bad-records [run-id])]
          {:entity_kind "batch"
           :request     request
           :runs        (if run [run] [])
           :batches     (vec batches)
           :bad_records bad-recs})))
    (catch Exception e
      (log/warn e "from-batch correlation failed" {:batch_id batch-id})
      {:entity_kind "batch"
       :request     nil
       :runs        []
       :batches     []
       :bad_records {:count 0 :sample []}})))
