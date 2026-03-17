(ns bitool.ingest.scheduler
  (:require [bitool.config :as config :refer [env]]
            [bitool.db :as db]
            [bitool.graph2 :as g2]
            [bitool.ingest.runtime :as runtime]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [mount.core :as mount]
            [next.jdbc :as jdbc]))

(def ^:private scheduler-state-table "scheduler_run_state")

(def ^:private weekday->int
  {"SUN" 0 "MON" 1 "TUE" 2 "WED" 3 "THU" 4 "FRI" 5 "SAT" 6})

(def ^:private month->int
  {"JAN" 1 "FEB" 2 "MAR" 3 "APR" 4 "MAY" 5 "JUN" 6
   "JUL" 7 "AUG" 8 "SEP" 9 "OCT" 10 "NOV" 11 "DEC" 12})

(defn- ensure-scheduler-state-table! []
  (jdbc/execute!
    db/ds
    [(str "CREATE TABLE IF NOT EXISTS " scheduler-state-table " ("
          "graph_id INTEGER NOT NULL, "
          "scheduler_node_id INTEGER NOT NULL, "
          "scheduled_for_utc TIMESTAMPTZ NOT NULL, "
          "triggered_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
          "finished_at_utc TIMESTAMPTZ NULL, "
          "status VARCHAR(64) NOT NULL, "
          "details TEXT NULL, "
          "PRIMARY KEY (graph_id, scheduler_node_id, scheduled_for_utc))")]))

(defn- parse-int [v]
  (try
    (Integer/parseInt (str v))
    (catch Exception _
      nil)))

(defn- named-token->int [s names]
  (or (get names (string/upper-case s))
      (parse-int s)))

(defn- field-values [expr min-v max-v names]
  (let [expr (string/upper-case (string/trim (str expr)))]
    (cond
      (= expr "*") (set (range min-v (inc max-v)))
      :else
      (->> (string/split expr #",")
           (mapcat
             (fn [part]
               (let [[base step-str] (string/split part #"/" 2)
                     step            (or (parse-int step-str) 1)
                     [start end]     (if (= base "*")
                                       [min-v max-v]
                                       (let [[a b] (string/split base #"-" 2)
                                             start (or (named-token->int a names) min-v)
                                             end   (or (named-token->int (or b a) names) max-v)]
                                         [start end]))]
                 (range start (inc end) step))))
           set))))

(defn- cron-match? [cron-expr zone-id instant]
  (let [[min-expr hour-expr day-expr month-expr weekday-expr] (string/split (string/trim cron-expr) #"\s+")
        zdt        (java.time.ZonedDateTime/ofInstant instant zone-id)
        weekday    (mod (.getValue (.getDayOfWeek zdt)) 7)]
    (and ((field-values min-expr 0 59 nil) (.getMinute zdt))
         ((field-values hour-expr 0 23 nil) (.getHour zdt))
         ((field-values day-expr 1 31 nil) (.getDayOfMonth zdt))
         ((field-values month-expr 1 12 month->int) (.getMonthValue zdt))
         ((field-values weekday-expr 0 6 weekday->int) weekday))))

(defn- enabled-scheduler? [node]
  (and (= "Sc" (:btype node))
       (not= false (:enabled node))
       (seq (string/trim (str (:cron_expression node))))))

(defn- reachable-api-node-ids [g scheduler-id]
  (loop [queue (keys (get-in g [:n scheduler-id :e]))
         visited #{}
         out []]
    (if-let [nid (first queue)]
      (if (contains? visited nid)
        (recur (rest queue) visited out)
        (let [node (get-in g [:n nid :na])]
          (recur (concat (rest queue) (keys (get-in g [:n nid :e])))
                 (conj visited nid)
                 (cond-> out (= "Ap" (:btype node)) (conj nid)))))
      (vec (distinct out)))))

(defn- scheduled-minute-utc [zone-id instant]
  (-> instant
      (java.time.ZonedDateTime/ofInstant zone-id)
      (.withSecond 0)
      (.withNano 0)
      (.withZoneSameInstant java.time.ZoneOffset/UTC)
      (.toInstant)))

(defn- claim-run-slot! [graph-id scheduler-node-id scheduled-for-utc]
  (let [result (jdbc/execute!
                 db/ds
                 [(str "INSERT INTO " scheduler-state-table
                       " (graph_id, scheduler_node_id, scheduled_for_utc, status) "
                       "VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING")
                  graph-id scheduler-node-id scheduled-for-utc "queued"])]
    (pos? (or (:next.jdbc/update-count (first result)) 0))))

(defn- finish-run-slot! [graph-id scheduler-node-id scheduled-for-utc status details]
  (jdbc/execute!
    db/ds
    [(str "UPDATE " scheduler-state-table
          " SET finished_at_utc = now(), status = ?, details = ? "
          "WHERE graph_id = ? AND scheduler_node_id = ? AND scheduled_for_utc = ?")
     status details graph-id scheduler-node-id scheduled-for-utc]))

(defn mark-slot-enqueued!
  [graph-id scheduler-node-id scheduled-for-utc details]
  (jdbc/execute!
    db/ds
    [(str "UPDATE " scheduler-state-table
          " SET status = 'queued', details = ? "
          "WHERE graph_id = ? AND scheduler_node_id = ? AND scheduled_for_utc = ?")
     (pr-str details) graph-id scheduler-node-id scheduled-for-utc]))

(defn complete-enqueued-slot!
  [graph-id scheduler-node-id scheduled-for-utc status details]
  (finish-run-slot! graph-id scheduler-node-id (java.time.Instant/parse (str scheduled-for-utc)) status (pr-str details)))

(defn execute-scheduler-node!
  [graph-id scheduler-node-id]
  (let [g        (db/getGraph graph-id)
        node     (g2/getData g scheduler-node-id)
        api-ids  (reachable-api-node-ids g scheduler-node-id)]
    (when-not (enabled-scheduler? node)
      (throw (ex-info "Scheduler node is disabled or missing cron_expression"
                      {:graph_id graph-id :scheduler_node_id scheduler-node-id})))
    (when-not (seq api-ids)
      (throw (ex-info "Scheduler node has no reachable API nodes"
                      {:graph_id graph-id :scheduler_node_id scheduler-node-id})))
    {:graph_id graph-id
     :scheduler_node_id scheduler-node-id
     :api_runs (mapv #(runtime/run-api-node! graph-id %) api-ids)}))

(defn run-scheduler-node!
  [graph-id scheduler-node-id]
  (execute-scheduler-node! graph-id scheduler-node-id))

(defn process-due-schedulers!
  ([] (process-due-schedulers! (java.time.Instant/now)))
  ([instant]
   (ensure-scheduler-state-table!)
   (reduce
     (fn [acc graph-id]
       (let [g (db/getGraph graph-id)
             schedulers (for [[node-id {:keys [na]}] (:n g)
                              :when (enabled-scheduler? na)]
                          [node-id na])]
         (into
          acc
          (keep
           (fn [[scheduler-node-id scheduler-node]]
             (let [zone-id (java.time.ZoneId/of (or (:timezone scheduler-node) "UTC"))]
               (when (cron-match? (:cron_expression scheduler-node) zone-id instant)
                 (let [scheduled-for-utc (scheduled-minute-utc zone-id instant)]
                   (when (claim-run-slot! graph-id scheduler-node-id scheduled-for-utc)
                     (try
                       (let [enqueue-fn (requiring-resolve 'bitool.ingest.execution/enqueue-scheduler-request!)
                             queued (enqueue-fn graph-id scheduler-node-id {:trigger-type "scheduler"
                                                                            :scheduled-for-utc scheduled-for-utc})]
                         (mark-slot-enqueued! graph-id scheduler-node-id scheduled-for-utc
                                              {:request_id (:request_id queued)
                                               :run_id (:run_id queued)})
                         {:graph_id graph-id
                          :scheduler_node_id scheduler-node-id
                          :scheduled_for_utc scheduled-for-utc
                          :request_id (:request_id queued)
                          :run_id (:run_id queued)})
                       (catch Exception e
                         (finish-run-slot! graph-id scheduler-node-id scheduled-for-utc "failed" (.getMessage e))
                         (log/error e "Failed to enqueue scheduler execution"
                                    {:graph-id graph-id
                                     :scheduler-node-id scheduler-node-id
                                     :scheduled-for-utc scheduled-for-utc})
                         {:graph_id graph-id
                          :scheduler_node_id scheduler-node-id
                          :scheduled_for_utc scheduled-for-utc
                          :status "failed"
                          :error (.getMessage e)})))))))
           schedulers))))
     []
     (db/list-graph-ids))))

(defn- log-scheduler-poll-failure!
  [e]
  (log/error e "Scheduler loop iteration failed"))

(defn- poll-schedulers-once!
  []
  (try
    (process-due-schedulers!)
    (catch Exception e
      (log-scheduler-poll-failure! e)
      nil)))

(mount/defstate ^{:on-reload :noop} scheduler-loop
  :start
  (when (config/enabled-role? :scheduler)
    (let [running?         (atom true)
          poll-interval-ms (or (some-> (get env :ingest-scheduler-poll-ms) parse-int)
                               30000)]
      (ensure-scheduler-state-table!)
      {:running? running?
       :future
       (future
         (while @running?
           (poll-schedulers-once!)
           (Thread/sleep poll-interval-ms)))}))
  :stop
  (when-let [running? (:running? scheduler-loop)]
    (reset! running? false)))
