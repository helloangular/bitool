(ns bitool.pipeline.deploy
  "Deploy orchestration for NL-generated pipelines.
   Applies the Bronze graph, publishes Silver/Gold releases, optionally executes
   them, and attaches a scheduler when the spec carries operational cadence."
  (:require [bitool.db :as db]
            [bitool.graph2 :as g2]
            [bitool.modeling.automation :as modeling]
            [bitool.pipeline.compiler :as compiler]
            [bitool.pipeline.sdp :as sdp]
            [clojure.set :as set]
            [clojure.string :as string]))

(defn- non-blank-str [value]
  (let [s (some-> value str string/trim)]
    (when (seq s) s)))

(defn- truthy?
  [value default]
  (cond
    (nil? value) default
    (instance? Boolean value) value
    :else (contains? #{"true" "1" "yes" "on"}
                     (some-> value str string/trim string/lower-case))))

(defn- scheduler-config
  [spec]
  (let [ops          (or (:ops spec) {})
        schedule-raw (:schedule ops)]
    (cond
      (string? schedule-raw)
      {:cron_expression (non-blank-str schedule-raw)
       :timezone        (or (non-blank-str (:timezone ops)) "UTC")
       :enabled         true
       :params          []}

      (map? schedule-raw)
      {:cron_expression (or (non-blank-str (:cron_expression schedule-raw))
                            (non-blank-str (:cron-expression schedule-raw))
                            (non-blank-str (:schedule schedule-raw)))
       :timezone        (or (non-blank-str (:timezone schedule-raw))
                            (non-blank-str (:timezone ops))
                            "UTC")
       :enabled         (truthy? (:enabled schedule-raw) true)
       :params          (vec (or (:params schedule-raw) []))}

      :else nil)))

(defn attach-scheduler!
  "Add and configure a scheduler node upstream of the Bronze API node."
  [graph-id api-node-id spec]
  (when-let [{:keys [cron_expression timezone enabled params]} (scheduler-config spec)]
    (when (seq cron_expression)
      (let [before-ids      (set (keys (:n (db/getGraph graph-id))))
            api-item        (g2/getData (db/getGraph graph-id) api-node-id)
            scheduler-x     (- (long (or (:x api-item) 320)) 220)
            scheduler-y     (long (or (:y api-item) 200))
            _               (g2/add-single-node graph-id nil "Schedule" "scheduler" scheduler-x scheduler-y)
            after-add       (db/getGraph graph-id)
            ;; add-single-node currently creates exactly one node, so the new scheduler
            ;; id is the set difference between the graph node ids before/after the call.
            scheduler-id    (first (sort (set/difference (set (keys (:n after-add))) before-ids)))
            _               (when-not scheduler-id
                              (throw (ex-info "Failed to create scheduler node"
                                              {:graph_id graph-id
                                               :api_node_id api-node-id})))
            _               (g2/connect-single-node graph-id scheduler-id api-node-id)
            graph-with-edge (db/getGraph graph-id)
            updated         (g2/save-sc graph-with-edge scheduler-id
                                        {:cron_expression cron_expression
                                         :timezone timezone
                                         :enabled enabled
                                         :params params})
            _               (db/insertGraph updated)
            final-graph     (db/getGraph graph-id)]
        {:scheduler_node_id scheduler-id
         :graph_id graph-id
         :graph_version (get-in final-graph [:a :v])
         :cron_expression cron_expression
         :timezone timezone
         :enabled enabled
         :params params
         :status "attached"}))))

(defn- publish-silver-releases!
  [silver-proposals created-by]
  (mapv (fn [{:keys [proposal_id]}]
          (modeling/publish-silver-proposal! proposal_id {:created_by created-by}))
        silver-proposals))

(defn- publish-gold-releases!
  [gold-proposals created-by]
  (mapv (fn [{:keys [proposal_id]}]
          (modeling/publish-gold-proposal! proposal_id {:created_by created-by}))
        gold-proposals))

(defn- execute-silver-releases!
  [silver-releases created-by]
  (mapv (fn [{:keys [release_id]}]
          (modeling/execute-silver-release! release_id {:created_by created-by}))
        silver-releases))

(defn- build-release-chain-steps
  "Convert published silver/gold release rows into sequential chain steps.
   Silvers run first (in order), then golds. Each step pins the published
   release_id so the worker doesn't pick up a newer active release."
  [silver-releases gold-releases]
  (vec (concat
         (map (fn [{:keys [release_id]}]
                {:kind    :silver_release
                 :binding {:mode "pinned" :pinned_release_id release_id}})
              silver-releases)
         (map (fn [{:keys [release_id]}]
                {:kind    :gold_release
                 :binding {:mode "pinned" :pinned_release_id release_id}})
              gold-releases))))

(defn- enqueue-bronze-with-chain!
  "Enqueue the Bronze API ingest, attaching a sequential silver/gold chain to
   its request_params. The execution worker advances the chain after each
   successful step. Returns the enqueue result (request_id, run_id, status)."
  [graph-id api-node-id chain-steps created-by]
  (let [enqueue! (requiring-resolve 'bitool.ingest.execution/enqueue-api-request!)
        request-params (when (seq chain-steps)
                         {:chain {:steps      chain-steps
                                  :created_by (or created-by "system")}})]
    (enqueue! graph-id api-node-id
              {:trigger-type   "auto-publish"
               :request-params request-params})))

(defn- silver-executions-ready-for-gold?
  [silver-executions]
  (every? #(= "succeeded" (:status %)) silver-executions))

(defn- execute-gold-releases!
  [gold-releases created-by]
  (mapv (fn [{:keys [release_id]}]
          (modeling/execute-gold-release! release_id {:created_by created-by}))
        gold-releases))

(defn deploy-pipeline!
  "Apply and deploy a planned pipeline.
   Defaults to publishing releases and attaching a scheduler when present.
   Release execution is opt-in because some targets require preconfigured jobs.

   :auto-publish (default false) runs the full Bronze→Silver→Gold chain through
   the execution queue: Bronze is enqueued, and on each successful run the
   worker advances the chain to the next published release. Implies
   publish-releases=true and bypasses the synchronous execute-releases path."
  [spec {:keys [created-by connection-id publish-releases execute-releases attach-schedule auto-publish]
         :or {created-by "system"
              publish-releases true
              execute-releases false
              attach-schedule true
              auto-publish false}
         :as opts}]
  (let [publish-releases  (or publish-releases auto-publish)
        execute-releases  (and execute-releases (not auto-publish))
        _                 (when (and execute-releases (not publish-releases))
                            (throw (ex-info "execute_releases requires publish_releases"
                                            {:status 400
                                             :error "bad_request"})))
        apply-result      (compiler/apply-pipeline! spec {:created-by created-by
                                                          :connection-id connection-id})
        bronze-result     (:bronze apply-result)
        silver-proposals  (:silver apply-result)
        gold-proposals    (:gold apply-result)
        silver-releases   (when publish-releases
                            (publish-silver-releases! silver-proposals created-by))
        gold-releases     (when publish-releases
                            (publish-gold-releases! gold-proposals created-by))
        silver-executions (when execute-releases
                            (execute-silver-releases! silver-releases created-by))
        gold-skipped?     (and execute-releases
                               (seq gold-releases)
                               (not (silver-executions-ready-for-gold? silver-executions)))
        gold-executions   (when (and execute-releases
                                     (seq gold-releases)
                                     (not gold-skipped?))
                            (execute-gold-releases! gold-releases created-by))
        chain-steps       (when auto-publish
                            (build-release-chain-steps silver-releases gold-releases))
        bronze-enqueue    (when (and auto-publish
                                     (:graph_id bronze-result)
                                     (:api_node_id bronze-result))
                            (enqueue-bronze-with-chain! (:graph_id bronze-result)
                                                        (:api_node_id bronze-result)
                                                        chain-steps
                                                        created-by))
        schedule-result   (when (and attach-schedule
                                     (:graph_id bronze-result)
                                     (:api_node_id bronze-result))
                            (attach-scheduler! (:graph_id bronze-result)
                                               (:api_node_id bronze-result)
                                               spec))
        warnings         (cond-> []
                           gold-skipped?
                           (conj "Gold execution was skipped because Silver releases did not finish synchronously."))
        sdp-artifacts    (sdp/generate-pipeline-sdp spec)]
    {:bronze bronze-result
     :silver silver-proposals
     :gold gold-proposals
     :published {:silver (vec (or silver-releases []))
                 :gold   (vec (or gold-releases []))}
     :executed {:silver (vec (or silver-executions []))
                :gold   (vec (or gold-executions []))}
     :auto_published (when auto-publish
                       {:bronze bronze-enqueue
                        :chain  {:steps chain-steps}})
     :scheduler schedule-result
     :sdp sdp-artifacts
     :warnings warnings
     :assumptions (:assumptions spec)
     :options {:publish_releases publish-releases
               :execute_releases execute-releases
               :auto_publish auto-publish
               :attach_schedule attach-schedule
               :connection_id connection-id
               :created_by created-by
               :raw opts}}))
