(ns bitool.pipeline-deploy-test
  (:require [bitool.ingest.execution :as ingest-execution]
            [bitool.modeling.automation :as modeling]
            [bitool.pipeline.compiler :as compiler]
            [bitool.pipeline.deploy :as deploy]
            [bitool.pipeline.sdp :as sdp]
            [clojure.test :refer :all]))

(deftest deploy-pipeline-publishes-and-attaches-schedule
  (let [captured (atom {})]
    (with-redefs [compiler/apply-pipeline! (fn [_spec opts]
                                             (swap! captured assoc :apply opts)
                                             {:bronze {:graph_id 700 :graph_version 3 :api_node_id 12}
                                              :silver [{:proposal_id 10 :target_model "silver_orders"}]
                                              :gold [{:proposal_id 20 :target_model "gold_orders_daily"}]})
                  modeling/publish-silver-proposal! (fn [proposal-id opts]
                                                     (swap! captured update :silver-publishes (fnil conj []) [proposal-id opts])
                                                     {:proposal_id proposal-id :release_id 101 :status "published"})
                  modeling/publish-gold-proposal! (fn [proposal-id opts]
                                                   (swap! captured update :gold-publishes (fnil conj []) [proposal-id opts])
                                                   {:proposal_id proposal-id :release_id 201 :status "published"})
                  deploy/attach-scheduler! (fn [graph-id api-node-id spec]
                                             (swap! captured assoc :scheduler [graph-id api-node-id (:ops spec)])
                                             {:scheduler_node_id 33
                                              :graph_version 4
                                              :status "attached"})
                  sdp/generate-pipeline-sdp (fn [_] {:combined "-- sql"})]
      (let [result (deploy/deploy-pipeline!
                    {:pipeline-name "Orders Pipeline"
                     :ops {:schedule "0 * * * *"}}
                    {:created-by "alice"
                     :connection-id 42
                     :publish-releases true
                     :execute-releases false
                     :attach-schedule true})]
        (is (= {:created-by "alice" :connection-id 42} (:apply @captured)))
        (is (= [[10 {:created_by "alice"}]] (:silver-publishes @captured)))
        (is (= [[20 {:created_by "alice"}]] (:gold-publishes @captured)))
        (is (= [700 12 {:schedule "0 * * * *"}] (:scheduler @captured)))
        (is (= 700 (get-in result [:bronze :graph_id])))
        (is (= 101 (get-in result [:published :silver 0 :release_id])))
        (is (= 33 (get-in result [:scheduler :scheduler_node_id])))
        (is (= "-- sql" (get-in result [:sdp :combined])))))))

(deftest deploy-pipeline-skips-gold-execution-when-silver-is-async
  (let [gold-executed (atom [])]
    (with-redefs [compiler/apply-pipeline! (fn [_spec _opts]
                                             {:bronze {:graph_id 700 :graph_version 3 :api_node_id 12}
                                              :silver [{:proposal_id 10}]
                                              :gold [{:proposal_id 20}]})
                  modeling/publish-silver-proposal! (fn [proposal-id _opts]
                                                     {:proposal_id proposal-id :release_id 101 :status "published"})
                  modeling/publish-gold-proposal! (fn [proposal-id _opts]
                                                   {:proposal_id proposal-id :release_id 201 :status "published"})
                  modeling/execute-silver-release! (fn [release-id _opts]
                                                    {:release_id release-id
                                                     :status "submitted"
                                                     :backend "databricks_job"})
                  modeling/execute-gold-release! (fn [release-id _opts]
                                                  (swap! gold-executed conj release-id)
                                                  {:release_id release-id :status "submitted"})
                  sdp/generate-pipeline-sdp (fn [_] {:combined "-- sql"})]
      (let [result (deploy/deploy-pipeline!
                    {:pipeline-name "Orders Pipeline"
                     :ops {}}
                    {:created-by "alice"
                     :publish-releases true
                     :execute-releases true
                     :attach-schedule false})]
        (is (= [] @gold-executed))
        (is (= "submitted" (get-in result [:executed :silver 0 :status])))
        (is (empty? (get-in result [:executed :gold])))
        (is (= ["Gold execution was skipped because Silver releases did not finish synchronously."]
               (:warnings result)))))))

;; ---------------------------------------------------------------------------
;; Phase 3 — auto_publish: queue-driven Bronze→Silver→Gold chain
;; ---------------------------------------------------------------------------

(deftest deploy-pipeline-auto-publish-enqueues-bronze-with-chain
  ;; auto_publish should publish Silver/Gold then enqueue Bronze with a chain
  ;; payload listing each silver_release then each gold_release as pinned steps.
  (let [captured (atom {})]
    (with-redefs [compiler/apply-pipeline! (fn [_spec _opts]
                                             {:bronze {:graph_id 700 :graph_version 3 :api_node_id 12}
                                              :silver [{:proposal_id 10 :target_model "silver_orders"}
                                                       {:proposal_id 11 :target_model "silver_customers"}]
                                              :gold [{:proposal_id 20 :target_model "gold_orders_daily"}]})
                  modeling/publish-silver-proposal! (fn [proposal-id _opts]
                                                     {:proposal_id proposal-id
                                                      :release_id (+ 100 proposal-id)
                                                      :status "published"})
                  modeling/publish-gold-proposal! (fn [proposal-id _opts]
                                                   {:proposal_id proposal-id
                                                    :release_id (+ 200 proposal-id)
                                                    :status "published"})
                  modeling/execute-silver-release! (fn [_ _]
                                                    (throw (ex-info "synchronous execute should not run under auto_publish" {})))
                  modeling/execute-gold-release! (fn [_ _]
                                                  (throw (ex-info "synchronous execute should not run under auto_publish" {})))
                  ingest-execution/enqueue-api-request! (fn [graph-id api-node-id opts]
                                                          (swap! captured assoc :enqueue [graph-id api-node-id opts])
                                                          {:request_id "req-1" :run_id "run-1" :status "queued"})
                  deploy/attach-scheduler! (fn [_ _ _] nil)
                  sdp/generate-pipeline-sdp (fn [_] {:combined "-- sql"})]
      (let [result (deploy/deploy-pipeline!
                    {:pipeline-name "Orders Pipeline" :ops {}}
                    {:created-by "harish"
                     :publish-releases false  ;; auto-publish should override to true
                     :execute-releases true   ;; auto-publish should suppress synchronous execute
                     :attach-schedule false
                     :auto-publish true})
            [graph-id api-node-id opts] (:enqueue @captured)
            chain (get-in opts [:request-params :chain])
            steps (:steps chain)]
        ;; Bronze enqueue used the apply'd graph + api-node id
        (is (= 700 graph-id))
        (is (= 12 api-node-id))
        (is (= "auto-publish" (:trigger-type opts)))
        (is (= "harish" (:created_by chain)))
        ;; 2 silvers + 1 gold = 3 chain steps, silvers first
        (is (= 3 (count steps)))
        (is (= [:silver_release :silver_release :gold_release]
               (mapv :kind steps)))
        ;; Bindings pin to the published release ids
        (is (= [110 111 220] (mapv #(get-in % [:binding :pinned_release_id]) steps)))
        (is (every? #(= "pinned" (get-in % [:binding :mode])) steps))
        ;; Result reports auto_published payload + suppresses synchronous execute
        (is (= "queued" (get-in result [:auto_published :bronze :status])))
        (is (empty? (get-in result [:executed :silver])))
        (is (empty? (get-in result [:executed :gold])))
        (is (true? (get-in result [:options :auto_publish])))
        (is (true? (get-in result [:options :publish_releases])))
        (is (false? (get-in result [:options :execute_releases])))))))

(deftest deploy-pipeline-auto-publish-without-bronze-skips-enqueue
  ;; Defensive: if apply-bronze somehow returns no graph_id/api_node_id, we
  ;; should not call enqueue-api-request! with nils.
  (let [enqueue-calls (atom 0)]
    (with-redefs [compiler/apply-pipeline! (fn [_ _]
                                             {:bronze {} :silver [] :gold []})
                  ingest-execution/enqueue-api-request! (fn [& _]
                                                         (swap! enqueue-calls inc)
                                                         {:request_id "x"})
                  deploy/attach-scheduler! (fn [_ _ _] nil)
                  sdp/generate-pipeline-sdp (fn [_] {:combined "-- sql"})]
      (deploy/deploy-pipeline! {:pipeline-name "x" :ops {}}
                               {:created-by "harish"
                                :auto-publish true
                                :attach-schedule false})
      (is (zero? @enqueue-calls)
          "Bronze enqueue must be skipped when graph_id/api_node_id are missing"))))
