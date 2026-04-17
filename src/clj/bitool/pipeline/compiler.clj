(ns bitool.pipeline.compiler
  "Stage 3: PipelineSpec -> Bronze GIL build plan + Silver/Gold proposal mutations.
   Deterministic — no LLM, no side effects in plan mode.
   Apply mode persists through existing Bitool APIs."
  (:require [bitool.db :as db]
            [bitool.graph2 :as g2]
            [bitool.modeling.automation :as modeling]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]))

;; ---------------------------------------------------------------------------
;; Bronze GIL plan generation
;; ---------------------------------------------------------------------------

(defn bronze-gil
  "Generate a GIL document from PipelineSpec bronze-nodes and bronze-edges.
   This is a pure data structure — no side effects."
  [{:keys [pipeline-id pipeline-name bronze-nodes bronze-edges]}]
  {:intent     :build
   :gil-version "1.0"
   :graph-name (or pipeline-name pipeline-id)
   :nodes      (conj (mapv (fn [{:keys [node-ref node-type config]}]
                             {:node-ref node-ref
                              :type     node-type
                              :config   config})
                           bronze-nodes)
                     {:node-ref "o1" :type "output"})
   :edges      (or bronze-edges [["api1" "tg1"] ["tg1" "o1"]])})

;; ---------------------------------------------------------------------------
;; Bronze apply — uses graph2 directly (thin adapter)
;; ---------------------------------------------------------------------------

(defn- find-api-node-config [spec]
  (some (fn [n] (when (= "api-connection" (:node-type n)) (:config n)))
        (:bronze-nodes spec)))

(defn- find-target-node-config [spec]
  (some (fn [n] (when (= "target" (:node-type n)) (:config n)))
        (:bronze-nodes spec)))

(defn apply-bronze!
  "Create the Bronze graph from a PipelineSpec.
   Creates graph, adds API source node + Target node, connects edges, saves configs.
   Returns {:graph-id, :graph-version, :api-node-id, :target-node-id}."
  [spec {:keys [created-by connection-id]}]
  (let [graph-name  (or (:pipeline-name spec) (:pipeline-id spec))
        api-config  (find-api-node-config spec)
        tg-config   (find-target-node-config spec)
        _           (when-not api-config
                      (throw (ex-info "PipelineSpec missing api-connection node" {})))
        _           (when-not tg-config
                      (throw (ex-info "PipelineSpec missing target node" {})))

        ;; Step 1: Create graph shell (Output node = id 1)
        g0          (g2/createGraph graph-name)
        g1          (db/insertGraph g0)
        gid         (get-in g1 [:a :id])

        ;; Step 2: Add API source node.
        ;; graph2/add-single-node expects the semantic node kind, not the compact btype code.
        _           (g2/add-single-node gid nil (or (:api_name api-config) "API Source") "api-connection" 100 200)
        g2          (db/getGraph gid)
        api-node-id (first (sort (remove #{1} (keys (:n g2)))))

        ;; Step 3: Add Target node
        _           (g2/add-single-node gid nil "Target" "target" 300 200)
        g3          (db/getGraph gid)
        tg-node-id  (first (sort (remove #{1 api-node-id} (keys (:n g3)))))

        ;; Step 4: Connect edges: API -> Output -> Target
        _           (g2/connect-single-node gid api-node-id 1)
        _           (g2/connect-single-node gid 1 tg-node-id)

        ;; Step 5: Save API config
        g4          (db/getGraph gid)
        g5          (g2/save-api g4 api-node-id
                                 (merge api-config
                                        {:id api-node-id}))
        _           (db/insertGraph g5)

        ;; Step 6: Save Target config
        g6          (db/getGraph gid)
        tg-params   (cond-> tg-config
                      connection-id (assoc :connection_id connection-id))
        g7          (g2/save-target g6 tg-node-id tg-params)
        _           (db/insertGraph g7)

        final-g     (db/getGraph gid)]
    {:graph_id      gid
     :graph_version (get-in final-g [:a :v])
     :api_node_id   api-node-id
     :target_node_id tg-node-id}))

;; ---------------------------------------------------------------------------
;; Silver proposal generation
;; ---------------------------------------------------------------------------

(defn plan-silver-proposals
  "Return Silver proposal plans from PipelineSpec.
   These are data structures — apply-silver-proposals! persists them."
  [spec bronze-result]
  (let [graph-id    (or (:graph-id bronze-result) (:graph_id bronze-result))
        api-node-id (or (:api-node-id bronze-result) (:api_node_id bronze-result))]
    (mapv (fn [sp]
            {:graph-id       graph-id
             :api-node-id    api-node-id
           :endpoint-name  (:source-endpoint sp)
           :target-model   (:target-model sp)
           :entity-kind    (:entity-kind sp)
           :business-keys  (:business-keys sp)
           :columns        (:columns sp)
             :processing-policy (:processing-policy sp)})
          (:silver-proposals spec))))

(defn apply-silver-proposals!
  "Create Silver proposals through existing modeling automation.
   Requires Bronze graph to exist and have been run at least once (for schema).
   Returns vector of {:proposal-id, :target-model}."
  [silver-plans {:keys [created-by]}]
  (mapv (fn [{:keys [graph-id api-node-id endpoint-name target-model entity-kind business-keys
                     columns processing-policy] :as plan}]
          (let [base-result (modeling/propose-silver-schema!
                             {:graph-id graph-id
                              :api-node-id api-node-id
                              :endpoint-name endpoint-name
                              :created-by created-by})
                proposal-id (:proposal_id base-result)
                patch       (cond-> {:target_model target-model
                                     :entity_kind entity-kind}
                              (seq business-keys)
                              (assoc :materialization {:mode "merge"
                                                       :keys (vec business-keys)})
                              (map? processing-policy)
                              (assoc :processing_policy processing-policy)
                              (seq columns)
                              (assoc :columns columns))
                final-result (if (seq patch)
                               (modeling/update-silver-proposal!
                                proposal-id
                                {:proposal patch
                                 :created_by created-by})
                               base-result)]
            {:proposal_id (:proposal_id final-result)
             :target_model (:target_model final-result)
             :status (:status final-result)
             :layer (:layer final-result)
             :source_endpoint endpoint-name}))
        silver-plans))

;; ---------------------------------------------------------------------------
;; Gold proposal generation
;; ---------------------------------------------------------------------------

(defn plan-gold-models
  "Return Gold model plans from PipelineSpec."
  [spec]
  (:gold-models spec))

(defn apply-gold-proposals!
  "Create Gold proposals through existing modeling automation.
   Requires Silver proposals to be published first.
   Returns vector of {:proposal-id, :target-model}."
  [gold-plans {:keys [created-by silver-proposal-ids]}]
  (let [proposal-id-by-model (into {}
                                  (keep (fn [proposal]
                                          (when (:proposal_id proposal)
                                            [(:target_model proposal) (:proposal_id proposal)])))
                                  silver-proposal-ids)]
    (mapv (fn [{:keys [target-model grain depends-on measures dimensions sql-template] :as plan}]
            (let [source-proposal-id (or (some proposal-id-by-model depends-on)
                                         (:proposal_id (first silver-proposal-ids)))]
              (when-not source-proposal-id
                (throw (ex-info "Gold proposal requires at least one applied Silver proposal"
                                {:status 400
                                 :target_model target-model
                                 :depends_on depends-on})))
              (let [base-result (modeling/propose-gold-schema!
                                 {:silver_proposal_id source-proposal-id
                                  :created_by created-by})
                    patch       (cond-> {:target_model target-model}
                                  grain (assoc :semantic_grain grain)
                                  (seq depends-on) (assoc :depends_on (vec depends-on))
                                  (seq measures) (assoc :measures (vec measures))
                                  (seq dimensions) (assoc :dimensions (vec dimensions))
                                  sql-template (assoc :sql_template sql-template))
                    final-result (if (seq patch)
                                   (modeling/update-gold-proposal!
                                    (:proposal_id base-result)
                                    {:proposal patch
                                     :created_by created-by})
                                   base-result)]
                {:proposal_id (:proposal_id final-result)
                 :target_model (:target_model final-result)
                 :status (:status final-result)
                 :layer (:layer final-result)
                 :depends_on (vec depends-on)
                 :source_proposal_id source-proposal-id})))
          gold-plans)))

;; ---------------------------------------------------------------------------
;; Full pipeline apply
;; ---------------------------------------------------------------------------

(defn apply-pipeline!
  "Apply the full pipeline: create Bronze graph + Silver/Gold proposal plans.
   Does NOT execute any runs.
   Returns {:bronze, :silver, :gold, :assumptions}."
  [spec {:keys [created-by connection-id] :as opts}]
  (let [;; Step A: Create Bronze graph
        bronze-result (apply-bronze! spec opts)

        ;; Step B: Plan Silver proposals (not yet applied — needs Bronze run first)
        silver-plans  (plan-silver-proposals spec bronze-result)
        silver-result (apply-silver-proposals! silver-plans opts)

        ;; Step C: Plan Gold models (not yet applied — needs Silver publish first)
        gold-plans    (plan-gold-models spec)
        gold-result   (apply-gold-proposals! gold-plans
                                             (assoc opts :silver-proposal-ids
                                                    silver-result))]
    {:bronze      bronze-result
     :silver      silver-result
     :gold        gold-result
     :assumptions (:assumptions spec)}))
