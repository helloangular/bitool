(ns bitool.pipeline.routes
  "API routes for intent-based pipeline creation."
  (:require [bitool.pipeline.intent :as intent]
            [bitool.pipeline.deploy :as deploy]
            [bitool.pipeline.planner :as planner]
            [bitool.pipeline.compiler :as compiler]
            [bitool.pipeline.preview :as preview]
            [bitool.pipeline.sdp :as sdp]
            [ring.util.http-response :as http-response]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.walk :as walk]
            [clojure.tools.logging :as log]))

(def ^:private max-text-length 4000)

(defn- apply-direct-edit
  "Try to apply common spec edits directly without LLM. Returns updated spec or nil."
  [spec edit-text]
  (let [text (string/lower-case (or edit-text ""))]
    (cond
      ;; Grain changes: "make X weekly/daily/monthly/hourly"
      (re-find #"(?:make|change|set)\s+(.+?)\s+(weekly|daily|monthly|hourly|minute)" text)
      (let [[_ model-hint grain] (re-find #"(?:make|change|set)\s+(.+?)\s+(weekly|daily|monthly|hourly|minute)" text)
            grain-map {"weekly" "week" "daily" "day" "monthly" "month" "hourly" "hour" "minute" "minute"}
            target-grain (get grain-map grain grain)
            model-words (string/split model-hint #"\s+")
            match-fn (fn [gm]
                       (let [name-lower (string/lower-case (or (:target-model gm) ""))]
                         (every? #(string/includes? name-lower %) model-words)))]
        (when-let [gold-models (:gold-models spec)]
          (let [updated (mapv (fn [gm]
                                (if (match-fn gm)
                                  (-> gm
                                      (assoc :grain target-grain)
                                      ;; Also update the model name if it contains the old grain
                                      (update :target-model
                                              #(string/replace % #"_(daily|weekly|monthly|hourly)"
                                                                       (str "_" (string/replace grain #"ly$" "")))))
                                  gm))
                              gold-models)]
            (when (not= gold-models updated)
              (-> spec
                  (assoc :gold-models updated)
                  ;; Update assumptions
                  (update :assumptions
                          (fn [a] (mapv (fn [s]
                                          (if (and (string? s) (string/includes? s "Gold models:"))
                                            (str "Gold models: " (string/join ", " (map :target-model updated)))
                                            s)) (or a [])))))))))

      ;; Add endpoint: "add fleet/X"
      (re-find #"(?:add|include)\s+(fleet/\S+|/\S+)" text)
      nil ;; Let LLM handle complex additions

      ;; Remove: "remove X"
      (re-find #"(?:remove|drop|delete)\s+(\S+)" text)
      nil ;; Let LLM handle removals

      :else nil)))

(defn- ok [data]
  (-> (http-response/ok (json/generate-string {:ok true :data data}))
      (assoc-in [:headers "Content-Type"] "application/json; charset=utf-8")))

(defn- error-response [status error msg]
  {:status  status
   :headers {"Content-Type" "application/json"}
   :body    (json/generate-string {:ok false :error error :message msg})})

(defn- parse-bool-param
  [value default]
  (cond
    (nil? value) default
    (instance? Boolean value) value
    :else
    (let [normalized (some-> value str string/trim string/lower-case)]
      (cond
        (#{"true" "1" "yes" "on"} normalized) true
        (#{"false" "0" "no" "off"} normalized) false
        :else default))))

(defn- parse-optional-int
  [value]
  (when (some? value)
    (cond
      (integer? value) value
      :else (Integer/parseInt (str value)))))

(defn- graph-id-of [result]
  (or (get-in result [:bronze :graph_id])
      (get-in result [:bronze :graph-id])
      (:graph_id result)
      (:graph-id result)))

(defn- graph-version-of [result]
  (or (get-in result [:scheduler :graph_version])
      (get-in result [:scheduler :graph-version])
      (get-in result [:bronze :graph_version])
      (get-in result [:bronze :graph-version])
      (:graph_version result)
      (:graph-version result)
      (:version result)))

(defn- resolve-intent-and-spec
  [params]
  (let [spec     (some-> (or (:spec params) (get params "spec")) walk/keywordize-keys)
        text     (some-> (or (:text params) (get params "text")) str)
        input    (some-> text string/trim)
        use-mock (parse-bool-param (or (:use_mock params) (get params "use_mock")) false)]
    (cond
      spec
      {:intent nil
       :spec spec}

      (seq input)
      (do
        (when (> (count input) max-text-length)
          (throw (ex-info "text is too long" {:status 413
                                              :error "payload_too_large"
                                              :max_length max-text-length})))
        (let [intent (if use-mock
                       (intent/parse-intent-mock input)
                       (try
                         (intent/parse-intent input)
                         (catch Exception llm-err
                           (log/warn llm-err "LLM intent parse failed during deploy, falling back to mock parser")
                           (intent/parse-intent-mock input))))]
          {:intent intent
           :spec (planner/plan-pipeline intent)}))

      :else
      (throw (ex-info "spec or text is required" {:status 400 :error "bad_request"})))))

;; ---------------------------------------------------------------------------
;; Route handlers
;; ---------------------------------------------------------------------------

(defn from-nl
  "POST /pipeline/from-nl
   Body: {text: '...natural language...', use_mock: true/false}
   Returns: PipelineSpec + preview"
  [request]
  (try
    (let [params (merge (:params request) (:body-params request))
          text   (or (:text params) (get params "text"))
          _      (when (empty? text)
                   (throw (ex-info "text is required" {:status 400 :error "bad_request"})))
          use-mock (boolean (or (:use_mock params) (get params "use_mock")))
          intent (if use-mock
                   (intent/parse-intent-mock text)
                   (try
                     (intent/parse-intent text)
                     (catch Exception llm-err
                       (log/warn llm-err "LLM intent parse failed, falling back to mock parser")
                       (intent/parse-intent-mock text))))
          spec   (planner/plan-pipeline intent)
          prev   (preview/generate-preview spec)]
      (ok {:intent  intent
           :spec    spec
           :preview prev
           :text    (preview/preview-text prev)}))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Pipeline from-nl failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn preview-pipeline
  "POST /pipeline/preview
   Body: PipelineSpec (or PipelineIntent)
   Returns: structured preview + text"
  [request]
  (try
    (let [raw-params (merge (:params request) (:body-params request))
          params (walk/keywordize-keys raw-params)
          ;; Accept either a full spec or an intent to plan
          spec   (if (:bronze-nodes params)
                   params ;; already a PipelineSpec
                   (planner/plan-pipeline params))
          prev   (preview/generate-preview spec)]
      (ok {:spec    spec
           :preview prev
           :text    (preview/preview-text prev)}))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Pipeline preview failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn apply-pipeline
  "POST /pipeline/apply
   Body: {spec: PipelineSpec, connection_id: int, created_by: string}
   Returns: {bronze: {...}, silver: [...], gold: [...]}
   Creates graph + proposal plans. Does NOT execute any runs."
  [request]
  (try
    (let [params        (merge (:params request) (:body-params request))
          spec          (or (:spec params) (get params "spec"))
          connection-id (parse-optional-int (:connection_id params))
          created-by    (or (:created_by params)
                            (get-in request [:session :user])
                            "system")
          _             (when-not spec
                          (throw (ex-info "spec is required" {:status 400 :error "bad_request"})))
          result        (compiler/apply-pipeline! spec
                                                  {:created-by created-by
                                                   :connection-id connection-id})]
      (ok result))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Pipeline apply failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn deploy-pipeline
  "POST /pipeline/deploy
   Body: {spec?: PipelineSpec, text?: string, connection_id?: int,
          publish_releases?: bool, execute_releases?: bool, attach_schedule?: bool,
          auto_publish?: bool}
   `auto_publish=true` enqueues Bronze and chains Silver/Gold execution through
   the worker queue (no manual review). Returns an end-to-end deployment summary."
  [request]
  (try
    (let [params            (merge (:params request) (:body-params request))
          {:keys [intent spec]} (resolve-intent-and-spec params)
          created-by        (or (:created_by params)
                                (get-in request [:session :user])
                                "system")
          connection-id     (parse-optional-int (:connection_id params))
          publish-releases  (parse-bool-param (or (:publish_releases params) (get params "publish_releases")) true)
          execute-releases  (parse-bool-param (or (:execute_releases params) (get params "execute_releases")) false)
          attach-schedule   (parse-bool-param (or (:attach_schedule params) (get params "attach_schedule")) true)
          auto-publish      (parse-bool-param (or (:auto_publish params) (get params "auto_publish")) false)
          result            (deploy/deploy-pipeline! spec
                                                    {:created-by created-by
                                                     :connection-id connection-id
                                                     :publish-releases publish-releases
                                                     :execute-releases execute-releases
                                                     :attach-schedule attach-schedule
                                                     :auto-publish auto-publish})
          prev              (preview/generate-preview spec)
          gid               (graph-id-of result)
          ver               (graph-version-of result)]
      (-> (ok {:intent intent
               :spec spec
               :preview prev
               :text (preview/preview-text prev)
               :result result})
          (assoc :session (assoc (:session request)
                                 :gid gid
                                 :ver ver))))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Pipeline deploy failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn list-connectors
  "GET /pipeline/connectors"
  [_request]
  (ok (planner/list-connectors)))

(defn get-connector
  "GET /pipeline/connectors/:system"
  [request]
  (let [system (get-in request [:path-params :system])]
    (if-let [ck (planner/load-connector-knowledge system)]
      (ok ck)
      (error-response 404 "not_found" (str "Connector not found: " system)))))

(defn list-metric-packages
  "GET /pipeline/metric-packages"
  [_request]
  (ok (planner/list-metric-packages)))

(defn generate-sdp
  "POST /pipeline/generate-sdp
   Body: PipelineSpec
   Returns: {silver: [...sql...], gold: [...sql...], combined: '...all sql...'}"
  [request]
  (try
    (let [spec (merge (:params request) (:body-params request))]
      (ok (sdp/generate-pipeline-sdp spec)))
    (catch Exception e
      (log/error e "SDP generation failed")
      (error-response 500 "internal_error" (.getMessage e)))))

(defn edit-pipeline
  "POST /pipeline/edit
   Body: {spec: PipelineSpec, text: 'make fleet utilization weekly'}
   Returns: updated PipelineSpec + preview"
  [request]
  (try
    (let [params       (merge (:params request) (:body-params request))
          current-spec (or (:spec params) (get params "spec"))
          edit-text    (or (:text params) (get params "text"))
          _            (when (empty? edit-text)
                         (throw (ex-info "text is required" {:status 400 :error "bad_request"})))
          _            (when-not current-spec
                         (throw (ex-info "spec is required" {:status 400 :error "bad_request"})))
          kw-spec      (walk/keywordize-keys current-spec)
          ;; Try direct spec mutation for common edits first
          direct-edit  (apply-direct-edit kw-spec edit-text)
          spec         (if direct-edit
                         direct-edit
                         ;; Fall back to LLM edit
                         (let [updated-intent (try
                                               (intent/edit-intent kw-spec edit-text)
                                               (catch Exception e
                                                 (log/warn e "LLM edit failed, returning original spec")
                                                 nil))]
                           (if updated-intent
                             (planner/plan-pipeline updated-intent)
                             kw-spec)))
          prev         (preview/generate-preview spec)]
      (ok {:intent  nil
           :spec    spec
           :preview prev
           :text    (preview/preview-text prev)}))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (error-response (or (:status d) 400) (or (:error d) "bad_request") (ex-message e))))
    (catch Exception e
      (log/error e "Pipeline edit failed")
      (error-response 500 "internal_error" (.getMessage e)))))

;; ---------------------------------------------------------------------------
;; Route table
;; ---------------------------------------------------------------------------

(defn pipeline-routes []
  ["/pipeline"
   ["/from-nl"                 {:post from-nl}]
   ["/edit"                    {:post edit-pipeline}]
   ["/preview"                 {:post preview-pipeline}]
   ["/apply"                   {:post apply-pipeline}]
   ["/deploy"                  {:post deploy-pipeline}]
   ["/connectors"              {:get list-connectors}]
   ["/connectors/:system"      {:get get-connector}]
   ["/metric-packages"         {:get list-metric-packages}]
   ["/generate-sdp"            {:post generate-sdp}]])
