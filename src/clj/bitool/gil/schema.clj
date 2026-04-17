(ns bitool.gil.schema
  (:require [bitool.graph2 :as g2]
            [clojure.string :as string]))

(def schema-version "1.0.0")
(def gil-version "1.0")

(def type-aliases
  {"ep" "endpoint", "Ep" "endpoint"
   "fi" "filter", "Fi" "filter"
   "au" "auth", "Au" "auth"
   "vd" "validator", "Vd" "validator"
   "rb" "response-builder", "Rb" "response-builder"
   "dx" "db-execute", "Dx" "db-execute"
   "rl" "rate-limiter", "Rl" "rate-limiter"
   "cr" "cors", "Cr" "cors"
   "lg" "logger", "Lg" "logger"
   "cq" "cache", "Cq" "cache"
   "ci" "circuit-breaker", "Ci" "circuit-breaker"
   "ev" "event-emitter", "Ev" "event-emitter"
   "sc" "scheduler", "Sc" "scheduler"
   "wh" "webhook", "Wh" "webhook"
   "fu" "function", "Fu" "function"
   "p" "projection", "P" "projection"
   "j" "join", "J" "join"
   "a" "aggregation", "A" "aggregation"
   "s" "sorter", "S" "sorter"
   "u" "union", "U" "union"
   "mp" "mapping", "Mp" "mapping"
   "c" "conditionals", "C" "conditionals"
   "t" "table", "T" "table"
   "kf" "kafka-source", "Kf" "kafka-source"
   "fs" "file-source", "Fs" "file-source"
   "tg" "target", "Tg" "target"
   "o" "output", "O" "output"})

(def node-types
  {"endpoint"         {:btype (get g2/btype-codes "endpoint")
                       :source? true
                       :terminal? false
                       :save-fn g2/save-endpoint
                       :config-keys [:http_method :route_path :path_params :query_params
                                     :body_schema :response_format :description]
                       :required-config [:http_method :route_path]}
   "filter"           {:btype (get g2/btype-codes "filter")
                       :source? false
                       :terminal? false
                       :save-fn nil
                       :config-keys [:sql]
                       :required-config []}
   "projection"       {:btype (get g2/btype-codes "projection")
                       :source? false
                       :terminal? false
                       :save-fn nil
                       :config-keys [:columns]
                       :required-config []}
   "join"             {:btype (get g2/btype-codes "join")
                       :source? false
                       :terminal? false
                       :save-fn nil
                       :config-keys [:join_type :left_key :right_key]
                       :required-config [:join_type]}
   "aggregation"      {:btype (get g2/btype-codes "aggregation")
                       :source? false
                       :terminal? false
                       :save-fn nil
                       :config-keys [:groupby :having]
                       :required-config [:groupby]}
   "sorter"           {:btype (get g2/btype-codes "sorter")
                       :source? false
                       :terminal? false
                       :save-fn nil
                       :config-keys [:sort_by]
                       :required-config [:sort_by]}
   "union"            {:btype (get g2/btype-codes "union")
                       :source? false
                       :terminal? false
                       :save-fn nil
                       :config-keys []
                       :required-config []}
   "mapping"          {:btype (get g2/btype-codes "mapping")
                       :source? false
                       :terminal? false
                       :save-fn nil
                       :config-keys [:mapping]
                       :required-config [:mapping]}
   "function"         {:btype (get g2/btype-codes "function")
                       :source? false
                       :terminal? false
                       :save-fn g2/save-logic
                       :config-keys [:fn_name :fn_params :fn_lets :fn_return :fn_outputs]
                       :required-config [:fn_name]}
   "conditionals"     {:btype (get g2/btype-codes "conditionals")
                       :source? false
                       :terminal? false
                       :save-fn g2/save-conditional
                       :config-keys [:cond_type :branches :default_branch :headers]
                       :required-config [:cond_type]}
   "table"            {:btype (get g2/btype-codes "table")
                       :source? true
                       :terminal? false
                       :save-fn nil
                       :config-keys []
                       :required-config []}
   "api-connection"   {:btype (get g2/btype-codes "api-connection")
                       :source? true
                       :terminal? false
                       :save-fn nil
                       :config-keys []
                       :required-config []}
   "kafka-source"     {:btype (get g2/btype-codes "kafka-source")
                       :source? true
                       :terminal? false
                       :save-fn g2/save-kafka-source
                       :config-keys [:source_system :connection_id :bootstrap_servers :security_protocol
                                     :consumer_group_id :topic_configs]
                       :required-config [:connection_id :topic_configs]}
   "file-source"      {:btype (get g2/btype-codes "file-source")
                       :source? true
                       :terminal? false
                       :save-fn g2/save-file-source
                       :config-keys [:source_system :connection_id :base_path :transport :file_configs]
                       :required-config [:file_configs]}
   "target"           {:btype (get g2/btype-codes "target")
                       :source? false
                       :terminal? false
                       :save-fn nil
                       :config-keys [:target_type :target_name :write_mode]
                       :required-config []}
   "output"           {:btype (get g2/btype-codes "output")
                       :source? false
                       :terminal? true
                       :save-fn nil
                       :config-keys []
                       :required-config []}
   "auth"             {:btype (get g2/btype-codes "auth")
                       :source? false
                       :terminal? false
                       :save-fn g2/save-auth
                       :config-keys [:auth_type :token_header :secret :claims_to_cols]
                       :required-config [:auth_type]}
   "validator"        {:btype (get g2/btype-codes "validator")
                       :source? false
                       :terminal? false
                       :save-fn g2/save-validator
                       :config-keys [:rules]
                       :required-config [:rules]}
   "rate-limiter"     {:btype (get g2/btype-codes "rate-limiter")
                       :source? false
                       :terminal? false
                       :save-fn g2/save-rl
                       :config-keys [:max_requests :window_seconds :key_type :burst]
                       :required-config [:max_requests :window_seconds]}
   "cors"             {:btype (get g2/btype-codes "cors")
                       :source? false
                       :terminal? false
                       :save-fn g2/save-cr
                       :config-keys [:allowed_origins :allowed_methods :allowed_headers
                                     :allow_credentials :max_age]
                       :required-config [:allowed_origins]}
   "logger"           {:btype (get g2/btype-codes "logger")
                       :source? false
                       :terminal? false
                       :save-fn g2/save-lg
                       :config-keys [:log_level :fields_to_log :destination :format :external_url]
                       :required-config []}
   "cache"            {:btype (get g2/btype-codes "cache")
                       :source? false
                       :terminal? false
                       :save-fn g2/save-cq
                       :config-keys [:cache_key :ttl_seconds :strategy]
                       :required-config [:ttl_seconds]}
   "circuit-breaker"  {:btype (get g2/btype-codes "circuit-breaker")
                       :source? false
                       :terminal? false
                       :save-fn g2/save-ci
                       :config-keys [:failure_threshold :reset_timeout :fallback_response]
                       :required-config [:failure_threshold]}
   "event-emitter"    {:btype (get g2/btype-codes "event-emitter")
                       :source? false
                       :terminal? false
                       :save-fn g2/save-ev
                       :config-keys [:topic :broker_url :key_template :format]
                       :required-config [:topic]}
   "response-builder" {:btype (get g2/btype-codes "response-builder")
                       :source? false
                       :terminal? false
                       :save-fn g2/save-response-builder
                       :config-keys [:status_code :response_type :headers :template]
                       :required-config []}
   "db-execute"       {:btype (get g2/btype-codes "db-execute")
                       :source? false
                       :terminal? false
                       :save-fn g2/save-dx
                       :config-keys [:connection_id :operation :sql_template :result_mode]
                       :required-config [:sql_template]}
   "scheduler"        {:btype (get g2/btype-codes "scheduler")
                       :source? true
                       :terminal? false
                       :save-fn g2/save-sc
                       :config-keys [:cron_expression :timezone :params]
                       :required-config [:cron_expression]}
   "webhook"          {:btype (get g2/btype-codes "webhook")
                       :source? true
                       :terminal? false
                       :save-fn g2/save-wh
                       :config-keys [:webhook_path :secret_header :secret_value :payload_format]
                       :required-config [:webhook_path]}})

(def http-methods #{"GET" "POST" "PUT" "DELETE" "PATCH"})
(def auth-types #{"api-key" "bearer" "basic" "oauth2"})
(def join-types #{"inner" "left" "right" "full"})
(def cond-types #{"if-else" "if-elif-else" "multi-if" "case" "cond" "pattern-match"})
(def vd-rule-types #{"required" "min" "max" "min-length" "max-length" "regex" "one-of" "type"})
(def dx-operations #{"SELECT" "INSERT" "UPDATE" "DELETE"})

(defn canonical-type
  "Resolve type aliases and btype shorthand to a canonical node type."
  [type-name]
  (let [t (some-> type-name str string/trim)]
    (when (seq t)
      (or (get type-aliases t)
          (get type-aliases (string/lower-case t))
          t))))

(defn valid-type? [type-name]
  (contains? node-types type-name))

(defn type-meta [type-name]
  (get node-types type-name))

(defn type->btype [type-name]
  (get-in node-types [type-name :btype]))

(defn btype->type [btype]
  (first
    (for [[type-name meta] node-types
          :when (= btype (:btype meta))]
      type-name)))

(defn source-type? [type-name]
  (true? (get-in node-types [type-name :source?])))

(defn terminal-type? [type-name]
  (true? (get-in node-types [type-name :terminal?])))

(defn valid-edge?
  "Check if parent-type -> child-type is legal per graph2/rectangles."
  [parent-type child-type]
  (let [parent-btype (type->btype parent-type)
        child-btype  (type->btype child-type)]
    (boolean
      (and parent-btype child-btype
           (some #{child-btype} (get g2/rectangles parent-btype))))))
