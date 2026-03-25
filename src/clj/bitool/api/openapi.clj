(ns bitool.api.openapi
  "Convert an OpenAPI 3 spec (as a Clojure map) into a BiTool graph.

  For each path+method the importer emits a chain:
    Ep → [Au] → [Rl] → [Vd] → Rb → O

  Node IDs are assigned sequentially starting from 2 (node 1 is always O).
  Nodes are arranged in a column layout: x fixed at 100, y increments by 120 per chain.
  Optional middleware nodes (Au, Rl, Vd) are only added when the spec contains
  the corresponding extension or schema."
  (:require [clojure.string :as string]
            [bitool.graph2 :as g2]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- blank [s] (or (nil? s) (string/blank? (str s))))

(defn- media-type->str
  "Normalize media-type map keys that may be strings or namespaced keywords."
  [k]
  (cond
    (string? k) k
    (keyword? k) (if-let [ns (namespace k)] (str ns "/" (name k)) (name k))
    :else (str k)))

(defn- pick-json-schema
  "Pick the best JSON-like schema from an OpenAPI `content` map.
   Falls back to the first media type when no JSON media type is present."
  [content]
  (let [entries (seq (or content {}))
        json-val (some (fn [[k v]]
                         (let [s (string/lower-case (media-type->str k))]
                           (when (or (= s "application/json")
                                     (string/includes? s "+json")
                                     (string/includes? s "/json")
                                     (string/includes? s "json"))
                             v)))
                       entries)]
    (or (:schema json-val)
        (some-> entries first val :schema))))

(defn- resolve-ref
  "Resolve a $ref pointer like '#/components/schemas/Pet' within the spec.
   Returns the referenced schema map, or the input unchanged if no $ref."
  [schema spec]
  (if-let [ref (:$ref schema)]
    (let [path (->> (string/split ref #"/")
                    (drop 1)  ;; drop the leading '#'
                    (mapv keyword))]
      (resolve-ref (get-in spec path) spec))  ;; recurse for nested refs
    schema))

(defn- resolve-schema
  "Resolve a schema, handling $ref, and flatten 'allOf' by merging properties."
  [schema spec]
  (let [s (resolve-ref schema spec)]
    (if-let [all-of (:allOf s)]
      (let [resolved (map #(resolve-schema % spec) all-of)]
        {:type "object"
         :required (vec (distinct (mapcat :required resolved)))
         :properties (apply merge (map :properties resolved))})
      s)))

(defn- resolve-parameter
  "Resolve a parameter object, including component-level $ref pointers."
  [parameter spec]
  (resolve-ref parameter spec))

(defn- resolve-parameters
  "Resolve all parameter objects in a vector."
  [parameters spec]
  (mapv #(resolve-parameter % spec) (or parameters [])))

(defn- oapi-type->rule-type
  "Map a JSON Schema type string to the closest Vd rule type."
  [t]
  (case (string/lower-case (or t ""))
    "integer" "integer"
    "number"  "number"
    "boolean" "boolean"
    "string"  nil           ; no type rule needed — string is the default
    nil))

(defn- schema-prop->vd-rules
  "Convert a single JSON Schema property definition to zero or more Vd rule maps."
  [field-name schema required?]
  (let [rules (cond-> []
                required?
                (conj {:field field-name :rule "required" :value "" :message (str field-name " is required")})

                (and (:type schema) (oapi-type->rule-type (:type schema)))
                (conj {:field field-name :rule "type"
                       :value (oapi-type->rule-type (:type schema))
                       :message (str field-name " must be " (:type schema))})

                (:minLength schema)
                (conj {:field field-name :rule "min-length"
                       :value (str (:minLength schema))
                       :message (str field-name " min length " (:minLength schema))})

                (:maxLength schema)
                (conj {:field field-name :rule "max-length"
                       :value (str (:maxLength schema))
                       :message (str field-name " max length " (:maxLength schema))})

                (:minimum schema)
                (conj {:field field-name :rule "min"
                       :value (str (:minimum schema))
                       :message (str field-name " must be >= " (:minimum schema))})

                (:maximum schema)
                (conj {:field field-name :rule "max"
                       :value (str (:maximum schema))
                       :message (str field-name " must be <= " (:maximum schema))})

                (seq (:enum schema))
                (conj {:field field-name :rule "one-of"
                       :value (string/join "," (map str (:enum schema)))
                       :message (str field-name " must be one of: " (string/join ", " (:enum schema)))}))]
    rules))

(defn- request-body->vd-rules
  "Extract Vd rules from an OpenAPI requestBody object."
  [request-body spec]
  (let [raw-schema (pick-json-schema (:content request-body))
        schema     (resolve-schema (or raw-schema {}) spec)
        props      (or (:properties schema) {})
        requireds  (set (map keyword (or (:required schema) [])))]
    (mapcat (fn [[k v]] (schema-prop->vd-rules (name k) (resolve-schema v spec) (contains? requireds k)))
            props)))

(defn- params->vd-rules
  "Extract Vd rules from OpenAPI parameters list (query + path params)."
  [parameters spec]
  (mapcat (fn [p]
            (let [field    (or (:name p) "")
                  required (boolean (:required p))
                  schema   (resolve-schema (or (:schema p) {}) spec)]
              (schema-prop->vd-rules field schema required)))
          parameters))

(defn- params->ep-params
  "Convert OpenAPI parameters to Ep path_params / query_params vectors."
  [parameters spec]
  (let [resolved (resolve-parameters parameters spec)
        path-p   (filter #(= (:in %) "path") resolved)
        query-p  (filter #(= (:in %) "query") resolved)
        data-type (fn [p]
                    (let [schema (resolve-schema (or (:schema p) {}) spec)]
                      (or (:type schema) "varchar")))]
    {:path_params  (mapv #(hash-map :param_name (:name %) :data_type (data-type %)) path-p)
     :query_params (mapv #(hash-map :param_name (:name %) :data_type (data-type %)) query-p)}))

(defn- body-schema->ep-body
  "Convert requestBody schema properties to Ep body_schema vector."
  [request-body spec]
  (let [raw-schema (pick-json-schema (:content request-body))
        schema     (resolve-schema (or raw-schema {}) spec)
        props      (or (:properties schema) {})]
    (mapv (fn [[k v]]
            (let [v (resolve-schema v spec)]
              {:field_name (name k) :data_type (or (:type v) "varchar")}))
          props)))

(defn- success-response-schema
  "Return the resolved schema map for the first 2xx response, or nil."
  [responses spec]
  (let [codes (keys responses)
        ok    (first (filter #(re-matches #"2\d\d" (name %)) codes))]
    (when ok
      (let [raw (pick-json-schema (get-in responses [ok :content]))]
        (when raw (resolve-schema raw spec))))))

(defn- schema->rb-template
  "Convert a response schema to a flat Rb template (identity mapping)."
  [schema]
  (let [props (or (:properties schema) {})]
    (mapv (fn [[k _]] {:output_key (name k) :source_column (name k)}) props)))

(defn- success-status
  "Return the first 2xx status code string from the responses map."
  [responses]
  (let [codes (keys responses)
        ok    (first (filter #(re-matches #"2\d\d" (name %)) codes))]
    (if ok (name ok) "200")))

;; ---------------------------------------------------------------------------
;; Node builders — return [updated-graph, node-id]
;; ---------------------------------------------------------------------------

(defn- next-id [g] (+ 1 (count (:n g))))

(defn- add-raw-node [g btype name attrs x y]
  (let [id (next-id g)
        na (merge {:name name :btype btype :tcols {}} attrs)]
    [(assoc-in g [:n id] {:na na :e {}}) id]))

(defn- wire [g from to]
  (assoc-in g [:n from :e to] {}))

;; ---------------------------------------------------------------------------
;; Per-operation chain builder
;; ---------------------------------------------------------------------------

(defn- build-chain
  "Build one Ep→…→Rb chain for a single path+method.
   Returns updated graph. output-id is always 1."
  [g path method op-map security-schemes spec x y]
  (let [method-str   (string/upper-case (name method))
        op-id        (or (:operationId op-map) (str method-str " " path))
        parameters   (or (:parameters op-map) [])
        request-body (when-let [rb (:requestBody op-map)] (resolve-ref rb spec))
        responses    (or (:responses op-map) {})
        security     (or (:security op-map) [])

        ;; Ep node
        {:keys [path_params query_params]} (params->ep-params parameters spec)
        body-schema  (body-schema->ep-body request-body spec)
        [g ep-id]    (add-raw-node g "Ep" op-id
                                   {:route_path   path
                                    :http_method  method-str
                                    :description  (or (:description op-map) (:summary op-map) "")
                                    :path_params  path_params
                                    :query_params query_params
                                    :body_schema  body-schema}
                                   x y)
        ;; Generate tcols so Ep columns show up in the UI
        ep-tcols     (g2/ep-params->tcols ep-id path_params query_params body-schema)
        g            (assoc-in g [:n ep-id :na :tcols] ep-tcols)

        ;; Au node — only when operation has a non-empty security requirement
        ;; Guard against `security: [{}]` (empty object = no-auth override in OpenAPI)
        first-sec    (first (filter #(seq %) security))
        [g prev-id]  (if first-sec
                       (let [sec-name   (-> first-sec keys first name)
                             sec-scheme (get security-schemes (keyword sec-name) {})
                             auth-type  (case (:type sec-scheme)
                                          "http"   (if (= (:scheme sec-scheme) "bearer") "bearer" "basic")
                                          "apiKey" "api-key"
                                          "bearer")
                             hdr        (or (:name sec-scheme) "authorization")
                             [g au-id]  (add-raw-node g "Au" (str op-id "-auth")
                                                      {:auth_type    auth-type
                                                       :token_header (string/lower-case hdr)
                                                       :secret       ""}
                                                      (+ x 200) y)]
                         [(wire g ep-id au-id) au-id])
                       [g ep-id])

        ;; Vd node — only when there are rules to add
        all-vd-rules (concat (params->vd-rules parameters spec)
                             (request-body->vd-rules request-body spec))
        [g prev-id]  (if (seq all-vd-rules)
                       (let [[g vd-id] (add-raw-node g "Vd" (str op-id "-validator")
                                                     {:rules (vec all-vd-rules)}
                                                     (+ x 400) y)]
                         [(wire g prev-id vd-id) vd-id])
                       [g prev-id])

        ;; Rb node
        resp-schema  (success-response-schema responses spec)
        rb-template  (if resp-schema (schema->rb-template resp-schema) [])
        status-code  (success-status responses)
        [g rb-id]    (add-raw-node g "Rb" (str op-id "-response")
                                   {:status_code   status-code
                                    :response_type "json"
                                    :headers       ""
                                    :template      rb-template}
                                   (+ x 600) y)
        g            (-> g
                         (wire prev-id rb-id)
                         (wire rb-id 1))]
    g))

;; ---------------------------------------------------------------------------
;; Public helpers
;; ---------------------------------------------------------------------------

(defn resolve-schema*
  "Public wrapper around resolve-schema for use outside this namespace.
   Resolves $ref and allOf within the given OpenAPI spec."
  [schema spec]
  (resolve-schema schema spec))

;; Public entry point
;; ---------------------------------------------------------------------------

;; Valid HTTP method keys per OpenAPI 3 spec
(def ^:private http-method-keys
  #{:get :put :post :delete :options :head :patch :trace})

(defn spec->graph
  "Convert a parsed OpenAPI 3 spec map into a BiTool graph map.
   The graph always has node 1 = Output. Each path+method gets its own chain.
   Path-level parameters are merged with operation-level parameters (operation wins on conflict)."
  [spec graph-name]
  (let [paths            (or (:paths spec) {})
        security-schemes (get-in spec [:components :securitySchemes] {})
        base-graph       {:a {:name graph-name :v 0 :id 0}
                          :n {1 {:na {:name "Output" :btype "O" :tcols {}} :e {}}}}
        [final-g _]      (reduce
                           (fn [[g row-idx] [path path-item]]
                             ;; Path-level params indexed by "in+name" for merge
                             (let [path-params-raw (resolve-parameters (:parameters path-item) spec)
                                   path-param-idx  (into {} (map #(vector (str (:in %) ":" (:name %)) %) path-params-raw))
                                   ;; Only iterate real HTTP method keys
                                   http-ops        (select-keys path-item http-method-keys)
                                   [g _]           (reduce
                                                     (fn [[g col-idx] [method op-map]]
                                                       (let [;; Merge path params with op params; op wins
                                                             op-params    (resolve-parameters (:parameters op-map) spec)
                                                             op-param-idx (into {} (map #(vector (str (:in %) ":" (:name %)) %) op-params))
                                                             merged-params (vals (merge path-param-idx op-param-idx))
                                                             op-map'      (assoc op-map :parameters (vec merged-params))
                                                             x (* col-idx 900)
                                                             y (* row-idx 150)
                                                             g (build-chain g (name path) method op-map'
                                                                            security-schemes spec x y)]
                                                         [g (inc col-idx)]))
                                                     [g 0]
                                                     http-ops)]
                               [g (inc row-idx)]))
                           [base-graph 0]
                           paths)]
    final-g))
