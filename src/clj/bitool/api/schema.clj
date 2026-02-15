(ns bitool.api.schema
  (:require [clj-http.client :as http]
            [bitool.macros :refer :all]
            [clj-yaml.core :as yaml]
            [clojure.string :as str]
            [clojure.walk :as walk]))

;; -----------------------------
;; Load + parse OpenAPI (YAML)
;; -----------------------------

(defn fetch-openapi!
  "Fetch and parse an OpenAPI YAML/JSON document from URL.
   Returns a keywordized Clojure map."
  [url]
  (let [resp (http/get url {:as :text
                            :throw-exceptions true})
        body (:body resp)
        m    (yaml/parse-string body)]
    (walk/keywordize-keys m)))

;; -----------------------------
;; $ref resolution & combinators
;; -----------------------------

(defn- json-pointer
  "Follow a JSON Pointer like \"#/components/schemas/Pokemon\" in map m.
   Returns nil if any segment is missing."
  [m ptr]
  (when (and (string? ptr) (str/starts-with? ptr "#/"))
    (let [segs (map (fn [s] (-> s
                                (str/replace "~1" "/")
                                (str/replace "~0" "~")
                                keyword))
                    (str/split (subs ptr 2) #"/"))]
      (reduce (fn [acc k] (when (map? acc) (get acc k)))
              m
              segs))))

(defn- ensure-vector [x] (if (sequential? x) (vec x) (if (nil? x) [] [x])))

(defn- merge-required [a b]
  (->> (concat (or (:required a) []) (or (:required b) []))
       (remove nil?)
       set vec))

(defn- merge-object
  "Merge two object schemas (both type object or with :properties map)."
  [a b]
  (let [props (merge-with (fn [x y] (merge x y))
                          (:properties a) (:properties b))]
    (-> (merge a b)
        (assoc :type "object"
               :properties props
               :required (merge-required a b)))))

(defn- combine-allOf
  "Combine an :allOf vector of schemas into a single schema.
   - Merges object properties & required.
   - If mixed types, returns a conservative union-like object with merged props."
  [schemas]
  (let [objs   (filter #(or (= "object" (:type %))
                            (map? (:properties %)))
                       schemas)
        others (remove #(or (= "object" (:type %))
                            (map? (:properties %)))
                       schemas)
        merged (reduce merge-object {} objs)]
    (cond
      (and (seq others) (seq objs))
      ;; keep non-object constraints too
      (merge merged {:allOf (vec others)})

      (seq objs) merged
      :else {:allOf (vec schemas)})))

(declare resolve-schema)

(defn resolve-ref
  "Resolve a schema that may contain a $ref. Tracks cycle detection via `seen`."
  [spec schema seen]
  (if-let [ref (:$ref schema)]
    (if (seen ref)
      ;; break cycles gracefully
      (assoc schema :$ref-cycle true)
      (let [target (json-pointer spec ref)]
        (if-not target
          (assoc schema :$ref-missing ref)
          (resolve-schema spec target (conj seen ref)))))
    (resolve-schema spec schema seen)))

(defn- resolve-combinator [spec k schemas seen]
  (let [resolved (mapv #(resolve-ref spec % seen) schemas)]
    (case k
      :allOf (combine-allOf resolved)
      :oneOf {:oneOf resolved}
      :anyOf {:anyOf resolved})))

(defn- normalize-nullable
  "OpenAPI v3 `nullable: true` -> represent as oneOf [<type>, null]."
  [schema]
  (if (true? (:nullable schema))
    (-> schema
        (dissoc :nullable)
        (update :oneOf #(vec (distinct (concat (or % [])
                                               [ (dissoc schema :oneOf :anyOf :allOf :type :properties :items
                                                         :required :additionalProperties :description
                                                         :enum :format :title :example :default
                                                         :$ref :$ref-cycle :$ref-missing)
                                                 {:type "null"}])))))
    schema))

(defn resolve-schema
  "Recursively resolve an OpenAPI Schema Object:
   - $ref (local)
   - allOf / oneOf / anyOf
   - arrays (items)
   - objects (properties, additionalProperties)
   Returns a resolved schema map."
  [spec schema seen]
  (let [schema (or schema {})
        ;; First resolve combinators and parts
        schema (cond-> schema
                 (:allOf schema) (-> (update :allOf #(mapv (fn [s] (resolve-ref spec s seen)) %))
                                     (as-> s (combine-allOf (:allOf s))))
                 (:oneOf schema) (update schema :oneOf #(mapv (fn [s] (resolve-ref spec s seen)) %))
                 (:anyOf schema) (update schema :anyOf #(mapv (fn [s] (resolve-ref spec s seen)) %)))]
    (cond
      (:$ref schema)
      (resolve-ref spec schema seen)

      (= "array" (:type schema))
      (-> schema
          (update :items #(when % (resolve-ref spec % seen)))
          normalize-nullable)

      (or (= "object" (:type schema))
          (map? (:properties schema))
          (contains? schema :additionalProperties))
      (-> schema
          (update :properties (fn [ps]
                                (when (map? ps)
                                  (into {}
                                        (for [[k v] ps]
                                          [k (resolve-ref spec v seen)])))))
          (update :additionalProperties (fn [ap]
                                          (cond
                                            (true? ap) true
                                            (false? ap) false
                                            (map? ap) (resolve-ref spec ap seen)
                                            :else ap)))
          (update :required #(vec (set %)))
          normalize-nullable)

      :else (normalize-nullable schema))))

;; -----------------------------------
;; Endpoint → resolved response schema
;; -----------------------------------

(defn- pick-response [responses]
  (or (:200 responses)
      (:201 responses)
      (get responses "200")
      (get responses "201")
      (second (first responses))))   ; fallback: first entry's value

(comment
(defn- pick-content-schema
  "From a response, pick a schema from content (prefer application/json)."
  [resp]
  (let [content (:content resp)
        ct      (or (get content "application/json")
                    (get content :application/json)
                    (-> content vals first))]
    (get ct :schema)))
)

(defn- pick-content-schema
  "From a response, pick a schema from content (OpenAPI 3.x) or direct schema (OpenAPI 2.x/Swagger)."
  [resp]
  (or
    ;; OpenAPI 2.x/Swagger: direct schema property
    (:schema resp)
    ;; OpenAPI 3.x: schema nested in content
    (let [content (:content resp)
          ct      (or (get content "application/json")
                      (get content :application/json)
                      (-> content vals first))]
      (get ct :schema))))


(defn find-path-key [spec wanted]
  (let [paths (->> (keys (:paths spec)) (map name) set)
        base  (str (str/replace-first wanted #"^/" ""))          ; drop leading slash
        cands #{base (str base "/") (str "api/v2/" base) (str "api/v2/" base "/")}]
    (some paths cands)))

(defn endpoint-schema [spec path method]
  (let [p* (or (find-path-key spec path) path)                    ; normalize
        ;; :paths keys are keywords; rebuild the keyword with leading slash.
        k  (keyword (str "/" p*))
        op (get-in spec [:paths k method])
        resp (when op (pick-response (:responses op)))
        sch  (some-> resp pick-content-schema)]
    (when sch (resolve-ref spec sch #{}))))


(defn extract-parameters
  "Extract all parameters for an endpoint from OpenAPI spec.
   Returns categorized params: path, query, header, cookie"
  [spec path method]
  (let [;; Get the operation (e.g., GET /api/v4/projects/{id}/issues)
        operation (get-in spec [:paths (keyword path) method])
        
        ;; Parameters can be defined at path level or operation level
        path-level-params (get-in spec [:paths (keyword path) :parameters])
        operation-params (:parameters operation)
        
        ;; Combine both (operation params override path params)
        all-params (concat (or path-level-params []) (or operation-params []))
        
        ;; Helper to resolve $ref if present
        resolve-param (fn [p]
                       (if-let [ref (:$ref p)]
                         (json-pointer spec ref)
                         p))
        
        ;; Resolve all $refs
        resolved-params (map resolve-param all-params)
        
        ;; Helper to extract param info
        extract-info (fn [p]
                      (let [schema (:schema p)]
                        {:name (:name p)
                         :in (:in p)
                         :required (boolean (:required p))
                         :description (:description p)
                         :type (get schema :type)
                         :format (get schema :format)
                         :enum (get schema :enum)
                         :default (get schema :default)
                         :minimum (get schema :minimum)
                         :maximum (get schema :maximum)
                         :min-length (get schema :minLength)
                         :max-length (get schema :maxLength)
                         :pattern (get schema :pattern)
                         :example (or (:example p) (get schema :example))
                         ;; OpenAPI 3.0 specific
                         :deprecated (:deprecated p)
                         :allow-empty-value (:allowEmptyValue p)
                         ;; Schema validation
                         :items (get schema :items)}))]
    
    ;; Group by parameter location
    {:path-params
     (->> resolved-params
          (filter #(= "path" (:in %)))
          (map extract-info)
          (map (fn [p] [(keyword (:name p)) (dissoc p :name :in)]))
          (into {}))
     
     :query-params
     (->> resolved-params
          (filter #(= "query" (:in %)))
          (map extract-info)
          (map (fn [p] [(keyword (:name p)) (dissoc p :name :in)]))
          (into {}))
     
     :header-params
     (->> resolved-params
          (filter #(= "header" (:in %)))
          (map extract-info)
          (map (fn [p] [(keyword (:name p)) (dissoc p :name :in)]))
          (into {}))
     
     :cookie-params
     (->> resolved-params
          (filter #(= "cookie" (:in %)))
          (map extract-info)
          (map (fn [p] [(keyword (:name p)) (dissoc p :name :in)]))
          (into {}))}))

(defn detect-pagination-strategy
  "Infer pagination type from parameter names and response schema"
  [params response-schema]
  (let [query-params (:query-params params)
        param-names (set (keys query-params))]
    
    (cond
      ;; Offset-based: has page + per_page (GitLab style)
      (and (param-names :page) (param-names :per_page))
      {:type :offset
       :page-param :page
       :per-page-param :per_page
       :page-default (get-in query-params [:page :default] 1)
       :per-page-default (get-in query-params [:per_page :default] 20)
       :per-page-max (get-in query-params [:per_page :maximum])}
      
      ;; Offset-based: has offset + limit
      (and (param-names :offset) (param-names :limit))
      {:type :offset
       :offset-param :offset
       :limit-param :limit
       :offset-default (get-in query-params [:offset :default] 0)
       :limit-default (get-in query-params [:limit :default] 50)
       :limit-max (get-in query-params [:limit :maximum])}
      
      ;; Jira style: startAt + maxResults
      (and (param-names :startAt) (param-names :maxResults))
      {:type :offset
       :offset-param :startAt
       :limit-param :maxResults
       :offset-default (get-in query-params [:startAt :default] 0)
       :limit-default (get-in query-params [:maxResults :default] 50)
       :limit-max (get-in query-params [:maxResults :maximum])}
      
      ;; Cursor-based
      (or (param-names :cursor)
          (param-names :after)
          (param-names :next_token)
          (param-names :continuation_token))
      {:type :cursor
       :cursor-param (or (when (param-names :cursor) :cursor)
                        (when (param-names :after) :after)
                        (when (param-names :next_token) :next_token)
                        (when (param-names :continuation_token) :continuation_token))
       :limit-param (or (when (param-names :limit) :limit)
                       (when (param-names :per_page) :per_page)
                       (when (param-names :page_size) :page_size))}
      
      ;; Token-based (similar to cursor)
      (param-names :page_token)
      {:type :token
       :token-param :page_token
       :limit-param (or (when (param-names :page_size) :page_size)
                       (when (param-names :per_page) :per_page))}
      
      ;; Keyset pagination: uses ID-based pagination
      (and (param-names :since_id) (param-names :max_id))
      {:type :keyset
       :since-param :since_id
       :max-param :max_id
       :limit-param (or (when (param-names :count) :count)
                       (when (param-names :limit) :limit))}
      
      ;; Link header only (no pagination params in query)
      :else
      {:type :link-header
       :note "Uses Link header (RFC 5988) for pagination"})))

(defn extract-auth-scheme
  "Extract authentication requirements from OpenAPI spec operation"
  [spec operation]
  (let [security-schemes (get-in spec [:components :securitySchemes])
        security-reqs (:security operation)
        global-security (get spec :security)]
    
    ;; Try operation-level security first, then global
    (when-let [sec-reqs (or security-reqs global-security)]
      (let [;; Get first security requirement
            sec-req (first sec-reqs)
            sec-name (-> sec-req keys first)
            scheme (get security-schemes sec-name)]
        
        (when scheme
          (case (:type scheme)
            "apiKey"
            {:type :api-key
             :location (keyword (:in scheme))  ; :header, :query, or :cookie
             :param-name (keyword (:name scheme))
             :description (:description scheme)}
            
            "http"
            (case (:scheme scheme)
              "bearer"
              {:type :bearer
               :bearer-format (:bearerFormat scheme)
               :description (:description scheme)}
              
              "basic"
              {:type :basic
               :description (:description scheme)})
            
            "oauth2"
            {:type :oauth2
             :flows (:flows scheme)
             :description (:description scheme)}
            
            "openIdConnect"
            {:type :openid-connect
             :openid-connect-url (:openIdConnectUrl scheme)
             :description (:description scheme)}
            
            nil))))))


;; -----------------------------------
;; Convenience: list endpoints & dump
;; -----------------------------------

(defn list-endpoints
  "Return a seq of [path method] for all operations in the spec."
  [spec]
  (for [[p ops] (:paths spec)
        [m _op] ops
        :when (#{:get :put :post :delete :patch :options :head :trace} m)]
    [(name p) m]))

(defn list-endpoints-from-url[url]
	(list-endpoints (fetch-openapi! url)))

;; --- Add to ns openapi.schema ----------------------------------------------
;; (ns openapi.schema
;;   (:require [clojure.string :as str] ...))

;; ============= Helpers for types & nullability ==============================

(defn- oas-type->kw
  "Map OpenAPI primitive types to your node :type keywords."
  [sch]
  (let [t (:type sch)
        fmt (:format sch)]
    (cond
      (= t "integer") :int
      (= t "number")  (if (#{"float" "double"} fmt) :float :number)
      (= t "string")  :string
      (= t "boolean") :boolean
      (= t "array")   :array
      (= t "object")  :map
      (= t "null")    :null
      :else           :any)))

(defn- has-null-branch?
  "True if schema allows null via oneOf/anyOf/type \"null\"."
  [sch]
  (or (= "null" (:type sch))
      (some #(= "null" (:type %)) (concat (:oneOf sch) (:anyOf sch)))))

(defn- nullable?
  "OpenAPI v3: either explicit :nullable true or union w/ null.
   (If you normalized nullable->oneOf earlier, this still works.)"
  [sch]
  (or (true? (:nullable sch)) (has-null-branch? sch)))

(defn- constraints
  "Collect length/size/range/pattern/enum hints for UI."
  [sch]
  (cond-> {}
    ;; strings
    (contains? sch :minLength) (assoc :minLength (:minLength sch))
    (contains? sch :maxLength) (assoc :maxLength (:maxLength sch))
    (contains? sch :pattern)   (assoc :pattern   (:pattern sch))
    (contains? sch :format)    (assoc :format    (:format sch))
    ;; numbers
    (contains? sch :minimum)   (assoc :minimum   (:minimum sch))
    (contains? sch :maximum)   (assoc :maximum   (:maximum sch))
    (contains? sch :exclusiveMinimum) (assoc :exclusiveMinimum (:exclusiveMinimum sch))
    (contains? sch :exclusiveMaximum) (assoc :exclusiveMaximum (:exclusiveMaximum sch))
    ;; arrays
    (contains? sch :minItems)  (assoc :minItems  (:minItems sch))
    (contains? sch :maxItems)  (assoc :maxItems  (:maxItems sch))
    ;; enums
    (seq (:enum sch))          (assoc :enum      (vec (:enum sch)))))

(defn- seg->s [seg]
  (cond
    (= seg "[]") "[]"
    (keyword? seg) (name seg)
    (string? seg)  seg
    :else          (str seg)))

(defn- path->string [path]
  (->> path
       (map seg->s)
       (reduce (fn [acc seg]
                 (cond
                   (empty? acc) seg
                   (= seg "[]") (str acc seg)     ;; no dot before []
                   :else (str acc "." seg)))
               "")))

;; ============= Schema → nodes (spec-based) ==================================

(defn- emit-node
  [path sch required?]
  {:path       (path->string path)
   :type       (oas-type->kw sch)
   :nullable?  (boolean (not required?))   ;; Spec’s `required` => NOT nullable structurally.
   :constraints (constraints sch)
   :raw        sch})

(defn schema->nodes-from-openapi
  "Walk a *resolved* OpenAPI schema (object/array/oneOf/etc.) and produce
   leaf nodes with useful metadata for mapping. Paths use $.a.b[].c style."
  ([schema] (schema->nodes-from-openapi schema ["$"] true))
  ([schema path required?]
   (let [t (:type schema)
         ;; When unions exist, prefer to descend into object/array members to get leaf paths;
         ;; for scalars, emit a single :one-of node.
         union (seq (concat (:oneOf schema) (:anyOf schema)))]
     (cond
       ;; If union: descend object/array branches; emit scalar unions as one node
       union
       (let [objs (filter #(or (= "object" (:type %)) (map? (:properties %))) union)
             arrs (filter #(= "array" (:type %)) union)
             scal (remove #(or (= "object" (:type %))
                               (= "array" (:type %))
                               (map? (:properties %)))
                          union)
             down (concat
                   (mapcat #(schema->nodes-from-openapi % path required?) objs)
                   (mapcat #(schema->nodes-from-openapi % path required?) arrs))]
         (if (seq down)
           down
           ;; only scalar alternatives – emit one node summarizing the union
           [(-> (emit-node path {:type "string"} required?) ; neutral base type
                (assoc :type :one-of
                       :nullable? (or (not required?) (has-null-branch? schema))
                       :constraints {:alternatives (mapv oas-type->kw scal)}))]))

       ;; Objects: dive into properties
       (or (= t "object") (map? (:properties schema)))
       (let [req-set (->> (:required schema) (map name) set)
             props   (or (:properties schema) {})]
         (mapcat
           (fn [[k v]]
             (let [child-required? (contains? req-set (name k))
                   child-path (conj path (name k))]
               (schema->nodes-from-openapi v child-path child-required?)))
           props))

       ;; Arrays: dive into items with [] path seg
       (= t "array")
       (schema->nodes-from-openapi (:items schema) (conj path "[]") true)

       ;; Scalars: emit leaf
       :else
       [(-> (emit-node path schema required?)
            ;; nullable? overrides: if spec is explicitly nullable, reflect that
            (assoc :nullable? (or (nullable? schema) (not required?))))]))))

;; ---------- Names-only tree from a resolved OpenAPI schema -------------------
(comment

(defn- schema->name-tree*
  "Return a vector with a single node {:name <seg> :children [...]}, recursively.
   - Objects → children are property names.
   - Arrays  → a child named \"[]\" whose children come from :items.
   - oneOf/anyOf → descend into object/array branches; ignore scalar-only unions for names."
  [schema seg]
  (let [t     (:type schema)
        union (seq (concat (:oneOf schema) (:anyOf schema)))]
    (cond
      union
      (let [branches (filter some?
                             (map (fn [s]
                                    (let [tt (:type s)]
                                      (cond
                                        (or (= tt "object") (map? (:properties s)))
                                        (schema->name-tree* s seg)

                                        (= tt "array")
                                        {:name seg
                                         :children (schema->name-tree* (:items s) "[]")}

                                        :else nil)))
                                  union))
            children (->> branches
                          (mapcat #(or (:children %) []))
                          vec)]
        [{:name seg :children (when (seq children) children)}])

      (or (= t "object") (map? (:properties schema)))
      (let [props (or (:properties schema) {})]
        [{:name seg
          :children (->> props
                         (map (fn [[k v]]
                                (first (schema->name-tree* v (name k)))))
                         vec)}])

      (= t "array")
      [{:name seg
        :children (schema->name-tree* (:items schema) "[]")}]

      :else
      [{:name seg}])))

(defn schema->name-tree
  "Wrap with a 'Root' node to match your requested output shape."
  [resolved-schema]
  (vec (schema->name-tree* resolved-schema "Root")))

;; Convenience: fetch endpoint schema and produce the tree.
(defn endpoint-name-tree
  [spec path method]
  (when-let [sch (endpoint-schema spec path method)]
    (schema->name-tree sch)))

)

;; ---- Replace your names-only tree helpers with these -----------------------

(defn- schema->name-tree* 
  "Return a vector with one node {:name <seg> :children [...]}, recursively.
   array-style:
     - :suffix => parent named with [] suffix (e.g., \"berries[]\") and no intermediate \"[]\" node
     - :inline => parent keeps name (e.g., \"berries\") and no intermediate \"[]\" node"
  ([schema seg] (schema->name-tree* schema seg :suffix))
  ([schema seg array-style]
   (let [t     (:type schema)
         union (seq (concat (:oneOf schema) (:anyOf schema)))
         array-parent (fn [seg] (if (= array-style :suffix) (str seg "[]") seg))]

     (cond
       ;; Unions: descend object/array branches; ignore scalar-only unions
       union
       (let [branches (keep (fn [s]
                              (let [tt (:type s)]
                                (cond
                                  (or (= tt "object") (map? (:properties s)))
                                  (first (schema->name-tree* s seg array-style))

                                  (= tt "array")
                                  (let [items-node (first (schema->name-tree* (:items s) (array-parent seg) array-style))]
                                    items-node)

                                  :else nil)))
                            union)
             children (->> branches (mapcat #(or (:children %) [])) vec)]
         [{:name seg :children (when (seq children) children)}])

       ;; Objects → recurse over properties
       (or (= t "object") (map? (:properties schema)))
       (let [props (or (:properties schema) {})]
         [{:name seg
           :children (->> props
                          (map (fn [[k v]]
                                 (first (schema->name-tree* v (name k) array-style))))
                          vec)}])

       ;; Arrays → inline item’s children under parent (no \"[]\" node)
       (= t "array")
       (let [parent-name (array-parent seg)
             items       (:items schema)
             item-type   (:type items)]
         (cond
           (or (= item-type "object") (map? (:properties items)))
           [{:name parent-name
             :children (->> (:properties items)
                            (map (fn [[k v]]
                                   (first (schema->name-tree* v (name k) array-style))))
                            vec)}]

           ;; array of arrays: keep suffixing/inlining recursively
           (= item-type "array")
           (schema->name-tree* items parent-name array-style)

           ;; scalar items: leaf, just mark parent
           :else
           [{:name parent-name}]))

       ;; Scalars → leaf
       :else
       [{:name seg}]))))

(defn schema->name-tree
  "Wrap with a 'Root' node to match requested output."
  ([resolved-schema] (schema->name-tree resolved-schema :suffix))
  ([resolved-schema array-style]
   (vec (schema->name-tree* resolved-schema "Root" array-style))))

(defn endpoint-name-tree
  "Get tree for an endpoint. array-style is :suffix or :inline (default :suffix)."
  ([spec path method] (endpoint-name-tree spec path method :suffix))
  ([spec path method array-style]
   (when-let [sch (endpoint-schema spec path method)]
     (schema->name-tree sch array-style))))


;; ============= Endpoint convenience =========================================

(defn endpoint-nodes
  "Fetch the resolved response schema for a given [path method], then flatten into nodes."
  [spec path method]
  (when-let [sch (endpoint-schema spec path method)]
    (vec (schema->nodes-from-openapi sch))))

(defn endpoint-nodes-from-url
      [specurl path method]
      (let [
		spec (fetch-openapi! specurl)
                iret (endpoint-nodes spec path method)
                _ (prn-v iret)
                
           ]
           (endpoint-name-tree spec path method)))

           

