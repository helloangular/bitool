(ns bitool.api.gschema
  (:require [clj-http.client :as http]
            [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.walk :as walk]))

(defn kind->kw
  "Normalize GraphQL kind value to a lowercase keyword.
   Accepts string or keyword."
  [k]
  (-> (if (keyword? k) (name k) k)
      str/lower-case
      keyword))


;; =============================================================================
;; 1) Fetch + parse GraphQL introspection
;; =============================================================================

(def default-introspection-query
  "Standard introspection query (shortened a bit but still complete enough
   for building a field tree UI)."
  "query IntrospectionQuery {
     __schema {
       queryType { name }
       mutationType { name }
       subscriptionType { name }
       types {
         kind
         name
         description
         fields(includeDeprecated: true) {
           name
           description
           isDeprecated
           deprecationReason
           args {
             name
             description
             defaultValue
             type { kind name ofType { kind name ofType { kind name ofType { kind name }}}}
           }
           type { kind name ofType { kind name ofType { kind name ofType { kind name }}}}
         }
         inputFields {
           name
           description
           defaultValue
           type { kind name ofType { kind name ofType { kind name ofType { kind name }}}}
         }
         interfaces { kind name }
         possibleTypes { kind name }
         enumValues(includeDeprecated: true) {
           name description isDeprecated deprecationReason
         }
       }
     }
   }")

(defn fetch-introspection!
  "POST introspection query to a GraphQL endpoint.
   headers should include auth if needed (eg GitHub: Authorization: bearer <token>).
   Returns a keywordized map of the raw introspection response."
  ([url headers]
   (let [resp (http/post url {:as :text
                             :throw-exceptions true
                             :headers headers
                             :content-type :json
                             :accept :json
                             :body (json/encode {:query default-introspection-query})})
         body (json/parse-string (:body resp))]
     (walk/keywordize-keys body)))
  ([url] (fetch-introspection! url {})))

(defn schema-from-introspection
  "Pulls out __schema tree from introspection response."
  [introspection-resp]
  (get-in introspection-resp [:data :__schema]))

;; =============================================================================
;; 2) Build indexes / helpers
;; =============================================================================

(defn index-types
  "Build {\"TypeName\" type-def} map from __schema.types."
  [schema]
  (into {}
        (for [t (:types schema)
              :when (:name t)]
          [(:name t) t])))

(defn unwrap-type
  "Normalize GraphQL nested type structure safely.
   Handles very deep ofType chains and guards against cycles."
  [t]
  (loop [cur t
         non-null? false
         depth 0
         seen #{}]
    (cond
      (nil? cur)
      {:kind :unknown
       :name nil
       :non-null? non-null?
       :list? false}

      (>= depth 50)
      ;; depth guard (50 wrappers is already extreme)
      {:kind (kind->kw (:kind cur))
       :name (:name cur)
       :non-null? non-null?
       :list? false
       :truncated? true}

      (contains? seen (System/identityHashCode cur))
      ;; cycle guard: stop unwrapping if we see same node again
      {:kind (kind->kw (:kind cur))
       :name (:name cur)
       :non-null? non-null?
       :list? false
       :cycle? true}

      :else
      (let [{:keys [kind name ofType]} cur
            k (kind->kw kind)
            seen' (conj seen (System/identityHashCode cur))]
        (case k
          :non_null (recur ofType true (inc depth) seen')
          :list     {:kind :list
                     :non-null? non-null?
                     :list? true
                     :of (unwrap-type ofType)}
          ;; named leaf/object/enum/interface/union/etc
          {:kind k
           :name name
           :non-null? non-null?
           :list? false})))))


(defn scalar-kind->kw [k]
  (case k
    :int :int
    :float :float
    :string :string
    :boolean :boolean
    :id :id
    :scalar :scalar
    :enum :enum
    :object :object
    :input_object :input-object
    :interface :interface
    :union :union
    :list :array
    :any))

(defn seg->s [seg]
  (cond
    (= seg "[]") "[]"
    (keyword? seg) (name seg)
    (string? seg) seg
    :else (str seg)))

(defn path->string
  "Matches your REST version: Root.a.b[].c"
  [path]
  (->> path
       (map seg->s)
       (reduce (fn [acc seg]
                 (cond
                   (empty? acc) seg
                   (= seg "[]") (str acc seg)
                   :else (str acc "." seg)))
               "")))

;; =============================================================================
;; 3) Name tree for UI (checkbox explorer)
;; =============================================================================

(declare type->name-tree*)

(defn field-children
  "Given a field (with :type), return children nodes if the field targets
   OBJECT/INTERFACE/UNION/LIST. Scalars -> nil children.
   Threads seen/depth to prevent cycles."
  [types-index field array-style seen depth]
  (let [t-desc (unwrap-type (:type field))]
    (cond
      (= :unknown (:kind t-desc))
      nil

      (= :list (:kind t-desc))
      (let [parent-name (if (= array-style :suffix)
                          (str (:name field) "[]")
                          (:name field))
            inner (-> t-desc :of)]
        (type->name-tree* types-index inner parent-name array-style seen (inc depth)))

      :else
      (type->name-tree* types-index t-desc (:name field) array-style seen (inc depth)))))

(defn type->name-tree*
  "Return a single node {:name seg :children [...]}, recursively.
   Adds cycle + depth guards:
   - seen is a set of type names already expanded on this path
   - depth is current expansion depth
   If a cycle is detected, we stop expanding and add {:cycle? true}.
   If depth is too large, stop and add {:truncated? true}."
  [types-index t-desc seg array-style seen depth]
  (let [{:keys [kind name]} t-desc
        depth-limit 20] ; tune as you like for UI size/perf
    (cond
      (>= depth depth-limit)
      [{:name seg :truncated? true}]

      ;; if it's a named composite type we've already expanded on this branch, stop
      (and name (contains? seen name) (#{:object :input_object :interface :union} kind))
      [{:name seg :cycle? true}]

      :else
      (case kind
        (:object :input_object)
        (let [tdef (get types-index name)
              fields (or (:fields tdef) (:inputFields tdef))
              seen' (conj seen name)
              children (->> fields
                            (map (fn [f]
                                   (let [kids (field-children types-index f array-style seen' (inc depth))]
                                     (cond-> {:name (:name f)}
                                       (seq kids) (assoc :children (:children (first kids)))))))
                            vec)]
          [{:name seg :children (when (seq children) children)}])

        (:interface :union)
        (let [tdef (get types-index name)
              ptypes (:possibleTypes tdef)
              seen' (conj seen name)
              children (->> ptypes
                            (mapcat (fn [pt]
                                      (let [pt-desc {:kind (kind->kw (:kind pt))
                                                     :name (:name pt)
                                                     :non-null? false
                                                     :list? false}]
                                        (type->name-tree* types-index
                                                          pt-desc
                                                          (str "on " (:name pt))
                                                          array-style
                                                          seen'
                                                          (inc depth)))))
                            vec)]
          [{:name seg :children (when (seq children) children)}])

        :list
        ;; should usually not hit because list handled in field-children,
        ;; but keep safe anyway
        (type->name-tree* types-index
                          (:of t-desc)
                          (if (= array-style :suffix) (str seg "[]") seg)
                          array-style
                          seen
                          (inc depth))

        ;; scalars/enums
        [{:name seg}]))))


(defn root-name-tree
  "Build a name tree for a chosen root field.
   op-type is one of :query/:mutation/:subscription.
   root-field-name is e.g. \"repository\"."
  ([schema op-type root-field-name]
   (root-name-tree schema op-type root-field-name :suffix))
  ([schema op-type root-field-name array-style]
   (let [types-index (index-types schema)
         root-type-name (case op-type
                          :query (get-in schema [:queryType :name])
                          :mutation (get-in schema [:mutationType :name])
                          :subscription (get-in schema [:subscriptionType :name]))
         root-type (get types-index root-type-name)
         root-field (first (filter #(= root-field-name (:name %)) (:fields root-type)))
         children (field-children types-index root-field array-style #{} 0)]
     [{:name "Root"
       :children (vec
                   (concat
                     [{:name root-field-name
                       :children (:children (first children))}]
                     []))}])))

;; =============================================================================
;; 4) Flattened leaf nodes (paths/types/nullable/args)
;; =============================================================================

(declare type->leaf-nodes*)

(defn emit-leaf
  [path t-desc required? args]
  {:path      (path->string path)
   :type      (scalar-kind->kw (:kind t-desc))
   :nullable? (not required?)
   :args      (vec (or args []))
   :raw-type  t-desc})

(defn field-required? [t-desc]
  ;; NON_NULL => required structural value
  (:non-null? t-desc))

(defn type->leaf-nodes*
  "Walk a type descriptor and produce leaf nodes.
   For lists, adds [] to path.
   For unions/interfaces, descends into possible types and tags path with fragment marker."
  [types-index t-desc path required? args seen]
  (let [{:keys [kind name]} t-desc]
    (cond
      (= kind :list)
      (type->leaf-nodes* types-index
                         (:of t-desc)
                         (conj path "[]")
                         true
                         args
                         seen)

      (#{:object :input_object} kind)
      (let [tdef (get types-index name)]
        (if (seen name)
          ;; cycle break
          [(emit-leaf path t-desc required? args)]
          (let [seen' (conj seen name)
                fields (or (:fields tdef) (:inputFields tdef))]
            (mapcat
              (fn [f]
                (let [child-desc (unwrap-type (:type f))
                      child-required? (field-required? child-desc)
                      child-path (conj path (:name f))
                      child-args (:args f)]
                  (type->leaf-nodes* types-index
                                     child-desc
                                     child-path
                                     child-required?
                                     child-args
                                     seen')))
              fields))))

      (#{:interface :union} kind)
      (let [tdef (get types-index name)
            ptypes (:possibleTypes tdef)]
        (mapcat
          (fn [pt]
(let [pt-desc {:kind (kind->kw (:kind pt))
               :name (:name pt)
               :non-null? false
               :list? false}]

              ;; add fragment marker to help UI
              (type->leaf-nodes* types-index
                                 pt-desc
                                 (conj path (str "on " (:name pt)))
                                 required?
                                 args
                                 seen)))
          ptypes))

      :else
      ;; scalar/enum
      [(emit-leaf path t-desc required? args)])))

(defn root-leaf-nodes
  "Flatten leaf nodes for a chosen root field."
  [schema op-type root-field-name]
  (let [types-index (index-types schema)
        root-type-name (case op-type
                         :query (get-in schema [:queryType :name])
                         :mutation (get-in schema [:mutationType :name])
                         :subscription (get-in schema [:subscriptionType :name]))
        root-type (get types-index root-type-name)
        root-field (first (filter #(= root-field-name (:name %)) (:fields root-type)))
        t-desc (unwrap-type (:type root-field))
        req? (field-required? t-desc)]
    (vec
      (type->leaf-nodes* types-index
                         t-desc
                         ["Root" root-field-name]
                         req?
                         (:args root-field)
                         #{}))))

;; =============================================================================
;; 5) Convenience entrypoints for your app
;; =============================================================================

(defn graphql-field-tree-from-url
  "Fetch schema from endpoint and return name tree.
   Example for GitHub:
   (graphql-field-tree-from-url \"https://api.github.com/graphql\"
                                {:authorization \"bearer <token>\"}
                                :query \"repository\")"
  ([url headers op-type root-field-name]
   (let [schema (-> (fetch-introspection! url headers)
                    schema-from-introspection)]
     (root-name-tree schema op-type root-field-name)))
  ([url op-type root-field-name]
   (graphql-field-tree-from-url url {} op-type root-field-name)))

(defn graphql-leaf-nodes-from-url
  "Fetch schema and return flattened leaf nodes."
  ([url headers op-type root-field-name]
   (let [schema (-> (fetch-introspection! url headers)
                    schema-from-introspection)]
     (root-leaf-nodes schema op-type root-field-name)))
  ([url op-type root-field-name]
   (graphql-leaf-nodes-from-url url {} op-type root-field-name)))

;; =============================================================================
;; Lazy tree API for GraphQL schemas
;; =============================================================================

(defn root-fields
  "Return list of root fields for :query/:mutation/:subscription.
   Each item includes name + return-type descriptor + args."
  [schema op-type]
  (let [types-index (index-types schema)
        root-type-name (case op-type
                         :query (get-in schema [:queryType :name])
                         :mutation (get-in schema [:mutationType :name])
                         :subscription (get-in schema [:subscriptionType :name]))
        root-type (get types-index root-type-name)]
    (->> (:fields root-type)
         (map (fn [f]
                (let [t-desc (unwrap-type (:type f))]
                  {:name (:name f)
                   :description (:description f)
                   :isDeprecated (:isDeprecated f)
                   :deprecationReason (:deprecationReason f)
                   :type t-desc
                   :args (vec (:args f))})))
         vec)))

(defn children-for-type
  "Return immediate children for a given named composite type.
   Does NOT recurse.
   Output nodes are UI-friendly:
   {:name \"issues\" :kind :object/:scalar/:list/:union/... :type <desc> :args [...] :expandable? bool}"
  ([schema type-name]
   (children-for-type schema type-name {:breadth-limit 200}))
  ([schema type-name {:keys [breadth-limit] :or {breadth-limit 200}}]
   (let [types-index (index-types schema)
         tdef (get types-index type-name)
         kind (kind->kw (:kind tdef))]
     (cond
       (nil? tdef)
       []

       (#{:object :input_object} kind)
       (let [fields (or (:fields tdef) (:inputFields tdef))]
         (->> fields
              (take breadth-limit)
              (map (fn [f]
                     (let [t-desc (unwrap-type (:type f))
                           expandable? (contains? #{:object :input_object :interface :union :list}
                                                 (:kind t-desc))]
                       {:name (:name f)
                        :description (:description f)
                        :isDeprecated (:isDeprecated f)
                        :deprecationReason (:deprecationReason f)
                        :type t-desc
                        :kind (:kind t-desc)
                        :nullable? (not (:non-null? t-desc))
                        :args (vec (:args f))
                        :expandable? expandable?})))
              vec))

       (#{:interface :union} kind)
       (let [ptypes (:possibleTypes tdef)]
         (->> ptypes
              (take breadth-limit)
              (map (fn [pt]
                     {:name (str "on " (:name pt))
                      :type {:kind (kind->kw (:kind pt))
                             :name (:name pt)
                             :non-null? false
                             :list? false}
                      :kind (kind->kw (:kind pt))
                      :expandable? true}))
              vec))

       :else
       []))))

(defn children-for-field
  "Given a parent type + field name, return the immediate children of that field's return type.
   Handles LIST by unwrapping to inner named type."
  [schema parent-type-name field-name]
  (let [types-index (index-types schema)
        parent (get types-index parent-type-name)
        fields (or (:fields parent) (:inputFields parent))
        fld (first (filter #(= field-name (:name %)) fields))
        t-desc (unwrap-type (:type fld))]
    (loop [d t-desc]
      (if (= :list (:kind d))
        (recur (:of d))
        (children-for-type schema (:name d))))))

(defn root-name-tree-bounded
  [schema op-type root-field-name {:keys [depth-limit breadth-limit]
                                   :or {depth-limit 4 breadth-limit 40}}]
  ;; same as root-name-tree, but pass depth/breadth to walkers
)


;; ========================
;; examples (wrapped in comment to avoid execution at load time)
;; ========================

(comment
(def github-url "https://api.github.com/graphql")

(def token "REDACTED_GITHUB_TOKEN") ;; keep this out of source control

(def headers
  {:authorization (str "Bearer " token)
   :user-agent "bitool"})   ;; good practice for GitHub APIs

(def schema (-> (fetch-introspection! github-url headers)
                schema-from-introspection))

(def root-type-name (get-in schema [:queryType :name]))
(def roots (root-fields schema :query))
(def repo-root (first (filter #(= "repository" (:name %)) roots)))


(def repo-children
  (children-for-field schema root-type-name "repository"))


(def two-level-tree
  [{:name "Root"
    :children
    [{:name "repository"
      :args (:args repo-root)
      :children (mapv #(select-keys % [:name :kind :nullable? :args :expandable?])
                      repo-children)}]}])
) ;; end comment

(defn root-plus-one-level
  [schema op-type root-field-name]
  (let [root-type-name (case op-type
                         :query (get-in schema [:queryType :name])
                         :mutation (get-in schema [:mutationType :name])
                         :subscription (get-in schema [:subscriptionType :name]))
        roots (root-fields schema op-type)
        root (first (filter #(= root-field-name (:name %)) roots))
        children (children-for-field schema root-type-name root-field-name)]
    [{:name "Root"
      :children
      [{:name root-field-name
        :args (:args root)
        :children (mapv #(select-keys % [:name :kind :nullable? :args :expandable?])
                        children)}]}]))

;; (root-plus-one-level schema :query "repository")

(defn root-field-names
  "Return just the names of root fields for :query/:mutation/:subscription."
  [schema op-type]
  (mapv :name (root-fields schema op-type)))


;; (root-field-names schema :query)
;; => ["repository" "user" "search" "viewer" ...]

(defn child-names-for-type
  "Return just immediate field names for a named type (no recursion)."
  [schema type-name]
  (mapv :name (children-for-type schema type-name)))


;; (child-names-for-type schema "Repository")
;; => ["id" "name" "description" "issues" "pullRequests" ...]


(defn child-names-for-root
  "Return just immediate field names for a root field.
   op-type is :query/:mutation/:subscription."
  [schema op-type root-field-name]
  (let [root-type-name (case op-type
                         :query (get-in schema [:queryType :name])
                         :mutation (get-in schema [:mutationType :name])
                         :subscription (get-in schema [:subscriptionType :name]))]
    (mapv :name (children-for-field schema root-type-name root-field-name))))


;; (child-names-for-root schema :query "repository")
;; => ["assignableUsers" "autoMergeAllowed" "branchProtectionRules"
;;     "codeOfConduct" "createdAt" "defaultBranchRef" ...]


(defn root-plus-one-level-names
  "Build a 2-level name-only tree for UI."
  [schema op-type root-field-name]
  [{:name "Root"
    :children
    [{:name root-field-name
      :children (mapv (fn [n] {:name n})
                      (child-names-for-root schema op-type root-field-name))}]}])


;; (root-plus-one-level-names schema :query "repository")
;; => [{:name "Root"
;;      :children
;;      [{:name "repository"
;;        :children [{:name "id"} {:name "name"} ...]}]}]


(defn fetch-schema!
  [url headers]
  (-> (fetch-introspection! url headers)
      schema-from-introspection))

(defn root-plus-one-level-names-from-url
  [url headers op-type root-field-name]
  (let [schema (fetch-schema! url headers)]
    (root-plus-one-level-names schema op-type root-field-name)))


;; (root-plus-one-level-names-from-url github-url headers :query "repository")


;; assumes you already have kind->kw, unwrap-type, index-types

(defn named-kind?
  "Kinds that have a named type you can traverse."
  [k]
  (contains? #{:object :input_object :interface :union} k))

(defn base-named-type
  "Given a type descriptor from unwrap-type, peel LIST wrappers to the named core."
  [t-desc]
  (loop [d t-desc]
    (if (= :list (:kind d))
      (recur (:of d))
      d)))

(defn next-type-names
  "From a named type, return the set of named types referenced by its fields."
  [types-index type-name]
  (let [tdef (get types-index type-name)
        kind (kind->kw (:kind tdef))]
    (cond
      (nil? tdef) #{}

      (#{:object :input_object} kind)
      (let [fields (or (:fields tdef) (:inputFields tdef))]
        (->> fields
             (map (fn [f]
                    (let [core (base-named-type (unwrap-type (:type f)))]
                      (when (named-kind? (:kind core))
                        (:name core)))))
             (remove nil?)
             set))

      (#{:interface :union} kind)
      (->> (:possibleTypes tdef)
           (map :name)
           (remove nil?)
           set)

      :else #{})))

(defn reachable-object-types
  "Return all OBJECT type names reachable from op root within depth-limit.
   Filters out introspection types (__*)."
  ([schema op-type]
   (reachable-object-types schema op-type 4))
  ([schema op-type depth-limit]
   (let [types-index (index-types schema)
         root-type-name (case op-type
                          :query (get-in schema [:queryType :name])
                          :mutation (get-in schema [:mutationType :name])
                          :subscription (get-in schema [:subscriptionType :name]))
         ;; BFS frontier
         step (fn [frontier seen depth]
                (if (or (empty? frontier) (>= depth depth-limit))
                  seen
                  (let [next (->> frontier
                                  (mapcat #(next-type-names types-index %))
                                  (remove seen)
                                  set)]
                    (recur next (into seen next) (inc depth)))))]
     (let [all-seen (step #{root-type-name} #{root-type-name} 0)]
       (->> all-seen
            (filter (fn [tn]
                      (let [tdef (get types-index tn)]
                        (and tdef
                             (= :object (kind->kw (:kind tdef)))
                             (not (str/starts-with? tn "__"))))))
            sort
            vec)))))


(defn startable-object-types
  "Types returned directly by root fields (true entry points)."
  [schema op-type]
  (let [types-index (index-types schema)
        root-type-name (case op-type
                         :query (get-in schema [:queryType :name])
                         :mutation (get-in schema [:mutationType :name])
                         :subscription (get-in schema [:subscriptionType :name]))
        root-type (get types-index root-type-name)]
    (->> (:fields root-type)
         (map (fn [f]
                (-> (unwrap-type (:type f))
                    base-named-type
                    :name)))
         (remove nil?)
         (remove #(str/starts-with? % "__"))
         distinct
         sort
         vec)))


(defn reachable-object-types-full
  "Return ALL OBJECT type names reachable from Query root (transitive closure).
   Cycle-safe, no depth limit.
   Filters out introspection types (__*)."
  [schema]
  (let [types-index (index-types schema)
        root-type-name (get-in schema [:queryType :name])]
    (loop [frontier #{root-type-name}
           seen #{root-type-name}]
      (if (empty? frontier)
        (->> seen
             (filter (fn [tn]
                       (let [tdef (get types-index tn)]
                         (and tdef
                              (= :object (kind->kw (:kind tdef)))
                              (not (str/starts-with? tn "__"))))))
            sort
             vec)
        (let [next (->> frontier
                        (mapcat #(next-type-names types-index %))
                        (remove seen)
                        set)]
          (recur next (into seen next)))))))

(defn reachable-object-types-full-op
  "Return ALL reachable OBJECT type names from given op root.
   op-type is :query / :mutation / :subscription.
   Cycle-safe, no depth limit.
   Filters out introspection types (__*)."
  [schema op-type]
  (let [types-index (index-types schema)
        root-type-name (case op-type
                         :query (get-in schema [:queryType :name])
                         :mutation (get-in schema [:mutationType :name])
                         :subscription (get-in schema [:subscriptionType :name]))]
    (loop [frontier #{root-type-name}
           seen #{root-type-name}]
      (if (empty? frontier)
        (->> seen
             (filter (fn [tn]
                       (let [tdef (get types-index tn)]
 (and tdef
                              (= :object (kind->kw (:kind tdef)))
                              (not (str/starts-with? tn "__"))))))
             sort
             vec)
        (let [next (->> frontier
                        (mapcat #(next-type-names types-index %))
                        (remove seen)
                        set)]
          (recur next (into seen next)))))))

(comment
(def schema (-> (fetch-introspection! github-url headers)
                schema-from-introspection))

(def all-reachable (reachable-object-types-full schema))

;;(count all-reachable)
;; => total reachable OBJECTs

;;(take 30 all-reachable)
;; => preview names
) ;; end comment


;; DORA objects

(def dora-keywords
  ;; broad, but works well on GitHub schema naming
  ["deployment" "environment" "release" "workflow" "check" "status"
   "pullrequest" "commit" "compare" "ref" "tag"
   "issue" "incident" "alert" "vulnerability" "codescanning" "dependabot"])

(def dora-roots
  #{"Repository" "RepositoryOwner" "User" "Organization" "SearchResultItemConnection"})


(defn dora-relevant-name?
  "Heuristic match: type name contains a DORA keyword."
  [type-name]
  (let [n (str/lower-case type-name)]
    (some #(str/includes? n %) dora-keywords)))

(defn wrapper-object?
  "Same wrapper noise filter as before."
  [type-name]
  (or (str/ends-with? type-name "Connection")
      (str/ends-with? type-name "Edge")
      (str/ends-with? type-name "Payload")
      (= type-name "PageInfo")))

(defn dora-noise?
  [type-name]
  (or (str/ends-with? type-name "Event")
      (str/ends-with? type-name "Parameters")
      (str/ends-with? type-name "Rule")
      (str/ends-with? type-name "PatternParameters")
      (str/ends-with? type-name "Template")
      (str/includes? type-name "Contributions")
      (str/includes? type-name "Hovercard")
      (str/starts-with? type-name "ProjectV2")
      (str/starts-with? type-name "Draft")
      (str/starts-with? type-name "ConvertedNote")
      (str/starts-with? type-name "Created")
      (str/starts-with? type-name "BaseRef")
      (str/starts-with? type-name "HeadRef")))


(defn dora-reachable-objects
  "Return reachable OBJECT types relevant to DORA, from :query root.
   Options:
   - include-wrappers? default false
   - keyword-filter?  default true"
  ([schema]
   (dora-reachable-objects schema {:include-wrappers? false
                                   :keyword-filter? true}))
  ([schema {:keys [include-wrappers? keyword-filter?]
            :or {include-wrappers? false keyword-filter? true}}]
   (let [all (reachable-object-types-full-op schema :query)]
     (->> all
          (remove dora-noise?)  
          (filter (fn [tn]
                    (and (or include-wrappers? (not (wrapper-object? tn)))
                         (or (not keyword-filter?) (contains? dora-roots tn) (dora-relevant-name? tn)))))
          sort
          vec))))

