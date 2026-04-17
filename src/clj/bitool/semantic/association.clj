(ns bitool.semantic.association
  "Lazy association resolver for the Semantic Layer.

   Relationships in the semantic model are lazy — defined once and resolved
   only when an ISL query references columns from an associated entity.
   This module implements BFS shortest-path join resolution with ambiguity
   detection and cycle prevention, matching SAP Datasphere's association pattern.

   Join-path resolution rules (applied in order):
   1. Direct relationship — exactly one rel connecting base → referenced entity
   2. Shortest path — fewest hops via BFS over relationship graph
   3. Explicit preference — :preferred true wins ties of equal length
   4. Cycle prevention — no entity may appear twice in a join chain
   5. Chained joins — multi-hop paths add all intermediate joins in order"
  (:require [clojure.set :as cset]
            [clojure.string :as string]))

;; ---------------------------------------------------------------------------
;; Relationship graph construction
;; ---------------------------------------------------------------------------

(defn build-relationship-graph
  "Build an adjacency list from model relationships.
   Each edge is bidirectional (associations can be traversed in either direction).
   Returns {entity-name #{neighbor-entity-name ...}} with edge metadata accessible
   via the `edges` return."
  [relationships]
  (let [adj   (atom {})
        edges (atom {})]
    (doseq [{:keys [from to from_column to_column type join preferred] :as rel} relationships]
      ;; Forward edge: from → to
      (swap! adj update from (fnil conj #{}) to)
      (swap! edges assoc [from to]
             (conj (get @edges [from to] [])
                   (assoc rel :direction :forward)))
      ;; Reverse edge: to → from
      (swap! adj update to (fnil conj #{}) from)
      (swap! edges assoc [to from]
             (conj (get @edges [to from] [])
                   (assoc rel :direction :reverse))))
    {:adjacency @adj
     :edges     @edges}))

;; ---------------------------------------------------------------------------
;; Entity reference extraction from ISL
;; ---------------------------------------------------------------------------

(defn- column-entity
  "Given a possibly-qualified column ref like 'drivers.region', return the entity name.
   Unqualified refs return nil."
  [col-ref]
  (let [parts (string/split (str col-ref) #"\.")]
    (when (> (count parts) 1)
      (first parts))))

(defn- drill-down-entity
  "If the ISL has a drill_down referencing a hierarchy whose entity differs
   from the base entity, return that hierarchy's entity name. Otherwise nil."
  [isl-doc model]
  (let [drill (or (get isl-doc :drill_down) (get isl-doc "drill_down"))
        hname (when drill (or (get drill :hierarchy) (get drill "hierarchy")))]
    (when hname
      (let [hier (some (fn [h] (when (= (:name h) hname) h))
                       (or (:hierarchies model) []))
            ent  (:entity hier)
            entities (or (:entities model) {})]
        (cond
          (contains? entities ent)           (name ent)
          (contains? entities (keyword ent)) (name (keyword ent))
          (string? ent)                      ent
          :else                              (some-> ent name))))))

(defn extract-referenced-entities
  "Scan an ISL document for entity references beyond the base table.
   Looks at columns, aggregates, filters, group_by, order_by for qualified refs.
   Also checks measures and drill_down against model entities.
   Returns a set of entity names that need to be joined."
  [isl-doc model]
  (let [base-entity (or (get isl-doc :table) (get isl-doc "table"))
        entities    (set (map name (keys (or (:entities model) {}))))
        ;; Collect all column references from ISL fields
        refs        (concat
                     (or (get isl-doc :columns) (get isl-doc "columns") [])
                     (map #(or (:column %) (get % "column"))
                          (or (get isl-doc :aggregates) (get isl-doc "aggregates") []))
                     (map #(or (:column %) (get % "column"))
                          (or (get isl-doc :filters) (get isl-doc "filters") []))
                     (or (get isl-doc :group_by) (get isl-doc "group_by") [])
                     (map #(or (:column %) (get % "column"))
                          (or (get isl-doc :order_by) (get isl-doc "order_by") [])))
        ;; Extract entity names from qualified column refs
        ref-entities (->> refs
                          (keep column-entity)
                          (filter #(contains? entities %))
                          set)
        drill-entity (drill-down-entity isl-doc model)
        with-drill   (cond-> ref-entities
                       (and drill-entity (contains? entities drill-entity))
                       (conj drill-entity))]
    (disj with-drill base-entity)))

;; ---------------------------------------------------------------------------
;; BFS shortest-path join resolution
;; ---------------------------------------------------------------------------

(defn- bfs-all-shortest-paths
  "BFS from `start` to `target` over the relationship adjacency graph.
   Returns ALL shortest paths (same minimum length) as a vector of path vectors.
   Returns [] if unreachable. This is critical for ambiguity detection — if two
   distinct routes of equal length exist (e.g. A→B→D and A→C→D), both must be
   found so the caller can flag the ambiguity.

   Uses layer-by-layer BFS: all paths at depth N are expanded before any path
   at depth N+1. Visited is only updated between layers, ensuring parallel
   paths through different intermediate nodes at the same depth are all discovered."
  [adjacency start target]
  (loop [current-layer [[start]]
         visited       #{start}]
    (cond
      ;; No more paths to explore — unreachable
      (empty? current-layer) []
      :else
      ;; Process entire current layer: check for target hits, expand to next layer
      (let [hits     (filterv #(= target (peek %)) current-layer)
            non-hits (filterv #(not= target (peek %)) current-layer)]
        (if (seq hits)
          ;; Found target(s) at this depth — return all of them, don't go deeper
          hits
          ;; Expand all non-hit paths to the next layer
          (let [next-layer (into []
                             (mapcat (fn [path]
                                       (let [current   (peek path)
                                             neighbors (get adjacency current #{})]
                                         (->> neighbors
                                              (remove visited)
                                              (mapv #(conj path %))))))
                             non-hits)
                ;; Now mark all newly-reached nodes as visited for future layers
                new-nodes (into #{} (map peek) next-layer)]
            (recur next-layer
                   (into visited new-nodes))))))))

(defn- select-best-edge
  "Given multiple edges between two entities, select the best one.
   Prefers :preferred true, then falls back to first.
   Returns {:edge ... :ambiguous? bool} if multiple non-preferred edges exist."
  [edges-between]
  (if (= 1 (count edges-between))
    {:edge (first edges-between) :ambiguous? false}
    ;; Multiple edges — check for :preferred
    (let [preferred (filter :preferred edges-between)]
      (if (= 1 (count preferred))
        {:edge (first preferred) :ambiguous? false}
        {:edge (first edges-between)
         :ambiguous? true
         :candidates edges-between}))))

(defn- path->join-specs
  "Convert a BFS path [A B C] into join specs using relationship edges.
   Returns {:resolved-joins [...] :ambiguous [...]} where ambiguous lists
   any pairs with multiple non-preferred edges."
  [path edges]
  (let [pairs     (partition 2 1 path)
        ambiguous (atom [])
        joins     (atom [])]
    (doseq [[from to] pairs]
      (let [edge-list (get edges [from to] [])
            {:keys [edge ambiguous?]} (select-best-edge edge-list)]
        (when ambiguous?
          (swap! ambiguous conj {:from from :to to :candidates (count edge-list)}))
        (when edge
          (swap! joins conj edge))))
    {:resolved-joins @joins
     :ambiguous      @ambiguous}))

;; ---------------------------------------------------------------------------
;; Public API
;; ---------------------------------------------------------------------------

(defn resolve-lazy-joins
  "Given an ISL document and a Semantic Model, determine which JOINs are needed.
   Only adds JOINs for entities whose columns are actually referenced.

   Uses BFS shortest-path from the base entity over declared relationships.
   Rejects ambiguous equal-length paths unless one relationship is marked :preferred.

   Returns {:join-specs [...] :joined-entities #{...}}
   or throws ExceptionInfo on ambiguity or unreachable entities."
  [isl-doc model]
  (let [referenced    (extract-referenced-entities isl-doc model)
        base-entity   (or (get isl-doc :table) (get isl-doc "table"))
        relationships (or (:relationships model) [])
        {:keys [adjacency edges]} (build-relationship-graph relationships)
        all-joins     (atom [])
        all-ambiguous (atom [])
        unreachable   (atom [])
        joined-set    (atom #{base-entity})]

    (doseq [target referenced]
      (let [paths (bfs-all-shortest-paths adjacency base-entity target)]
        (cond
          ;; No path found — entity is unreachable
          (empty? paths)
          (swap! unreachable conj target)

          ;; Multiple distinct shortest paths — ambiguous route through graph
          ;; Check if any path has all edges with :preferred, otherwise reject
          (> (count paths) 1)
          (let [path-specs  (mapv #(path->join-specs % edges) paths)
                ;; A path is "preferred" if every edge along it is :preferred
                preferred  (filterv (fn [ps]
                                      (and (empty? (:ambiguous ps))
                                           (every? :preferred (:resolved-joins ps))))
                                    path-specs)]
            (if (= 1 (count preferred))
              ;; Exactly one fully-preferred path — use it
              (let [{:keys [resolved-joins]} (first preferred)]
                (doseq [join resolved-joins]
                  (let [join-entity (if (= (:direction join) :forward) (:to join) (:from join))]
                    (when-not (contains? @joined-set join-entity)
                      (swap! joined-set conj join-entity)
                      (swap! all-joins conj join)))))
              ;; Multiple or zero preferred paths — ambiguous
              (swap! all-ambiguous conj
                     {:target target
                      :path-count (count paths)
                      :paths (mapv (fn [p] (string/join " → " p)) paths)})))

          ;; Exactly one shortest path
          :else
          (let [{:keys [resolved-joins ambiguous]} (path->join-specs (first paths) edges)]
            (swap! all-ambiguous into ambiguous)
            (doseq [join resolved-joins]
              (let [join-entity (if (= (:direction join) :forward) (:to join) (:from join))]
                (when-not (contains? @joined-set join-entity)
                  (swap! joined-set conj join-entity)
                  (swap! all-joins conj join))))))))

    (when (seq @unreachable)
      (throw (ex-info (str "Cannot resolve join path to: " (string/join ", " @unreachable))
                      {:base base-entity
                       :unreachable @unreachable})))

    (when (seq @all-ambiguous)
      (throw (ex-info "Ambiguous join paths — mark one relationship as :preferred or remove duplicates"
                      {:base base-entity
                       :ambiguous @all-ambiguous})))

    {:join-specs      @all-joins
     :joined-entities @joined-set}))

(defn relationship->isl-join-spec
  "Convert a resolved relationship edge into an ISL join spec
   that compile-isl understands."
  [rel]
  (let [forward? (= (:direction rel) :forward)]
    {:table (if forward? (:to rel) (:from rel))
     :type  (or (:join rel) "LEFT")
     :on    {(str (if forward? (:from rel) (:to rel)) "." (:from_column rel))
             (str (if forward? (:to rel) (:from rel)) "." (:to_column rel))}}))

(defn inject-lazy-joins
  "High-level: resolve lazy joins and inject them into the ISL document.
   Returns the ISL document with 'join' field populated.
   Only modifies the ISL if no explicit joins are already present."
  [isl-doc model]
  (let [existing-joins (or (get isl-doc :join) (get isl-doc "join") [])]
    (if (seq existing-joins)
      ;; User/LLM already specified explicit joins — don't override
      isl-doc
      (let [{:keys [join-specs]} (resolve-lazy-joins isl-doc model)
            isl-joins (mapv relationship->isl-join-spec join-specs)]
        (if (seq isl-joins)
          (assoc isl-doc :join isl-joins)
          isl-doc)))))

(defn validate-relationship-uniqueness
  "Check that no two relationships between the same entity pair exist
   unless one is marked :preferred. Called at model-save time to prevent
   ambiguity from reaching query time.
   Returns {:valid true} or {:valid false :errors [...]}."
  [relationships]
  (let [errors (atom [])
        ;; Group by normalized entity pair (ignore direction)
        by-pair (group-by (fn [r]
                            (set [(:from r) (:to r)]))
                          relationships)]
    (doseq [[pair rels] by-pair]
      (when (> (count rels) 1)
        (let [preferred-count (count (filter :preferred rels))]
          (when (not= 1 preferred-count)
            (swap! errors conj
                   (str "Multiple relationships between " (string/join " and " pair)
                        " — exactly one must be marked :preferred"
                        " (found " (count rels) " relationships, "
                        preferred-count " marked preferred)"))))))
    (if (empty? @errors)
      {:valid true}
      {:valid false :errors @errors})))
