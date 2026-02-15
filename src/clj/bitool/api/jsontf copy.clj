(ns bitool.api.jsontf 
  (:require [clojure.string :as str]))

;; ---------- Path parsing ----------

(defn parse-path
  "Turn \"$.orders[].items[].sku\" -> [\"$\" \"orders\" \"[]\" \"items\" \"[]\" \"sku\"]."
  [path]
  (let [p (str/replace path #"^\$\.?" "")]
    (->> (str/split p #"\.")
         (mapcat (fn [seg]
                   (if (str/ends-with? seg "[]")
                     [(subs seg 0 (- (count seg) 2)) "[]"]
                     [seg])))
         (remove #(= "" %))
         (into ["$"]))))

(defn- lcp2 [a b]
  (vec (map first (take-while (fn [[x y]] (= x y))
                              (map vector a b)))))

(defn- longest-common-prefix [paths]
  (reduce lcp2 paths))

(defn- last-array-idx [segs]
  (->> (map-indexed vector segs)
       (keep (fn [[i s]] (when (= s "[]") i)))
       last))

(defn- pick-spine
  "LCP across all paths, cut at last [] so we fan out there."
  [seg-vecs]
  (let [lcp (longest-common-prefix seg-vecs)
        i   (last-array-idx lcp)]
    (if i (subvec lcp 0 (inc i)) lcp)))

(defn- prop-get [m k]
  (when (map? m) (or (get m (keyword k)) (get m k))))

;; ---------- Spine traversal (transducer-friendly) ----------

(defn- walk-spine
  "Traverse the spine over data, returning seq of context maps (or values)."
  [data spine]
  (reduce
    (fn [vals seg]
      (if (= seg "[]")
        (sequence (comp
                    (mapcat (fn [v]
                              (cond
                                (sequential? v) v
                                (nil? v)        []
                                :else           []))))
                  vals)
        (sequence (comp
                    (map #(if (= seg "$") % (prop-get % seg)))
                    (remove nil?))
                  vals)))
    [data]
    spine))

;; ---------- Tail evaluation (always keeps a seq of values) ----------

(defn- eval-tail
  "From a context and remaining segments, return *all* extracted values as a seq."
  [ctx segs]
  (reduce
    (fn [vals seg]
      (if (= seg "[]")
        (sequence (comp (mapcat (fn [v]
                                  (cond
                                    (sequential? v) v
                                    (nil? v)        []
                                    :else           []))))
                  vals)
        (sequence (comp (map #(prop-get % seg))
                        (remove nil?))
                  vals)))
    [ctx]
    segs))

;; ---------- Public API ----------

;; Paste into bitool.api.jsontf (or wherever you're working)
(defn rows-from-json2
  "Like rows-from-json, but fully-qualified to avoid shadowing and with a robust :explode-by."
  ([data path->col] (rows-from-json2 data path->col {}))
  ([data path->col {:keys [row-mode explode-key join-arrays? join-delim]
                    :or   {row-mode :per-context join-arrays? true join-delim ","}}]
   (let [parse-path (fn [path]
                      (let [p (clojure.string/replace path #"^\$\.?" "")]
                        (->> (clojure.string/split p #"\.")
                             (mapcat (fn [seg]
                                       (if (clojure.string/ends-with? seg "[]")
                                         [(subs seg 0 (- (count seg) 2)) "[]"]
                                         [seg])))
                             (remove #(= "" %))
                             (into ["$"]))))
         prop-get   (fn [m k] (when (map? m) (or (get m (keyword k)) (get m k))))
         lcp2       (fn [a b] (vec (map first (take-while (fn [[x y]] (= x y))
                                                          (map vector a b)))))
         longest-common-prefix (fn [paths] (reduce lcp2 paths))
         last-array-idx (fn [segs]
                          (->> (map-indexed vector segs)
                               (keep (fn [[i s]] (when (= s "[]") i)))
                               last))
         pick-spine (fn [seg-vecs]
                      (let [lcp (longest-common-prefix seg-vecs)
                            i   (last-array-idx lcp)]
                        (if i (subvec lcp 0 (inc i)) lcp)))
         walk-spine (fn [data spine]
                      (reduce
                        (fn [vals seg]
                          (if (= seg "[]")
                            (sequence
                              (comp (mapcat (fn [v]
                                              (cond
                                                (sequential? v) v
                                                (nil? v)        []
                                                :else           []))))
                              vals)
                            (sequence
                              (comp (map #(if (= seg "$") % (prop-get % seg)))
                                    (remove nil?))
                              vals)))
                        [data] spine))
         eval-tail  (fn [ctx segs]
                      (reduce
                        (fn [vals seg]
                          (if (= seg "[]")
                            (sequence
                              (comp (mapcat (fn [v]
                                              (cond
                                                (sequential? v) v
                                                (nil? v)        []
                                                :else           []))))
                              vals)
                            (sequence
                              (comp (map #(prop-get % seg))
                                    (remove nil?))
                              vals)))
                        [ctx] segs))
         join-or-first (fn [vals]
                         (if join-arrays?
                           (->> vals
                                (map #(if (nil? %) "" (str %)))
                                (clojure.string/join join-delim))
                           (first vals)))

         parsed   (mapv (fn [[p c]] [(parse-path p) c p]) path->col)
         segs     (mapv first parsed)
         cols     (mapv second parsed)
         keys-in  (mapv #(nth % 2) parsed)
         spine    (pick-spine segs)
         tails    (mapv #(vec (drop (count spine) %)) segs)
         contexts (walk-spine data spine)]
     (case row-mode
       :per-context
       (into []
             (map (fn [ctx]
                    (reduce (fn [row [tail col]]
                              (let [vals (eval-tail ctx tail)]
                                (assoc row col (join-or-first vals))))
                            {}
                            (map vector tails cols))))
             contexts)

       :explode-by
       (let [idx (.indexOf keys-in (or explode-key ::not-found))]
         (when (neg? idx)
           (throw (ex-info "explode-key must be one of the mapping keys"
                           {:explode-key explode-key
                            :available   (vec keys-in)})))
         (let [driver-tail (nth tails idx)]
           (into []
                 (mapcat
                   (fn [ctx]
                     (let [driver (vec (eval-tail ctx driver-tail))
                           n      (count driver)]
                       (map (fn [i]
                              ;; Build one row per driver index i
                              (reduce (fn [row [tail col]]
                                        (let [vals (vec (eval-tail ctx tail))
                                              cnt  (count vals)
                                              v    (cond
                                                     ;; aligned with driver
                                                     (= cnt n)   (get vals i)
                                                     ;; broadcast singletons
                                                     (= cnt 1)   (first vals)
                                                     ;; nothing
                                                     (zero? cnt) nil
                                                     ;; mismatch: join/first fallback
                                                     :else       (join-or-first vals))]
                                          (assoc row col v)))
                                      {}
                                      (map vector tails cols))))
                            (range n))))
                 contexts)))

       (throw (ex-info "Unknown :row-mode" {:row-mode row-mode}))))))

(defn rows-from-json2-debug
  "Debug version of rows-from-json2.
   Prints spine, tails, contexts, and values extracted for each tail."
  ([data path->col] (rows-from-json2-debug data path->col {}))
  ([data path->col {:keys [row-mode explode-key join-arrays? join-delim]
                    :or   {row-mode :per-context join-arrays? true join-delim ","}}]
   (println "\n=== DEBUG rows-from-json2 ===")
   (println "row-mode:" row-mode "explode-key:" explode-key)

   (let [parse-path (fn [path]
                      (let [p (clojure.string/replace path #"^\$\.?" "")]
                        (->> (clojure.string/split p #"\.")
                             (mapcat (fn [seg]
                                       (if (clojure.string/ends-with? seg "[]")
                                         [(subs seg 0 (- (count seg) 2)) "[]"]
                                         [seg])))
                             (remove #(= "" %))
                             (into ["$"]))))
         prop-get   (fn [m k]
                      (when (map? m)
                        (or (get m (keyword k)) (get m k))))
         lcp2       (fn [a b]
                      (vec (map first (take-while (fn [[x y]] (= x y))
                                                  (map vector a b)))))
         longest-common-prefix (fn [paths] (reduce lcp2 paths))
         last-array-idx (fn [segs]
                          (->> (map-indexed vector segs)
                               (keep (fn [[i s]] (when (= s "[]") i)))
                               last))
         pick-spine (fn [seg-vecs]
                      (let [lcp (longest-common-prefix seg-vecs)
                            i   (last-array-idx lcp)]
                        (if i (subvec lcp 0 (inc i)) lcp)))
         walk-spine (fn [data spine]
                      (reduce
                        (fn [vals seg]
                          (println "SPINE step" seg "on" (count vals) "vals")
                          (if (= seg "[]")
                            (mapcat #(cond
                                       (sequential? %) %
                                       (nil? %)        []
                                       :else           [])
                                    vals)
                            (keep #(if (= seg "$") % (prop-get % seg)) vals)))
                        [data] spine))
         eval-tail  (fn [ctx segs]
                      (reduce
                        (fn [vals seg]
                          (println "  TAIL step" seg "vals-in" (count vals)
                                   "types:" (take 3 (map type vals)))
                          (let [out (if (= seg "[]")
                                      (mapcat #(cond
                                                 (sequential? %) %
                                                 (nil? %)        []
                                                 :else           [])
                                              vals)
                                      (keep #(prop-get % seg) vals))]
                            (println "  -> vals-out" (count out)
                                     "sample:" (take 4 out))
                            out))
                        [ctx] segs))
         join-or-first (fn [vals]
                         (if join-arrays?
                           (->> vals
                                (map #(if (nil? %) "" (str %)))
                                (clojure.string/join join-delim))
                           (first vals)))

         parsed   (mapv (fn [[p c]] [(parse-path p) c p]) path->col)
         segs     (mapv first parsed)
         cols     (mapv second parsed)
         keys-in  (mapv #(nth % 2) parsed)
         spine    (pick-spine segs)
         tails    (mapv #(vec (drop (count spine) %)) segs)
         contexts (walk-spine data spine)]

     (println "\nSPINE =" spine)
     (println "TAILS =" tails)
     (println "CONTEXTS found:" (count contexts))
     (doseq [i (range (min 3 (count contexts)))]
       (println " ctx" i ":")
       (clojure.pprint/pprint (nth contexts i)))

     (case row-mode
       :per-context
       (into []
             (map (fn [ctx]
                    (reduce (fn [row [tail col]]
                              (let [vals (eval-tail ctx tail)]
                                (println "   EXTRACT per-context" tail "=>" (take 4 vals))
                                (assoc row col (join-or-first vals))))
                            {}
                            (map vector tails cols))))
             contexts)

       :explode-by
;; ---------- explode by a tail path (driver) ----------
:explode-by
(let [idx (.indexOf keys-in (or explode-key ::not-found))]
  (when (neg? idx)
    (throw (ex-info "explode-key must be one of the mapping keys"
                    {:explode-key explode-key
                     :available   (vec keys-in)})))
  (let [driver-tail (nth tails idx)]
    (println "Driver tail =" driver-tail)
    (into []
          (mapcat
            (fn [ctx]
              (let [driver (vec (eval-tail ctx driver-tail))
                    n      (count driver)]
                (println "Context driver values:" driver)
                ;; IMPORTANT: build a row for each i — do NOT return (range n)
                (map (fn [i]
                       (println " Row index" i)
                       (reduce (fn [row [tail col orig-k]]
                                 (let [vals (vec (eval-tail ctx tail))
                                       cnt  (count vals)
                                       v    (cond
                                              ;; aligned with driver
                                              (= cnt n)   (get vals i)
                                              ;; broadcast singletons (parent fields)
                                              (= cnt 1)   (first vals)
                                              ;; nothing found
                                              (zero? cnt) nil
                                              ;; length mismatch — fallback to join/first
                                              :else       (join-or-first vals))]
                                   (println "   col" col "from" orig-k
                                            "vals" vals "-> v" v)
                                   (assoc row col v)))
                               {}
                               ;; keep orig key only for debug prints
                               (map vector tails cols keys-in)))
                     (range n))))
            contexts))))

       (throw (ex-info "Unknown :row-mode" {:row-mode row-mode}))))))

;; drop-in, self-contained, no external deps beyond clojure.core
(defn rows-from-json
  "Robust JSON flatten/explode utility with debug prints.
   Supports arrays-of-arrays, sparse values, and root scalars."
  ([data path->col] (rows-from-json data path->col {}))
  ([data path->col {:keys [row-mode explode-key join-arrays? join-delim filter-nils-before-join?]
                    :or   {row-mode :per-context
                           join-arrays? true
                           join-delim ","
                           filter-nils-before-join? true}}]
   (println "\n=== rows-from-json-final-debug ===")
   (println "row-mode =" row-mode "explode-key =" explode-key)

   (let [parse-path (fn [path]
                      (let [p (clojure.string/replace path #"^\$\.?" "")]
                        (->> (clojure.string/split p #"\.")
                             (mapcat (fn [seg]
                                       (if (clojure.string/ends-with? seg "[]")
                                         [(subs seg 0 (- (count seg) 2)) "[]"]
                                         [seg])))
                             (remove #(= "" %))
                             (into ["$"]))))
         prop-get   (fn [m k] (when (map? m) (or (get m (keyword k)) (get m k))))
         lcp2       (fn [a b] (vec (map first (take-while (fn [[x y]] (= x y))
                                                          (map vector a b)))))
         lcp        (fn [paths] (reduce lcp2 paths))
         last-idx   (fn [segs] (->> (map-indexed vector segs)
                                    (keep (fn [[i s]] (when (= s "[]") i)))
                                    last))
         pick-spine (fn [seg-vecs] (let [p (lcp seg-vecs) i (last-idx p)]
                                     (if i (subvec p 0 (inc i)) p)))

         ;; Flatten one level when seg == "[]"; handle arrays-of-arrays; ignore scalars.
         walk-spine (fn [d spine]
                      (reduce
                        (fn [vals seg]
                          (println "SPINE step" seg "on" (count vals) "vals")
                          (let [out (if (= seg "[]")
                                      (mapcat #(cond
                                                 ;; array of arrays: flatten one level
                                                 (and (sequential? %) (every? sequential? %)) (apply concat %)
                                                 (sequential? %) %
                                                 (map? %)        [%]
                                                 (nil? %)        []
                                                 :else           [])
                                              vals)
                                      (mapcat #(cond
                                                 (= seg "$") [%]
                                                 (map? %)    (let [v (prop-get % seg)]
                                                               (cond (sequential? v) v
                                                                     (map? v)        [v]
                                                                     (nil? v)        []
                                                                     :else           [v]))
                                                 :else [])
                                              vals))]
                            out))
                        [d] spine))

         ;; eval-tail starting from ctx over segs (supports repeated "[]")
         eval-tail  (fn [ctx segs]
                      (if (empty? segs)
                        [ctx]
                        (reduce
                          (fn [vals seg]
                            (println "  TAIL step" seg "vals-in" (count vals))
                            (let [out (if (= seg "[]")
                                        (mapcat #(cond
                                                   (and (sequential? %) (every? sequential? %)) (apply concat %)
                                                   (sequential? %) %
                                                   (map? %)        [%]
                                                   (nil? %)        []
                                                   :else           [])
                                                vals)
                                        (mapcat #(cond
                                                   (map? %) (let [v (prop-get % seg)]
                                                              (cond (sequential? v) v
                                                                    (map? v)        [v]
                                                                    (nil? v)        []
                                                                    :else           [v]))
                                                   :else [])
                                                vals))]
                              (println "  -> vals-out" (count out) "sample:" (take 4 out))
                              out))
                          [ctx] segs)))

         ;; safer join-or-first: "" for empty, preserve numeric type on singleton, join otherwise
         join-or-first (fn [vals]
                         (let [vals* (if filter-nils-before-join?
                                       (remove nil? vals)
                                       vals)]
                           (cond
                             (empty? vals*) ""
                             (not join-arrays?) (first vals*)
                             (= 1 (count vals*)) (first vals*)
                             :else (->> vals*
                                        (map #(if (nil? %) "" (str %)))
                                        (clojure.string/join join-delim)))))

         ;; parse inputs
         parsed   (mapv (fn [[p c]] [(parse-path p) c p]) path->col)
         segs     (mapv first parsed)
         cols     (mapv second parsed)
         keys-in  (mapv #(nth % 2) parsed)
         spine    (pick-spine segs)
         tails    (mapv #(vec (drop (count spine) %)) segs)
         contexts (do
                    (println "\nSPINE =" spine)
                    (println "TAILS =" tails)
                    (let [ctxs (->> (walk-spine data spine)
                                    (filter #(or (map? %) (sequential? %))))]
                      (println "CONTEXTS =" (count ctxs))
                      (doseq [i (range (min 3 (count ctxs)))]
                        (println " ctx" i ":") (clojure.pprint/pprint (nth ctxs i)))
                      ctxs))]

     (if (= row-mode :explode-by)
       (let [idx (.indexOf keys-in (or explode-key ::not-found))]
         (when (neg? idx)
           (throw (ex-info "explode-key must be one of mapping keys"
                           {:explode-key explode-key :available (vec keys-in)})))
         (let [driver-tail (nth tails idx)
               driver-base (let [pos (->> (map-indexed vector driver-tail)
                                          (keep (fn [[i s]] (when (= s "[]") i)))
                                          last)]
                             (subvec driver-tail 0 (inc pos)))
               tail-descs (mapv (fn [tail]
                                  (let [aligned?   (and (<= (count driver-base) (count tail))
                                                        (= driver-base (subvec tail 0 (count driver-base))))
                                        tail-after (if aligned?
                                                     (subvec tail (count driver-base))
                                                     tail)]
                                    {:aligned? aligned? :tail-after tail-after :full tail}))
                                tails)]
           (into []
                 (mapcat
                   (fn [ctx]
                     (let [raw-elems (eval-tail ctx driver-base)
                           driver-elems (vec (cond
                                               (sequential? raw-elems) raw-elems
                                               (nil? raw-elems)        []
                                               :else                   [raw-elems]))
                           n (count driver-elems)]
                       (when (pos? n)
                         (map (fn [i]
                                (reduce
                                  (fn [row [desc col _orig]]
                                    (let [{:keys [aligned? tail-after full]} desc
                                          v (if aligned?
                                              (join-or-first (eval-tail (nth driver-elems i) tail-after))
                                              (let [vals (vec (eval-tail ctx full))
                                                    cnt  (count vals)]
                                                (cond
                                                  (= cnt n)   (nth vals i)
                                                  (= cnt 1)   (join-or-first vals)
                                                  (zero? cnt) ""
                                                  :else       (join-or-first vals))))]
                                      (assoc row col v)))
                                  {}
                                  (map vector tail-descs cols keys-in)))
                              (range n))))))
                 contexts))))
       ;; :per-context
       (into []
             (map (fn [ctx]
                    (reduce (fn [row [tail col _orig]]
                              (let [vals (eval-tail ctx tail)]
                                (assoc row col (join-or-first vals))))
                            {}
                            (map vector tails cols keys-in))))
             contexts))))

