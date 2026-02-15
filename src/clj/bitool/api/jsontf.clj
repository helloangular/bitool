(ns bitool.api.jsontf 
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]))

;; ---------- Path parsing ----------

(defn parse-path
  "Turn \"$.orders[].items[].sku\" -> [\"$\" \"orders\" \"[]\" \"items\" \"[]\" \"sku\"].
   Also handles $.notes[][] properly."
  [path]
  (let [p (str/replace path #"^\$\.?" "")]
    (loop [segments []
           remaining p]
      (if (empty? remaining)
        (into ["$"] (remove #(= "" %) segments))
        (let [;; Try to match a segment ending with []
              array-match (re-find #"^([^\.\[]*)\[\](.*)$" remaining)]
          (if array-match
            (let [[_ prop rest] array-match]
              (recur (conj segments prop "[]") rest))
            ;; No array notation, find next dot or end
            (let [dot-idx (str/index-of remaining ".")]
              (if dot-idx
                (recur (conj segments (subs remaining 0 dot-idx))
                       (subs remaining (inc dot-idx)))
                (recur (conj segments remaining) "")))))))))

(defn- lcp2 [a b]
  (vec (map first (take-while (fn [[x y]] (= x y))
                              (map vector a b)))))

(defn- longest-common-prefix [paths]
  (reduce lcp2 paths))

(defn- last-array-idx [segs]
  (->> (map-indexed vector segs)
       (keep (fn [[i s]] (when (= s "[]") i)))
       last))

(defn- first-array-idx [segs]
  (->> (map-indexed vector segs)
       (keep (fn [[i s]] (when (= s "[]") i)))
       first))

(defn- pick-spine
  "For :per-context: include up to last [] in common prefix
   For :explode-by: include up to last [] in common prefix"
  [seg-vecs]
  (let [lcp (longest-common-prefix seg-vecs)
        i (last-array-idx lcp)]
    (if i (subvec lcp 0 (inc i)) lcp)))

(defn- prop-get [m k]
  (when (map? m) (or (get m (keyword k)) (get m k))))

;; ---------- Core traversal functions ----------

(defn- walk-spine
  "Traverse the spine over data, returning seq of context values."
  [data spine]
  (reduce
    (fn [vals seg]
      (log/debug "SPINE step" seg "on" (count vals) "vals")
      (if (= seg "[]")
        ;; Flatten arrays, including arrays-of-arrays
        (let [result (mapcat (fn [v]
                               (cond
                                 (sequential? v) v
                                 (map? v) [v]
                                 (nil? v) []
                                 :else [v]))  ; Include scalars!
                             vals)]
          result)
        ;; Navigate into property
        (keep #(if (= seg "$") % (prop-get % seg)) vals)))
    [data]
    spine))

(defn- eval-tail
  "From a context and remaining segments, return *all* extracted values as a seq."
  [ctx segs]
  (if (empty? segs)
    ;; Empty tail: return the context itself as a single-element seq
    [ctx]
    (reduce
      (fn [vals seg]
        (log/debug "  TAIL step" seg "vals-in" (count vals))
        (let [out (if (= seg "[]")
                    ;; Flatten arrays
                    (mapcat (fn [v]
                              (cond
                                (sequential? v) v
                                (map? v) [v]
                                (nil? v) []
                                :else [v]))  ; Include scalars!
                            vals)
                    ;; Navigate into property
                    (keep #(prop-get % seg) vals))]
          (log/debug "  -> vals-out" (count out) "sample:" (take 4 out))
          out))
      [ctx]
      segs)))

;; ---------- Public API ----------

(defn rows-from-json
  "Extract rows from JSON data using path mappings.
   
   data        - parsed JSON (keyword keys recommended)
   path->col   - {\"$.a[].b\" :colB, ...}
   opts        - {:row-mode :per-context | :explode-by
                  :explode-key \"<one of the mapping keys>\"
                  :join-arrays? true|false
                  :join-delim \",\"
                  :filter-nils-before-join? true|false}"
  ([data path->col] (rows-from-json data path->col {}))
  ([data path->col {:keys [row-mode explode-key join-arrays? join-delim filter-nils-before-join?]
                    :or   {row-mode :per-context
                           join-arrays? true
                           join-delim ","
                           filter-nils-before-join? true}}]
   (log/debug "\n=== rows-from-json3-debug ===")
   (log/debug "row-mode =" row-mode "explode-key =" explode-key)

   (let [;; Helper to join or take first value
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
                                        (str/join join-delim)))))

         ;; Parse all paths and build metadata
         parsed   (mapv (fn [[p c]] [(parse-path p) c p]) path->col)
         segs     (mapv first parsed)
         cols     (mapv second parsed)
         keys-in  (mapv #(nth % 2) parsed)
         spine    (pick-spine segs)
         tails    (mapv #(vec (drop (count spine) %)) segs)
         
         ;; Walk the spine to get contexts
         contexts (do
                    (log/debug "\nSPINE =" spine)
                    (log/debug "TAILS =" tails)
                    (let [raw (walk-spine data spine)
                          ctxs (if (empty? raw) [data] (vec raw))]
                      (log/debug "CONTEXTS =" (count ctxs))
                      (doseq [i (range (min 3 (count ctxs)))]
                        (log/debug " ctx" i ":") (with-out-str (clojure.pprint/pprint (nth ctxs i))))
                      ctxs))]

     (if (= row-mode :explode-by)
       ;; :explode-by mode
       (let [idx (.indexOf keys-in (or explode-key ::not-found))]
         (when (neg? idx)
           (throw (ex-info "explode-key must be one of mapping keys"
                           {:explode-key explode-key :available (vec keys-in)})))
         (let [driver-tail (nth tails idx)
               ;; Find the driver array base (up to and including the explode [])
               driver-base (let [pos (->> (map-indexed vector driver-tail)
                                          (keep (fn [[i s]] (when (= s "[]") i)))
                                          last)]
                             (if pos
                               (subvec driver-tail 0 (inc pos))
                               driver-tail))
               
               ;; Classify each tail relative to the driver base
               tail-descs
               (mapv (fn [tail]
                       (let [aligned? (and (<= (count driver-base) (count tail))
                                           (= driver-base (subvec tail 0 (min (count driver-base) (count tail)))))
                             tail-after (if aligned?
                                          (subvec tail (count driver-base))
                                          tail)]
                         {:aligned? aligned? :tail-after tail-after :full tail}))
                     tails)]
           
           (into []
                 (mapcat
                   (fn [ctx]
                     (let [raw-elems   (eval-tail ctx driver-base)
                           driver-elems (vec raw-elems)
                           n           (count driver-elems)]
                       (when (pos? n)
                         (map (fn [i]
                                (reduce
                                  (fn [row [desc col _orig]]
                                    (let [{:keys [aligned? tail-after full]} desc
                                          v (if aligned?
                                              ;; Evaluate from the i-th driver element
                                              (join-or-first (eval-tail (nth driver-elems i) tail-after))
                                              ;; Not aligned: evaluate from ctx
                                              (let [vals (vec (eval-tail ctx full))
                                                    cnt  (count vals)]
                                                (cond
                                                  (= cnt n)   (nth vals i "")
                                                  (= cnt 1)   (join-or-first vals)
                                                  (zero? cnt) ""
                                                  :else       (join-or-first vals))))]
                                      (assoc row col v)))
                                  {}
                                  (map vector tail-descs cols keys-in)))
                              (range n)))))
                 contexts))))
       
       ;; :per-context mode
       ;; If spine ends with [] and contexts are mostly scalars, aggregate
       ;; If spine ends with [] and contexts are maps, one row per map
       (let [spine-ends-with-array? (and (seq spine) (= "[]" (last spine)))
             contexts-are-maps? (and (seq contexts) 
                                     (> (count (filter map? contexts))
                                        (/ (count contexts) 2)))]  ; Majority are maps
         (if (and spine-ends-with-array? (not contexts-are-maps?))
           ;; Aggregate: collect all values from all contexts into one row
           [(reduce (fn [row [tail col _orig]]
                      (let [all-vals (mapcat #(eval-tail % tail) contexts)]
                        (assoc row col (join-or-first all-vals))))
                    {}
                    (map vector tails cols keys-in))]
           ;; One row per context
           (into []
                 (map (fn [ctx]
                        (reduce (fn [row [tail col _orig]]
                                  (let [vals (eval-tail ctx tail)]
                                    (assoc row col (join-or-first vals))))
                                {}
                                (map vector tails cols keys-in))))
                 contexts)))))))
