(ns bitool.api.jsontf 
  (:require [clojure.string :as str]
            [clojure.pprint  :refer [pprint]]))

;; --- Path parsing -----------------------------------------------------------

(defn parse-path [path]
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

(defn- longest-common-prefix [paths] (reduce lcp2 paths))

(defn- last-array-idx [segs]
  (->> (map-indexed vector segs)
       (keep (fn [[i s]] (when (= s "[]") i)))
       last))

(defn- pick-spine [seg-vecs]
  (let [lcp (longest-common-prefix seg-vecs)
        i   (last-array-idx lcp)]
    (if i (subvec lcp 0 (inc i)) lcp)))

(defn- prop-get [m k]
  (when (map? m) (or (get m (keyword k))
                     (get m k))))

;; --- Spine traversal + tail eval (ALWAYS debug prints) ----------------------

(defn- walk-spine [data spine]
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
    [data]
    spine))

(defn- eval-tail [ctx segs]
  (reduce
    (fn [vals seg]
      (println "  TAIL step" seg "vals-in:" (count vals)
               "types:" (take 3 (map type vals)))
      (let [out (if (= seg "[]")
                  (mapcat #(cond
                             (sequential? %) %
                             (nil? %)        []
                             :else           [])
                          vals)
                  (keep #(prop-get % seg) vals))]
        (println "  -> vals-out:" (count out) "sample:" (take 5 out))
        out))
    [ctx]
    segs))

;; --- Public: ALWAYS-DEBUG version -------------------------------------------

(defn rows-from-json-debug
  "ALWAYS prints debug. Returns vector of row maps (one per spine context, joined tails)."
  [data path->col]
  (println "=== DEBUG rows-from-json ===")
  (println "DATA root type:" (type data))
  (doseq [[p c] path->col]
    (println "PATH" p "=>" (parse-path p) "-> column" c))

  (let [parsed   (mapv (fn [[p c]] [(parse-path p) c p]) path->col)
        segs     (mapv first parsed)
        cols     (mapv second parsed)
        keys-in  (mapv #(nth % 2) parsed)
        spine    (pick-spine segs)
        tails    (mapv #(vec (drop (count spine) %)) segs)]

    (println "\nSPINE =" spine)
    (println "TAILS =" tails)

    (let [contexts (walk-spine data spine)]
      (println "\nCONTEXTS found:" (count contexts))
      (doseq [i (range (min 3 (count contexts)))]
        (println " ctx" i ":") (pprint (nth contexts i)))

      (into []
            (map (fn [ctx]
                   (reduce (fn [row [tail col k]]
                             (let [vals (eval-tail ctx tail)]
                               (println " EXTRACT" k "tail" tail "=>"
                                        (take 6 vals) (when (> (count vals) 6) '...))
                               (assoc row col (first vals))))
                           {}
                           (map vector tails cols keys-in))))
            contexts))))

