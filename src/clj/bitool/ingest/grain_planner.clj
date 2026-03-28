(ns bitool.ingest.grain-planner
  (:require [clojure.string :as string]))

(defn- normalize-token
  [value]
  (-> (str value)
      string/trim
      (string/replace #"^(data_items_|data_item_|data_|items_|item_)" "")
      (string/replace #"^:+" "")
      string/upper-case))

(defn- singularize-token
  [token]
  (let [token (str token)]
    (cond
      (and (string/ends-with? token "ies") (> (count token) 3))
      (str (subs token 0 (- (count token) 3)) "y")

      (and (string/ends-with? token "s") (> (count token) 3))
      (subs token 0 (dec (count token)))

      :else token)))

(defn- endpoint-entity-tokens
  [endpoint]
  (let [raw (str (:endpoint_name endpoint) " " (:endpoint_url endpoint))]
    (->> (string/split raw #"[^A-Za-z0-9]+")
         (map string/trim)
         (remove string/blank?)
         (filter #(>= (count %) 2))
         (mapcat (fn [token]
                   (let [token (string/lower-case token)]
                     [token (singularize-token token)])))
         distinct
         vec)))

(defn- normalize-records-path
  [path]
  (let [clean (-> (str path)
                  string/trim
                  (string/replace #"^\$\.?" ""))]
    (if (string/blank? clean)
      nil
      (let [segments (->> (string/split clean #"\.")
                          (remove string/blank?)
                          (map #(string/replace % #"\[\]$" "")))]
        (if (seq segments)
          (string/join "." segments)
          "$")))))

(defn- split-json-path
  [path]
  (let [clean (-> (str path)
                  string/trim
                  (string/replace #"^\$\.?" ""))]
    (->> (string/split clean #"\.")
         (remove string/blank?)
         (mapv (fn [segment]
                 {:raw segment
                  :array? (string/ends-with? segment "[]")
                  :name (string/replace segment #"\[\]$" "")})))))

(defn- field-tail
  [field]
  (let [segments (split-json-path (:path field))]
    (or (:name (last segments))
        (str (:column_name field)))))

(defn- field-tail-normalized
  [field]
  (normalize-token (field-tail field)))

(defn- timestamp-field?
  [field]
  (or (re-find #"TIMESTAMP|DATE" (str (:type field)))
      (re-find #"(?i)(date|time|timestamp|created|updated|modified|event)" (str (field-tail field)))
      (re-find #"(?i)(date|time|timestamp|created|updated|modified|event)" (str (:column_name field)))))

(defn- stable-key-score
  [field entity-tokens]
  (let [target (field-tail-normalized field)
        coverage (double (or (:sample_coverage field) 0.0))
        nullable? (not= false (:nullable field))
        base (cond
               (= target "ID") 1000
               (string/ends-with? target "_ID") 850
               (string/ends-with? target "_KEY") 650
               (string/ends-with? target "_NUM") 360
               (string/ends-with? target "_NUMBER") 360
               :else 0)
        entity-score (reduce (fn [score token]
                               (let [token (normalize-token token)]
                                 (cond
                                   (= target (str token "_ID")) (+ score 1250)
                                   (string/includes? target (str token "_")) (+ score 450)
                                   (string/includes? target token) (+ score 180)
                                   :else score)))
                             0
                             entity-tokens)]
    (+ base
       entity-score
       (if (not nullable?) 90 0)
       (if (>= coverage 0.99) 45 0)
       (if (>= coverage 0.90) 20 0))))

(defn- watermark-score
  [field]
  (when (timestamp-field? field)
    (let [target (field-tail-normalized field)
          coverage (double (or (:sample_coverage field) 0.0))]
      (+ (cond
           (re-find #"UPDATED|LAST_UPDATE|MODIFIED" target) 900
           (re-find #"EVENT(_|)TIME" target) 760
           (re-find #"CREATED" target) 540
           (re-find #"DATE|TIME|TIMESTAMP" target) 360
           :else 220)
         (if (>= coverage 0.99) 60 0)
         (if (>= coverage 0.90) 25 0)))))

(defn- candidate-entry
  [path]
  {:path path
   :direct-fields []
   :nested-array-paths #{}
   :depth (if (= "$" path) 0 (count (re-seq #"\." path)))})

(defn analyze-endpoint-structure
  [endpoint inferred-fields]
  (let [fields (vec (filter map? inferred-fields))
        candidates
        (reduce
         (fn [acc field]
           (let [segments (split-json-path (:path field))
                 array-indexes (keep-indexed (fn [idx segment] (when (:array? segment) idx)) segments)]
             (reduce
              (fn [acc idx]
                (let [path (let [names (->> (take (inc idx) segments)
                                            (map :name)
                                            (remove string/blank?))]
                             (if (seq names) (string/join "." names) "$"))
                      nested-idx (first (filter #(< idx %) array-indexes))
                      nested-path (when nested-idx
                                    (let [nested-names (->> (take (inc nested-idx) segments)
                                                            (map :name)
                                                            (remove string/blank?))]
                                      (if (seq nested-names) (string/join "." nested-names) "$")))
                      acc (update acc path #(or % (candidate-entry path)))]
                  (if nested-path
                    (update-in acc [path :nested-array-paths] conj nested-path)
                    (update-in acc [path :direct-fields] conj field))))
              acc
              array-indexes)))
         {}
         fields)]
    {:endpoint endpoint
     :fields fields
     :candidates (->> candidates vals (sort-by :path) vec)}))

(defn recommend-endpoint-grain
  [endpoint {:keys [candidates]} & [{:keys [detected-records-path configured-records-path]}]]
  (let [entity-tokens (endpoint-entity-tokens endpoint)
        configured-path (normalize-records-path configured-records-path)
        detected-path (normalize-records-path detected-records-path)
        ranked
        (->> candidates
             (map (fn [candidate]
                    (let [fields (:direct-fields candidate)
                          id-ranked (->> fields
                                         (map (fn [field]
                                                {:field field
                                                 :score (stable-key-score field entity-tokens)}))
                                         (filter #(pos? (:score %)))
                                         (sort-by (juxt (comp - :score) #(field-tail-normalized (:field %)))))
                          wm-ranked (->> fields
                                         (map (fn [field]
                                                {:field field
                                                 :score (or (watermark-score field) 0)}))
                                         (filter #(pos? (:score %)))
                                         (sort-by (comp - :score)))
                          base-score (+ 50
                                        (min 20 (* 2 (count fields)))
                                        (if (seq id-ranked) 25 0)
                                        (if (seq wm-ranked) 15 0)
                                        (if (= (:path candidate) configured-path) 25 0)
                                        (if (= (:path candidate) detected-path) 20 0)
                                        (if (> (count fields) 5) 10 0)
                                        (if (> (:depth candidate) 1) -15 0)
                                        (if (empty? fields) -30 0))]
                      (assoc candidate
                             :score (max 0 (min 100 base-score))
                             :id-ranked id-ranked
                             :wm-ranked wm-ranked))))
             (sort-by (juxt (comp - :score) :depth :path)))]
    (first ranked)))

(defn- entity-name-from-path
  [path]
  (-> path
      (string/split #"\.")
      last
      singularize-token
      string/lower-case))

(defn recommend-endpoint-config
  ([endpoint inferred-fields]
   (recommend-endpoint-config endpoint inferred-fields {}))
  ([endpoint inferred-fields {:keys [detected-records-path configured-records-path]}]
   (let [{:keys [candidates] :as facts} (analyze-endpoint-structure endpoint inferred-fields)
         best (recommend-endpoint-grain endpoint facts {:detected-records-path detected-records-path
                                                        :configured-records-path configured-records-path})]
     (when best
       (let [best-pk-field (some-> best :id-ranked first :field)
             best-pk (when best-pk-field [(field-tail best-pk-field)])
             best-wm-field (some-> best :wm-ranked first :field)
             child-candidates
             (->> candidates
                  (filter (fn [candidate]
                            (and (not= (:path candidate) (:path best))
                                 (string/starts-with? (str (:path candidate) ".")
                                                      (str (:path best) ".")))))
                  (sort-by :path)
                  (mapv (fn [candidate]
                          (let [child-id-ranked (->> (:direct-fields candidate)
                                                     (map (fn [field]
                                                            {:field field
                                                             :score (stable-key-score field (endpoint-entity-tokens endpoint))}))
                                                     (filter #(pos? (:score %)))
                                                     (sort-by (comp - :score)))
                                child-id (some-> child-id-ranked first :field field-tail)
                                parent-keys (vec (or best-pk []))
                                child-keys (vec (cond-> []
                                                  child-id (conj child-id)))
                                child-reasons (cond-> []
                                                (seq parent-keys) (conj (str "Carries parent key " (string/join " + " parent-keys)))
                                                child-id (conj (str "Detected child key " child-id))
                                                (seq (:direct-fields candidate)) (conj (str (:path candidate)
                                                                                           " contains "
                                                                                           (count (:direct-fields candidate))
                                                                                           " direct fields")))]
                            {:path (:path candidate)
                             :entityName (entity-name-from-path (:path candidate))
                             :idCandidates child-keys
                             :parentKeys parent-keys
                             :confidence (max 0 (min 100 (+ 40
                                                            (if child-id 25 0)
                                                            (if (seq parent-keys) 15 0)
                                                            (min 15 (count (:direct-fields candidate)))
                                                            (if (> (:depth candidate) (inc (:depth best))) -10 0))))
                             :reasons child-reasons}))))
             reasons (cond-> [(str (:path best) "[] is an array-backed candidate with "
                                   (count (:direct-fields best))
                                   " direct fields")]
                       best-pk-field (conj (str (field-tail best-pk-field) " is a stable ID candidate"))
                       best-wm-field (conj (str (:column_name best-wm-field) " is a timestamp with high coverage"))
                       (seq child-candidates) (conj (str (count child-candidates) " nested child array(s) detected")))]
         {:grain {:path (:path best)
                  :confidence (:score best)}
          :pk (when best-pk
                {:fields best-pk
                 :confidence (max 0 (min 100 (+ 15 (or (some-> best :id-ranked first :score) 0))))})
          :watermark (when best-wm-field
                       {:field (:column_name best-wm-field)
                        :confidence (max 0 (min 100 (+ 10 (or (some-> best :wm-ranked first :score) 0))))})
          :children child-candidates
          :reasons reasons
          :json_explode_rules [{:path (:path best)}]
          :primary_key_fields best-pk
          :watermark_column (:column_name best-wm-field)})))))
