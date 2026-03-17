(ns bitool.connector.file
  (:require [cheshire.core :as json]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.logging :as log])
  (:import [java.math BigDecimal BigInteger RoundingMode]
           [java.nio.charset Charset]
           [java.net HttpURLConnection URL]
           [java.nio.file Files Path Paths]
           [java.security MessageDigest]))

(def ^:private encoding-aliases
  {"EBCDIC" "Cp037"
   "IBM037" "Cp037"
   "CP037" "Cp037"
   "IBM1047" "Cp1047"
   "CP1047" "Cp1047"})

(defn- sha256-hex
  [^bytes bytes]
  (let [digest (MessageDigest/getInstance "SHA-256")]
    (.update digest bytes)
    (format "%064x" (BigInteger. 1 (.digest digest)))))

(defn- path-bytes
  [path]
  (Files/readAllBytes (.toPath (io/file path))))

(defn- resolve-charset
  [encoding]
  (Charset/forName (or (get encoding-aliases (some-> encoding str string/upper-case))
                       (or encoding "UTF-8"))))

(defn- local-file-checksum
  [path]
  (sha256-hex (path-bytes path)))

(defn- parse-jsonl-lines
  [content]
  (->> (string/split-lines content)
       (map string/trim)
       (remove string/blank?)
       (mapv #(json/parse-string % true))))

(defn- csv-line->fields
  [line delimiter]
  (loop [chars (seq line)
         field ""
         fields []
         quoted? false]
    (if-let [ch (first chars)]
      (cond
        (= ch \")
        (if (and quoted? (= (second chars) \"))
          (recur (nnext chars) (str field \") fields quoted?)
          (recur (next chars) field fields (not quoted?)))

        (and (not quoted?) (= ch (first delimiter)))
        (recur (next chars) "" (conj fields field) quoted?)

        :else
        (recur (next chars) (str field ch) fields quoted?))
      (conj fields field))))

(defn- parse-csv-content
  [content {:keys [delimiter has_header] :or {delimiter "," has_header true}}]
  (let [lines   (->> (string/split-lines content)
                     (remove string/blank?)
                     vec)
        rows    (mapv #(csv-line->fields % delimiter) lines)
        headers (if has_header
                  (mapv keyword (first rows))
                  (mapv #(keyword (str "col_" (inc %))) (range (count (first rows)))))]
    (mapv #(zipmap headers %)
          (if has_header (rest rows) rows))))

(defn- pic-digits-and-scale
  [pic]
  (let [normalized (-> (or pic "")
                       string/upper-case
                       (string/replace #"COMP-3|COMP-?3|COMP|BINARY" "")
                       string/trim)
        [int-part frac-part] (string/split normalized #"V" 2)
        part-digits (fn [segment]
                      (reduce + 0
                              (map (fn [[_ chars repeat]]
                                     (let [token (or chars "")
                                           repeat (some-> repeat Integer/parseInt)]
                                       (or repeat (count token))))
                                   (re-seq #"([X9SZ+\-]+)(?:\((\d+)\))?" (or segment "")))))
        digits (+ (part-digits int-part) (part-digits frac-part))
        scale  (part-digits frac-part)]
    {:digits digits
     :scale scale}))

(defn- copybook-field-type
  [pic usage]
  (let [pic   (string/upper-case (or pic ""))
        usage (string/upper-case (or usage ""))]
    (cond
      (re-find #"COMP-3|COMP-?3" usage) "packed_decimal"
      (re-find #"COMP|BINARY" usage) "binary_number"
      (re-find #"X" pic) "string"
      :else "number")))

(defn- copybook-field-length
  [pic usage]
  (let [{:keys [digits]} (pic-digits-and-scale pic)
        usage (string/upper-case (or usage ""))]
    (cond
      (re-find #"COMP-3|COMP-?3" usage) (long (Math/ceil (/ (double (inc digits)) 2.0)))
      (re-find #"COMP|BINARY" usage) (cond
                                       (<= digits 4) 2
                                       (<= digits 9) 4
                                       :else 8)
      :else (max 1 digits))))

(defn- occurs-count
  [line]
  (some-> (re-find #"(?i)\bOCCURS\s+(\d+)\s+TIMES\b" line) second Integer/parseInt))

(defn- parse-copybook-entry
  [line]
  (let [line      (string/trim (or line ""))
        [_ level name] (re-find #"(?i)^(\d+)\s+([A-Z0-9-]+)" line)
        pic       (some-> (re-find #"(?i)\bPIC\s+([SX9V0-9\(\)+\-]+)" line) second)
        usage     (some-> (re-find #"(?i)\b(COMP-3|COMP-?3|COMP|BINARY)\b" line) second)
        redefines (some-> (re-find #"(?i)\bREDEFINES\s+([A-Z0-9-]+)\b" line) second)
        occurs    (or (occurs-count line) 1)]
    (when (and level name pic)
      {:level (Integer/parseInt level)
       :name (string/lower-case (string/replace name #"-" "_"))
       :pic (string/upper-case pic)
       :usage (some-> usage string/upper-case)
       :redefines (some-> redefines string/lower-case (string/replace #"-" "_"))
       :occurs occurs
       :type (copybook-field-type pic usage)
       :length (copybook-field-length pic usage)
       :scale (:scale (pic-digits-and-scale pic))})))

(defn- expand-occurs-entry
  [entry start-offset]
  (let [occurs (:occurs entry)
        base   (dissoc entry :occurs :level :pic :usage)]
    (mapv (fn [idx]
            (assoc base
                   :name (if (> occurs 1)
                           (str (:name entry) "_" (inc idx))
                           (:name entry))
                   :start (+ start-offset (* idx (:length entry)))))
          (range occurs))))

(defn parse-copybook
  [copybook]
  (loop [entries (->> (string/split-lines (or copybook ""))
                      (map parse-copybook-entry)
                      (remove nil?))
         offset 1
         specs []]
    (if-let [entry (first entries)]
      (let [skip? (some? (:redefines entry))
            expanded (if skip?
                       []
                       (expand-occurs-entry entry offset))
            next-offset (if skip?
                          offset
                          (+ offset (* (:length entry) (:occurs entry))))]
        (recur (next entries) next-offset (into specs expanded)))
      (vec specs))))

(defn- packed-decimal->number
  [^bytes bytes scale]
  (let [hex       (->> bytes
                       (map #(format "%02x" (bit-and 0xFF %)))
                       (apply str)
                       string/upper-case)
        sign      (last hex)
        negative? (= sign \D)
        digits    (subs hex 0 (dec (count hex)))
        unscaled  (bigint (if (seq digits) digits "0"))
        signed    (if negative? (- unscaled) unscaled)]
    (if (pos? (long scale))
      (.setScale (BigDecimal. signed) (int scale) RoundingMode/UNNECESSARY)
      signed)))

(defn- binary-bytes->long
  [^bytes bytes]
  (reduce (fn [acc b]
            (+ (bit-shift-left acc 8) (bit-and 0xFF b)))
          0
          bytes))

(defn- display-number->value
  [text scale]
  (when (seq text)
    (let [normalized (-> text
                         string/trim
                         (string/replace #"," ""))]
      (if (pos? (long scale))
        (let [negative? (string/starts-with? normalized "-")
              digits    (string/replace normalized #"^[+-]" "")
              digits    (if (> (count digits) scale)
                          digits
                          (str (apply str (repeat (- scale (count digits) -1) \0)) digits))
              split-at  (- (count digits) scale)
              numeric   (str (subs digits 0 split-at) "." (subs digits split-at))
              numeric   (if negative? (str "-" numeric) numeric)]
          (BigDecimal. numeric))
        (Long/parseLong normalized)))))

(defn- slice-bytes
  [^bytes bytes start length]
  (let [safe-start (max 0 (dec (int (or start 1))))
        safe-end   (min (alength bytes) (+ safe-start (max 0 (int length))))]
    (java.util.Arrays/copyOfRange bytes safe-start safe-end)))

(defn- parse-fixed-width-record
  [^bytes record-bytes field-specs charset]
  (reduce (fn [row {:keys [name start length type scale]}]
            (let [chunk-bytes (slice-bytes record-bytes start length)
                  text        (-> (String. chunk-bytes charset) string/trim)
                  coerced     (case type
                                "packed_decimal" (when (some #(not= 0 %) chunk-bytes)
                                                   (packed-decimal->number chunk-bytes (or scale 0)))
                                "binary_number" (when (some #(not= 0 %) chunk-bytes)
                                                  (binary-bytes->long chunk-bytes))
                                "number" (when (seq text)
                                           (try
                                             (display-number->value text (or scale 0))
                                             (catch Exception e
                                               (throw (ex-info "Invalid fixed-width number"
                                                               {:field name
                                                                :value text}
                                                               e)))))
                                text)]
              (assoc row (keyword name) coerced)))
          {}
          field-specs))

(defn- split-fixed-width-records
  [^bytes bytes field-specs]
  (let [record-length (->> field-specs
                           (map (fn [{:keys [start length]}]
                                  (+ (max 0 (dec (int (or start 1)))) (int length))))
                           (reduce max 0))
        has-newlines? (some #{10 13} (seq bytes))]
    (cond
      has-newlines?
      (->> (String. bytes (Charset/forName "ISO-8859-1"))
           string/split-lines
           (remove string/blank?)
           (mapv #(.getBytes ^String % "ISO-8859-1")))

      (pos? record-length)
      (->> (range 0 (alength bytes) record-length)
           (map (fn [offset]
                  (java.util.Arrays/copyOfRange bytes
                                               offset
                                               (min (alength bytes) (+ offset record-length)))))
           (remove #(zero? (alength ^bytes %)))
           vec)

      :else [])))

(defn- parse-fixed-width-content
  [^bytes bytes {:keys [field_specs copybook encoding]}]
  (let [field-specs (if (seq field_specs) field_specs (parse-copybook copybook))
        charset     (resolve-charset encoding)]
    (when-not (seq field-specs)
      (throw (ex-info "fixed_width format requires field_specs or copybook"
                      {:failure_class "config_error"})))
    (->> (split-fixed-width-records bytes field-specs)
         (mapv (fn [line]
                 (try
                   (parse-fixed-width-record line field-specs charset)
                   (catch Exception e
                     {:_record (String. ^bytes line charset)
                      :_bitool_parse_error (.getMessage e)})))))))

(defn- remote-http-bytes
  [url headers]
  (let [^HttpURLConnection conn (.openConnection (URL. url))]
    (.setRequestMethod conn "GET")
    (doseq [[k v] headers]
      (.setRequestProperty conn (str k) (str v)))
    (with-open [stream (.getInputStream conn)]
      (let [bytes (.readAllBytes stream)]
        (.disconnect conn)
        bytes))))

(defn- transport-read-fn
  [source-node file-config]
  (or (:transport_read_fn file-config)
      (:transport-read-fn file-config)
      (:transport_read_fn source-node)
      (:transport-read-fn source-node)))

(defn- resolve-remote-url
  [source-node file-config path]
  (let [options (merge (:options source-node) (:options file-config))
        signed-url-map (or (:signed_url_map options) (:signed-url-map options))
        base-url (or (:remote_base_url options) (:remote-base-url options))]
    (cond
      (re-find #"^https?://" (str path)) (str path)
      (map? signed-url-map) (get signed-url-map path)
      (and base-url (seq (str path))) (str (string/replace (str base-url) #"/+$" "") "/" (string/replace (str path) #"^/+" ""))
      :else nil)))

(defn transport-bytes
  [source-node file-config path]
  (let [transport (some-> (:transport file-config) string/lower-case)
        options   (merge (:options source-node) (:options file-config))
        read-fn   (transport-read-fn source-node file-config)]
    (case transport
      "local" (path-bytes path)
      ("s3" "azure_blob" "sftp")
      (cond
        read-fn
        (let [result (read-fn {:source-node source-node
                               :file-config file-config
                               :path path})]
          (cond
            (bytes? result) result
            (map? result) (:bytes result)
            :else (throw (ex-info "transport_read_fn must return bytes or {:bytes ...}"
                                  {:transport transport
                                   :path path}))))

        (resolve-remote-url source-node file-config path)
        (remote-http-bytes (resolve-remote-url source-node file-config path)
                           (or (:headers options) {}))

        :else
        (throw (ex-info "Remote file transport requires transport_read_fn or signed URL configuration"
                        {:transport transport
                         :path path
                         :failure_class "unsupported"})))
      (throw (ex-info "Unsupported file transport"
                      {:transport transport
                       :failure_class "unsupported"})))))

(defn file-checksum
  [source-node file-config path]
  (sha256-hex (transport-bytes source-node file-config path)))

(defn- parse-file-content
  [source-node path file-config]
  (let [bytes   (transport-bytes source-node file-config path)
        charset (resolve-charset (:encoding file-config))
        content (String. bytes charset)]
    (case (some-> (:format file-config) string/lower-case)
      "jsonl" (parse-jsonl-lines content)
      "csv" (parse-csv-content content file-config)
      "fixed_width" (parse-fixed-width-content bytes file-config)
      "parquet" (throw (ex-info "Parquet ingestion requires a warehouse-native or library-backed parser"
                                {:format "parquet"
                                 :failure_class "unsupported"}))
      (throw (ex-info "Unsupported file format"
                      {:format (:format file-config)
                       :failure_class "config_error"})))))

(defn- resolve-local-paths
  [{:keys [base-path]} {:keys [paths]}]
  (mapv (fn [path]
          (let [path-file (io/file path)]
            (str (.normalize (.toPath (if (.isAbsolute path-file)
                                        path-file
                                        (io/file (or base-path "") path)))))))
        paths))

(defn- resolve-paths
  [source-node file-config]
  (case (some-> (:transport file-config) string/lower-case)
    "local" (resolve-local-paths source-node file-config)
    ("s3" "sftp" "azure_blob") (vec (:paths file-config))
    (throw (ex-info "Unsupported file transport"
                    {:transport (:transport file-config)
                     :failure_class "unsupported"}))))

(defn fetch-files-async
  [{:keys [source-node file-config]}]
  (let [pages-ch  (async/chan 500)
        errors-ch (async/chan 10)
        stop?     (atom false)
        cursor*   (atom {})]
    (async/thread
      (try
        (doseq [[idx path] (map-indexed vector (resolve-paths source-node file-config))]
          (when @stop?
            (throw (ex-info "File scan cancelled" {:stop? true})))
          (let [checksum (file-checksum source-node file-config path)
                records  (parse-file-content source-node path file-config)]
            (swap! cursor* assoc path checksum)
            (async/>!! pages-ch {:body records
                                 :page (inc idx)
                                 :state {:cursor (json/generate-string @cursor*)
                                         :path path
                                         :checksum checksum}
                                 :response {:status 200}})))
        (async/>!! pages-ch {:stop-reason :eof :state nil :http-status 200})
        (catch Throwable t
          (when-not (:stop? (ex-data t))
            (async/>!! errors-ch {:type :file-error :error t}))
          (async/>!! pages-ch {:stop-reason (if (:stop? (ex-data t)) :cancelled :error)
                               :state nil
                               :http-status nil}))
        (finally
          (async/close! pages-ch)
          (async/close! errors-ch))))
    {:pages pages-ch
     :errors errors-ch
     :cancel (fn [] (reset! stop? true))}))
