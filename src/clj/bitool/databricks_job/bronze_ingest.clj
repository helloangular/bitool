(ns bitool.databricks-job.bronze-ingest
  "Standalone Bronze API ingestion for Databricks JAR task.
   No Postgres, no web server, no ops console.
   Reads API, writes to Delta tables via Spark/JDBC.

   Usage as Databricks JAR task:
     Main class: bitool.databricks_job.bronze_ingest
     Parameters: --endpoints fleet/vehicles,fleet/drivers
                 --source-system samsara
                 --base-url https://api.samsara.com
                 --token-env SAMSARA_API_TOKEN
                 --catalog main
                 --schema bronze
                 --explode-path data
                 --watermark-field updatedAtTime
                 --key-field id"
  (:require [clj-http.client :as http]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]])
  (:import [java.security MessageDigest]
           [java.time Instant LocalDate ZoneOffset]
           [java.util UUID HexFormat]
           [java.sql DriverManager Types])
  (:gen-class))

;; ---------------------------------------------------------------------------
;; Config
;; ---------------------------------------------------------------------------

(def cli-options
  [["-e" "--endpoints ENDPOINTS" "Comma-separated endpoint paths"
    :default "fleet/vehicles"
    :parse-fn #(string/split % #",")]
   ["-s" "--source-system SYSTEM" "Source system name"
    :default "samsara"]
   ["-b" "--base-url URL" "API base URL"
    :default "https://api.samsara.com"]
   ["-t" "--token-env VAR" "Env var name for API token"
    :default "SAMSARA_API_TOKEN"]
   ["-c" "--catalog CATALOG" "Unity Catalog name"
    :default "main"]
   ["-S" "--schema SCHEMA" "Bronze schema name"
    :default "bronze"]
   ["-x" "--explode-path PATH" "JSON path to records array"
    :default "data"]
   ["-w" "--watermark-field FIELD" "Watermark field name in records"
    :default "updatedAtTime"]
   ["-k" "--key-field FIELD" "Primary key field name"
    :default "id"]
   ["-p" "--page-size SIZE" "Page size"
    :default 100 :parse-fn #(Integer/parseInt %)]
   [nil "--callback-url URL" "Bitool callback URL (optional)"
    :default nil]
   ["-h" "--help"]])

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- sha256 [^String s]
  (let [digest (.digest (doto (MessageDigest/getInstance "SHA-256")
                          (.update (.getBytes s "UTF-8"))))]
    (.formatHex (HexFormat/of) digest)))

(defn- now-utc [] (Instant/now))
(defn- today [] (LocalDate/ofInstant (now-utc) ZoneOffset/UTC))
(defn- uuid [] (str (UUID/randomUUID)))

(defn- sanitize-table-name [s]
  (-> (str s)
      (string/replace #"[^a-zA-Z0-9_]" "_")
      (string/replace #"_+" "_")
      (string/replace #"^_|_$" "")
      string/lower-case))

(defn- table-name [catalog schema source-system endpoint]
  (str catalog "." schema "."
       (sanitize-table-name (str source-system "_" (string/replace endpoint #"/" "_") "_raw"))))

;; ---------------------------------------------------------------------------
;; API client
;; ---------------------------------------------------------------------------

(defn- fetch-page [base-url endpoint token cursor]
  (let [url (str base-url "/" endpoint)
        params (cond-> {}
                 cursor (assoc :after cursor))
        resp (http/get url {:headers {"Authorization" (str "Bearer " token)}
                            :query-params params
                            :as :json
                            :throw-exceptions true
                            :socket-timeout 30000
                            :connection-timeout 10000})]
    {:body (:body resp)
     :status (:status resp)}))

(defn- extract-records [body explode-path]
  (let [data (get body (keyword explode-path) (get body explode-path))]
    (cond
      (sequential? data) (vec data)
      (some? data) [data]
      :else [])))

;; ---------------------------------------------------------------------------
;; Bronze row builder
;; ---------------------------------------------------------------------------

(defn- build-bronze-rows [records {:keys [run-id batch-id source-system endpoint
                                          request-url page-number cursor status
                                          watermark-field key-field]}]
  (let [now-str (str (now-utc))
        partition-date (str (today))]
    (mapv (fn [record]
            (let [payload-json (json/generate-string record)]
              {"ingestion_id"    run-id
               "run_id"          run-id
               "batch_id"        batch-id
               "source_system"   source-system
               "endpoint_name"   endpoint
               "extracted_at_utc" now-str
               "ingested_at_utc" now-str
               "api_request_url" request-url
               "api_page_number" page-number
               "api_cursor"      cursor
               "http_status_code" status
               "record_hash"     (sha256 payload-json)
               "source_record_id" (str (or (get record (keyword key-field))
                                           (get record key-field)
                                           ""))
               "event_time_utc"  (str (or (get record (keyword watermark-field))
                                          (get record watermark-field)
                                          ""))
               "partition_date"  partition-date
               "load_date"       partition-date
               "payload_json"    payload-json}))
          records)))

;; ---------------------------------------------------------------------------
;; JDBC writer (Databricks)
;; ---------------------------------------------------------------------------

(defn- get-jdbc-url []
  (let [host     (or (System/getenv "DATABRICKS_HOST") "")
        port     (or (System/getenv "DATABRICKS_PORT") "443")
        http-path (or (System/getenv "DATABRICKS_HTTP_PATH") "")
        token    (or (System/getenv "DATABRICKS_TOKEN")
                     (System/getenv "DATABRICKS_PAT") "")]
    (format "jdbc:databricks://%s:%s/default;transportMode=http;ssl=1;AuthMech=3;httpPath=%s;UID=token;PWD=%s"
            host port http-path token)))

(defn- get-connection []
  (DriverManager/getConnection (get-jdbc-url)))

(defn- run-ddl! [sql-str]
  (let [conn (get-connection)]
    (try
      (.execute (.createStatement conn) sql-str)
      (finally (.close conn)))))

(defn- ensure-schema! [catalog schema]
  (run-ddl! (str "CREATE SCHEMA IF NOT EXISTS " catalog "." schema)))

(defn- ensure-table! [fq-table-name]
  (run-ddl! (str "CREATE TABLE IF NOT EXISTS " fq-table-name " ("
                  "ingestion_id STRING NOT NULL, "
                  "run_id STRING NOT NULL, "
                  "batch_id STRING NOT NULL, "
                  "source_system STRING NOT NULL, "
                  "endpoint_name STRING NOT NULL, "
                  "extracted_at_utc STRING NOT NULL, "
                  "ingested_at_utc STRING NOT NULL, "
                  "api_request_url STRING, "
                  "api_page_number INT, "
                  "api_cursor STRING, "
                  "http_status_code INT, "
                  "record_hash STRING, "
                  "source_record_id STRING, "
                  "event_time_utc STRING, "
                  "partition_date DATE, "
                  "load_date DATE, "
                  "payload_json STRING"
                  ") USING DELTA PARTITIONED BY (partition_date)")))

(defn- insert-rows! [fq-table-name rows]
  (when (seq rows)
    (let [conn (get-connection)
          cols ["ingestion_id" "run_id" "batch_id" "source_system" "endpoint_name"
                "extracted_at_utc" "ingested_at_utc" "api_request_url" "api_page_number"
                "api_cursor" "http_status_code" "record_hash" "source_record_id"
                "event_time_utc" "partition_date" "load_date" "payload_json"]
          placeholders (string/join ", " (repeat (count cols) "?"))
          col-names (string/join ", " cols)
          sql (str "INSERT INTO " fq-table-name " (" col-names ") VALUES (" placeholders ")")]
      (try
        (let [ps (.prepareStatement conn sql)]
          (doseq [row rows]
            (doseq [[i col] (map-indexed vector cols)]
              (let [v (get row col)]
                (if (nil? v)
                  (.setNull ps (inc i) Types/VARCHAR)
                  (.setObject ps (inc i) v))))
            (.addBatch ps))
          (.executeBatch ps))
        (finally (.close conn))))))

;; ---------------------------------------------------------------------------
;; Main ingestion loop
;; ---------------------------------------------------------------------------

(defn- ingest-endpoint! [{:keys [base-url token source-system endpoint catalog schema
                                  explode-path watermark-field key-field]}]
  (let [run-id (uuid)
        fq-table (table-name catalog schema source-system endpoint)
        _ (println (str "=== Ingesting " endpoint " -> " fq-table " ==="))
        _ (println (str "  Run ID: " run-id))]
    (try
      (ensure-schema! catalog schema)
      (ensure-table! fq-table)
      (loop [cursor nil page-num 0 total-rows 0]
        (let [page-num (inc page-num)
              batch-id (format "%s-b%06d" run-id page-num)
              {:keys [body status]} (fetch-page base-url endpoint token cursor)
              records (extract-records body explode-path)
              rows (build-bronze-rows records
                                      {:run-id run-id :batch-id batch-id
                                       :source-system source-system :endpoint endpoint
                                       :request-url (str base-url "/" endpoint)
                                       :page-number page-num :cursor cursor
                                       :status status :watermark-field watermark-field
                                       :key-field key-field})
              _ (insert-rows! fq-table rows)
              new-total (+ total-rows (count rows))
              pagination (or (get body :pagination) (get body "pagination") {})
              has-next (or (get pagination :hasNextPage) (get pagination "hasNextPage") false)
              next-cursor (or (get pagination :endCursor) (get pagination "endCursor"))]
          (println (str "  Page " page-num ": " (count records) " records, total=" new-total))
          (if (and has-next next-cursor (< page-num 100))
            (recur next-cursor page-num new-total)
            {:endpoint endpoint
             :table fq-table
             :run-id run-id
             :rows-written new-total
             :pages-fetched page-num
             :status "success"})))
      (catch Exception e
        (println (str "  FAILED: " (.getMessage e)))
        {:endpoint endpoint
         :table fq-table
         :run-id run-id
         :rows-written 0
         :status "failed"
         :error (.getMessage e)}))))

;; ---------------------------------------------------------------------------
;; Callback to Bitool (optional)
;; ---------------------------------------------------------------------------

(defn- send-callback! [callback-url results]
  (when (seq callback-url)
    (try
      (http/post (str callback-url "/ops/pipeline/bronzeCallback")
                 {:headers {"Content-Type" "application/json"}
                  :body (json/generate-string {:results results
                                               :completed_at_utc (str (now-utc))})
                  :socket-timeout 10000})
      (println "Callback sent to Bitool")
      (catch Exception e
        (println (str "Callback failed (non-fatal): " (.getMessage e)))))))

;; ---------------------------------------------------------------------------
;; Entry point
;; ---------------------------------------------------------------------------

(defn -main [& args]
  (let [{:keys [options errors summary]} (parse-opts args cli-options)]
    (when (:help options)
      (println "Bronze API Ingestion — Databricks JAR Task")
      (println summary)
      (System/exit 0))
    (when errors
      (doseq [e errors] (println "ERROR:" e))
      (System/exit 1))

    (let [{:keys [endpoints source-system base-url token-env catalog schema
                   explode-path watermark-field key-field callback-url]} options
          token (or (System/getenv token-env) "")]
      (when (empty? token)
        (println (str "WARNING: " token-env " not set — API calls will fail")))

      (println "Bronze Ingestion Starting")
      (println (str "  Source: " source-system " (" base-url ")"))
      (println (str "  Endpoints: " (string/join ", " endpoints)))
      (println (str "  Target: " catalog "." schema))
      (println)

      (let [results (mapv (fn [endpoint]
                            (ingest-endpoint! {:base-url base-url
                                               :token token
                                               :source-system source-system
                                               :endpoint endpoint
                                               :catalog catalog
                                               :schema schema
                                               :explode-path explode-path
                                               :watermark-field watermark-field
                                               :key-field key-field}))
                          endpoints)
            total-rows (reduce + 0 (map :rows-written results))
            failed (count (filter #(= "failed" (:status %)) results))]

        (println)
        (println "=== Summary ===")
        (doseq [r results]
          (println (str "  [" (if (= "success" (:status r)) "OK" "FAIL") "] "
                        (:endpoint r) ": " (:rows-written r) " rows -> " (:table r))))
        (println (str "Total: " total-rows " rows, " failed " failed"))

        (send-callback! callback-url results)

        (when (pos? failed)
          (System/exit 1))))))
