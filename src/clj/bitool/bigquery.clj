(ns bitool.bigquery
  (:require [cheshire.core :as json]
            [clj-http.client :as client]
            [clojure.string :as string])
  (:import [java.nio.charset StandardCharsets]
           [java.security KeyFactory Signature]
           [java.security.spec PKCS8EncodedKeySpec]
           [java.time Instant]
           [java.util Base64]))

(def ^:private oauth-scope
  "https://www.googleapis.com/auth/bigquery")

(def ^:private default-token-uri
  "https://oauth2.googleapis.com/token")

(def ^:private default-api-root
  "https://bigquery.googleapis.com/bigquery/v2")

(def ^:private poll-sleep-ms 1000)
(def ^:private default-timeout-ms 60000)
(def ^:private max-results 1000)
(def ^:private token-refresh-buffer-ms 300000)
(def ^:private token-cache-ttl-ms 3000000)

(def ^:private access-token-cache (atom {}))

(declare row->map)

(defn- non-blank-str
  [value]
  (let [value (some-> value str string/trim)]
    (when (seq value) value)))

(defn- base64url-encode
  [^bytes data]
  (.encodeToString (-> (Base64/getUrlEncoder) (.withoutPadding)) data))

(defn- parse-json-string
  [raw]
  (cond
    (map? raw) raw
    (string? raw) (json/parse-string raw true)
    :else (throw (ex-info "Expected JSON object string for BigQuery credentials"
                          {:value_type (type raw)}))))

(defn- normalize-db-spec
  [db-spec]
  ;; BigQuery connections are stored in the generic connection table, so `host`
  ;; carries the project id and `dbname` carries the dataset name.
  (let [project-id (or (non-blank-str (:project-id db-spec))
                       (non-blank-str (:project_id db-spec))
                       (non-blank-str (:host db-spec)))
        dataset    (or (non-blank-str (:dataset db-spec))
                       (non-blank-str (:dbname db-spec)))
        location   (or (non-blank-str (:location db-spec))
                       (non-blank-str (:schema db-spec))
                       "US")
        credentials-raw (or (:service-account-json db-spec)
                            (:service_account_json db-spec)
                            (:token db-spec)
                            (:password db-spec))
        credentials (parse-json-string credentials-raw)]
    (when-not project-id
      (throw (ex-info "BigQuery project_id is required"
                      {:field :project_id})))
    (when-not dataset
      (throw (ex-info "BigQuery dataset is required"
                      {:field :dataset})))
    (when-not (map? credentials)
      (throw (ex-info "BigQuery service account JSON is required"
                      {:field :token})))
    {:project-id project-id
     :dataset dataset
     :location location
     :credentials credentials}))

(defn- pem->private-key
  [pem]
  (let [sanitized (-> (or pem "")
                      (string/replace "-----BEGIN PRIVATE KEY-----" "")
                      (string/replace "-----END PRIVATE KEY-----" "")
                      (string/replace #"\s+" ""))
        encoded   (.decode (Base64/getDecoder) sanitized)
        key-spec  (PKCS8EncodedKeySpec. encoded)]
    (.generatePrivate (KeyFactory/getInstance "RSA") key-spec)))

(defn- sign-rs256
  [private-key signing-input]
  (let [signature (Signature/getInstance "SHA256withRSA")
        data      (.getBytes signing-input StandardCharsets/UTF_8)]
    (.initSign signature private-key)
    (.update signature data)
    (.sign signature)))

(defn- service-account-assertion
  [{:keys [credentials]}]
  (let [client-email (non-blank-str (:client_email credentials))
        private-key  (non-blank-str (:private_key credentials))
        token-uri    (or (non-blank-str (:token_uri credentials)) default-token-uri)
        _            (when-not client-email
                       (throw (ex-info "BigQuery service account JSON is missing client_email"
                                       {:field :client_email})))
        _            (when-not private-key
                       (throw (ex-info "BigQuery service account JSON is missing private_key"
                                       {:field :private_key})))
        now          (.getEpochSecond (Instant/now))
        header       {:alg "RS256" :typ "JWT"}
        payload      {:iss client-email
                      :scope oauth-scope
                      :aud token-uri
                      :iat now
                      :exp (+ now 3600)}
        header-json  (json/generate-string header)
        payload-json (json/generate-string payload)
        signing-input (str (base64url-encode (.getBytes header-json StandardCharsets/UTF_8))
                           "."
                           (base64url-encode (.getBytes payload-json StandardCharsets/UTF_8)))
        signature    (sign-rs256 (pem->private-key private-key) signing-input)]
    {:token-uri token-uri
     :assertion (str signing-input "." (base64url-encode signature))}))

(defn- parse-response-body
  [response]
  (cond
    (map? (:body response)) (:body response)
    (string? (:body response)) (json/parse-string (:body response) true)
    :else {}))

(defn- response-error-message
  [body]
  (or (get-in body [:error :message])
      (some->> (get-in body [:error :errors])
               (map :message)
               (remove string/blank?)
               seq
               (string/join "; "))
      (:message body)
      "BigQuery request failed"))

(defn- throw-on-error!
  [response]
  (let [status (:status response)
        body   (parse-response-body response)]
    (when (>= (long (or status 500)) 400)
      (throw (ex-info (response-error-message body)
                      {:status status
                       :response body})))
    body))

(defn- parse-long-safe
  [value]
  (when (some? value)
    (Long/parseLong (str value))))

(defn- access-token-cache-key
  [{:keys [project-id dataset location credentials]}]
  {:project-id project-id
   :dataset dataset
   :location location
   :credentials-hash (hash (select-keys credentials
                                        [:client_email
                                         :private_key_id
                                         :private_key
                                         :token_uri]))})

(defn- cached-access-token
  [cache-key]
  (let [{:keys [access-token expires-at-ms]} (get @access-token-cache cache-key)
        now-ms (System/currentTimeMillis)]
    (when (and access-token
               expires-at-ms
               (> expires-at-ms now-ms))
      access-token)))

(defn- cache-access-token!
  [cache-key access-token expires-in]
  (let [now-ms        (System/currentTimeMillis)
        expires-in-ms (some-> expires-in parse-long-safe (* 1000))
        ttl-ms        (if expires-in-ms
                        (max 60000
                             (min token-cache-ttl-ms
                                  (max 0 (- expires-in-ms token-refresh-buffer-ms))))
                        token-cache-ttl-ms)
        expires-at-ms (+ now-ms ttl-ms)]
    (swap! access-token-cache assoc cache-key {:access-token access-token
                                               :expires-at-ms expires-at-ms})
    access-token))

(defn- access-token!
  [db-spec]
  (let [db-spec    (normalize-db-spec db-spec)
        cache-key  (access-token-cache-key db-spec)]
    (or (cached-access-token cache-key)
        (locking access-token-cache
          (or (cached-access-token cache-key)
              (let [{:keys [token-uri assertion]} (service-account-assertion db-spec)
                    response (client/post token-uri
                                          {:throw-exceptions false
                                           :content-type :x-www-form-urlencoded
                                           :form-params {"grant_type" "urn:ietf:params:oauth:grant-type:jwt-bearer"
                                                         "assertion" assertion}})
                    body     (throw-on-error! response)
                    access-token (:access_token body)]
                (when-not access-token
                  (throw (ex-info "BigQuery OAuth token response did not include access_token" {})))
                (cache-access-token! cache-key access-token (:expires_in body))))))))

(defn- query-endpoint
  [{:keys [project-id]}]
  (str default-api-root "/projects/" project-id "/queries"))

(defn- query-results-endpoint
  [{:keys [project-id]} job-id]
  (str default-api-root "/projects/" project-id "/queries/" job-id))

(defn- request-headers
  [access-token]
  {"Authorization" (str "Bearer " access-token)
   "Content-Type" "application/json"})

(defn- post-json!
  [url headers payload]
  (-> (client/post url
                   {:throw-exceptions false
                    :headers headers
                    :body (json/generate-string payload)})
      throw-on-error!))

(defn- get-json!
  [url headers query-params]
  (-> (client/get url
                  {:throw-exceptions false
                   :headers headers
                   :query-params query-params})
      throw-on-error!))

(defn- field-value
  [field cell]
  (let [value (:v cell)]
    (cond
      (nil? value) nil
      (and (= "REPEATED" (:mode field)) (vector? value))
      (mapv #(field-value (assoc field :mode nil) %) value)
      (and (= "RECORD" (:type field)) (vector? (:f value)))
      (row->map (:fields field) value)
      :else value)))

(defn- row->map
  [fields row]
  (into {}
        (map (fn [field cell]
               [(:name field) (field-value field cell)])
             fields
             (:f row))))

(defn- extract-rows
  [body]
  (let [fields (vec (get-in body [:schema :fields]))]
    (mapv #(row->map fields %) (or (:rows body) []))))

(defn- dml-update-count
  [body]
  (parse-long-safe (:numDmlAffectedRows body)))

(defn- query-result
  [project-id job-id location page rows]
  {:rows rows
   :update-count (dml-update-count page)
   :job {:project_id project-id
         :job_id job-id
         :location location
         :job_complete (:jobComplete page)}
   :raw page})

(defn- execute-query-loop!
  [db-spec headers body]
  (let [project-id    (:project-id db-spec)
        job-id        (get-in body [:jobReference :jobId])
        location      (or (get-in body [:jobReference :location])
                          (:location db-spec))
        deadline-ms   (+ (System/currentTimeMillis) default-timeout-ms)]
    (loop [page body
           rows (if (dml-update-count body) [] (extract-rows body))]
      (cond
        (false? (:jobComplete page))
        (do
          (when (> (System/currentTimeMillis) deadline-ms)
            (throw (ex-info "BigQuery query timed out"
                            {:timeout_ms default-timeout-ms
                             :job_id job-id
                             :project_id project-id})))
          (Thread/sleep poll-sleep-ms)
          (recur (get-json! (query-results-endpoint db-spec job-id)
                            headers
                            {"location" location
                             "maxResults" max-results})
                 rows))

        (dml-update-count page)
        (query-result project-id job-id location page [])

        (seq (:pageToken page))
        (let [next-page (get-json! (query-results-endpoint db-spec job-id)
                                   headers
                                   {"location" location
                                    "pageToken" (:pageToken page)
                                    "maxResults" max-results})]
          (if (dml-update-count next-page)
            (query-result project-id job-id location next-page [])
            (recur next-page (into rows (extract-rows next-page)))))

        :else
        (query-result project-id job-id location page rows)))))

(defn dry-run-sql!
  [db-spec sql]
  (let [db-spec      (normalize-db-spec db-spec)
        access-token (access-token! db-spec)
        headers      (request-headers access-token)
        response     (post-json! (query-endpoint db-spec)
                                 headers
                                 {:query sql
                                  :useLegacySql false
                                  :dryRun true
                                  :location (:location db-spec)})]
    (cond-> response
      (:totalBytesProcessed response)
      (assoc :estimated_bytes_processed (parse-long-safe (:totalBytesProcessed response))))))

(defn execute-sql!
  [db-spec sql]
  (let [db-spec      (normalize-db-spec db-spec)
        access-token (access-token! db-spec)
        headers      (request-headers access-token)
        initial-body (post-json! (query-endpoint db-spec)
                                 headers
                                 {:query sql
                                  :useLegacySql false
                                  :location (:location db-spec)
                                  :timeoutMs 10000
                                  :maxResults max-results})]
    (execute-query-loop! db-spec headers initial-body)))

(defn test-connection!
  [db-spec]
  (execute-sql! db-spec "SELECT 1 AS ok")
  true)
