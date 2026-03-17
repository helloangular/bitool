(ns bitool.databricks.jobs
  (:require [bitool.db :as db]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.string :as string]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql :as sql]))

(defn- normalize-workspace-url [host]
  (let [host (string/trim (str host))]
    (cond
      (string/blank? host) nil
      (re-find #"^https?://" host) host
      :else (str "https://" host))))

(defn- databricks-connection [conn-id]
  (sql/get-by-id db/ds :connection conn-id {:builder-fn rs/as-unqualified-lower-maps}))

(defn- parse-job-id [job-id]
  (when-let [job-id (some-> job-id str string/trim not-empty)]
    (try
      (Long/parseLong job-id)
      (catch Exception _
        job-id))))

(defn- run-now-url [connection]
  (when-let [workspace-url (normalize-workspace-url (:host connection))]
    (str workspace-url "/api/2.1/jobs/run-now")))

(defn- runs-get-url [connection]
  (when-let [workspace-url (normalize-workspace-url (:host connection))]
    (str workspace-url "/api/2.1/jobs/runs/get")))

(defn- run-now-body [job-id params]
  (cond-> {:job_id (parse-job-id job-id)}
    (seq params) (assoc :job_parameters params)))

(defn trigger-job!
  [conn-id job-id params]
  (let [job-id     (some-> job-id str string/trim not-empty)
        connection (databricks-connection conn-id)
        token      (or (:token connection) (:password connection))
        url        (run-now-url connection)]
    (when-not job-id
      (throw (ex-info "Databricks Jobs API trigger requires a non-blank job id"
                      {:connection_id conn-id})))
    (when (string/blank? token)
      (throw (ex-info "Databricks connection is missing token for Jobs API trigger"
                      {:connection_id conn-id})))
    (when-not url
      (throw (ex-info "Databricks connection is missing host for Jobs API trigger"
                      {:connection_id conn-id})))
    (let [response (http/post url {:headers {"Authorization" (str "Bearer " token)
                                             "Content-Type" "application/json"}
                                   :body (json/generate-string (run-now-body job-id params))
                                   :accept :json
                                   :content-type :json
                                   :as :json
                                   :throw-exceptions false})]
      (if (<= 200 (:status response) 299)
        {:job_id job-id
         :http_status (:status response)
         :run_id (or (get-in response [:body :run_id])
                     (get-in response [:body "run_id"]))
         :number_in_job (or (get-in response [:body :number_in_job])
                            (get-in response [:body "number_in_job"]))}
        (throw (ex-info "Databricks Jobs API trigger failed"
                        {:connection_id conn-id
                         :job_id job-id
                         :http_status (:status response)
                         :response_body (:body response)}))))))

(defn get-run!
  [conn-id run-id]
  (let [run-id     (some-> run-id str string/trim not-empty)
        connection (databricks-connection conn-id)
        token      (or (:token connection) (:password connection))
        url        (runs-get-url connection)]
    (when-not run-id
      (throw (ex-info "Databricks Jobs API get-run requires a non-blank run id"
                      {:connection_id conn-id})))
    (when (string/blank? token)
      (throw (ex-info "Databricks connection is missing token for Jobs API get-run"
                      {:connection_id conn-id})))
    (when-not url
      (throw (ex-info "Databricks connection is missing host for Jobs API get-run"
                      {:connection_id conn-id})))
    (let [response (http/get url {:headers {"Authorization" (str "Bearer " token)}
                                  :query-params {:run_id run-id}
                                  :accept :json
                                  :as :json
                                  :throw-exceptions false})]
      (if (<= 200 (:status response) 299)
        {:run_id run-id
         :http_status (:status response)
         :body (:body response)}
        (throw (ex-info "Databricks Jobs API get-run failed"
                        {:connection_id conn-id
                         :run_id run-id
                         :http_status (:status response)
                         :response_body (:body response)}))))))
