(ns bitool.bigquery-test
  (:require [bitool.bigquery :as bigquery]
            [clojure.test :refer :all]))

(deftest service-account-assertion-validates-private-key-before-signing
  (with-redefs-fn {#'bitool.bigquery/pem->private-key (fn [_]
                                                        (throw (ex-info "pem parser should not run" {})))}
    (fn []
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"missing private_key"
                            (#'bitool.bigquery/service-account-assertion
                             {:credentials {:client_email "svc@example.com"}}))))))

(deftest access-token-reuses-cached-oauth-token
  (let [oauth-calls (atom 0)]
    (reset! @#'bitool.bigquery/access-token-cache {})
    (with-redefs-fn {#'bitool.bigquery/service-account-assertion (fn [_]
                                                                   {:token-uri "https://oauth2.googleapis.com/token"
                                                                    :assertion "jwt"})
                     #'clj-http.client/post (fn [_ _]
                                              (swap! oauth-calls inc)
                                              {:status 200
                                               :body "{\"access_token\":\"cached-token\",\"expires_in\":3600}"})}
      (fn []
        (is (= "cached-token"
               (#'bitool.bigquery/access-token! {:project-id "demo-project"
                                                 :dataset "analytics"
                                                 :token "{}"})))
        (is (= "cached-token"
               (#'bitool.bigquery/access-token! {:project-id "demo-project"
                                                 :dataset "analytics"
                                                 :token "{}"})))
        (is (= 1 @oauth-calls))))))

(deftest execute-query-loop-short-circuits-dml-pagination
  (let [poll-calls (atom 0)]
    (with-redefs-fn {#'bitool.bigquery/get-json! (fn [_ _ _]
                                                   (swap! poll-calls inc)
                                                   {:jobReference {:jobId "job-1" :location "US"}
                                                    :jobComplete true
                                                    :numDmlAffectedRows "9"
                                                    :pageToken "still-more"})}
      (fn []
        (let [result (#'bitool.bigquery/execute-query-loop! {:project-id "demo-project"
                                                             :location "US"}
                                                            {}
                                                            {:jobReference {:jobId "job-1" :location "US"}
                                                             :jobComplete false})]
          (is (= 1 @poll-calls))
          (is (= [] (:rows result)))
          (is (= 9 (:update-count result))))))))

(deftest dry-run-sql-exposes-estimated-bytes
  (with-redefs-fn {#'bitool.bigquery/access-token! (fn [_] "token")
                   #'bitool.bigquery/post-json! (fn [_ _ _]
                                                  {:jobReference {:jobId "job-1"}
                                                   :totalBytesProcessed "2048"})}
    (fn []
      (let [result (bigquery/dry-run-sql! {:project-id "demo-project"
                                           :dataset "analytics"
                                           :token "{}"}
                                          "SELECT 1")]
        (is (= 2048 (:estimated_bytes_processed result)))))))
