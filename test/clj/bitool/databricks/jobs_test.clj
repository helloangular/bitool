(ns bitool.databricks.jobs-test
  (:require [bitool.databricks.jobs :as jobs]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.test :refer [deftest is testing]]))

(deftest trigger-job-normalizes-job-parameters
  (let [captured-body (atom nil)]
    (with-redefs [bitool.databricks.jobs/databricks-connection (fn [_]
                                                                 {:host "https://example.databricks.com"
                                                                  :token "token"})
                  http/post (fn [_ opts]
                              (reset! captured-body (json/parse-string (:body opts) true))
                              {:status 200
                               :body {:run_id 123}})]
      (jobs/trigger-job! 478
                         "1095954711115234"
                         {:graph_id 2435
                          :api_node_id "2"
                          :endpoint_configs_json "[{\"endpoint_name\":\"ap/invoices\"}]"
                          :bitool_callback_url nil}))
    (testing "it uses job_parameters with only string key/value pairs"
      (is (= 1095954711115234 (:job_id @captured-body)))
      (is (= {:graph_id "2435"
              :api_node_id "2"
              :endpoint_configs_json "[{\"endpoint_name\":\"ap/invoices\"}]"}
             (:job_parameters @captured-body)))
      (is (nil? (:notebook_params @captured-body))))))
