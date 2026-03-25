(ns bitool.connector-api-test
  (:require [bitool.connector.api :as api]
            [clj-http.client :as http]
            [clojure.test :refer :all]))

(deftest do-request-retries-transient-failures
  (let [calls (atom 0)]
    (with-redefs [http/request (fn [_]
                                 (swap! calls inc)
                                 (if (= 1 @calls)
                                   {:status 429 :headers {"retry-after" "0"} :body "{\"error\":\"slow down\"}"}
                                   {:status 200 :headers {"content-type" "application/json"} :body "{\"ok\":true}"}))]
      (let [resp (api/do-request {:method :get
                                  :url "https://api.example.com"
                                  :retry-policy {:max-retries 2 :base-backoff-ms 1}})]
        (is (= 2 @calls))
        (is (= 200 (:status resp)))
        (is (= true (get-in resp [:body :ok])))
        (is (= 1 (:retry-count resp)))))))

(deftest next-url+request-joins-base-url-and-endpoint-with-missing-slash
  (let [request (@#'bitool.connector.api/next-url+request
                 {:base-url "http://localhost:3001"
                  :endpoint "fleet/vehicles"
                  :state {}
                  :first? true
                  :pagination "none"
                  :query-builder {}
                  :body-builder {}
                  :out-key-map {}
                  :pagination-location :query})]
    (is (= "http://localhost:3001/fleet/vehicles" (:url request)))))
