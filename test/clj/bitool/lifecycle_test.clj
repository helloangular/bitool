(ns bitool.lifecycle-test
  (:require [bitool.handler :as handler]
            [bitool.lifecycle :as lifecycle]
            [bitool.middleware :as middleware]
            [cheshire.core :as json]
            [clojure.test :refer :all]))

(use-fixtures :each
  (fn [f]
    (lifecycle/clear-draining!)
    (try
      (f)
      (finally
        (lifecycle/clear-draining!)))))

(deftest readiness-gate-blocks-non-health-requests-while-draining
  (lifecycle/mark-draining!)
  (let [handler* (middleware/wrap-readiness-gate (fn [_] {:status 200 :body "ok"}))
        response (handler* {:uri "/pipeline/deploy"})]
    (is (= 503 (:status response)))
    (is (= false (:ready (json/parse-string (:body response) true))))))

(deftest readiness-gate-allows-health-requests-while-draining
  (lifecycle/mark-draining!)
  (let [handler* (middleware/wrap-readiness-gate (fn [_] {:status 200 :body "ok"}))
        response (handler* {:uri "/health"})]
    (is (= 200 (:status response)))))

(deftest ready-route-reflects-drain-state
  (let [ready-before (handler/ready-route {})
        _ (lifecycle/mark-draining!)
        ready-during (handler/ready-route {})
        during-body (json/parse-string (:body ready-during) true)]
    (is (= 200 (:status ready-before)))
    (is (= 503 (:status ready-during)))
    (is (= "draining" (:status during-body)))
    (is (= false (:ready during-body)))))
