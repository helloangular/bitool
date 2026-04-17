(ns bitool.pipeline-routes-test
  (:require [bitool.pipeline.deploy :as deploy]
            [bitool.pipeline.intent :as intent]
            [bitool.pipeline.planner :as planner]
            [bitool.pipeline.preview :as preview]
            [bitool.pipeline.routes :as routes]
            [cheshire.core :as json]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [reitit.ring :as ring]
            [ring.mock.request :as mock]))

(defn- test-handler []
  (ring/ring-handler
   (ring/router
    [(routes/pipeline-routes)])))

(defn- response-body->map [response]
  (let [body (:body response)
        body-str (cond
                   (or (map? body) (vector? body)) body
                   (string? body) body
                   (instance? java.io.InputStream body) (slurp body)
                   :else (str body))]
    (walk/keywordize-keys
     (if (string? body-str)
       (json/parse-string body-str true)
       body-str))))

(deftest pipeline-deploy-route-applies-result-and-updates-session
  (let [spec {:pipeline-name "Orders Pipeline"
              :bronze-nodes []
              :silver-proposals []
              :gold-models []
              :ops {}}]
    (with-redefs [preview/generate-preview (fn [_]
                                            {:pipeline-name "Orders Pipeline"
                                             :target {:platform "databricks" :catalog "main"}
                                             :bronze {:endpoints []}
                                             :silver {:entities []}
                                             :gold {:models []}
                                             :assumptions []})
                  preview/preview-text (fn [_] "preview")
                  deploy/deploy-pipeline! (fn [_ opts]
                                            {:bronze {:graph_id 700 :graph_version 3 :api_node_id 9}
                                             :scheduler {:graph_version 4}
                                             :options opts})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/pipeline/deploy")
                             :params {:spec spec
                                      :connection_id "42"
                                      :publish_releases "true"
                                      :execute_releases "false"
                                      :attach_schedule "true"}
                             :session {:user "demo"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= true (:ok body)))
        (is (= 700 (get-in response [:session :gid])))
        (is (= 4 (get-in response [:session :ver])))
        (is (= 42 (get-in body [:data :result :options :connection_id])))
        (is (= "demo" (get-in body [:data :result :options :created_by])))))))

(deftest pipeline-deploy-route-can-plan-from-text-with-fallback-parser
  (let [intent-doc {:intent-type :pipeline
                    :pipeline-name "Text Pipeline"
                    :source {:system "samsara"}
                    :target {:platform "databricks"}
                    :bronze []
                    :gold []
                    :ops {}}
        spec {:pipeline-name "Text Pipeline"
              :bronze-nodes []
              :silver-proposals []
              :gold-models []
              :ops {}}]
    (with-redefs [intent/parse-intent (fn [_]
                                        (throw (ex-info "missing_api_key" {:error "missing_api_key"})))
                  intent/parse-intent-mock (fn [_] intent-doc)
                  planner/plan-pipeline (fn [_] spec)
                  preview/generate-preview (fn [_]
                                            {:pipeline-name "Text Pipeline"
                                             :target {:platform "databricks" :catalog "main"}
                                             :bronze {:endpoints []}
                                             :silver {:entities []}
                                             :gold {:models []}
                                             :assumptions []})
                  preview/preview-text (fn [_] "preview")
                  deploy/deploy-pipeline! (fn [_ _]
                                            {:bronze {:graph_id 701 :graph_version 2 :api_node_id 9}})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/pipeline/deploy")
                             :params {:text "Build a samsara pipeline"}
                             :session {}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= "Text Pipeline" (get-in body [:data :spec :pipeline-name])))
        (is (= "samsara" (get-in body [:data :intent :source :system])))
        (is (= 701 (get-in response [:session :gid])))))))
