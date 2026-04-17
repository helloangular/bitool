(ns bitool.transform-routes-test
  (:require [bitool.ai.llm :as llm]
            [bitool.gil.compiler :as compiler]
            [bitool.transform :as transform]
            [bitool.transform.routes :as routes]
            [cheshire.core :as json]
            [clojure.walk :as walk]
            [clojure.test :refer :all]
            [reitit.ring :as ring]
            [ring.mock.request :as mock]))

(defn- test-handler []
  (ring/ring-handler
   (ring/router
    [(routes/transform-routes)])))

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

(deftest validate-transform-gil-rejects-unknown-stage-output
  (let [gil {:gil-version "1.0"
             :intent :build
             :graph-name "Bad Graph"
             :nodes [{:node-ref "src"
                      :type "endpoint"
                      :alias "request_input"
                      :config {:http_method "GET"
                               :route_path "/ai/bad"
                               :query_params [{:param_name "amount" :data_type "string"}]}}
                     {:node-ref "n1"
                      :type "function"
                      :alias "normalize_input"
                      :config {:fn_name "normalize_input"
                               :fn_params [{:param_name "amount_raw" :source_column "amount"}]
                               :fn_lets [{:variable "amount_num" :expression "tonumber(amount_raw)"}]
                               :fn_outputs [{:output_name "amount_num" :expression "amount_num"}]}}
                     {:node-ref "n2"
                      :type "function"
                      :alias "decision_logic"
                      :config {:fn_name "decision_logic"
                               :fn_params [{:param_name "amount_num" :source_column "normalize_input.total_amount"}]
                               :fn_lets []
                               :fn_outputs [{:output_name "double_amount" :expression "amount_num * 2"}]}}
                     {:node-ref "rb"
                      :type "response-builder"
                      :alias "response_builder"
                      :config {:template [{:output_key "doubleAmount" :source_column "double_amount"}]}}
                     {:node-ref "out" :type "output" :alias "Output"}]
             :edges [["src" "n1"] ["n1" "n2"] ["n2" "rb"] ["rb" "out"]]}
        validation (transform/validate-transform-gil gil)]
    (is (false? (:valid validation)))
    (is (some #(= :unknown-stage-output (:code %)) (:errors validation)))))

(deftest transform-from-nl-route-returns-preview-and-valid-gil
  (with-redefs [llm/call-llm (fn [& _]
                               (throw (ex-info "missing_api_key" {:error "missing_api_key"})))]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/transform/from-nl")
                           :params {:text "Create an API flow that normalizes customer name and amount, classifies VIP customers, and returns a discount and score."}
                           :session {}))
          body (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= true (:ok body)))
      (is (= true (get-in body [:data :validation :valid])))
      (is (= "Customer Decision Flow" (get-in body [:data :preview :graph_name])))
      (is (= "Endpoint -> Logic(normalize_input) -> Conditional(customer_segment) -> Logic(offer_logic) -> Response Builder -> Output"
             (get-in body [:data :preview :shape])))
      (is (= "fallback" (get-in body [:data :planner :mode])))
      (is (= "function" (get-in body [:data :gil :nodes 1 :type])))
      (is (= "conditionals" (get-in body [:data :gil :nodes 2 :type]))))))

(deftest transform-from-nl-route-repairs-invalid-llm-output-before-fallback
  (let [calls (atom 0)]
    (with-redefs [llm/call-llm (fn [& _]
                                 (case (swap! calls inc)
                                   1 {:graph_name "Broken Flow"
                                      :source {:kind "endpoint"
                                               :http_method "GET"
                                               :route_path "/ai/broken"}
                                      :stages []}
                                   {:graph_name "Repaired Flow"
                                    :goal "Normalize and route records"
                                    :source {:kind "endpoint"
                                             :http_method "GET"
                                             :route_path "/ai/repaired"
                                             :query_params ["name" "amount"]}
                                    :stages [{:kind "logic"
                                              :name "normalize_input"
                                              :inputs {"name_raw" "name"
                                                       "amount_raw" "amount"}
                                              :assignments [{:var "name_clean" :expr "trim(name_raw)"}
                                                            {:var "amount_num" :expr "tonumber(amount_raw)"}]
                                              :outputs {"name_clean" "name_clean"
                                                        "amount_num" "amount_num"}}
                                             {:kind "logic"
                                              :name "final_projection"
                                              :inputs {"name_clean" "normalize_input.name_clean"
                                                       "amount_num" "normalize_input.amount_num"}
                                              :assignments [{:var "label" :expr "concat(name_clean, ' processed')"}]
                                              :outputs {"label" "label"
                                                        "amount_num" "amount_num"}}]
                                    :response {:kind "response-builder"
                                               :template {"label" "label"
                                                          "amount" "amount_num"}}}))]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/transform/from-nl")
                             :params {:text "Create a repaired transform"}
                             :session {}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= true (get-in body [:data :validation :valid])))
        (is (= "llm_repair" (get-in body [:data :planner :mode])))
        (is (= true (get-in body [:data :planner :repair_attempted?])))
        (is (= "Repaired Flow" (get-in body [:data :preview :graph_name])))
        (is (= 2 @calls))))))

(deftest transform-from-nl-route-uses-expanded-mastering-fallback-template
  (with-redefs [llm/call-llm (fn [& _]
                               (throw (ex-info "missing_api_key" {:error "missing_api_key"})))]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/transform/from-nl")
                           :params {:text "Create a customer mastering flow with golden record stewardship and publish routing."}
                           :session {}))
          body (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "fallback" (get-in body [:data :planner :mode])))
      (is (= "Customer Mastering Flow" (get-in body [:data :preview :graph_name])))
      (is (= "quality_route" (get-in body [:data :gil :nodes 2 :alias]))))))

(deftest transform-apply-route-applies-normalized-gil-and-sets-session
  (with-redefs [llm/call-llm (fn [& _]
                               (throw (ex-info "missing_api_key" {:error "missing_api_key"})))
                compiler/apply-gil (fn [gil _session]
                                     {:graph-id 700
                                      :version 4
                                      :panel [{:id 1 :alias "Output"}]
                                      :graph gil})]
    (let [plan (transform/plan-from-text "Build an invoice review transform that routes missing PO numbers to exceptions and computes payment priority.")
          response ((test-handler)
                    (assoc (mock/request :post "/transform/apply")
                           :params {:gil (:gil plan)}
                           :session {:user "demo"}))
          body (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= true (:ok body)))
      (is (= 700 (get-in response [:session :gid])))
      (is (= 4 (get-in response [:session :ver])))
      (is (= "demo" (get-in response [:session :user])))
      (is (= 700 (get-in body [:data :result :graph-id])))
      (is (= "Invoice Review Flow" (get-in body [:data :gil :graph-name]))))))
