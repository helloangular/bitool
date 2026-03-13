(ns bitool.execute-graph-test
  "Integration tests for execute-graph: full pipeline order and response shaping.
   db/getGraph is stubbed via with-redefs — no DB or HTTP server required."
  (:require [clojure.test :refer :all]
            [cheshire.core :as json]
            [bitool.db :as db]
            [bitool.endpoint :as ep]))

;; ---------------------------------------------------------------------------
;; Graph builder helpers
;; ---------------------------------------------------------------------------

(defn- node
  "Build a node map with btype and optional extra attrs."
  [btype & {:as extra}]
  {:na (merge {:name (str btype "-node") :btype btype :tcols {}} extra) :e {}})

(defn- graph
  "Build a minimal graph. nodes is a map of {id node-map}, edges is [[from to] ...]."
  [nodes edges]
  {:a {:name "test" :v 0 :id 99}
   :n (reduce (fn [acc [from to]]
                (assoc-in acc [from :e to] {}))
              nodes
              edges)})

(defn- ep-graph
  "Ep(2) → Rb(3) → O(1). Optional extra-nodes merged into :n before edge wiring."
  [rb-attrs & extra-nodes]
  (let [base {1 (node "O")
              2 (node "Ep" :route_path "/test" :http_method "GET")
              3 (node "Rb" rb-attrs)}
        nodes (apply merge base extra-nodes)]
    (graph nodes [[2 3] [3 1]])))

(defn- with-graph [g f]
  (with-redefs [db/getGraph (fn [_] g)]
    (f)))

(def empty-request {:headers {} :remote-addr "127.0.0.1"})

(defn- run
  "Call execute-graph; catch ExceptionInfo and convert to Ring response map
   the same way build-handler does, so tests can assert on :status uniformly."
  [graph-id node-id params request]
  (try
    (ep/execute-graph graph-id node-id params request)
    (catch clojure.lang.ExceptionInfo e
      (let [data (ex-data e)
            body (cond-> {:error (.getMessage e)}
                   (:errors data) (assoc :errors (:errors data)))]
        {:status  (or (:status data) 500)
         :headers (merge {"Content-Type" "application/json"}
                         (or (:headers data) {}))
         :body    (cheshire.core/generate-string body)}))))

;; ---------------------------------------------------------------------------
;; Baseline: no middleware, plain Rb response
;; ---------------------------------------------------------------------------

(deftest execute-graph-json-response
  (with-graph
    (ep-graph {:status_code "200" :response_type "json" :template [] :headers ""})
    #(let [resp (run 1 2 {:path {:id "42"} :query {} :body {}} empty-request)]
       (is (= 200 (:status resp)))
       (is (= "application/json" (get-in resp [:headers "Content-Type"])))
       (let [body (json/parse-string (:body resp))]
         (is (= "42" (get body "id")))))))

(deftest execute-graph-custom-status-code
  (with-graph
    (ep-graph {:status_code "201" :response_type "json" :template [] :headers ""})
    #(let [resp (run 1 2 {:path {} :query {} :body {}} empty-request)]
       (is (= 201 (:status resp))))))

(deftest execute-graph-template-mapping
  (with-graph
    (ep-graph {:status_code "200" :response_type "json"
               :template [{:output_key "userId" :source_column "id"}]
               :headers ""})
    #(let [resp (run 1 2 {:path {:id "7"} :query {} :body {}} empty-request)]
       (let [body (json/parse-string (:body resp))]
         (is (= {"userId" "7"} body))))))

(deftest execute-graph-template-fallback-to-flat-when-no-columns-match
  (with-graph
    (ep-graph {:status_code "200" :response_type "json"
               :template [{:output_key "id" :source_column "id"}]
               :headers ""})
    #(let [resp (run 1 2 {:path {:petId "1"} :query {} :body {}} empty-request)]
       (let [body (json/parse-string (:body resp))]
         (is (= {"petId" "1"} body))))))

(deftest execute-graph-xml-response
  (with-graph
    (ep-graph {:status_code "200" :response_type "xml" :template [] :headers ""})
    #(let [resp (run 1 2 {:path {:foo "bar"} :query {} :body {}} empty-request)]
       (is (= "application/xml" (get-in resp [:headers "Content-Type"])))
       (is (clojure.string/includes? (:body resp) "<foo>bar</foo>")))))

(deftest execute-graph-no-rb-echoes-params
  (let [g (graph {1 (node "O")
                  2 (node "Ep" :route_path "/test" :http_method "GET")}
                 [[2 1]])]
    (with-graph g
      #(let [resp (run 1 2 {:path {:x "1"} :query {} :body {}} empty-request)]
         (is (= 200 (:status resp)))
         (let [body (json/parse-string (:body resp))]
           (is (contains? body "echo")))))))

;; ---------------------------------------------------------------------------
;; Auth (Au) node
;; ---------------------------------------------------------------------------

(defn- graph-with-au [au-attrs rb-attrs]
  (let [nodes {1 (node "O")
               2 (node "Ep" :route_path "/test" :http_method "GET")
               4 (node "Au" au-attrs)
               3 (node "Rb" rb-attrs)}]
    (graph nodes [[2 4] [4 3] [3 1]])))

(deftest execute-graph-bearer-auth-passes
  (with-graph
    (graph-with-au {:auth_type "bearer" :token_header "authorization" :secret "tok123"}
                   {:status_code "200" :response_type "json" :template [] :headers ""})
    #(let [req  (assoc-in empty-request [:headers "authorization"] "Bearer tok123")
           resp (run 1 2 {:path {} :query {} :body {}} req)]
       (is (= 200 (:status resp))))))

(deftest execute-graph-bearer-auth-fails-401
  (with-graph
    (graph-with-au {:auth_type "bearer" :token_header "authorization" :secret "tok123"}
                   {:status_code "200" :response_type "json" :template [] :headers ""})
    #(let [req  (assoc-in empty-request [:headers "authorization"] "Bearer wrong")
           resp (run 1 2 {:path {} :query {} :body {}} req)]
       (is (= 401 (:status resp))))))

(deftest execute-graph-api-key-auth-passes
  (with-graph
    (graph-with-au {:auth_type "api-key" :token_header "x-api-key" :secret "key-abc"}
                   {:status_code "200" :response_type "json" :template [] :headers ""})
    #(let [req  (assoc-in empty-request [:headers "x-api-key"] "key-abc")
           resp (run 1 2 {:path {} :query {} :body {}} req)]
       (is (= 200 (:status resp))))))

(deftest execute-graph-api-key-auth-fails-401
  (with-graph
    (graph-with-au {:auth_type "api-key" :token_header "x-api-key" :secret "key-abc"}
                   {:status_code "200" :response_type "json" :template [] :headers ""})
    #(let [resp (run 1 2 {:path {} :query {} :body {}} empty-request)]
       (is (= 401 (:status resp))))))

;; ---------------------------------------------------------------------------
;; Rate limiter (Rl) node
;; ---------------------------------------------------------------------------

(defn- graph-with-rl [rl-attrs rb-attrs]
  (let [nodes {1 (node "O")
               2 (node "Ep" :route_path "/test" :http_method "GET")
               5 (node "Rl" rl-attrs)
               3 (node "Rb" rb-attrs)}]
    (graph nodes [[2 5] [5 3] [3 1]])))

(deftest execute-graph-rate-limit-allows-under-limit
  ;; Fresh rate limiter with limit=100 — first call always passes. Use unique graph-id=1001.
  (with-graph
    (graph-with-rl {:max_requests 100 :window_seconds 60 :burst 0 :key_type "ip"}
                   {:status_code "200" :response_type "json" :template [] :headers ""})
    #(let [resp (run 1001 2 {:path {} :query {} :body {}} empty-request)]
       (is (= 200 (:status resp))))))

(deftest execute-graph-rate-limit-blocks-at-limit
  ;; Limit of 1, burst 0 — second call from same IP must be rejected. Use unique graph-id=1002.
  (let [g (graph-with-rl {:max_requests 1 :window_seconds 60 :burst 0 :key_type "ip"}
                          {:status_code "200" :response_type "json" :template [] :headers ""})]
    (with-graph g
      #(do
         ;; First call consumes the single slot
         (run 1002 2 {:path {} :query {} :body {}} empty-request)
         ;; Second call must be rate-limited
         (let [resp (run 1002 2 {:path {} :query {} :body {}} empty-request)]
           (is (= 429 (:status resp)))
           (is (contains? (:headers resp) "Retry-After")))))))

;; ---------------------------------------------------------------------------
;; Validator (Vd) node
;; ---------------------------------------------------------------------------

(defn- graph-with-vd [vd-rules rb-attrs]
  (let [nodes {1 (node "O")
               2 (node "Ep" :route_path "/test" :http_method "GET")
               6 (node "Vd" :rules vd-rules)
               3 (node "Rb" rb-attrs)}]
    (graph nodes [[2 6] [6 3] [3 1]])))

(deftest execute-graph-validation-passes
  (with-graph
    (graph-with-vd [{:field "name" :rule "required" :value "" :message "Name required"}]
                   {:status_code "200" :response_type "json" :template [] :headers ""})
    #(let [resp (run 1 2 {:path {} :query {:name "Alice"} :body {}} empty-request)]
       (is (= 200 (:status resp))))))

(deftest execute-graph-validation-fails-422
  (with-graph
    (graph-with-vd [{:field "name" :rule "required" :value "" :message "Name required"}]
                   {:status_code "200" :response_type "json" :template [] :headers ""})
    #(let [resp (run 1 2 {:path {} :query {} :body {}} empty-request)]
       (is (= 422 (:status resp)))
       (let [body (json/parse-string (:body resp))]
         (is (contains? body "errors"))
         (is (contains? (get body "errors") "name"))))))

;; ---------------------------------------------------------------------------
;; CORS (Cr) node
;; ---------------------------------------------------------------------------

(defn- graph-with-cr [cr-attrs rb-attrs]
  (let [nodes {1 (node "O")
               2 (node "Ep" :route_path "/test" :http_method "GET")
               7 (node "Cr" cr-attrs)
               3 (node "Rb" rb-attrs)}]
    (graph nodes [[2 7] [7 3] [3 1]])))

(deftest execute-graph-cors-headers-present
  (with-graph
    (graph-with-cr {:allowed_origins   ["https://example.com"]
                    :allowed_methods   ["GET"]
                    :allowed_headers   ["Content-Type"]
                    :allow_credentials false
                    :max_age           3600}
                   {:status_code "200" :response_type "json" :template [] :headers ""})
    #(let [req  (assoc-in empty-request [:headers "origin"] "https://example.com")
           resp (run 1 2 {:path {} :query {} :body {}} req)]
       (is (= "https://example.com"
              (get-in resp [:headers "Access-Control-Allow-Origin"]))))))

(deftest execute-graph-cors-absent-for-non-matching-origin
  (with-graph
    (graph-with-cr {:allowed_origins   ["https://example.com"]
                    :allowed_methods   ["GET"]
                    :allowed_headers   []
                    :allow_credentials false
                    :max_age           86400}
                   {:status_code "200" :response_type "json" :template [] :headers ""})
    #(let [req  (assoc-in empty-request [:headers "origin"] "https://evil.com")
           resp (run 1 2 {:path {} :query {} :body {}} req)]
       (is (nil? (get-in resp [:headers "Access-Control-Allow-Origin"]))))))

;; ---------------------------------------------------------------------------
;; Pipeline order: auth fires before validation, validation before Rb
;; ---------------------------------------------------------------------------

(deftest execute-graph-auth-checked-before-validation
  ;; Au with wrong token + Vd that would also fail — must get 401, not 422
  (let [nodes {1 (node "O")
               2 (node "Ep" :route_path "/test" :http_method "GET")
               4 (node "Au" :auth_type "bearer" :token_header "authorization" :secret "secret")
               6 (node "Vd" :rules [{:field "name" :rule "required" :value "" :message "req"}])
               3 (node "Rb" {:status_code "200" :response_type "json" :template [] :headers ""})}
        g     (graph nodes [[2 4] [4 6] [6 3] [3 1]])]
    (with-graph g
      #(let [resp (run 1 2 {:path {} :query {} :body {}} empty-request)]
         (is (= 401 (:status resp)))))))

(deftest execute-graph-rate-limit-checked-before-validation
  ;; Rl with limit=1 exhausted first, then Vd that would also fail — must get 429, not 422.
  ;; Use unique graph-id=1003 to avoid cross-test rl-state pollution.
  (let [nodes {1 (node "O")
               2 (node "Ep" :route_path "/test" :http_method "GET")
               5 (node "Rl" :max_requests 1 :window_seconds 60 :burst 0 :key_type "global")
               6 (node "Vd" :rules [{:field "name" :rule "required" :value "" :message "req"}])
               3 (node "Rb" {:status_code "200" :response_type "json" :template [] :headers ""})}
        g     (graph nodes [[2 5] [5 6] [6 3] [3 1]])]
    (with-graph g
      #(do
         ;; Exhaust the single global slot
         (run 1003 2 {:path {} :query {:name "ok"} :body {}} empty-request)
         (let [resp (run 1003 2 {:path {} :query {} :body {}} empty-request)]
           (is (= 429 (:status resp))))))))
