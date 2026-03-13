(ns bitool.nodes-test
  "Unit tests for all 8 new node types (Au, Dx, Rl, Cr, Lg, Cq, Ev, Ci)
   and endpoint runtime helpers (auth, path matching, template application).
   All tests are pure — no DB or HTTP server required."
  (:require [clojure.test :refer :all]
            [bitool.graph2 :as g2]
            [bitool.db :as db]
            [bitool.endpoint :as ep])
)

;; ---------------------------------------------------------------------------
;; Test graph helpers
;; ---------------------------------------------------------------------------

(defn- base-graph
  "Minimal graph with an Output node (id=1) and one source node at given id."
  [node-id btype]
  {:a {:name "test" :v 0 :id 99}
   :n {1       {:na {:name "O" :btype "O" :tcols {}} :e {}}
       node-id {:na {:name (str btype "-node") :btype btype :tcols {}} :e {1 {}}}}})

(defn- graph-with-tcols
  "Source node at node-id carries tcols so passthrough nodes can inherit."
  [node-id btype cols]
  (let [g (base-graph node-id btype)]
    (assoc-in g [:n node-id :na :tcols] {node-id cols})))

(def sample-cols
  [{:column_name "id"   :data_type "integer" :is_nullable "NO"}
   {:column_name "name" :data_type "varchar" :is_nullable "YES"}])

;; ---------------------------------------------------------------------------
;; Auth node (Au)
;; ---------------------------------------------------------------------------

(deftest save-auth-basic
  (let [g  (base-graph 2 "Au")
        g' (g2/save-auth g 2 {:auth_type    "bearer"
                              :token_header "authorization"
                              :secret       "my-secret"
                              :claims_to_cols []})]
    (testing "auth_type stored"
      (is (= "bearer" (:auth_type (g2/getData g' 2)))))
    (testing "secret stored"
      (is (= "my-secret" (:secret (g2/getData g' 2)))))))

(deftest save-auth-preserves-secret-when-blank
  (let [g  (base-graph 2 "Au")
        g1 (g2/save-auth g 2 {:auth_type "bearer" :secret "original" :claims_to_cols []})
        g2 (g2/save-auth g1 2 {:auth_type "bearer" :secret "" :claims_to_cols []})]
    (testing "blank secret preserves existing"
      (is (= "original" (:secret (g2/getData g2 2)))))))

(deftest get-auth-item-never-returns-secret
  (let [g  (base-graph 2 "Au")
        g' (g2/save-auth g 2 {:auth_type "bearer" :secret "my-secret" :claims_to_cols []})
        item (g2/get-auth-item 2 g')]
    (testing "secret not in response"
      (is (nil? (get item "secret"))))
    (testing "secret_set is true"
      (is (true? (get item "secret_set"))))
    (testing "auth_type present"
      (is (= "bearer" (get item "auth_type"))))))

(deftest get-auth-item-secret-set-false-when-empty
  (let [g    (base-graph 2 "Au")
        g'   (g2/save-auth g 2 {:auth_type "bearer" :secret "" :claims_to_cols []})
        item (g2/get-auth-item 2 g')]
    (is (false? (get item "secret_set")))))

;; ---------------------------------------------------------------------------
;; DB Execute node (Dx)
;; ---------------------------------------------------------------------------

(deftest validate-dx-sql-allows-select
  (is (nil? (g2/validate-dx-sql "SELECT id, name FROM users" "SELECT"))))

(deftest validate-dx-sql-allows-insert
  (is (nil? (g2/validate-dx-sql "INSERT INTO log VALUES (1)" "INSERT"))))

(deftest validate-dx-sql-blocks-ddl-in-select
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"DDL not permitted"
                        (g2/validate-dx-sql "CREATE TABLE x (id INT)" "SELECT"))))

(deftest validate-dx-sql-blocks-bad-operation
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"disallowed SQL operation"
                        (g2/validate-dx-sql "EXEC sp_foo" "EXEC"))))

(deftest save-dx-select-parses-columns
  (let [g  (base-graph 2 "Dx")
        g' (g2/save-dx g 2 {:sql_template  "SELECT id, name, email FROM users"
                             :operation     "SELECT"
                             :connection_id 5
                             :result_mode   "single"})]
    (let [tcols (get (:tcols (g2/getData g' 2)) 2)]
      (is (= 3 (count tcols)) "columns parsed from SELECT")
      (is (= "id" (:column_name (first tcols))) "first column name"))))

(deftest save-dx-non-select-yields-affected-rows
  (let [g    (base-graph 2 "Dx")
        g'   (g2/save-dx g 2 {:sql_template  "UPDATE users SET active=1"
                               :operation     "UPDATE"
                               :connection_id 5
                               :result_mode   "single"})
        tcols (get (:tcols (g2/getData g' 2)) 2)]
    (is (= "affected_rows" (:column_name (first tcols))))))

(deftest get-dx-item-fields
  (let [g  (base-graph 2 "Dx")
        g' (g2/save-dx g 2 {:sql_template  "SELECT x FROM t"
                             :operation     "SELECT"
                             :connection_id 7
                             :result_mode   "multi"})
        item (g2/get-dx-item 2 g')]
    (is (= "SELECT" (get item "operation")))
    (is (= 7 (get item "connection_id")))
    (is (= "multi" (get item "result_mode")))))

;; ---------------------------------------------------------------------------
;; Rate Limiter (Rl) — passthrough with safe-int
;; ---------------------------------------------------------------------------

(deftest save-rl-stores-fields
  (let [g  (graph-with-tcols 2 "Rl" sample-cols)
        g' (g2/save-rl g 2 {:max_requests 50 :window_seconds 120 :key_type "user" :burst 10})]
    (is (= 50  (:max_requests   (g2/getData g' 2))))
    (is (= 120 (:window_seconds (g2/getData g' 2))))
    (is (= "user" (:key_type    (g2/getData g' 2))))))

(deftest save-rl-safe-int-on-blank
  (let [g  (graph-with-tcols 2 "Rl" sample-cols)
        g' (g2/save-rl g 2 {:max_requests "" :window_seconds "" :key_type "ip" :burst ""})]
    (testing "blank max_requests falls back to default 100"
      (is (= 100 (:max_requests (g2/getData g' 2)))))
    (testing "blank window_seconds falls back to default 60"
      (is (= 60 (:window_seconds (g2/getData g' 2)))))))

;; ---------------------------------------------------------------------------
;; CORS (Cr)
;; ---------------------------------------------------------------------------

(deftest save-cr-stores-fields
  (let [g  (graph-with-tcols 2 "Cr" sample-cols)
        g' (g2/save-cr g 2 {:allowed_origins   ["https://example.com"]
                             :allowed_methods   ["GET" "POST"]
                             :allowed_headers   ["Content-Type"]
                             :allow_credentials true
                             :max_age           3600})]
    (is (= 3600 (:max_age (g2/getData g' 2))))
    (is (true?  (:allow_credentials (g2/getData g' 2))))))

(deftest save-cr-string-origins-split
  (let [g  (graph-with-tcols 2 "Cr" sample-cols)
        g' (g2/save-cr g 2 {:allowed_origins "https://a.com, https://b.com"
                             :allowed_methods []
                             :allowed_headers []
                             :max_age 86400})]
    (is (= ["https://a.com" "https://b.com"]
           (:allowed_origins (g2/getData g' 2))))))

;; ---------------------------------------------------------------------------
;; Logger (Lg)
;; ---------------------------------------------------------------------------

(deftest save-lg-stores-fields
  (let [g  (graph-with-tcols 2 "Lg" sample-cols)
        g' (g2/save-lg g 2 {:log_level "WARN" :destination "external"
                             :external_url "https://log.io" :format "text"
                             :fields_to_log ["id"]})]
    (is (= "WARN"            (:log_level   (g2/getData g' 2))))
    (is (= "external"        (:destination (g2/getData g' 2))))
    (is (= "https://log.io"  (:external_url (g2/getData g' 2))))))

(deftest save-lg-defaults
  (let [g  (graph-with-tcols 2 "Lg" sample-cols)
        g' (g2/save-lg g 2 {})]
    (is (= "INFO"    (:log_level   (g2/getData g' 2))))
    (is (= "console" (:destination (g2/getData g' 2))))
    (is (= "json"    (:format      (g2/getData g' 2))))))

;; ---------------------------------------------------------------------------
;; Cache (Cq)
;; ---------------------------------------------------------------------------

(deftest save-cq-stores-fields
  (let [g  (graph-with-tcols 2 "Cq" sample-cols)
        g' (g2/save-cq g 2 {:cache_key "{{id}}" :ttl_seconds 600 :strategy "write-through"})]
    (is (= "{{id}}"       (:cache_key   (g2/getData g' 2))))
    (is (= 600            (:ttl_seconds (g2/getData g' 2))))
    (is (= "write-through" (:strategy   (g2/getData g' 2))))))

(deftest save-cq-blank-ttl-uses-default
  (let [g  (graph-with-tcols 2 "Cq" sample-cols)
        g' (g2/save-cq g 2 {:cache_key "" :ttl_seconds ""})]
    (is (= 300 (:ttl_seconds (g2/getData g' 2))))))

;; ---------------------------------------------------------------------------
;; Event Emitter (Ev)
;; ---------------------------------------------------------------------------

(deftest save-ev-stores-fields
  (let [g  (graph-with-tcols 2 "Ev" sample-cols)
        g' (g2/save-ev g 2 {:topic "orders" :broker_url "kafka://localhost:9092"
                             :key_template "{{id}}" :format "avro"})]
    (is (= "orders"              (:topic        (g2/getData g' 2))))
    (is (= "kafka://localhost:9092" (:broker_url (g2/getData g' 2))))
    (is (= "avro"                (:format       (g2/getData g' 2))))))

;; ---------------------------------------------------------------------------
;; Circuit Breaker (Ci)
;; ---------------------------------------------------------------------------

(deftest save-ci-stores-fields
  (let [g  (graph-with-tcols 2 "Ci" sample-cols)
        g' (g2/save-ci g 2 {:failure_threshold 3 :reset_timeout 60
                             :fallback_response "{\"ok\":false}"})]
    (is (= 3   (:failure_threshold (g2/getData g' 2))))
    (is (= 60  (:reset_timeout     (g2/getData g' 2))))))

(deftest save-ci-blank-fields-use-defaults
  (let [g  (graph-with-tcols 2 "Ci" sample-cols)
        g' (g2/save-ci g 2 {:failure_threshold "" :reset_timeout ""})]
    (is (= 5  (:failure_threshold (g2/getData g' 2))))
    (is (= 30 (:reset_timeout     (g2/getData g' 2))))))

;; ---------------------------------------------------------------------------
;; remove-node (graph2)
;; ---------------------------------------------------------------------------

(deftest remove-node-deletes-node
  (let [g  (base-graph 2 "Rl")
        g' (g2/remove-node g 2)]
    (is (nil? (get-in g' [:n 2])))))

(deftest remove-node-cleans-up-edges
  (let [;; 3 → 2 → 1 (output)
        g  {:a {:name "t" :v 0 :id 0}
            :n {1 {:na {:name "O" :btype "O" :tcols {}} :e {}}
                2 {:na {:name "n" :btype "Rl" :tcols {}} :e {1 {}}}
                3 {:na {:name "s" :btype "Ep" :tcols {}} :e {2 {}}}}}
        g' (g2/remove-node g 2)]
    (testing "node removed"
      (is (nil? (get-in g' [:n 2]))))
    (testing "edge from 3 to removed node cleaned up"
      (is (empty? (get-in g' [:n 3 :e]))))))

;; ---------------------------------------------------------------------------
;; Endpoint runtime: path matching
;; ---------------------------------------------------------------------------

(deftest path-matches-exact
  (is (ep/path-matches? "/users" "/users")))

(deftest path-matches-with-param
  (is (ep/path-matches? "/users/:id" "/users/42")))

(deftest path-matches-multi-param
  (is (ep/path-matches? "/orgs/:org/users/:id" "/orgs/acme/users/7")))

(deftest path-no-match-different-segment
  (is (not (ep/path-matches? "/users/profile" "/users/42"))))

(deftest path-no-match-different-depth
  (is (not (ep/path-matches? "/users/:id" "/users/42/extra"))))

;; ---------------------------------------------------------------------------
;; Endpoint runtime: path param extraction
;; ---------------------------------------------------------------------------

(deftest extract-path-params-single
  (is (= {:id "42"}
         (ep/extract-path-params "/users/:id" "/users/42"))))

(deftest extract-path-params-multiple
  (is (= {:org "acme" :uid "7"}
         (ep/extract-path-params "/orgs/:org/users/:uid" "/orgs/acme/users/7"))))

(deftest extract-path-params-none
  (is (= {} (ep/extract-path-params "/health" "/health"))))

;; ---------------------------------------------------------------------------
;; Endpoint runtime: flat-params
;; ---------------------------------------------------------------------------

(deftest flat-params-merges-all-sources
  (let [result (ep/flat-params {:path  {:id "5"}
                                :query {:limit "10"}
                                :body  {:name "alice"}})]
    (is (= {"id" "5" "limit" "10" "name" "alice"} result))))

(deftest flat-params-empty
  (is (= {} (ep/flat-params {:path {} :query {} :body {}}))))

;; ---------------------------------------------------------------------------
;; Endpoint runtime: apply-template
;; ---------------------------------------------------------------------------

(deftest apply-template-maps-keys
  (let [tmpl   [{:output_key "userId" :source_column "id"}
                {:output_key "fullName" :source_column "name"}]
        flat   {"id" "42" "name" "Alice" "extra" "ignored"}
        result (ep/apply-template tmpl flat)]
    (is (= {"userId" "42" "fullName" "Alice"} result))))

(deftest apply-template-skips-missing-source
  (let [tmpl   [{:output_key "userId" :source_column "id"}
                {:output_key "missing" :source_column "no_such_col"}]
        flat   {"id" "42"}
        result (ep/apply-template tmpl flat)]
    (is (= {"userId" "42"} result))))

(deftest apply-template-empty-template-returns-flat
  (let [flat {"a" "1" "b" "2"}]
    ;; When no template the caller falls back to flat — test apply-template directly returns empty
    (is (= {} (ep/apply-template [] flat)))))

;; ---------------------------------------------------------------------------
;; Endpoint runtime: HMAC-SHA256
;; ---------------------------------------------------------------------------

(deftest hmac-sha256-deterministic
  (let [sig1 (ep/hmac-sha256 "secret" "hello")
        sig2 (ep/hmac-sha256 "secret" "hello")]
    (is (= sig1 sig2) "same input yields same signature")
    (is (= 64 (count sig1)) "SHA-256 hex is 64 chars")))

(deftest hmac-sha256-different-bodies
  (let [sig-a (ep/hmac-sha256 "secret" "body-a")
        sig-b (ep/hmac-sha256 "secret" "body-b")]
    (is (not= sig-a sig-b) "different bodies yield different signatures")))

(deftest hmac-sha256-different-secrets
  (let [sig-a (ep/hmac-sha256 "secret1" "payload")
        sig-b (ep/hmac-sha256 "secret2" "payload")]
    (is (not= sig-a sig-b) "different secrets yield different signatures")))

;; ---------------------------------------------------------------------------
;; Endpoint runtime: register-endpoint! Wh vs Ep routing
;; ---------------------------------------------------------------------------

(deftest register-endpoint-ep-uses-route-path
  (let [cfg {:btype "Ep" :route_path "/users/:id" :http_method "GET"}]
    (ep/register-endpoint! 99 10 cfg)
    (let [entry (get-in @ep/endpoint-registry [99 10])]
      (is (= :get    (:method entry)))
      (is (= "/users/:id" (:path entry))))
    ;; clean up
    (ep/unregister-endpoint! 99 10)))

(deftest register-endpoint-wh-uses-webhook-path
  (let [cfg {:btype "Wh" :webhook_path "/hooks/orders" :secret_value "s" :secret_header "X-Hub-Sig"}]
    (ep/register-endpoint! 99 11 cfg)
    (let [entry (get-in @ep/endpoint-registry [99 11])]
      (is (= :post         (:method entry)))
      (is (= "/hooks/orders" (:path entry))))
    ;; clean up
    (ep/unregister-endpoint! 99 11)))

(deftest register-endpoint-normalizes-openapi-brace-params
  (let [cfg {:btype "Ep" :route_path "/pets/{petId}" :http_method "GET"}]
    (ep/register-endpoint! 99 12 cfg)
    (let [entry (get-in @ep/endpoint-registry [99 12])]
      (is (= :get (:method entry)))
      (is (= "/pets/:petId" (:path entry))))
    ;; clean up
    (ep/unregister-endpoint! 99 12)))

(deftest deploy-graph-endpoints-registers-ep-and-wh
  (let [g {:a {:name "deploy-test" :v 0 :id 42}
           :n {1 {:na {:name "Output" :btype "O"} :e {}}
               2 {:na {:name "getPet" :btype "Ep" :route_path "/pets/{petId}" :http_method "GET"} :e {}}
               3 {:na {:name "incomingHook" :btype "Wh" :webhook_path "/hooks/order"} :e {}}
               4 {:na {:name "table" :btype "T"} :e {}}}}]
    (with-redefs [db/getGraph (fn [_] g)]
      (let [resp (ep/deploy-graph-endpoints! 42)
            methods+urls (set (map (fn [x] [(get x :method) (get x :url)])
                                   (:endpoints resp)))]
        (is (= 42 (:graph_id resp)))
        (is (= 2 (:count resp)))
        (is (contains? methods+urls ["GET" "/api/v1/ep/42/pets/:petId"]))
        (is (contains? methods+urls ["POST" "/api/v1/ep/42/hooks/order"])))))) 

(deftest dynamic-endpoint-handler-extracts-path-params-from-ep-path
  (let [g {:a {:name "runtime-path-test" :v 0 :id 7}
           :n {1 {:na {:name "Output" :btype "O"} :e {}}
               2 {:na {:name "getPet" :btype "Ep" :route_path "/pet/{petId}" :http_method "GET"} :e {3 {}}}
               3 {:na {:name "vd" :btype "Vd"
                       :rules [{:field "petId" :rule "type" :value "integer" :message "petId must be integer"}]}
                  :e {4 {}}}
               4 {:na {:name "rb" :btype "Rb" :status_code "200" :response_type "json" :template []} :e {1 {}}}}}
        request {:request-method :get
                 :uri "/api/v1/ep/7/pet/1"
                 :query-params {}
                 :params {}
                 :headers {}}]
    (reset! ep/endpoint-registry {})
    (with-redefs [db/getGraph (fn [_] g)]
      (ep/register-endpoint! 7 2 (get-in g [:n 2 :na]))
      (let [resp (ep/dynamic-endpoint-handler request)]
        (is (= 200 (:status resp))
            "Path params should be extracted from matched endpoint path (/pet/1), not /api/v1/ep/7/pet/1")))))

;; ---------------------------------------------------------------------------
;; Validator runtime: validate-vd-node
;; ---------------------------------------------------------------------------

(def ^:private validate-vd-node #'ep/validate-vd-node)

(deftest vd-required-passes-when-present
  (is (nil? (validate-vd-node {:rules [{:field "name" :rule "required" :value "" :message ""}]}
                               {"name" "Alice"}))))

(deftest vd-required-fails-when-blank
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Validation failed"
        (validate-vd-node {:rules [{:field "name" :rule "required" :value "" :message "Name is required"}]}
                          {"name" ""}))))

(deftest vd-min-passes
  (is (nil? (validate-vd-node {:rules [{:field "age" :rule "min" :value "18" :message ""}]}
                               {"age" "25"}))))

(deftest vd-min-fails
  (is (thrown? clojure.lang.ExceptionInfo
        (validate-vd-node {:rules [{:field "age" :rule "min" :value "18" :message "Too young"}]}
                          {"age" "10"}))))

(deftest vd-max-length-fails
  (is (thrown? clojure.lang.ExceptionInfo
        (validate-vd-node {:rules [{:field "bio" :rule "max-length" :value "5" :message "Too long"}]}
                          {"bio" "hello world"}))))

(deftest vd-regex-passes
  (is (nil? (validate-vd-node {:rules [{:field "code" :rule "regex" :value "^[A-Z]{3}$" :message ""}]}
                               {"code" "ABC"}))))

(deftest vd-regex-fails
  (is (thrown? clojure.lang.ExceptionInfo
        (validate-vd-node {:rules [{:field "code" :rule "regex" :value "^[A-Z]{3}$" :message "Bad code"}]}
                          {"code" "abc"}))))

(deftest vd-one-of-passes
  (is (nil? (validate-vd-node {:rules [{:field "status" :rule "one-of" :value "new,paid,shipped" :message ""}]}
                               {"status" "paid"}))))

(deftest vd-one-of-fails
  (is (thrown? clojure.lang.ExceptionInfo
        (validate-vd-node {:rules [{:field "status" :rule "one-of" :value "new,paid" :message "Bad status"}]}
                          {"status" "unknown"}))))

(deftest vd-max-passes
  (is (nil? (validate-vd-node {:rules [{:field "qty" :rule "max" :value "100" :message ""}]}
                               {"qty" "50"}))))

(deftest vd-max-fails
  (is (thrown? clojure.lang.ExceptionInfo
        (validate-vd-node {:rules [{:field "qty" :rule "max" :value "100" :message "Too large"}]}
                          {"qty" "200"}))))

(deftest vd-min-length-passes
  (is (nil? (validate-vd-node {:rules [{:field "pw" :rule "min-length" :value "8" :message ""}]}
                               {"pw" "password1"}))))

(deftest vd-min-length-fails
  (is (thrown? clojure.lang.ExceptionInfo
        (validate-vd-node {:rules [{:field "pw" :rule "min-length" :value "8" :message "Too short"}]}
                          {"pw" "abc"}))))

(deftest vd-type-integer-passes
  (is (nil? (validate-vd-node {:rules [{:field "age" :rule "type" :value "integer" :message ""}]}
                               {"age" "42"}))))

(deftest vd-type-integer-fails
  (is (thrown? clojure.lang.ExceptionInfo
        (validate-vd-node {:rules [{:field "age" :rule "type" :value "integer" :message "Must be int"}]}
                          {"age" "3.14"}))))

(deftest vd-type-integer-camelcase-field-case-insensitive
  (is (nil? (validate-vd-node {:rules [{:field "petId" :rule "type" :value "integer" :message ""}]}
                               {"petid" "1"}))))

(deftest vd-type-integer-keyword-field-passes
  (is (nil? (validate-vd-node {:rules [{:field :petId :rule "type" :value "integer" :message ""}]}
                               {"petId" "1"}))))

(deftest vd-type-boolean-passes
  (is (nil? (validate-vd-node {:rules [{:field "flag" :rule "type" :value "boolean" :message ""}]}
                               {"flag" "true"}))))

(deftest vd-type-boolean-fails
  (is (thrown? clojure.lang.ExceptionInfo
        (validate-vd-node {:rules [{:field "flag" :rule "type" :value "boolean" :message "Not bool"}]}
                          {"flag" "yes"}))))

(deftest vd-min-rejects-non-numeric
  (is (thrown? clojure.lang.ExceptionInfo
        (validate-vd-node {:rules [{:field "age" :rule "min" :value "0" :message "Must be number"}]}
                          {"age" "abc"}))))

(deftest vd-max-rejects-non-numeric
  (is (thrown? clojure.lang.ExceptionInfo
        (validate-vd-node {:rules [{:field "score" :rule "max" :value "100" :message "Must be number"}]}
                          {"score" "abc"}))))

(deftest vd-nil-node-is-noop
  (is (nil? (validate-vd-node nil {"anything" "goes"}))))

;; ---------------------------------------------------------------------------
;; CORS runtime: cors-headers
;; ---------------------------------------------------------------------------

(def ^:private cors-headers #'ep/cors-headers)

(deftest cors-no-node-returns-empty
  (is (= {} (cors-headers nil {:headers {"origin" "https://example.com"}}))))

(deftest cors-matching-origin
  (let [hdrs (cors-headers {:allowed_origins   ["https://example.com"]
                             :allowed_methods   ["GET" "POST"]
                             :allowed_headers   ["Content-Type"]
                             :allow_credentials true
                             :max_age           3600}
                            {:headers {"origin" "https://example.com"}})]
    (is (= "https://example.com" (get hdrs "Access-Control-Allow-Origin")))
    (is (= "true"                (get hdrs "Access-Control-Allow-Credentials")))
    (is (= "3600"                (get hdrs "Access-Control-Max-Age")))))

(deftest cors-wildcard-origin
  (let [hdrs (cors-headers {:allowed_origins ["*"] :allowed_methods ["GET"]
                             :allowed_headers [] :allow_credentials false :max_age 86400}
                            {:headers {"origin" "https://any.com"}})]
    (is (= "*" (get hdrs "Access-Control-Allow-Origin")))))

(deftest cors-non-matching-origin-returns-empty
  (let [hdrs (cors-headers {:allowed_origins ["https://allowed.com"] :allowed_methods ["GET"]
                             :allowed_headers [] :allow_credentials false :max_age 86400}
                            {:headers {"origin" "https://evil.com"}})]
    (is (= {} hdrs))))

;; Wildcard + credentials is forbidden by the CORS spec — must degrade to exact-match only
(deftest cors-wildcard-with-credentials-uses-exact-match
  (let [cr {:allowed_origins ["*"] :allowed_methods ["GET"]
             :allowed_headers [] :allow_credentials true :max_age 86400}
        req {:headers {"origin" "https://trusted.com"}}
        hdrs (cors-headers cr req)]
    ;; Must not emit "*" when credentials are enabled
    (is (not= "*" (get hdrs "Access-Control-Allow-Origin")))
    ;; Exact origin is not in the list either, so result should be empty
    (is (= {} hdrs))))

(deftest cors-wildcard-with-credentials-exact-origin-in-list
  (let [cr {:allowed_origins ["*" "https://trusted.com"] :allowed_methods ["POST"]
             :allowed_headers ["Authorization"] :allow_credentials true :max_age 600}
        req {:headers {"origin" "https://trusted.com"}}
        hdrs (cors-headers cr req)]
    (is (= "https://trusted.com" (get hdrs "Access-Control-Allow-Origin")))
    (is (= "true" (get hdrs "Access-Control-Allow-Credentials")))))
