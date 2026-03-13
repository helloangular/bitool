(ns bitool.openapi-import-test
  "Unit tests for OpenAPI spec → BiTool graph conversion.
   All tests are pure — no DB or HTTP server required."
  (:require [clojure.test :refer :all]
            [clojure.string :as string]
            [bitool.api.openapi :as openapi]
            [bitool.graph2 :as g2]))

;; ---------------------------------------------------------------------------
;; Minimal OpenAPI spec fixtures
;; ---------------------------------------------------------------------------

(def simple-spec
  "Single GET endpoint, no auth, no request body."
  {:openapi "3.0.0"
   :info {:title "Simple API" :version "1.0"}
   :paths {"/users" {:get {:operationId "listUsers"
                           :parameters [{:name "limit" :in "query" :required false
                                         :schema {:type "integer"}}]
                           :responses {"200" {:description "OK"
                                              :content {"application/json"
                                                        {:schema {:properties {:id {:type "integer"}
                                                                               :name {:type "string"}}}}}}}}}}})

(def spec-with-auth
  "POST endpoint with bearer auth and request body validation."
  {:openapi "3.0.0"
   :info {:title "Auth API" :version "1.0"}
   :components {:securitySchemes {:bearerAuth {:type "http" :scheme "bearer"}}}
   :paths {"/items" {:post {:operationId "createItem"
                            :security [{:bearerAuth []}]
                            :parameters []
                            :requestBody {:content {"application/json"
                                                    {:schema {:required ["name"]
                                                              :properties {:name {:type "string" :minLength 1}
                                                                           :price {:type "number"}}}}}}
                            :responses {"201" {:description "Created"
                                               :content {"application/json"
                                                         {:schema {:properties {:id {:type "integer"}
                                                                                :name {:type "string"}}}}}}}}}}})

(def spec-with-path-params
  "GET with path params and query params."
  {:openapi "3.0.0"
   :info {:title "Path Param API" :version "1.0"}
   :paths {"/users/{id}" {:parameters [{:name "id" :in "path" :required true
                                        :schema {:type "integer"}}]
                           :get {:operationId "getUser"
                                 :parameters [{:name "fields" :in "query" :required false
                                               :schema {:type "string"}}]
                                 :responses {"200" {:description "OK"}}}}}})

(def multi-endpoint-spec
  "Multiple endpoints to test chain separation."
  {:openapi "3.0.0"
   :info {:title "Multi API" :version "1.0"}
   :paths {"/a" {:get {:operationId "getA" :responses {"200" {:description "OK"}}}}
           "/b" {:post {:operationId "postB" :responses {"201" {:description "Created"}}}}}})

(def spec-with-refs
  "Spec using $ref for request body and response schemas."
  {:openapi "3.0.0"
   :info {:title "Ref API" :version "1.0"}
   :components {:schemas {:Pet {:type "object"
                                :required ["name"]
                                :properties {:id   {:type "integer"}
                                             :name {:type "string"}
                                             :tag  {:type "string"}}}}}
   :paths {"/pets" {:post {:operationId "createPet"
                           :requestBody {:content {"application/json"
                                                   {:schema {:$ref "#/components/schemas/Pet"}}}}
                           :responses {"201" {:description "Created"
                                              :content {"application/json"
                                                        {:schema {:$ref "#/components/schemas/Pet"}}}}}}}}})

(def spec-petstore-like
  "Petstore-like shape:
   - parameters are component $ref entries
   - response content key is keywordized (:application/json) as happens after JSON parse keywordization."
  {:openapi "3.0.0"
   :info {:title "Petstore-like API" :version "1.0"}
   :components {:parameters {:PetId {:name "petId" :in "path" :required true
                                     :schema {:type "integer"}}
                             :Status {:name "status" :in "query" :required false
                                      :schema {:type "string"}}}
                :schemas {:Pet {:type "object"
                                :properties {:id {:type "integer"}
                                             :name {:type "string"}}}}}
   :paths {"/pet/{petId}" {:get {:operationId "getPetById"
                                 :parameters [{:$ref "#/components/parameters/PetId"}
                                              {:$ref "#/components/parameters/Status"}]
                                 :responses {"200" {:description "OK"
                                                    :content {:application/json
                                                              {:schema {:$ref "#/components/schemas/Pet"}}}}}}}}})

;; ---------------------------------------------------------------------------
;; Tests
;; ---------------------------------------------------------------------------

(deftest test-graph-has-output-node
  (let [g (openapi/spec->graph simple-spec "Test")]
    (is (= "O" (get-in g [:n 1 :na :btype])) "Node 1 is always Output")
    (is (= "Output" (get-in g [:n 1 :na :name])))))

(deftest test-graph-name
  (let [g (openapi/spec->graph simple-spec "My Graph")]
    (is (= "My Graph" (get-in g [:a :name])))))

(deftest test-simple-get-creates-ep-and-rb
  (let [g (openapi/spec->graph simple-spec "Test")
        nodes (vals (:n g))
        btypes (set (map #(get-in % [:na :btype]) nodes))]
    (is (contains? btypes "Ep") "Has Endpoint node")
    (is (contains? btypes "Rb") "Has Response Builder node")
    (is (not (contains? btypes "Au")) "No Auth node (no security)")
    ;; Vd may or may not be present — query param "limit" is not required
    ))

(deftest test-ep-node-attributes
  (let [g (openapi/spec->graph simple-spec "Test")
        ep-node (->> (vals (:n g))
                     (filter #(= "Ep" (get-in % [:na :btype])))
                     first
                     :na)]
    (is (= "/users" (:route_path ep-node)))
    (is (= "GET" (:http_method ep-node)))
    (is (= "listUsers" (:name ep-node)))
    (is (= 1 (count (:query_params ep-node))) "One query param (limit)")))

(deftest test-rb-node-wires-to-output
  (let [g (openapi/spec->graph simple-spec "Test")
        rb-entry (->> (:n g)
                      (filter (fn [[_ v]] (= "Rb" (get-in v [:na :btype]))))
                      first)
        rb-id (first rb-entry)
        rb-edges (get-in g [:n rb-id :e])]
    (is (contains? rb-edges 1) "Rb wires to Output (node 1)")))

(deftest test-rb-node-has-template
  (let [g (openapi/spec->graph simple-spec "Test")
        rb-node (->> (vals (:n g))
                     (filter #(= "Rb" (get-in % [:na :btype])))
                     first
                     :na)]
    (is (= "200" (:status_code rb-node)))
    (is (= "json" (:response_type rb-node)))
    (is (vector? (:template rb-node)))
    (is (= 2 (count (:template rb-node))) "Template has 2 fields (id, name)")))

(deftest test-auth-creates-au-node
  (let [g (openapi/spec->graph spec-with-auth "Test")
        nodes (vals (:n g))
        btypes (set (map #(get-in % [:na :btype]) nodes))]
    (is (contains? btypes "Au") "Has Auth node")
    (let [au-node (->> nodes
                       (filter #(= "Au" (get-in % [:na :btype])))
                       first
                       :na)]
      (is (= "bearer" (:auth_type au-node)))
      (is (= "authorization" (:token_header au-node))))))

(deftest test-request-body-creates-vd-node
  (let [g (openapi/spec->graph spec-with-auth "Test")
        nodes (vals (:n g))
        btypes (set (map #(get-in % [:na :btype]) nodes))]
    (is (contains? btypes "Vd") "Has Validator node")
    (let [vd-node (->> nodes
                       (filter #(= "Vd" (get-in % [:na :btype])))
                       first
                       :na)
          rules (:rules vd-node)
          rule-fields (set (map :field rules))]
      (is (pos? (count rules)) "Validator has rules")
      (is (contains? rule-fields "name") "Has rule for 'name' field"))))

(deftest test-ep-body-schema-from-request-body
  (let [g (openapi/spec->graph spec-with-auth "Test")
        ep-node (->> (vals (:n g))
                     (filter #(= "Ep" (get-in % [:na :btype])))
                     first
                     :na)]
    (is (= "POST" (:http_method ep-node)))
    (is (= "/items" (:route_path ep-node)))
    (is (pos? (count (:body_schema ep-node))) "Ep has body_schema from requestBody")))

(deftest test-path-params-merged-with-op-params
  (let [g (openapi/spec->graph spec-with-path-params "Test")
        ep-node (->> (vals (:n g))
                     (filter #(= "Ep" (get-in % [:na :btype])))
                     first
                     :na)]
    (is (= "/users/{id}" (:route_path ep-node)))
    (is (= 1 (count (:path_params ep-node))) "Has 1 path param (id)")
    (is (= 1 (count (:query_params ep-node))) "Has 1 query param (fields)")))

(deftest test-multi-endpoint-creates-separate-chains
  (let [g (openapi/spec->graph multi-endpoint-spec "Test")
        ep-nodes (->> (vals (:n g))
                      (filter #(= "Ep" (get-in % [:na :btype]))))
        rb-nodes (->> (vals (:n g))
                      (filter #(= "Rb" (get-in % [:na :btype]))))]
    (is (= 2 (count ep-nodes)) "Two Ep nodes for two endpoints")
    (is (= 2 (count rb-nodes)) "Two Rb nodes for two endpoints")))

(deftest test-chain-wiring-ep-to-rb
  (let [g (openapi/spec->graph simple-spec "Test")
        node-pairs (:n g)
        ep-id (->> node-pairs
                   (filter (fn [[_ v]] (= "Ep" (get-in v [:na :btype]))))
                   ffirst)
        ;; Walk edges from Ep to find Rb
        walk (fn walk [id depth]
               (if (> depth 10) nil
                   (let [btype (get-in g [:n id :na :btype])]
                     (if (= "Rb" btype) id
                         (some #(walk (first %) (inc depth))
                               (get-in g [:n id :e]))))))]
    (is (some? ep-id) "Found Ep node")
    (is (some? (walk ep-id 0)) "Can walk from Ep to Rb via edges")))

(deftest test-no-auth-when-empty-security
  (let [spec {:openapi "3.0.0"
              :info {:title "No Auth" :version "1.0"}
              :paths {"/open" {:get {:operationId "openEndpoint"
                                     :security [{}]
                                     :responses {"200" {:description "OK"}}}}}}
        g (openapi/spec->graph spec "Test")
        btypes (set (map #(get-in % [:na :btype]) (vals (:n g))))]
    (is (not (contains? btypes "Au")) "Empty security {} does not create Au node")))

(deftest test-ref-resolution-ep-body-schema
  (let [g (openapi/spec->graph spec-with-refs "Test")
        ep-node (->> (vals (:n g))
                     (filter #(= "Ep" (get-in % [:na :btype])))
                     first :na)
        field-names (set (map :field_name (:body_schema ep-node)))]
    (is (= 3 (count (:body_schema ep-node))) "Ep body_schema has 3 fields from resolved Pet $ref")
    (is (contains? field-names "id"))
    (is (contains? field-names "name"))
    (is (contains? field-names "tag"))))

(deftest test-ref-resolution-rb-template
  (let [g (openapi/spec->graph spec-with-refs "Test")
        rb-node (->> (vals (:n g))
                     (filter #(= "Rb" (get-in % [:na :btype])))
                     first :na)
        output-keys (set (map :output_key (:template rb-node)))]
    (is (= 3 (count (:template rb-node))) "Rb template has 3 fields from resolved Pet $ref")
    (is (contains? output-keys "id"))
    (is (contains? output-keys "name"))
    (is (contains? output-keys "tag"))))

(deftest test-ref-resolution-vd-rules
  (let [g (openapi/spec->graph spec-with-refs "Test")
        vd-node (->> (vals (:n g))
                     (filter #(= "Vd" (get-in % [:na :btype])))
                     first :na)
        rule-fields (set (map :field (:rules vd-node)))]
    (is (some? vd-node) "Vd node created from $ref schema with required fields")
    (is (contains? rule-fields "name") "Has required rule for 'name' from resolved $ref")))

(deftest test-parameter-ref-resolution-for-ep-params
  (let [g (openapi/spec->graph spec-petstore-like "Test")
        ep-node (->> (vals (:n g))
                     (filter #(= "Ep" (get-in % [:na :btype])))
                     first :na)]
    (is (= 1 (count (:path_params ep-node))) "Path param from component $ref is imported")
    (is (= 1 (count (:query_params ep-node))) "Query param from component $ref is imported")
    (is (= "petId" (:param_name (first (:path_params ep-node)))))
    (is (= "status" (:param_name (first (:query_params ep-node)))))))

(deftest test-keywordized-json-content-produces-rb-template
  (let [g (openapi/spec->graph spec-petstore-like "Test")
        rb-node (->> (vals (:n g))
                     (filter #(= "Rb" (get-in % [:na :btype])))
                     first :na)
        output-keys (set (map :output_key (:template rb-node)))]
    (is (= 2 (count (:template rb-node))) "Rb template is generated from :application/json schema")
    (is (contains? output-keys "id"))
    (is (contains? output-keys "name"))))

(deftest test-ep-tcols-populated
  (testing "Ep nodes have populated tcols after import (not empty)"
    (let [g (openapi/spec->graph simple-spec "Test")
          ep-node (->> (vals (:n g))
                       (filter #(= "Ep" (get-in % [:na :btype])))
                       first :na)
          tcols (:tcols ep-node)]
      (is (map? tcols) "tcols is a map")
      (is (pos? (count (first (vals tcols)))) "tcols has columns from query params"))))

(deftest test-ep-tcols-body-schema
  (testing "Ep tcols includes body_schema fields"
    (let [g (openapi/spec->graph spec-with-auth "Test")
          ep-node (->> (vals (:n g))
                       (filter #(= "Ep" (get-in % [:na :btype])))
                       first :na)
          cols (first (vals (:tcols ep-node)))
          col-names (set (map :column_name cols))]
      (is (contains? col-names "name") "tcols has 'name' column from body schema")
      (is (contains? col-names "price") "tcols has 'price' column from body schema"))))

(deftest test-ep-params-use-correct-keys
  (testing "Ep path_params use :param_name, body_schema uses :field_name"
    (let [g (openapi/spec->graph spec-with-path-params "Test")
          ep (:na (->> (vals (:n g))
                       (filter #(= "Ep" (get-in % [:na :btype])))
                       first))]
      (is (= "id" (:param_name (first (:path_params ep)))) "path_params use :param_name")
      (is (= "fields" (:param_name (first (:query_params ep)))) "query_params use :param_name"))
    (let [g (openapi/spec->graph spec-with-refs "Test")
          ep (:na (->> (vals (:n g))
                       (filter #(= "Ep" (get-in % [:na :btype])))
                       first))]
      (is (every? :field_name (:body_schema ep)) "body_schema uses :field_name"))))

(deftest test-graph-has-id-zero-for-insert
  (testing "spec->graph sets :id to 0 so insertGraph generates a new ID"
    (let [g (openapi/spec->graph simple-spec "Test")]
      (is (= 0 (get-in g [:a :id])) "Graph :id must be 0 (not nil) for insertGraph to assign a new ID"))))

(deftest test-graph-has-version
  (testing "spec->graph sets :v to 0"
    (let [g (openapi/spec->graph simple-spec "Test")]
      (is (= 0 (get-in g [:a :v])) "Graph :v is 0"))))

(deftest test-get-endpoint-item-shows-imported-params
  (let [g     (openapi/spec->graph spec-petstore-like "Test")
        ep-id (->> (:n g)
                   (filter (fn [[_ v]] (= "Ep" (get-in v [:na :btype]))))
                   ffirst)
        item  (g2/get-endpoint-item ep-id g)
        path-params  (or (get item "path_params") (get item :path_params) [])
        query-params (or (get item "query_params") (get item :query_params) [])]
    (is (some? ep-id) "Found Ep node")
    (is (= 1 (count path-params)) "Endpoint item includes imported path params")
    (is (= 1 (count query-params)) "Endpoint item includes imported query params")
    (is (= "petId" (or (get-in path-params [0 "param_name"])
                        (get-in path-params [0 :param_name]))))
    (is (= "status" (or (get-in query-params [0 "param_name"])
                         (get-in query-params [0 :param_name]))))))

(deftest test-get-response-builder-item-shows-upstream-vars
  (let [g      (openapi/spec->graph spec-petstore-like "Test")
        rb-id  (->> (:n g)
                    (filter (fn [[_ v]] (= "Rb" (get-in v [:na :btype]))))
                    ffirst)
        item   (g2/get-response-builder-item rb-id g)
        items  (or (get item "items") (get item :items) [])
        fields (set (map #(last (string/split (or (get % "technical_name")
                                                  (get % :technical_name)
                                                  "")
                                              #"\."))
                         items))]
    (is (some? rb-id) "Found Rb node")
    (is (pos? (count items)) "Response builder item has upstream fields for UI variables")
    (is (contains? fields "petId"))
    (is (contains? fields "status"))))

(deftest test-save-endpoint-accepts-string-keyed-payload
  (let [g {:a {:name "Test" :v 0 :id 0}
           :n {1 {:na {:name "Output" :btype "O" :tcols {}} :e {}}
               2 {:na {:name "ep" :btype "Ep" :tcols {}} :e {}}}}
        params {"http_method" "post"
                "route_path" "/user/{username}"
                "path_params" [{"param_name" "username" "data_type" "varchar"}]
                "query_params" [{"param_name" "expand" "data_type" "varchar" "required" true}]
                "body_schema" [{"field_name" "email" "data_type" "string" "required" false}]
                "response_format" "json"
                "description" "Get user profile"}
        saved-g (g2/save-endpoint g 2 params)
        item    (g2/get-endpoint-item 2 saved-g)]
    (is (= "POST" (get item "http_method")) "http_method is normalized and preserved")
    (is (= "/user/{username}" (get item "route_path")))
    (is (= "Get user profile" (get item "description")))
    (is (= 1 (count (get item "path_params"))) "path params are saved")
    (is (= 1 (count (get item "query_params"))) "query params are saved")
    (is (= 1 (count (get item "body_schema"))) "body schema is saved")
    (is (= "username" (or (get-in item ["path_params" 0 :param_name])
                          (get-in item ["path_params" 0 "param_name"]))))
    (is (= "expand" (or (get-in item ["query_params" 0 :param_name])
                        (get-in item ["query_params" 0 "param_name"]))))
    (is (= "email" (or (get-in item ["body_schema" 0 :field_name])
                       (get-in item ["body_schema" 0 "field_name"]))))))

(deftest test-save-response-builder-accepts-string-keyed-payload
  (let [ep-cols [{:column_name "username" :data_type "varchar" :is_nullable "NO"}]
        g {:a {:name "Test" :v 0 :id 0}
           :n {1 {:na {:name "Output" :btype "O" :tcols {}} :e {}}
               2 {:na {:name "ep" :btype "Ep" :tcols {2 ep-cols}} :e {3 {}}}
               3 {:na {:name "rb" :btype "Rb" :tcols {}} :e {1 {}}}}}
        params {"status_code" "201"
                "response_type" "json"
                "headers" ""
                "template" [{"output_key" "username"
                             "source_column" "username"}]}
        saved-g (g2/save-response-builder g 3 params)
        item    (g2/get-response-builder-item 3 saved-g)
        items   (or (get item "items") [])]
    (is (= "201" (get item "status_code")))
    (is (= "json" (get item "response_type")))
    (is (= 1 (count (or (get item "template") []))) "template rows are saved")
    (is (pos? (count items)) "upstream vars are available for Rb UI")
    (is (some #(string/includes? (or (get % "technical_name")
                                     (get % :technical_name)
                                     "")
                                "username")
              items))))
