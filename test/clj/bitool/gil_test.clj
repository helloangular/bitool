(ns bitool.gil-test
  (:require [clojure.test :refer :all]
            [bitool.gil.normalize :as normalize]
            [bitool.gil.validator :as validator]
            [bitool.gil.compiler :as compiler]))

(def valid-build
  {:gil-version "1.0"
   :intent :build
   :graph-name "users-api"
   :nodes [{:node-ref "ep1"
            :type "endpoint"
            :alias "get-users"
            :config {:http_method "GET"
                     :route_path "/users"}}
           {:node-ref "rb1"
            :type "response-builder"
            :config {:status_code 200
                     :response_type "json"
                     :headers ""
                     :template [{:output_key "id" :source_column "id"}]}}
           {:node-ref "out-main"
            :type "output"
            :config {}}]
   :edges [["ep1" "rb1"] ["rb1" "out-main"]]})

(deftest normalize-aliases-and-keys
  (let [raw {"intent" "build"
             "graph_name" "sample"
             "nodes" [{"node_ref" "ep1" "type" "Ep" "config" {"http_method" "GET" "route_path" "/x"}}
                      {"node_ref" "o1" "type" "O"}]
             "edges" [["ep1" "o1"]]}
        n (normalize/normalize raw)]
    (is (= :build (:intent n)))
    (is (= "sample" (:graph-name n)))
    (is (= "endpoint" (get-in n [:nodes 0 :type])))
    (is (= "output" (get-in n [:nodes 1 :type])))
    (is (= "/x" (get-in n [:nodes 0 :config :route_path])))))

(deftest normalize-config-keys-to-snake-case
  (let [raw {"intent" "build"
             "graph_name" "sample"
             "nodes" [{"node_ref" "ep1"
                       "type" "Ep"
                       "config" {"httpMethod" "GET"
                                 "route-path" "/pets/{petId}"}} 
                      {"node_ref" "o1" "type" "O"}]
             "edges" [["ep1" "o1"]]}
        n (normalize/normalize raw)]
    (is (= "GET" (get-in n [:nodes 0 :config :http_method])))
    (is (= "/pets/{petId}" (get-in n [:nodes 0 :config :route_path])))))

(deftest normalize-patches-uses-correct-arg-order
  (let [raw {"intent" "patch"
             "patches" [{"op" "add-edge" "from" "ep1" "to" "rb1"}
                        {"op" "remove-node" "ref" "rb1"}]}
        n (normalize/normalize raw)]
    (is (= :patch (:intent n)))
    (is (= [:add-edge :remove-node] (mapv :op (:patches n))))
    (is (= "ep1" (get-in n [:patches 0 :from])))
    (is (= "rb1" (get-in n [:patches 1 :ref])))))

(deftest validate-valid-build
  (let [v (validator/validate valid-build)]
    (is (:valid v))
    (is (empty? (:errors v)))))

(deftest validate-catches-illegal-edge
  (let [bad (assoc valid-build :edges [["rb1" "ep1"] ["ep1" "out-main"]])
        v   (validator/validate bad)]
    (is (not (:valid v)))
    (is (some #(= :illegal-edge (:code %)) (:errors v)))))

(deftest validate-requires-single-output
  (let [bad (update valid-build :nodes conj {:node-ref "o2" :type "output" :config {}})
        v   (validator/validate bad)]
    (is (not (:valid v)))
    (is (some #(= :invalid-output-count (:code %)) (:errors v)))))

(deftest validate-join-needs-two-parents
  (let [gil {:gil-version "1.0"
             :intent :build
             :graph-name "join-test"
             :nodes [{:node-ref "t1" :type "table" :config {}}
                     {:node-ref "j1" :type "join" :config {:join_type "inner"}}
                     {:node-ref "o1" :type "output" :config {}}]
             :edges [["t1" "j1"] ["j1" "o1"]]}
        v (validator/validate gil)]
    (is (not (:valid v)))
    (is (some #(= :join-needs-two-parents (:code %)) (:errors v)))))

(deftest validate-catches-cycles
  (let [bad (assoc valid-build :edges [["ep1" "rb1"] ["rb1" "ep1"] ["rb1" "out-main"]])
        v   (validator/validate bad)]
    (is (not (:valid v)))
    (is (some #(= :cycle-detected (:code %)) (:errors v)))))

(deftest validate-auth-type
  (let [gil {:gil-version "1.0"
             :intent :build
             :graph-name "auth-test"
             :nodes [{:node-ref "ep1" :type "endpoint" :config {:http_method "GET" :route_path "/x"}}
                     {:node-ref "au1" :type "auth" :config {:auth_type "bogus"}}
                     {:node-ref "o1" :type "output" :config {}}]
             :edges [["ep1" "au1"] ["au1" "o1"]]}
        v (validator/validate gil)]
    (is (not (:valid v)))
    (is (some #(= :invalid-auth-type (:code %)) (:errors v)))))

(deftest validate-patch-with-existing-graph
  (let [g {:n {1 {:na {:name "Output" :btype "O" :gil_ref "out-main"}}
               2 {:na {:name "users-endpoint" :btype "Ep" :gil_ref "ep1"}}}}
        gil {:gil-version "1.0"
             :intent :patch
             :patches [{:op :update-config
                        :ref "ep1"
                        :config {:http_method "GET" :route_path "/users"}}]}
        v (validator/validate gil g)]
    (is (:valid v))
    (is (empty? (:errors v)))))

(deftest plan-gil-build-is-pure
  (let [plan (compiler/plan-gil valid-build)]
    (is (map? plan))
    (is (seq (:steps plan)))
    (is (seq (:layout plan)))
    (is (some #(= {:action :create-graph :name "users-api"} %)
              (:steps plan)))
    (is (some #(and (= :create-node (:action %))
                    (= "ep1" (:ref %)))
              (:steps plan)))))

(deftest plan-gil-patch-is-pure
  (let [gil {:gil-version "1.0"
             :intent :patch
             :patches [{:op :remove-node :ref "rb1"} {:op :add-edge :from "ep1" :to "o1"}]}
        plan (compiler/plan-gil gil)]
    (is (map? plan))
    (is (= 2 (count (:steps plan))))
    (is (= [:patch :patch] (mapv :action (:steps plan))))))

(deftest resolve-node-ref-prefers-gil-ref
  (let [g {:n {1 {:na {:name "Output" :btype "O" :gil_ref "out-main"}}
               2 {:na {:name "users-endpoint" :btype "Ep" :gil_ref "ep1"}}}}]
    (is (= 2 (compiler/resolve-node-ref g "ep1")))
    (is (= 2 (compiler/resolve-node-ref g "users-endpoint")))
    (is (= 1 (compiler/resolve-node-ref g "1")))))
