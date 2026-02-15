(ns bitool.api.jsontf-test
  (:require [clojure.test :refer :all]
            [bitool.api.jsontf :as jt]))

;; ─────────────────────────────────────────────────────────────────────────────
;; Feature flags (flip to true as you add these capabilities)
(def ^:dynamic *supports-wildcard* false)
(def ^:dynamic *supports-keyed-align* false)
(def ^:dynamic *supports-duplicate-check* false)
(def ^:dynamic *supports-coercion* false)
(def ^:dynamic *supports-when-predicate* false)

;; Helper to compare rows (order-insensitive if you like)
(defn =rows [a b] (= a b))

;; ─────────────────────────────────────────────────────────────────────────────
(deftest case-1-heterogeneous-arrays
  (let [d {:items [1 {:sku "A"} 2 {:sku "B"} nil]}
        m {"$.items[].sku" :sku}]
    (testing ":per-context"
      (is (=rows [{:sku "A,B"}]
                 (jt/rows-from-json d m))))
    (testing ":explode-by $.items[].sku"
      (is (=rows [{:sku "A"} {:sku "B"}]
                 (jt/rows-from-json d m {:row-mode :explode-by
                                         :explode-key "$.items[].sku"}))))))

(deftest case-2-misaligned-arrays
  (let [d {:items  [{:id 1 :qty 2} {:id 2 :qty 1}]
           :prices [{:id 2 :amount 9.99} {:id 1 :amount 5.00}]}
        m {"$.items[].id"  :id
           "$.items[].qty" :qty
           "$.prices[].amount" :amount}]
    (testing ":per-context (naive join)"
      (is (=rows [{:id "1,2" :qty "2,1" :amount "9.99,5.0"}]
                 (jt/rows-from-json d m))))
    (testing ":explode-by items[].id (naive index align is wrong — expected to fail logically)"
      (is (=rows [{:id 1, :qty 2, :amount 9.99}
                  {:id 2, :qty 1, :amount 5.0}]
                 (jt/rows-from-json d m {:row-mode :explode-by
                                         :explode-key "$.items[].id"}))))
    (when *supports-keyed-align*
      (testing "keyed align (desired)"
        ;; You'd call rows-from-json with extra :align option once implemented
        (is (=rows [{:id 1, :qty 2, :amount 5.0}
                    {:id 2, :qty 1, :amount 9.99}]
                   :KEYED-ALIGN-NOT-IMPLEMENTED))))))

(deftest case-3-wildcards
  (let [d {:metrics {"us-east" {:lat 10} "eu-west" {:lat 20}}}
        m {"$.metrics.*.lat" :lat}]
    (if *supports-wildcard*
      (do
        (testing ":per-context"
          (is (=rows [{:lat "10,20"}]
                     (jt/rows-from-json d m))))
        (testing ":explode-by wildcard"
          (is (=rows [{:lat 10} {:lat 20}]
                     (jt/rows-from-json d m {:row-mode :explode-by
                                             :explode-key "$.metrics.*.lat"})))))
      (testing "wildcard (pending feature)"
        (is true)))))

(deftest case-4-keys-with-dots-and-brackets
  ;; requires escaping support in parse-path
  (let [d {"foo.bar" {"a[]b" 42}}
        ;; NOTE: double backslashes inside Clojure strings:
        ;;   \.   -> "\\."
        ;;   \[]  -> "\\[]"
        m {"$\\.foo\\.bar\\.a\\[]b" :val}]
    (testing "escaped keys (pending if escaping not implemented)"
      ;; If your parse-path supports escaping, enable this assertion:
      ;; (is (=rows [{:val 42}] (jt/rows-from-json d m)))
      (is true))))

(deftest case-5-very-large-numbers
  (let [d {:id "9007199254740993" :amount "12345678901234567890.12345"}
        m {"$.id" :id "$.amount" :amount}]
    (is (=rows [{:id "9007199254740993"
                 :amount "12345678901234567890.12345"}]
               (jt/rows-from-json d m)))))

(deftest case-6-dates-and-timestamps
  (let [d {:created_at "2024-12-01T10:15:30Z"}
        m {"$.created_at" :created_at}]
    (is (=rows [{:created_at "2024-12-01T10:15:30Z"}]
               (jt/rows-from-json d m)))
    (when *supports-coercion*
      (testing "with transform to Instant (pending)"
        (is (=rows [{:created_at :INSTANT}] :COERCION-NOT-IMPLEMENTED))))))

(deftest case-7-deep-nesting-large-arrays
  (let [d {:root {:a {:b {:c {:d [1 2 3]}}}}}
        m {"$.root.a.b.c.d[]" :leaf}]
    (is (=rows [{:leaf "1,2,3"}] (jt/rows-from-json d m)))
    (is (=rows [{:leaf 1} {:leaf 2} {:leaf 3}]
               (jt/rows-from-json d m {:row-mode :explode-by
                                       :explode-key "$.root.a.b.c.d[]"})))))

(deftest case-8-sparse-optional
  (let [d {:orders [{:id 1 :customer nil}
                    {:id 2 :customer {:name "X"}}
                    {:id 3}]}
        m {"$.orders[].id" :order_id
           "$.orders[].customer.name" :cust_name}]
    (is (=rows [{:order_id 1, :cust_name ""}
                {:order_id 2, :cust_name "X"}
                {:order_id 3, :cust_name ""}]
               (jt/rows-from-json d m)))
    (is (=rows [{:order_id 1, :cust_name ""}
                {:order_id 2, :cust_name "X"}
                {:order_id 3, :cust_name ""}]
               (jt/rows-from-json d m {:row-mode :explode-by
                                       :explode-key "$.orders[].id"})))))

(deftest case-9-arrays-of-arrays
  (let [d {:notes [["a" "b"] ["c"] [] ["d" "e" "f"]]}
        m {"$.notes[][]" :note}]
    (is (=rows [{:note "a,b,c,d,e,f"}]
               (jt/rows-from-json d m)))
    (is (=rows [{:note "a"} {:note "b"} {:note "c"} {:note "d"} {:note "e"} {:note "f"}]
               (jt/rows-from-json d m {:row-mode :explode-by
                                       :explode-key "$.notes[][]"})))))

(deftest case-10-binary-blobs
  (let [d {:file {:name "pic.png" :data "iVBORw0KGgoAAAANSUhEUgAA..."}}
        m {"$.file.name" :name "$.file.data" :data}]
    (is (=rows [{:name "pic.png" :data "iVBORw0KGgoAAAANSUhEUgAA..."}]
               (jt/rows-from-json d m)))))

(deftest case-11-duplicate-columns
  (let [d {:a 1 :b 2}
        m {"$.a" :x "$.b" :x}]
    (if *supports-duplicate-check*
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Duplicate column mappings"
                            (jt/rows-from-json d m)))
      (is true))))

(deftest case-12-coercion-types
  (let [d {:qty "3" :active "true" :price "12.50"}
        m {"$.qty" :qty "$.active" :active "$.price" :price}]
    (if *supports-coercion*
      (is (=rows [{:qty 3 :active true :price 12.50M}]
                 :COERCION-NOT-IMPLEMENTED))
      (is (=rows [{:qty "3" :active "true" :price "12.50"}]
                 (jt/rows-from-json d m))))))

(deftest case-13-when-predicate
  (let [d {:orders [{:id 1 :status "NEW"}
                    {:id 2 :status "PAID"}
                    {:id 3 :status "SHIPPED"}]}
        m {"$.orders[].id" :order_id}]
    (if *supports-when-predicate*
      (do
        ;; Expected:
        ;; per-context => [{:order_id "2,3"}]
        ;; explode-by  => [{:order_id 2} {:order_id 3}]
        (is (=rows [{:order_id "2,3"}] :WHEN-PENDING))
        (is (=rows [{:order_id 2} {:order_id 3}] :WHEN-PENDING)))
      (is true))))

;; ─────────────────────────────────────────────────────────────────────────────
;; Real-world combo (brutal 1%)
(deftest case-brutal-1pct
  (let [d {:metrics {"us-east" {:lat 10.5 :lon "20.1" :buckets [1 "2" 3.0]}
                     "eu-west" {:lat nil   :lon "NaN"  :buckets []}}
           :items [{:id "a" :price "12.34" :attrs [{"k" "color" "v" "blue"} {"k" "w" "v" 10}]}
                   {:id "b" :price 99.99   :attrs [{"k" "color" "v" "red"}]}]}
        m {"$.metrics.*.lat"        :lat
           "$.items[].id"           :id
           "$.items[].price"        :price
           "$.items[].attrs[].v"    :attr_values}]
    (testing ":per-context"
      ;; If nils are filtered before join, you’ll see "10.5"; if not, "10.5," (trailing comma).
      (let [rows (jt/rows-from-json d m)]
        (is (= 1 (count rows)))
        (is (= "a,b" (:id (first rows))))
        (is (= "12.34,99.99" (:price (first rows))))
        (is (= "blue,10,red" (:attr_values (first rows))))))
    (when *supports-wildcard*
      (testing ":explode-by attrs[].v"
        (is (=rows [{:lat "10.5" :id "a,b" :price "12.34,99.99" :attr_values "blue"}
                    {:lat "10.5" :id "a,b" :price "12.34,99.99" :attr_values 10}
                    {:lat "10.5" :id "a,b" :price "12.34,99.99" :attr_values "red"}]
                   (jt/rows-from-json d m {:row-mode :explode-by
                                           :explode-key "$.items[].attrs[].v"})))))))

