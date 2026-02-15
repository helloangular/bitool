(ns bitool.connector.pagination-smoke-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [<! >! go chan close! <!!]]
            [bitool.connector.pagination :as pg]))

;; Helpers ---------------------------------------------------------------------

(defn drain!
  "Take all values from ch until it closes; return a vector of them."
  [ch]
  (let [out (transient [])]
    (loop []
      (if-some [v (<!! ch)]
        (do (conj! out v) (recur))
        (persistent! out)))))

(defn mk-fetch-ch [] (chan 100))

(defn wait-close!
  "Blocks until the channel closes (consumes remaining values). Returns count."
  [ch]
  (count (drain! ch)))

;; Smoke tests -----------------------------------------------------------------

(deftest offset-pagination-with-total
  (testing "offset pagination advances correctly and stops at total"
    (let [calls (atom 0)
          ;; Simulate an API that returns :items and :total=3, page-size=2
          fake-http (fn [_url {:keys [query-params]}]
                      (let [offset (or (:offset query-params) 0)
                            limit  (or (:limit  query-params) 2)
                            data   (subvec [1 2 3] offset (min 3 (+ offset limit)))]
                        (swap! calls inc)
                        {:status 200
                         :headers {}
                         :body {:items data :total 3}}))
          cfg {:pagination {:strategy :offset
                            :page-size 2
                            :params {}
                            :data-path [:items]
                            :total-path :total
                            :max-pages 10}}
          strategy (pg/->ConfigurableOffsetPagination)
          fetch-ch (mk-fetch-ch)]
      (with-redefs [pg/fetch-page-with-config
                    (fn [url params config]
                      ;; Respect our patched fetcher contract: returns a channel
                      (async/thread
                        (try
                          (let [resp (fake-http url {:query-params params})]
                            (assoc resp :success? (< (:status resp) 400)))
                          (catch Exception e
                            {:success? false :error (.getMessage e)}))))]
        (pg/start-configurable-seq-fetch! strategy "http://api.example.com/p" cfg fetch-ch)
        (let [pages (drain! fetch-ch)
              seen  (mapcat (comp :items :body) pages)]
          (is (= 2 @calls) "Should call twice (2 + 1 items)")
          (is (= [1 2 3] seen))
          (is (= [2 1] (map (comp count :items :body) pages))))))))

(deftest link-header-pagination-follows-next
  (testing "link header pagination uses :next-url and stops when absent"
    (let [calls (atom [])
          fake-http (fn [url _]
                      (swap! calls conj url)
                      (cond
                        (= url "http://api.example.com/p")
                        {:status 200
                         :headers {"link" "<http://api.example.com/p?page=2>; rel=\"next\""}
                         :body {:items [1 2]}}

                        (= url "http://api.example.com/p?page=2")
                        {:status 200
                         :headers {"link" ""} ;; no next
                         :body {:items [3]}}

                        :else {:status 404 :headers {} :body {}}))
          cfg {:pagination {:strategy :link-header
                            :data-path [:items]
                            :link-header-name "link"
                            :next-rel-name "next"
                            :max-pages 10}}
          strategy (pg/->ConfigurableLinkHeaderPagination)
          fetch-ch (mk-fetch-ch)]
      (with-redefs [pg/fetch-page-with-config
                    (fn [url params config]
                      (async/thread
                        (let [resp (fake-http url {:query-params params})]
                          (assoc resp :success? (< (:status resp) 400)))))]
        (pg/start-configurable-seq-fetch! strategy "http://api.example.com/p" cfg fetch-ch)
        (let [pages (drain! fetch-ch)
              seen  (mapcat (comp :items :body) pages)]
          (is (= ["http://api.example.com/p"
                  "http://api.example.com/p?page=2"] @calls))
          (is (= [1 2 3] seen))
          (is (= 2 (count pages))))))))

(deftest rate-limiter-guards
  (testing "rate limiter works with nil/0 and high values without throwing"
    (is (fn? (pg/create-rate-limiter nil)))
    (is (fn? (pg/create-rate-limiter 0)))
    (is (fn? (pg/create-rate-limiter 10000)))))

;; Runner ----------------------------------------------------------------------

(defn -main [& _]
  (let [res (run-tests 'bitool.connector.pagination-smoke-test)]
    (when (pos? (+ (:fail res) (:error res)))
      (System/exit 1))))

(comment
  ;; How to run (from project root):
  ;; 1) Ensure src path contains bitool/connector/pagination.clj from pagination_fixed.clj
  ;; 2) Save this file as test/bitool/connector/pagination_smoke_test.clj
  ;; 3) Run:
  ;;    clj -M:test -m bitool.connector.pagination-smoke-test
  ;; or if using Leiningen:
  ;;    lein test :only bitool.connector.pagination-smoke-test
  )
