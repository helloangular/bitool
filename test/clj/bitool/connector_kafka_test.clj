(ns bitool.connector-kafka-test
  (:require [bitool.connector.kafka :as kafka]
            [clojure.core.async :as async]
            [clojure.test :refer :all]))

(deftest fetch-kafka-async-emits-pages-and-commits-offsets
  (let [polls    (atom 0)
        commits  (atom [])
        {:keys [pages errors commit!]}
        (kafka/fetch-kafka-async
         {:topic-name "orders.events"
          :topic-config {:key_deserializer "string"
                         :value_deserializer "json"}
          :poll-fn (fn [_]
                     (swap! polls inc)
                     (case @polls
                       1 {:records [{:topic "orders.events"
                                     :partition 0
                                     :offset 41
                                     :key "k1"
                                     :value "{\"id\":\"o1\"}"}]}
                       {:type :stop :stop-reason :eof}))
          :commit-fn (fn [offsets] (swap! commits conj offsets))})]
    (let [page (async/<!! pages)
          terminal (async/<!! pages)]
      (is (= {:id "o1"} (:_record (first (:body page)))))
      (is (= "{\"orders.events\":{\"0\":41}}" (get-in page [:state :cursor])))
      (commit! (get-in page [:state :offsets]))
      (is (= [{"0" 41}] @commits))
      (is (= :eof (:stop-reason terminal)))
      (is (nil? (async/<!! errors))))))

(deftest fetch-kafka-async-reports-consumer-errors
  (let [{:keys [pages errors]}
        (kafka/fetch-kafka-async
         {:topic-name "orders.events"
          :topic-config {:key_deserializer "string"
                         :value_deserializer "json"}
          :poll-fn (fn [_]
                     (throw (ex-info "boom" {:failure_class "consumer_error"})))})]
    (is (= :error (:stop-reason (async/<!! pages))))
    (is (= :consumer-error (:type (async/<!! errors))))))

(deftest fetch-kafka-async-cancel-closes-consumer-once
  (let [close-count (atom 0)
        {:keys [pages cancel]}
        (kafka/fetch-kafka-async
         {:topic-name "orders.events"
          :topic-config {:value_deserializer "json"}
          :poll-fn (fn [_]
                     (Thread/sleep 10)
                     {:type :stop :stop-reason :eof})
          :close-fn (fn [] (swap! close-count inc))})]
    (cancel)
    (async/<!! pages)
    (Thread/sleep 25)
    (is (= 1 @close-count))))
