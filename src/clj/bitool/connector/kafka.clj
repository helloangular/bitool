(ns bitool.connector.kafka
  (:require [cheshire.core :as json]
            [clojure.core.async :as async]
            [clojure.string :as string]
            [clojure.tools.logging :as log]))

(defn- parse-json-payload
  [value]
  (cond
    (nil? value) nil
    (map? value) value
    (vector? value) value
    (bytes? value) (json/parse-string (String. ^bytes value "UTF-8") true)
    :else (json/parse-string (str value) true)))

(defn- deserialize-value
  [deserializer value]
  (case (some-> deserializer name string/lower-case)
    ("string") (cond
                 (nil? value) nil
                 (bytes? value) (String. ^bytes value "UTF-8")
                 :else (str value))
    ("json") (parse-json-payload value)
    ("bytes") value
    ("avro" "protobuf")
    (throw (ex-info "Deserializer requires an external decoder function"
                    {:deserializer deserializer
                     :failure_class "config_error"}))
    value))

(defn- normalize-record
  [record {:keys [key_deserializer value_deserializer value_decoder key_decoder]}]
  (let [key*   (if key_decoder (key_decoder record) (:key record))
        value* (if value_decoder (value_decoder record) (:value record))]
    {:topic (:topic record)
     :partition (:partition record)
     :offset (:offset record)
     :timestamp (:timestamp record)
     :headers (:headers record)
     :key (if key_decoder key* (deserialize-value key_deserializer key*))
     :value (if value_decoder value* (deserialize-value value_deserializer value*))}))

(defn- offsets->cursor
  [topic offsets]
  (json/generate-string {topic offsets}))

(defn- records->page
  [topic poll-number records opts]
  (let [normalized (mapv #(normalize-record % opts) records)
        offsets    (reduce (fn [acc {:keys [partition offset]}]
                             (assoc acc (str partition) (long offset)))
                           {}
                           normalized)
        body       (mapv (fn [{:keys [key value topic partition offset timestamp headers]}]
                           (cond-> {:_record value
                                    :kafka_topic topic
                                    :kafka_partition partition
                                    :kafka_offset offset
                                    :kafka_timestamp timestamp}
                             (some? key) (assoc :kafka_key key)
                             (seq headers) (assoc :kafka_headers headers)))
                         normalized)]
    {:body body
     :page poll-number
     :state {:offsets offsets
             :cursor (offsets->cursor topic offsets)}
     :response {:status 200}}))

(defn- default-stop?
  [poll-result]
  (or (nil? poll-result)
      (= :stop (:type poll-result))
      (true? (:stop? poll-result))))

(defn fetch-kafka-async
  [{:keys [topic-name topic-config initial-cursor poll-timeout-ms rate-limit-ms poll-fn commit-fn close-fn]
    :or {poll-timeout-ms 1000 rate-limit-ms 0}}]
  (let [pages-ch  (async/chan 500)
        errors-ch (async/chan 10)
        stop?     (atom false)
        closed?   (atom false)
        safe-close (fn []
                     (when (compare-and-set! closed? false true)
                       (when close-fn
                         (try
                           (close-fn)
                           (catch Exception e
                             (log/warn e "Failed to close Kafka consumer"))))))
        state*    (atom {:cursor initial-cursor})]
    (async/thread
      (loop [poll-number 0]
        (when-not @stop?
          (let [next-state
                (try
                  (let [poll-result (poll-fn {:poll-number poll-number
                                              :topic-name topic-name
                                              :poll-timeout-ms poll-timeout-ms
                                              :state @state*})]
                    (cond
                      (default-stop? poll-result)
                      (do
                        (async/>!! pages-ch {:stop-reason (or (:stop-reason poll-result) :eof)
                                             :state @state*
                                             :http-status 200})
                        ::stop)

                      (seq (:records poll-result))
                      (let [page (records->page topic-name (inc poll-number) (:records poll-result) topic-config)]
                        (reset! state* (:state page))
                        (async/>!! pages-ch page)
                        (when (pos? (long rate-limit-ms))
                          (Thread/sleep (long rate-limit-ms)))
                        (inc poll-number))

                      :else poll-number))
                  (catch Throwable t
                    (async/>!! errors-ch {:type :consumer-error
                                          :error t})
                    (async/>!! pages-ch {:stop-reason :error
                                         :state @state*
                                         :http-status nil})
                    ::stop))]
            (when-not (= ::stop next-state)
              (recur next-state)))))
      (async/close! pages-ch)
      (async/close! errors-ch)
      (safe-close))
    {:pages pages-ch
     :errors errors-ch
     :commit! (fn [offsets]
                (when commit-fn
                  (commit-fn offsets)))
     :cancel (fn []
               (reset! stop? true))}))
