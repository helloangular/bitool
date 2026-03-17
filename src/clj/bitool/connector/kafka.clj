(ns bitool.connector.kafka
  (:require [cheshire.core :as json]
            [clojure.core.async :as async]
            [clojure.string :as string]
            [clojure.tools.logging :as log])
  (:import [java.lang.reflect InvocationTargetException]
           [java.time Duration]
           [java.util Collections Properties]))

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

(defn kafka-client-available?
  []
  (try
    (Class/forName "org.apache.kafka.clients.consumer.KafkaConsumer")
    true
    (catch Throwable _
      false)))

(defn- kafka-class!
  [class-name]
  (try
    (Class/forName class-name)
    (catch Throwable t
      (throw (ex-info "Kafka client libraries are not available on the classpath"
                      {:failure_class "unsupported"
                       :class_name class-name}
                      t)))))

(defn- invocation-target
  [throwable]
  (if (instance? InvocationTargetException throwable)
    (or (.getTargetException ^InvocationTargetException throwable) throwable)
    throwable))

(defn- wakeup-exception?
  [throwable]
  (= "org.apache.kafka.common.errors.WakeupException"
     (.getName (class throwable))))

(defn- map->properties
  [m]
  (let [props (Properties.)]
    (doseq [[k v] m]
      (when (some? v)
        (.setProperty props (str k) (str v))))
    props))

(defn- parse-json-safe
  [value]
  (cond
    (nil? value) nil
    (map? value) value
    (string? value) (try
                      (json/parse-string value true)
                      (catch Exception _
                        nil))
    :else nil))

(defn- resolved-initial-offsets
  [topic-name initial-cursor]
  (let [cursor-map (parse-json-safe initial-cursor)
        topic-map  (cond
                     (map? (get cursor-map topic-name)) (get cursor-map topic-name)
                     (map? cursor-map) cursor-map
                     :else nil)]
    (into {}
          (keep (fn [[partition offset]]
                  (when (some? offset)
                    [(str partition) (long offset)])))
          topic-map)))

(defn- topic-partition-instance
  [topic partition]
  (let [clazz (kafka-class! "org.apache.kafka.common.TopicPartition")
        ctor  (.getConstructor clazz (into-array Class [String Integer/TYPE]))]
    (.newInstance ctor (object-array [(str topic) (int partition)]))))

(defn- offset-metadata-instance
  [offset]
  (let [clazz (kafka-class! "org.apache.kafka.clients.consumer.OffsetAndMetadata")
        ctor  (.getConstructor clazz (into-array Class [Long/TYPE]))]
    (.newInstance ctor (object-array [(long offset)]))))

(defn- record->map
  [record]
  {:topic (.topic record)
   :partition (.partition record)
   :offset (.offset record)
   :timestamp (.timestamp record)
   :headers (some->> (.headers record)
                     iterator-seq
                     (mapv (fn [header]
                             {:key (.key header)
                              :value (.value header)})))
   :key (.key record)
   :value (.value record)})

(defn- assign-and-seek!
  [consumer topic-name initial-offsets poll-timeout-ms sought?]
  (when (and (not @sought?)
             (seq initial-offsets))
    (let [assignment (.assignment consumer)]
      (when (.isEmpty assignment)
        (.poll consumer (Duration/ofMillis (long (or poll-timeout-ms 1000)))))
      (let [assignment (.assignment consumer)]
        (when-not (.isEmpty assignment)
          (doseq [tp assignment]
            (when-let [offset (get initial-offsets (str (.partition tp)))]
              ;; Checkpoints store the last successfully processed offset.
              ;; Seek to the next offset so normal restart does not replay the same message.
              (.seek consumer tp (long (inc (long offset))))))
          (reset! sought? true))))))

(defn native-consumer-ops
  [{:keys [source-node topic-config topic-name initial-cursor stop-flag pause-flag]
    :or {stop-flag (atom false)
         pause-flag (atom false)}}]
  (when-not (kafka-client-available?)
    (throw (ex-info "Kafka client libraries are not available on the classpath"
                    {:failure_class "unsupported"})))
  (let [topic-name      (or topic-name (:topic_name topic-config))
        options         (merge (:options source-node) (:options topic-config))
        poll-timeout-ms (long (or (:poll_timeout_ms topic-config)
                                  (:poll_timeout_ms source-node)
                                  1000))
        consumer-class  (kafka-class! "org.apache.kafka.clients.consumer.KafkaConsumer")
        ctor            (.getConstructor consumer-class (into-array Class [java.util.Map]))
        consumer        (.newInstance ctor
                                      (object-array
                                       [(map->properties
                                         (merge {"bootstrap.servers" (:bootstrap_servers source-node)
                                                 "group.id" (or (:consumer_group_id source-node)
                                                                (str "bitool-" topic-name))
                                                 "enable.auto.commit" "false"
                                                 "auto.offset.reset" (or (:auto_offset_reset topic-config) "earliest")
                                                 "key.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
                                                 "value.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
                                                 "security.protocol" (or (:security_protocol source-node) "PLAINTEXT")
                                                 "max.poll.records" (str (or (:max_poll_records topic-config) 500))}
                                                options))]))
        sought?         (atom false)
        closed?         (atom false)
        initial-offsets (resolved-initial-offsets topic-name initial-cursor)
        safe-close      (fn []
                          (when (compare-and-set! closed? false true)
                            (try
                              (.close consumer)
                              (catch Throwable t
                                (log/warn t "Failed to close Kafka consumer"
                                          {:topic topic-name})))))
        pause!          (fn []
                          (reset! pause-flag true)
                          (try
                            (let [assignment (.assignment consumer)]
                              (when-not (.isEmpty assignment)
                                (.pause consumer assignment)))
                            (catch Throwable t
                              (log/warn t "Failed to pause Kafka consumer"
                                        {:topic topic-name}))))
        resume!         (fn []
                          (reset! pause-flag false)
                          (try
                            (let [assignment (.assignment consumer)]
                              (when-not (.isEmpty assignment)
                                (.resume consumer assignment)))
                            (catch Throwable t
                              (log/warn t "Failed to resume Kafka consumer"
                                        {:topic topic-name}))))
        stop!           (fn []
                          (reset! stop-flag true)
                          (try
                            (.wakeup consumer)
                            (catch Throwable _
                              nil)))]
    (try
      (.subscribe consumer (Collections/singletonList topic-name))
      (catch Throwable t
        (safe-close)
        (throw t)))
    {:poll-timeout-ms poll-timeout-ms
     :poll-fn (fn [_]
                (cond
                  @stop-flag
                  {:type :stop :stop-reason :cancelled}

                  @pause-flag
                  (do
                    (Thread/sleep (min poll-timeout-ms 1000))
                    {:records []})

                  :else
                  (try
                    (assign-and-seek! consumer topic-name initial-offsets poll-timeout-ms sought?)
                    (let [records (.poll consumer (Duration/ofMillis poll-timeout-ms))]
                      {:records (mapv record->map records)})
                    (catch Throwable t
                      (let [root (invocation-target t)]
                        (if (and @stop-flag (wakeup-exception? root))
                          {:type :stop :stop-reason :cancelled}
                          (throw root)))))))
     :commit-fn (fn [offsets]
                  (when (seq offsets)
                    (let [offset-map (java.util.HashMap.)]
                      (doseq [[partition offset] offsets]
                        (.put offset-map
                              (topic-partition-instance topic-name (Integer/parseInt (str partition)))
                              (offset-metadata-instance (inc (long offset)))))
                      (.commitSync consumer offset-map))))
     :close-fn safe-close
     :wakeup-fn stop!
     :pause! pause!
     :resume! resume!
     :stop! stop!}))

(defn- offer-with-stop!
  [ch stop? value]
  (loop []
    (cond
      @stop? false
      :else
      (let [timeout-ch (async/timeout 250)
            [_ port]   (async/alts!! [[ch value] timeout-ch] :priority true)]
        (cond
          (= port ch) true
          @stop? false
          :else (recur))))))

(defn- drain-pending-commit!
  [pending-commit commit-fn]
  (when commit-fn
    (when-let [offsets (let [pending @pending-commit]
                         (when pending
                           (reset! pending-commit nil)
                           pending))]
      (commit-fn offsets))))

(defn fetch-kafka-async
  [{:keys [topic-name topic-config initial-cursor poll-timeout-ms rate-limit-ms poll-fn commit-fn close-fn wakeup-fn]
    :or {poll-timeout-ms 1000 rate-limit-ms 0}}]
  (let [pages-ch  (async/chan 500)
        errors-ch (async/chan 10)
        stop?     (atom false)
        pending-commit (atom nil)
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
                  (drain-pending-commit! pending-commit commit-fn)
                  (let [poll-result (poll-fn {:poll-number poll-number
                                              :topic-name topic-name
                                              :poll-timeout-ms poll-timeout-ms
                                              :state @state*})]
                    (cond
                      (default-stop? poll-result)
                      (do
                        (offer-with-stop! pages-ch stop? {:stop-reason (or (:stop-reason poll-result) :eof)
                                                          :state @state*
                                                          :http-status 200})
                        ::stop)

                      (seq (:records poll-result))
                      (let [page (records->page topic-name (inc poll-number) (:records poll-result) topic-config)]
                        (reset! state* (:state page))
                        (offer-with-stop! pages-ch stop? page)
                        (when (pos? (long rate-limit-ms))
                          (Thread/sleep (long rate-limit-ms)))
                        (inc poll-number))

                      :else poll-number))
                  (catch Throwable t
                    (offer-with-stop! errors-ch stop? {:type :consumer-error
                                                       :error t})
                    (offer-with-stop! pages-ch stop? {:stop-reason :error
                                                      :state @state*
                                                      :http-status nil})
                    ::stop))]
            (when-not (= ::stop next-state)
              (recur next-state)))))
      (try
        (drain-pending-commit! pending-commit commit-fn)
        (catch Throwable t
          (offer-with-stop! errors-ch stop? {:type :commit-error
                                             :error t})))
      (async/close! pages-ch)
      (async/close! errors-ch)
      (safe-close))
    {:pages pages-ch
     :errors errors-ch
     :commit! (fn [offsets]
                (when (seq offsets)
                  (if wakeup-fn
                    (swap! pending-commit #(merge (or % {}) offsets))
                    (when commit-fn
                      (commit-fn offsets)))))
     :cancel (fn []
               (reset! stop? true)
               (when wakeup-fn
                 (try
                   (wakeup-fn)
                   (catch Exception e
                     (log/warn e "Failed to wake up Kafka consumer during cancel")))))}))
