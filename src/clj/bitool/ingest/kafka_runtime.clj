(ns bitool.ingest.kafka-runtime
  (:require [bitool.ingest.runtime :as runtime]))

(defn run-kafka-node!
  ([graph-id node-id] (runtime/run-kafka-node! graph-id node-id))
  ([graph-id node-id opts] (runtime/run-kafka-node! graph-id node-id opts)))
