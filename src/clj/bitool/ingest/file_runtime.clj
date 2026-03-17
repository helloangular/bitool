(ns bitool.ingest.file-runtime
  (:require [bitool.ingest.runtime :as runtime]))

(defn run-file-node!
  ([graph-id node-id] (runtime/run-file-node! graph-id node-id))
  ([graph-id node-id opts] (runtime/run-file-node! graph-id node-id opts)))
