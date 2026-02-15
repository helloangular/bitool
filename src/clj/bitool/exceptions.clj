(ns bitool.exceptions)

(defn exfota ;;ex-info-data
  "Helper to create exception data"
  [type message data]
  (ex-info message (assoc data :type type)))

;; Custom exception creators
(defn valid-err [message data]
  (exfota :valid-err message data))

(defn not-found-err [message data]
  (exfota :not-found message data))

(defn db-err [message data]
  (exfota :database-err message data))
