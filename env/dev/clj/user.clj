(ns user
  "Userspace functions you can run by default in your local REPL."
  (:require
    ;; Luminus / app dev stuff
    [bitool.config :refer [env]]
    [clojure.pprint :as pp]
    [clojure.spec.alpha :as s]
    [expound.alpha :as expound]
    [mount.core :as mount]
    [bitool.core :refer [start-app]]

    ;; Your helper requires from custom user.clj
    [next.jdbc :as jdbc]
    [bitool.db :as db]
    [bitool.connector.api :as api]
    [clojure.core.async :as async
     :refer [chan go-loop >! <! close! thread pipeline pipeline-blocking]]
    [bitool.api.conn :as co]
    [bitool.api.schema :as sc]
    [bitool.api.gschema :as gs]
    [bitool.api.jsontf :as jt]
    [bitool.graph2 :as g2]
    [com.rpl.specter :as sp]
    [cheshire.core :as json]
    [bitool.routes.home :as h]
    [taoensso.timbre :as log]
    [taoensso.telemere :as tel]))

;; nicer spec errors
(alter-var-root #'s/*explain-out* (constantly expound/printer))

;; pretty tap> output
(add-tap (bound-fn* pp/pprint))

(defn start
  "Starts application.
  You'll usually want to run this on startup."
  []
  (mount/start-without #'bitool.core/repl-server))

(defn stop
  "Stops application."
  []
  (mount/stop-except #'bitool.core/repl-server))

(defn restart
  "Restarts application."
  []
  (stop)
  (start))



