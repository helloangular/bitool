(ns bitool.env
  (:require
    [selmer.parser :as parser]
    [clojure.tools.logging :as log]
    [bitool.dev-middleware :refer [wrap-dev]]))

(def defaults
  {:init
   (fn []
     (parser/cache-off!)
     (log/info "\n-=[bitool started successfully using the development profile]=-"))
   :stop
   (fn []
     (log/info "\n-=[bitool has shut down successfully]=-"))
   :middleware wrap-dev})
