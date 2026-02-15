(ns bitool.env
  (:require [clojure.tools.logging :as log]))

(def defaults
  {:init
   (fn []
     (log/info "\n-=[bitool started successfully]=-"))
   :stop
   (fn []
     (log/info "\n-=[bitool has shut down successfully]=-"))
   :middleware identity})
