(ns user
                            (:require [next.jdbc :as jdbc]
                          [bitool.db :as db]
                           [bitool.connector.api :as api]
 			[clojure.core.async :as async :refer [chan go-loop >! <! close! thread pipeline pipeline-blocking]]
        ;;                   (:require [bitool.connector.schema :as sch])
        ;;                   (:require [bitool.connector.api-discovery :as dsc])
                             [bitool.api.conn :as co]
                             [bitool.api.schema :as sc]
                             [bitool.api.gschema :as gs]
                             [bitool.api.jsontf :as jt]
                             [bitool.graph2 :as g2]
                           [com.rpl.specter :as sp]
                           [cheshire.core :as json]
                           [bitool.routes.home :as h]
                             [clojure.pprint :as pp]
                            [taoensso.timbre :as log]
                            [taoensso.telemere :as tel]))
