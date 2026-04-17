(ns bitool.scripts.refresh-graph-proposals
  (:require [bitool.modeling.automation :as modeling]
            [clojure.pprint :as pp]))

(defn -main
  [& args]
  (let [graph-id (some-> (first args) Long/parseLong)
        result   (modeling/refresh-proposals-for-graph-target! graph-id {:updated_by "script"})]
    (pp/pprint result)))
