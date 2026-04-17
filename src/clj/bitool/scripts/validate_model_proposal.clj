(ns bitool.scripts.validate-model-proposal
  (:require [bitool.modeling.automation :as modeling]
            [clojure.pprint :as pp]))

(defn -main
  [& args]
  (let [[layer proposal-id-str] args
        proposal-id (Long/parseLong proposal-id-str)
        result      (case layer
                      "silver" (modeling/validate-silver-proposal! proposal-id {:created_by "script"})
                      "gold" (modeling/validate-gold-proposal! proposal-id {:created_by "script"})
                      (throw (ex-info "layer must be silver or gold" {:layer layer})))]
    (pp/pprint (select-keys result [:proposal_id :status :compiled_sql]))))
