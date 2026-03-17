(ns bitool.utils-test
  (:require [bitool.utils :as utils]
            [clojure.test :refer :all]))

(deftest path->name-sanitizes-array-and-special-character-paths
  (is (= "data_items_id" (utils/path->name "$.data[].id")))
  (is (= "data_items_vehicle_id" (utils/path->name "$.data[].vehicle.id")))
  (is (= "data_items_tags_items" (utils/path->name "$.data[].tags[]")))
  (is (= "col_2026_value" (utils/path->name "$.2026-value"))))

(deftest path->name-handles-empty-and-nil-inputs-deterministically
  (is (= "col" (utils/path->name "")))
  (is (= "col" (utils/path->name nil)))
  (is (= "col" (utils/path->name "$.")))
  (is (= "items_items" (utils/path->name "[][]")))
  (is (= (utils/path->name "$.data[].vehicle.id")
         (utils/path->name "$.data[].vehicle.id"))))
