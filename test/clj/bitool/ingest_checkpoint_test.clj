(ns bitool.ingest-checkpoint-test
  (:require [bitool.ingest.checkpoint :as checkpoint]
            [clojure.test :refer :all]))

(deftest watermark-query-param-prefers-explicit-watermark-param
  (is (= "updated_after"
         (checkpoint/watermark-query-param
          {:watermark_param "updated_after"
           :time_param "updated_since"
           :watermark_column "data_updatedAtTime"}))))

(deftest watermark-query-params-falls-back-to-time-param
  (let [checkpoint-row {:last_successful_watermark "2026-03-18T14:12:06.462Z"}
        endpoint-config {:time_param "updated_since"
                         :watermark_column "data_updatedAtTime"}
        now (java.time.Instant/parse "2026-03-23T00:00:00Z")]
    (is (= {:updated_since "2026-03-18T14:12:06.462Z"}
           (checkpoint/watermark-query-params checkpoint-row endpoint-config now)))))

(deftest watermark-query-params-supports-oracle-utc-timestamp-format
  (let [checkpoint-row {:last_successful_watermark "2023-12-08 08:18:47.000000 UTC"}
        endpoint-config {:time_param "updatedAfter"
                         :watermark_column "data_items_LAST_UPDATE_DATE"}
        now (java.time.Instant/parse "2026-03-25T00:00:00Z")]
    (is (= {:updatedAfter "2023-12-08T08:18:47Z"}
           (checkpoint/watermark-query-params checkpoint-row endpoint-config now)))))
