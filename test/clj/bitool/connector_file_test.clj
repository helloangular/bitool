(ns bitool.connector-file-test
  (:require [bitool.connector.file :as file-connector]
            [bitool.ingest.bronze :as bronze]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.test :refer :all]))

(defn- temp-file!
  [name content]
  (let [file (java.io.File/createTempFile name ".tmp")]
    (spit file content)
    (.deleteOnExit file)
    (.getAbsolutePath file)))

(deftest fetch-files-async-reads-jsonl-and-builds-checksum-cursor
  (let [path (temp-file! "orders" "{\"id\":\"o1\"}\n{\"id\":\"o2\"}\n")
        {:keys [pages errors]}
        (file-connector/fetch-files-async
         {:source-node {:base_path ""}
          :file-config {:transport "local"
                        :format "jsonl"
                        :paths [path]}})]
    (let [page (async/<!! pages)
          terminal (async/<!! pages)]
      (is (= 2 (count (:body page))))
      (is (= "o1" (:id (first (:body page)))))
      (is (string? (get-in page [:state :cursor])))
      (is (= :eof (:stop-reason terminal)))
      (is (nil? (async/<!! errors))))))

(deftest fetch-files-async-parses-fixed-width-copybook
  (let [path (temp-file! "fixed" "ABC12\nXYZ34\n")
        {:keys [pages]}
        (file-connector/fetch-files-async
         {:source-node {:base_path ""}
          :file-config {:transport "local"
                        :format "fixed_width"
                        :paths [path]
                        :copybook "05 NAME PIC X(3).\n05 COUNT PIC 9(2)."}})]
    (is (= {:name "ABC" :count 12}
           (first (:body (async/<!! pages)))))))

(deftest parse-copybook-expands-occurs-and-skips-redefines
  (is (= [{:name "item_1" :length 2 :type "string" :scale 0 :start 1}
          {:name "item_2" :length 2 :type "string" :scale 0 :start 3}]
         (mapv #(select-keys % [:name :length :type :scale :start])
               (file-connector/parse-copybook
                "05 ITEM PIC X(2) OCCURS 2 TIMES.\n05 ALT REDEFINES ITEM PIC 9(4).")))))

(deftest fetch-files-async-keeps-malformed-fixed-width-rows-in-page
  (let [path (temp-file! "fixed-bad" "ABC12\nXYZNA\n")
        {:keys [pages errors]}
        (file-connector/fetch-files-async
         {:source-node {:base_path ""}
          :file-config {:transport "local"
                        :format "fixed_width"
                        :paths [path]
                        :copybook "05 NAME PIC X(3).\n05 COUNT PIC 9(2)."}})
        page (:body (async/<!! pages))]
    (is (= 2 (count page)))
    (is (= {:name "ABC" :count 12} (first page)))
    (is (= "XYZNA" (:_record (second page))))
    (is (string? (:_bitool_parse_error (second page))))
    (is (= :eof (:stop-reason (async/<!! pages))))
    (is (nil? (async/<!! errors)))))

(deftest fetch-files-async-supports-remote-transport-read-fn
  (let [{:keys [pages errors]}
        (file-connector/fetch-files-async
         {:source-node {:base_path ""}
          :file-config {:transport "s3"
                        :format "jsonl"
                        :paths ["bucket/orders.jsonl"]
                        :transport_read_fn (fn [{:keys [path]}]
                                             (.getBytes (str "{\"id\":\"" path "\"}\n") "UTF-8"))}})]
    (let [page (async/<!! pages)]
      (is (= "bucket/orders.jsonl" (:id (first (:body page)))))
      (is (string? (get-in page [:state :checksum])))
      (is (= :eof (:stop-reason (async/<!! pages))))
      (is (nil? (async/<!! errors))))))

(deftest fetch-files-async-parses-ebcdic-and-packed-decimal
  (let [file (java.io.File/createTempFile "fixed-ebcdic" ".tmp")
        name-bytes (.getBytes "ABC" "Cp037")
        amount-bytes (byte-array [(unchecked-byte 0x12) (unchecked-byte 0x3C)])
        record (byte-array (concat (seq name-bytes) (seq amount-bytes)))]
    (.deleteOnExit file)
    (with-open [out (java.io.FileOutputStream. file)]
      (.write out record))
    (let [{:keys [pages]} (file-connector/fetch-files-async
                           {:source-node {:base_path ""}
                            :file-config {:transport "local"
                                          :format "fixed_width"
                                          :encoding "EBCDIC"
                                          :paths [(.getAbsolutePath file)]
                                          :copybook "05 NAME PIC X(3).\n05 AMOUNT PIC 9(3) COMP-3."}})]
      (is (= {:name "ABC" :amount 123N}
             (first (:body (async/<!! pages))))))))

(deftest build-record-rows-routes-fixed-width-parse-errors-to-bad-records
  (let [result (bronze/build-record-rows
                [{:_record "XYZNA"
                  :_bitool_parse_error "Invalid fixed-width number"}]
                {:endpoint_name "orders"
                 :primary_key_fields ["id"]}
                {:run-id "run-1"
                 :source-system "file"
                 :now (java.time.Instant/now)
                 :request-url "file:///tmp/orders.txt"
                 :page-number 1
                 :cursor nil
                 :http-status 200})]
    (is (= [] (:rows result)))
    (is (= 1 (count (:bad-records result))))
    (is (= "Invalid fixed-width number"
           (:error_message (first (:bad-records result)))))))
