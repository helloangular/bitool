# Connector Tech Design: Kafka Source, File/Mainframe Source, Snowflake Destination

## 1. Purpose

This document defines the technical design for three new connectors in BiTool:

1. **Kafka source** — consume topics/consumer-groups into Bronze.
2. **File and mainframe file source** — ingest local/remote files (CSV, JSON, fixed-width EBCDIC/ASCII copybook files) into Bronze.
3. **Snowflake destination** — write Bronze/Silver/Gold tables to Snowflake.

Each connector follows BiTool's existing architecture: graph node registration, plugin-based execution dispatch, bronze layer standardization, checkpoint-based resumption, and manifest-tracked batch commits.

---

## 2. Architecture Context

### 2.1 Existing patterns this design extends

| Concern | Existing pattern | Reference |
|---------|-----------------|-----------|
| Source node | `Ap` (api-connection) in `graph2.clj` | `btype-codes`, `node-keys`, `rectangles` |
| Destination node | `Tg` (target) in `graph2.clj` | `save-target`, `get-target-item` |
| Execution dispatch | Plugin registry in `platform/plugins.clj` | `register-execution-handler!` |
| Streaming ingestion | `fetch-paged-async` in `connector/api.clj` | Channel-based producer-consumer |
| Bronze envelope | `bronze-base-columns` in `ingest/bronze.clj` | 16 system columns + promoted columns |
| Checkpoint | `ingest/checkpoint.clj` | Watermark, cursor, batch identity |
| Manifest lifecycle | `preparing` → `pending_checkpoint` → `committed` | `flush-batch!` in `runtime.clj` |
| Connection registry | `create-dbspec-from-id` in `db.clj` | JDBC spec per dbtype |
| RBAC + audit | `ensure-authorized!` + `record-audit-event!` | `routes/home.clj`, `control_plane.clj` |

### 2.2 New btypes

| Connector | Alias | Btype code | Role |
|-----------|-------|------------|------|
| Kafka source | `kafka-source` | `Kf` | Source node (like Ap) |
| File source | `file-source` | `Fs` | Source node (like Ap) |
| Snowflake destination | (extends Tg) | `Tg` | Uses existing Target node with `target_kind = "snowflake"` |

Snowflake does **not** need a new btype. It extends the existing `Tg` node with a new `target_kind` value and connection type, following the same pattern as Databricks and PostgreSQL.

---

## 3. Kafka Source Connector

### 3.1 Goals

- Consume records from one or more Kafka topics into Bronze tables.
- Support consumer-group semantics with manual offset commit tied to batch manifest commit.
- Resume from last committed offset on restart (checkpoint-based).
- Support Avro, JSON, and Protobuf value deserialization.
- Support SASL/SSL authentication.
- Backpressure: bounded in-memory batch buffer (same as API connector).

### 3.2 Non-goals

- Kafka Connect integration (this is a native consumer).
- Exactly-once semantics across Kafka transactions (we provide at-least-once with delete-before-insert idempotency).
- Schema Registry write-back.

### 3.3 Graph node: `Kf` (kafka-source)

#### 3.3a `btype-codes` addition

```clojure
"kafka-source" "Kf"
```

#### 3.3b `rectangles` addition

Kf is a source node. Same downstream targets as Ap:

```clojure
"Kf" ["J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "C" "O"]
```

#### 3.3c `node-keys` addition

```clojure
"kafka-source" [:source_system :connection_id :bootstrap_servers :security_protocol
                :sasl_mechanism :sasl_jaas_config_ref :ssl_truststore_ref
                :consumer_group_id :topic_configs]
```

#### 3.3d Topic configuration (stored in `:topic_configs` array)

Each element in `topic_configs` mirrors the structure of `endpoint_configs` in the Ap node:

```clojure
{:topic_name           "orders.events"        ;; Kafka topic
 :enabled              true
 :key_deserializer     "string"               ;; string | avro | protobuf | bytes
 :value_deserializer   "json"                 ;; string | json | avro | protobuf | bytes
 :schema_registry_url  "https://sr.example.com"  ;; for avro/protobuf
 :schema_registry_auth_ref {}                 ;; auth for schema registry
 :json_explode_rules   [{:path "$.payload"}]  ;; reuse existing extraction
 :primary_key_fields   ["order_id"]
 :watermark_column     "event_timestamp"
 :bronze_table_name    "lake.bronze.orders_raw"
 :schema_mode          "infer"                ;; manual | infer | hybrid
 :inferred_fields      []
 :max_poll_records     500                    ;; Kafka consumer max.poll.records
 :max_poll_interval_ms 300000
 :auto_offset_reset    "earliest"             ;; earliest | latest
 :batch_flush_rows     1000                   ;; rows per batch flush
 :batch_flush_bytes    52428800               ;; 50MB per batch flush
 :rate_limit_per_poll_ms 0}                   ;; throttle between polls
```

### 3.4 Connection storage

New `dbtype` value in the `connection` table:

```clojure
{:dbtype "kafka"
 :host "broker1:9092,broker2:9092"   ;; bootstrap_servers
 :username "kafka-user"              ;; SASL username (optional)
 :password "kafka-pass"              ;; SASL password (or secret_ref)
 :token nil
 :catalog nil
 :schema nil
 :http_path nil
 ;; Extended fields (new columns or JSON in options):
 :security_protocol "SASL_SSL"       ;; PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
 :sasl_mechanism "PLAIN"             ;; PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
 :ssl_truststore_location "/path/to/truststore.jks"
 :ssl_truststore_password_ref "managed:kafka-truststore-pw"}
```

`create-dbspec-from-id` returns the raw config map for Kafka (no JDBC URL) — the Kafka consumer factory uses it directly.

### 3.5 New files

| File | Purpose |
|------|---------|
| `src/clj/bitool/connector/kafka.clj` | Kafka consumer lifecycle, polling, deserialization |
| `src/clj/bitool/ingest/kafka_runtime.clj` | `run-kafka-node!` — orchestrates consume → bronze → checkpoint |
| `resources/public/kafkaSourceComponent.js` | Web Component for Kf config panel |
| `resources/public/kafkaSourceComponent.html` | HTML template for config panel |

### 3.6 Runtime: `run-kafka-node!`

#### 3.6a Execution handler registration

```clojure
;; In bitool.ingest.kafka-runtime (ns init)
(plugins/register-execution-handler! :kafka
  {:description "Kafka topic consumer → Bronze"
   :workload-classifier (fn [{:keys [trigger-type]}]
                          (if (= "replay" trigger-type) "replay" "kafka"))
   :execute (fn [request-row request-params]
              (kafka-runtime/run-kafka-node!
               (:graph_id request-row)
               (:node_id request-row)
               request-params))})
```

#### 3.6b Streaming architecture

```
KafkaConsumer.poll()
    ↓
ConsumerRecords (batch of records)
    ↓
deserialize-value (JSON/Avro/Protobuf → Clojure map)
    ↓
bronze/build-kafka-rows (mirror of build-page-rows)
    ↓
add-to-batch-buffer
    ↓
should-flush-batch? (row count / byte threshold)
    ↓
flush-batch! (reuse existing flush-batch! from runtime.clj)
    ↓
commit-offsets! (after manifest committed)
```

#### 3.6c `fetch-kafka-async` (connector/kafka.clj)

Mirrors `fetch-paged-async` but for Kafka:

```clojure
(defn fetch-kafka-async
  [{:keys [consumer-config topic-name poll-timeout-ms rate-limit-ms]}]
  (let [pages-ch (async/chan 500)
        errors-ch (async/chan 10)
        cancel (atom false)
        consumer (create-consumer consumer-config)]
    (.subscribe consumer [topic-name])
    (async/go-loop []
      (when-not @cancel
        (try
          (let [records (.poll consumer (Duration/ofMillis poll-timeout-ms))]
            (when (pos? (.count records))
              (async/>! pages-ch
                {:body (records->maps records)
                 :page (.count records)
                 :state {:offsets (partition-offsets records)}
                 :response {:status 200}})))
          (catch Exception e
            (async/>! errors-ch {:type :consumer-error :error e})))
        (when (pos? rate-limit-ms)
          (async/<! (async/timeout rate-limit-ms)))
        (recur)))
    {:pages pages-ch
     :errors errors-ch
     :cancel (fn [] (reset! cancel true) (.wakeup consumer))
     :consumer consumer}))
```

#### 3.6d Checkpoint semantics

| Field | Kafka meaning |
|-------|---------------|
| `last_successful_cursor` | JSON-encoded `{topic: {partition: offset}}` map |
| `last_successful_watermark` | Max event timestamp across consumed records |
| `last_successful_batch_id` | Batch ID of last committed batch |
| `rows_ingested` | Cumulative record count |

**Offset commit strategy**: Kafka consumer offsets are committed **only after** the batch manifest reaches `committed` status. This ties Kafka's consumer-group offset to BiTool's manifest lifecycle:

1. Poll records → buffer → flush-batch! → manifest `committed`
2. `consumer.commitSync({topic-partition: offset+1})` for all partitions in the batch
3. On crash before offset commit: Kafka redelivers from last committed offset → delete-before-insert idempotency handles duplicates (same batch_id from checkpoint)

#### 3.6e Bronze columns

Kafka records reuse `bronze-base-columns` with these semantic mappings:

| Bronze column | Kafka source |
|---------------|-------------|
| `source_system` | Kf node `:source_system` |
| `endpoint_name` | Topic name |
| `api_request_url` | `kafka://{bootstrap_servers}/{topic}` |
| `api_page_number` | Kafka partition number |
| `api_cursor` | `{partition}:{offset}` |
| `http_status_code` | Always `200` (synthetic) |
| `event_time_utc` | Record timestamp (CreateTime or LogAppendTime) |
| `payload_json` | Serialized value |

Promoted columns work identically to API — `json_explode_rules` + `selected_nodes` or `inferred_fields`.

### 3.7 Failure recovery

| Failure | Recovery |
|---------|----------|
| Consumer poll timeout | Retry in next poll loop iteration |
| Deserialization error | Route to bad-records table (same as API coercion failures) |
| Broker unavailable | Kafka client retries internally; surfaces as error after `max.poll.interval.ms` |
| Flush failure (DB) | Same as API: manifest stays `preparing`, reconciled on next run |
| Process crash | Consumer group rebalance assigns partitions to new consumer; checkpoint resume from last committed offset |
| Offset commit failure after manifest commit | Next run re-consumes from old offset; delete-before-insert idempotency prevents duplicates |

### 3.8 Adaptive backpressure

Reuses `adaptive-backpressure-state` atom from runtime.clj. Key: `{source_system}::{topic_name}`. Rate-limited detection: consumer lag exceeding threshold or broker throttling.

---

## 4. File and Mainframe File Source Connector

### 4.1 Goals

- Ingest files from local filesystem, S3, SFTP, or Azure Blob into Bronze.
- Support CSV, JSON (newline-delimited), Parquet, and fixed-width (EBCDIC/ASCII copybook) formats.
- Mainframe copybook support: parse COBOL copybook definitions to derive record layout and field types.
- File-level checkpointing: track which files have been ingested.
- Support glob patterns and directory watching for incremental pickup.

### 4.2 Non-goals

- Real-time file watching (CDC). Files are ingested on-demand or on schedule.
- Streaming very large single files with sub-file checkpointing (first version treats each file as an atomic unit).
- Mainframe JCL execution or EBCDIC transcoding beyond record parsing.

### 4.3 Graph node: `Fs` (file-source)

#### 4.3a `btype-codes` addition

```clojure
"file-source" "Fs"
```

#### 4.3b `rectangles` addition

```clojure
"Fs" ["J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "C" "O"]
```

#### 4.3c `node-keys` addition

```clojure
"file-source" [:source_system :connection_id :transport :base_path :file_configs]
```

#### 4.3d Transport configuration

The `:transport` field selects the file access method. Connection credentials are stored in the `connection` table:

| Transport | Connection fields | Example |
|-----------|------------------|---------|
| `local` | `:host` = local path prefix | `/data/inbound/` |
| `s3` | `:host` = bucket, `:token` = AWS access key, `:password` = secret key, `:schema` = region | `my-bucket` / `us-east-1` |
| `sftp` | `:host`, `:port`, `:username`, `:password` or `:token` (SSH key ref) | `sftp.corp.com:22` |
| `azure_blob` | `:host` = account, `:token` = SAS token or connection string, `:schema` = container | `storageacct` / `inbound` |

`create-dbspec-from-id` returns the raw config map for file transports (no JDBC URL).

#### 4.3e File configuration (stored in `:file_configs` array)

```clojure
{:file_config_name     "daily_orders"         ;; logical name (acts as endpoint_name)
 :enabled              true
 :file_pattern         "orders_*.csv"         ;; glob pattern
 :file_format          "csv"                  ;; csv | json_lines | json | parquet | fixed_width
 :compression          "gzip"                 ;; none | gzip | bzip2 | zstd | snappy
 :encoding             "UTF-8"               ;; UTF-8 | ASCII | EBCDIC (CP037, CP500, etc.)

 ;; CSV options
 :csv_delimiter        ","
 :csv_quote            "\""
 :csv_header           true                   ;; first row is header
 :csv_skip_rows        0                      ;; skip N rows after header

 ;; Fixed-width / mainframe options
 :copybook_ref         "managed:order-copybook"  ;; secret_ref pointing to copybook text
 :copybook_inline      nil                    ;; alternative: inline copybook definition
 :record_length        256                    ;; fixed record length in bytes
 :segment_id_field     nil                    ;; for multi-segment files: field name containing segment ID
 :segment_id_values    nil                    ;; map of segment_id → copybook_ref for multi-layout files

 ;; Common fields (mirror API endpoint_configs)
 :primary_key_fields   ["order_id"]
 :watermark_column     "order_date"
 :bronze_table_name    "lake.bronze.daily_orders_raw"
 :schema_mode          "manual"               ;; manual | infer
 :inferred_fields      []
 :field_descriptors    []                     ;; manual field definitions
 :batch_flush_rows     5000
 :batch_flush_bytes    104857600}             ;; 100MB
```

#### 4.3f Copybook parsing

For mainframe files, BiTool parses COBOL copybook definitions to derive:
- Field names, positions, lengths
- PIC clause → data type mapping
- COMP/COMP-3 (packed decimal) handling
- REDEFINES (union types) → multiple possible layouts
- OCCURS (arrays) → flattened columns

Type mapping from PIC clauses:

| PIC clause | Bronze type | Example |
|------------|-------------|---------|
| `PIC X(n)` | STRING | Alphanumeric |
| `PIC 9(n)` | INT or BIGINT | Display numeric |
| `PIC 9(n)V9(m)` | DOUBLE | Implied decimal |
| `PIC S9(n) COMP-3` | INT or BIGINT | Packed decimal |
| `PIC S9(n) COMP` | INT or BIGINT | Binary |

### 4.4 New files

| File | Purpose |
|------|---------|
| `src/clj/bitool/connector/file.clj` | File transport abstraction (local, S3, SFTP, Azure Blob) |
| `src/clj/bitool/connector/file_format.clj` | Format parsers (CSV, JSON, Parquet, fixed-width) |
| `src/clj/bitool/connector/copybook.clj` | COBOL copybook parser → field descriptors |
| `src/clj/bitool/ingest/file_runtime.clj` | `run-file-node!` — orchestrates file → bronze → checkpoint |
| `resources/public/fileSourceComponent.js` | Web Component for Fs config panel |
| `resources/public/fileSourceComponent.html` | HTML template for config panel |

### 4.5 Runtime: `run-file-node!`

#### 4.5a Execution handler registration

```clojure
(plugins/register-execution-handler! :file
  {:description "File/mainframe file ingest → Bronze"
   :workload-classifier (fn [{:keys [trigger-type]}]
                          (case trigger-type
                            "scheduled" "scheduled"
                            "replay" "replay"
                            "file"))
   :execute (fn [request-row request-params]
              (file-runtime/run-file-node!
               (:graph_id request-row)
               (:node_id request-row)
               request-params))})
```

#### 4.5b Streaming architecture

```
list-files (transport + glob pattern)
    ↓
filter-already-ingested (checkpoint file manifest)
    ↓
for each file:
    ↓
    open-stream (transport-specific: local IO, S3 GetObject, SFTP read, etc.)
        ↓
    decompress (gzip, bzip2, zstd, snappy)
        ↓
    decode (UTF-8, ASCII, EBCDIC)
        ↓
    parse-records (format-specific: CSV rows, JSON objects, fixed-width records)
        ↓
    bronze/build-file-rows (mirror of build-page-rows)
        ↓
    add-to-batch-buffer
        ↓
    should-flush-batch? → flush-batch!
    ↓
mark-file-ingested (file manifest row)
```

#### 4.5c `fetch-file-async` (connector/file.clj)

```clojure
(defn fetch-file-async
  [{:keys [transport-config file-config file-path]}]
  (let [pages-ch (async/chan 500)
        errors-ch (async/chan 10)
        cancel (atom false)]
    (async/thread
      (try
        (with-open [stream (open-file-stream transport-config file-path)]
          (let [reader (-> stream
                          (maybe-decompress (:compression file-config))
                          (maybe-decode (:encoding file-config)))
                parser (format-parser (:file_format file-config) file-config)]
            (loop [page-num 1
                   batch []]
              (when-not @cancel
                (if-let [record (parser reader)]
                  (let [batch' (conj batch record)]
                    (if (>= (count batch') (:max_poll_records file-config 500))
                      (do
                        (async/>!! pages-ch {:body batch'
                                             :page page-num
                                             :state {:file-path file-path
                                                     :records-read (* page-num (count batch'))}
                                             :response {:status 200}})
                        (recur (inc page-num) []))
                      (recur page-num batch')))
                  ;; EOF
                  (do
                    (when (seq batch)
                      (async/>!! pages-ch {:body batch
                                           :page page-num
                                           :state {:file-path file-path
                                                   :records-read (+ (* (dec page-num) 500) (count batch))}
                                           :response {:status 200}}))
                    (async/>!! pages-ch {:stop-reason :eof
                                         :state {:file-path file-path}
                                         :http-status 200})))))))
        (catch Exception e
          (async/>!! errors-ch {:type :file-read-error :error e :file-path file-path}))
        (finally
          (async/close! pages-ch))))
    {:pages pages-ch
     :errors errors-ch
     :cancel (fn [] (reset! cancel true))}))
```

#### 4.5d Checkpoint semantics

File source uses a **file manifest** approach rather than cursor/watermark:

| Field | File meaning |
|-------|-------------|
| `last_successful_cursor` | JSON-encoded list of ingested file paths with checksums |
| `last_successful_watermark` | Max file modification timestamp or watermark_column value |
| `last_successful_batch_id` | Batch ID of last committed batch |

A file is considered "already ingested" if its path + SHA-256 checksum appears in the checkpoint's cursor JSON. This handles:
- **Same filename, new content**: New checksum → re-ingest.
- **Same content, new filename**: Different path → ingest (dedup via primary_key_fields if configured).
- **Unchanged file**: Skip.

#### 4.5e Bronze columns for file source

| Bronze column | File source |
|---------------|-------------|
| `source_system` | Fs node `:source_system` |
| `endpoint_name` | `file_config_name` |
| `api_request_url` | `{transport}://{base_path}/{file_path}` |
| `api_page_number` | Batch number within file |
| `api_cursor` | `{file_path}:{record_offset}` |
| `http_status_code` | `200` (synthetic) |
| `event_time_utc` | File modification timestamp or watermark column value |
| `payload_json` | Serialized record |

#### 4.5f Mainframe-specific flow

For `file_format = "fixed_width"`:

```
resolve-copybook (from copybook_ref or copybook_inline)
    ↓
parse-copybook → [{:field_name "ORDER-ID" :pic "9(10)" :offset 0 :length 10 :type :display-numeric} ...]
    ↓
for each record (record_length bytes):
    ↓
    decode-ebcdic (if encoding = EBCDIC variant)
        ↓
    extract-fields (offset + length slicing)
        ↓
    coerce-packed-decimal (COMP-3 fields)
        ↓
    build-map {:ORDER_ID 1234567890 :CUSTOMER_NAME "ACME CORP" ...}
```

Multi-segment files (multiple record layouts in one file):
1. Read `segment_id_field` from each record.
2. Look up copybook for that segment from `segment_id_values` map.
3. Parse with segment-specific layout.
4. Route to segment-specific Bronze table (or single table with `segment_type` column).

### 4.6 Failure recovery

| Failure | Recovery |
|---------|----------|
| File not found | Log warning, skip file, continue to next |
| Parse error (bad record) | Route to bad-records table with error message |
| EBCDIC decode error | Route to bad-records table with hex dump in `row_json` |
| Transport error (S3 timeout) | Retry with exponential backoff (3 attempts) |
| Flush failure | Same as API: manifest stays `preparing`, reconciled on next run |
| Partial file ingestion (crash mid-file) | File not marked in checkpoint → full re-ingest on next run; delete-before-insert idempotency handles duplicates |
| Copybook mismatch (wrong record length) | Throw with `failure_class: "copybook_mismatch"`, fail the file |

---

## 5. Snowflake Destination Connector

### 5.1 Goals

- Write Bronze, Silver, and Gold tables to Snowflake.
- Support Snowflake's `PUT` + `COPY INTO` bulk loading pattern for performance.
- Support standard JDBC INSERT for small batches.
- Support Snowflake stages (internal and external S3/Azure/GCS).
- DDL generation for Snowflake (CREATE TABLE IF NOT EXISTS, column types).
- Downstream job triggering via Snowflake Tasks or external orchestrators.

### 5.2 Non-goals

- Snowpipe streaming (first version uses batch COPY INTO).
- Snowflake Streams and Tasks as a CDC source (future work).
- Snowflake-native schema evolution (ALTER TABLE ADD COLUMN is supported; type changes are not).

### 5.3 Graph node: extends existing `Tg`

Snowflake is a new `target_kind` value on the existing Target node:

```clojure
:target_kind "snowflake"
```

No new btype is needed. The Tg node already supports `target_kind`-specific behavior.

#### 5.3a Additional node-keys for Snowflake targets

The existing `node-keys` for `"target"` are sufficient. Snowflake-specific options go in the `:options` JSON map:

```clojure
{:target_kind "snowflake"
 :connection_id 42
 :catalog "ANALYTICS_DB"             ;; Snowflake database
 :schema "BRONZE"                    ;; Snowflake schema
 :table_name "SAMARA_TRIPS_RAW"
 :write_mode "append"                ;; append | merge | overwrite
 :partition_columns []               ;; Snowflake cluster keys
 :merge_keys ["source_record_id"]    ;; for merge write_mode
 :options {:load_method "copy_into"  ;; copy_into | jdbc_insert
           :stage_name "@BRONZE.INGEST_STAGE"  ;; internal stage
           :file_format "BRONZE.INGEST_JSON_FORMAT"  ;; Snowflake file format
           :warehouse "INGEST_WH"    ;; compute warehouse
           :on_error "CONTINUE"      ;; CONTINUE | SKIP_FILE | ABORT_STATEMENT
           :purge_staged_files true   ;; delete staged files after COPY INTO
           :snowflake_task_name nil   ;; optional: trigger Snowflake Task after load
           :external_stage_url nil}}  ;; for external stage (S3/Azure/GCS URL)
```

### 5.4 Connection storage

New `dbtype` value in the `connection` table:

```clojure
{:dbtype "snowflake"
 :host "account.snowflakecomputing.com"  ;; Snowflake account URL
 :port 443
 :username "INGEST_USER"
 :password "password"                    ;; or password_ref for managed secret
 :token nil                              ;; alternative: OAuth token or key-pair
 :catalog "ANALYTICS_DB"                 ;; database
 :schema "BRONZE"                        ;; schema
 :http_path nil
 ;; Extended fields:
 :warehouse "INGEST_WH"                  ;; default warehouse
 :role "INGEST_ROLE"                     ;; Snowflake role
 :private_key_ref nil}                   ;; managed secret ref for key-pair auth
```

#### 5.4a `create-dbspec-from-id` addition

```clojure
"snowflake"
{:jdbcUrl (format "jdbc:snowflake://%s:%s/?db=%s&schema=%s&warehouse=%s&role=%s"
                  host port catalog schema
                  (or warehouse "COMPUTE_WH")
                  (or role "PUBLIC"))
 :dbtype "snowflake"
 :user username
 :password password}
```

### 5.5 New files

| File | Purpose |
|------|---------|
| `src/clj/bitool/snowflake/loader.clj` | PUT + COPY INTO logic, stage management |
| `src/clj/bitool/snowflake/ddl.clj` | Snowflake-specific DDL generation |

The existing `targetComponent.js` and `targetComponent.html` are extended with Snowflake-specific fields (conditional on `target_kind = "snowflake"`).

### 5.6 Modified files

| File | Change |
|------|--------|
| `src/clj/bitool/db.clj` | Add `"snowflake"` case to `create-dbspec-from-id` |
| `src/clj/bitool/ingest/runtime.clj` | Add `"snowflake"` to `load-rows!` dispatch, `ensure-table!` dispatch |
| `src/clj/bitool/graph2.clj` | Add Snowflake validation to `save-target` |
| `resources/public/targetComponent.js` | Snowflake-specific options fields |

### 5.7 DDL generation (snowflake/ddl.clj)

```sql
CREATE TABLE IF NOT EXISTS "ANALYTICS_DB"."BRONZE"."SAMARA_TRIPS_RAW" (
  "ingestion_id"      VARCHAR NOT NULL,
  "run_id"            VARCHAR NOT NULL,
  "batch_id"          VARCHAR,
  "source_system"     VARCHAR NOT NULL,
  "endpoint_name"     VARCHAR NOT NULL,
  "extracted_at_utc"  VARCHAR NOT NULL,
  "ingested_at_utc"   VARCHAR NOT NULL,
  "api_request_url"   VARCHAR,
  "api_page_number"   INTEGER,
  "api_cursor"        VARCHAR,
  "http_status_code"  INTEGER,
  "record_hash"       VARCHAR NOT NULL,
  "source_record_id"  VARCHAR,
  "event_time_utc"    VARCHAR,
  "partition_date"    DATE NOT NULL,
  "load_date"         DATE NOT NULL,
  "payload_json"      VARIANT NOT NULL
)
CLUSTER BY (partition_date);
```

Snowflake type mapping:

| Bronze type | Snowflake type |
|-------------|---------------|
| `STRING` | `VARCHAR` |
| `INT` | `INTEGER` |
| `BIGINT` | `BIGINT` |
| `DOUBLE` | `DOUBLE` |
| `BOOLEAN` | `BOOLEAN` |
| `DATE` | `DATE` |
| `TIMESTAMP` | `TIMESTAMP_NTZ` |
| `payload_json` | `VARIANT` (Snowflake-native semi-structured) |

### 5.8 Bulk loading: PUT + COPY INTO (snowflake/loader.clj)

For batches exceeding a threshold (default: 1000 rows), use Snowflake's bulk loading:

```clojure
(defn bulk-load-rows!
  [conn-id stage-name file-format table-name rows opts]
  (let [ds (db/get-datasource conn-id)
        ndjson (rows->ndjson rows)
        temp-file (write-temp-ndjson ndjson)
        staged-path (str stage-name "/" (.getName temp-file))]
    ;; 1. PUT file to internal stage
    (jdbc/execute! ds [(str "PUT 'file://" (.getAbsolutePath temp-file) "' '" stage-name "'"
                            " AUTO_COMPRESS=TRUE OVERWRITE=TRUE")])
    ;; 2. COPY INTO from stage
    (jdbc/execute! ds [(str "COPY INTO " (quoted-table-name table-name)
                            " FROM '" staged-path "'"
                            " FILE_FORMAT = (FORMAT_NAME = '" file-format "')"
                            " ON_ERROR = '" (or (:on_error opts) "CONTINUE") "'"
                            " PURGE = " (if (:purge_staged_files opts) "TRUE" "FALSE"))])
    ;; 3. Cleanup temp file
    (.delete temp-file)))
```

For small batches (< 1000 rows), use standard JDBC INSERT via the existing `load-rows!` path.

#### 5.8a COPY INTO error handling

The `COPY INTO` command returns a result set with per-file load status. Parse this to:
- Count rows loaded, rows with errors
- If `ON_ERROR = CONTINUE`: log errors, count as bad records
- If `ON_ERROR = ABORT_STATEMENT`: throw with `failure_class: "snowflake_copy_error"`

### 5.9 Integration with `flush-batch!`

The Snowflake path hooks into `load-rows!` in runtime.clj:

```clojure
(defn- load-rows! [conn-id table-name rows]
  (when (seq rows)
    (let [dbtype (connection-dbtype conn-id)]
      (if (and (= "snowflake" dbtype)
               (>= (count rows) snowflake-bulk-threshold)
               (snowflake-stage-configured? conn-id))
        (snowflake/bulk-load-rows! conn-id
                                    (snowflake-stage-name conn-id)
                                    (snowflake-file-format conn-id)
                                    table-name
                                    rows
                                    (snowflake-load-opts conn-id))
        ;; Fall through to standard JDBC insert
        (db/load-rows! conn-id nil table-name rows (key->col (first rows)))))))
```

This is transparent to the batch manifest lifecycle — `flush-batch!` calls `load-rows!`, which dispatches internally.

### 5.10 Merge write mode

For `write_mode = "merge"`, generate Snowflake MERGE statement:

```sql
MERGE INTO target_table t
USING (SELECT * FROM @stage/file.json.gz (FILE_FORMAT => 'fmt')) s
ON t.source_record_id = s.source_record_id
WHEN MATCHED THEN UPDATE SET t.payload_json = s.payload_json, t.ingested_at_utc = s.ingested_at_utc, ...
WHEN NOT MATCHED THEN INSERT (col1, col2, ...) VALUES (s.col1, s.col2, ...)
```

Merge keys come from `target.merge_keys`.

### 5.11 Downstream triggers

After Bronze load, Snowflake targets can trigger:

| Mechanism | Configuration |
|-----------|--------------|
| Snowflake Task | `:snowflake_task_name` in options → `ALTER TASK ... RESUME` or `EXECUTE TASK` |
| Databricks Job | Existing `silver_job_id` / `gold_job_id` (cross-platform) |
| Webhook | Future: HTTP POST to external orchestrator |

### 5.12 Failure recovery

| Failure | Recovery |
|---------|----------|
| PUT failure (disk/network) | Retry up to 3 times; fail batch |
| COPY INTO partial failure (ON_ERROR=CONTINUE) | Rows with errors logged; batch succeeds with bad_record_count |
| COPY INTO failure (ON_ERROR=ABORT) | Batch fails; manifest stays `preparing`; retry |
| Snowflake warehouse suspended | `ALTER WAREHOUSE ... RESUME` before retry; exponential backoff |
| Authentication failure | Fail with `failure_class: "auth_error"` |
| Table does not exist | `ensure-table!` creates it; retry |

---

## 6. Execution Framework Changes

### 6.1 New request_kind values

| request_kind | Handler | Workload classes |
|-------------|---------|-----------------|
| `"kafka"` | `kafka-runtime/run-kafka-node!` | `"kafka"`, `"replay"` |
| `"file"` | `file-runtime/run-file-node!` | `"file"`, `"scheduled"`, `"replay"` |

Snowflake does **not** need a new request_kind — it uses the existing `"api"` kind. The Snowflake-specific behavior is in the write path, not the execution dispatch.

### 6.2 Execution table changes

Add new values to `source_system` and `credential_ref` columns (already VARCHAR, no DDL change needed).

Add new values to `workload_class` column:
- `"kafka"` — Kafka consumer workloads
- `"file"` — File ingestion workloads

### 6.3 Source/credential concurrency

Kafka and file connectors participate in the existing source/credential concurrency governance:

| Connector | source_system | credential_ref | source_max_concurrency | credential_max_concurrency |
|-----------|--------------|----------------|----------------------|---------------------------|
| Kafka | `{source_system}` from Kf node | `{consumer_group_id}` | Default: 1 (single consumer per group per topic) | Default: 1 |
| File | `{source_system}` from Fs node | `{connection_id}` | Default: 4 (parallel file processing) | Default: 2 (SFTP connection limit) |
| Snowflake | (set by source connector) | (set by source connector) | N/A (destination) | N/A (destination) |

---

## 7. Route Handlers

### 7.1 New routes in `home-routes`

```clojure
["/saveKafkaSource" {:post save-kafka-source}]
["/runKafkaIngestion" {:post run-kafka-ingestion}]
["/saveFileSource" {:post save-file-source}]
["/runFileIngestion" {:post run-file-ingestion}]
["/previewCopybookSchema" {:post preview-copybook-schema}]
```

### 7.2 Route handlers

Follow the existing `run-api-ingestion` pattern:

```clojure
(defn run-kafka-ingestion [request]
  (try
    (ensure-authorized! request :kafka.execute)
    (let [params  (:params request)
          gid     (parse-required-int (or (:gid params) (:gid (:session request))) :gid)
          node-id (parse-required-int (:id params) :id)
          topic   (:topic_name params)
          result  (ingest-execution/enqueue-request! gid node-id
                    {:request-kind "kafka"
                     :endpoint-name topic
                     :trigger-type "manual"})]
      (record-audit-event! request "kafka.enqueue" {:graph_id gid :node_id node-id :topic_name topic})
      (queue-response result))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e)) 403 http-response/forbidden http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))
```

---

## 8. Frontend Components

### 8.1 Kafka Source (`kafkaSourceComponent.js`)

Panel layout:
- **Source System**: Text input
- **Bootstrap Servers**: Text input (comma-separated)
- **Security**: Protocol dropdown + SASL mechanism + credential refs
- **Consumer Group ID**: Text input
- **Topic Configs**: Editable table (add/remove topics)
  - Topic Name, Key Deserializer, Value Deserializer, Schema Registry URL
  - Explode Rules, Primary Key Fields, Watermark Column
  - Bronze Table Name, Schema Mode, Batch Settings

### 8.2 File Source (`fileSourceComponent.js`)

Panel layout:
- **Source System**: Text input
- **Transport**: Dropdown (local, s3, sftp, azure_blob)
- **Connection**: Connection ID selector
- **Base Path**: Text input
- **File Configs**: Editable table
  - Config Name, File Pattern, Format, Compression, Encoding
  - CSV options (conditional), Copybook ref (conditional for fixed_width)
  - Primary Key Fields, Watermark Column, Bronze Table Name

### 8.3 Target Component updates

Add conditional Snowflake fields when `target_kind = "snowflake"`:
- **Load Method**: Dropdown (copy_into, jdbc_insert)
- **Stage Name**: Text input
- **File Format**: Text input
- **Warehouse**: Text input
- **On Error**: Dropdown (CONTINUE, SKIP_FILE, ABORT_STATEMENT)
- **Purge Staged Files**: Checkbox
- **Snowflake Task Name**: Text input (optional)

---

## 9. Testing Strategy

### 9.1 Unit tests

| Test file | Coverage |
|-----------|----------|
| `test/clj/bitool/connector/kafka_test.clj` | Consumer config building, deserialization, offset tracking |
| `test/clj/bitool/connector/file_test.clj` | Transport abstraction, format parsing, glob matching |
| `test/clj/bitool/connector/copybook_test.clj` | Copybook parsing, EBCDIC decoding, COMP-3 unpacking |
| `test/clj/bitool/ingest/kafka_runtime_test.clj` | run-kafka-node! with stubbed consumer, checkpoint resume, offset commit ordering |
| `test/clj/bitool/ingest/file_runtime_test.clj` | run-file-node! with stubbed transport, file checkpointing, multi-file batching |
| `test/clj/bitool/snowflake/loader_test.clj` | PUT + COPY INTO SQL generation, error result parsing |
| `test/clj/bitool/snowflake/ddl_test.clj` | CREATE TABLE DDL, type mapping, VARIANT for payload_json |

### 9.2 Integration tests (require live services)

| Test | Fixture |
|------|---------|
| Kafka end-to-end | Embedded Kafka (testcontainers) |
| S3 file transport | LocalStack S3 |
| SFTP file transport | Embedded SFTP server |
| Snowflake COPY INTO | Snowflake trial account (gated by env var) |

### 9.3 Proof suite additions

Add to `scripts/run-api-bronze-proof-suite.sh`:
- Phase 4: Kafka connector load-shape tests
- Phase 5: File connector parse + checkpoint tests
- Phase 6: Snowflake COPY INTO idempotency tests

---

## 10. Implementation Order

| Step | Scope | Dependencies |
|------|-------|-------------|
| 1 | Snowflake destination: connection, DDL, JDBC insert path | `db.clj`, `graph2.clj` |
| 2 | Snowflake bulk loading: PUT + COPY INTO, stage management | Step 1 |
| 3 | Kafka source: graph node, connection, consumer lifecycle | `graph2.clj`, `plugins.clj` |
| 4 | Kafka runtime: run-kafka-node!, checkpoint, offset commit | Step 3, `runtime.clj` patterns |
| 5 | File source: graph node, transport abstraction (local + S3) | `graph2.clj`, `plugins.clj` |
| 6 | File formats: CSV, JSON lines, Parquet parser | Step 5 |
| 7 | Mainframe: copybook parser, EBCDIC decoder, fixed-width parser | Step 6 |
| 8 | File runtime: run-file-node!, file checkpoint, multi-file batching | Steps 5-7, `runtime.clj` patterns |
| 9 | Frontend: Kf panel, Fs panel, Tg Snowflake fields | Steps 1-8 |
| 10 | Integration tests + proof suite | All steps |

Snowflake is first because it extends existing infrastructure (Tg node, `load-rows!`) with minimal new abstraction. Kafka is next because its streaming model is closest to the existing API connector. File/mainframe is last because it requires the most new abstractions (transport, format, copybook).

---

## 11. Dependencies

| Dependency | Version | Purpose |
|------------|---------|---------|
| `org.apache.kafka/kafka-clients` | 3.7+ | Kafka consumer |
| `io.confluent/kafka-avro-serializer` | 7.6+ | Avro deserialization (optional) |
| `com.google.protobuf/protobuf-java` | 3.25+ | Protobuf deserialization (optional) |
| `net.snowflake/snowflake-jdbc` | 3.16+ | Snowflake JDBC driver |
| `com.jcraft/jsch` | 0.1.55+ | SFTP transport |
| `software.amazon.awssdk/s3` | 2.25+ | S3 transport |
| `com.azure/azure-storage-blob` | 12.25+ | Azure Blob transport |

Avro, Protobuf, SFTP, S3, and Azure dependencies are optional — loaded only when the corresponding connector is configured. Use dynamic classloader or `requiring-resolve` to avoid mandatory classpath inclusion.

---

## 12. Security Considerations

| Concern | Mitigation |
|---------|-----------|
| Kafka credentials in config | Use `managed:` secret refs → AES-GCM encrypted in `secret_store` table |
| Snowflake password/key-pair | Same managed secret system |
| SFTP keys | `ssh_key_ref` → managed secret |
| S3 credentials | AWS access key/secret → managed secret; or IAM role (no credentials stored) |
| Copybook content | Stored as managed secret or inline in node config (no sensitive data typically) |
| RBAC gates | New roles: `kafka.execute`, `file.execute`, `snowflake.admin` |
| Audit events | New event types: `kafka.enqueue`, `file.enqueue`, `snowflake.copy_into` |
