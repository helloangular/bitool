# API to Bronze Best Practices

## 1. Purpose

This document defines recommended practices for designing `API -> Bronze` ingestion in BiTool.

It is written for the current product shape:

- `Ap` node defines endpoint extraction config,
- scheduler/manual trigger starts execution,
- runtime writes Bronze plus audit state,
- Databricks or other warehouse targets store Bronze tables,
- Silver and Gold remain downstream concerns.

This is not a generic medallion guide. It is specific to how BiTool currently models and runs API ingestion.

Related docs:

- [API-to-Bronze Production Readiness Checklist](API-to-Bronze-Production-Readiness-Checklist.md)
- [API-to-Bronze Production Tech Design](API-to-Bronze-Production-Tech-Design.md)

---

## 2. Core Principle

Bronze should optimize for:

- replayability,
- operational safety,
- source fidelity,
- auditability.

Bronze should not optimize for:

- perfect business modeling,
- aggressive normalization,
- heavy downstream semantics,
- final analyst-facing table shape.

---

## 3. Recommended Bronze Contract

Every Bronze table should preserve three things:

1. Raw source truth.
2. Operational ingestion metadata.
3. A small promoted field set for useful indexing and downstream convenience.

The current BiTool Bronze model already fits this direction through:

- `payload_json`,
- standard envelope columns,
- `selected_nodes` promoted columns,
- endpoint-level checkpointing and run detail.

---

## 4. Best Practices

### 4.1 Keep Bronze append-only

Do not treat Bronze as the main business upsert layer.

Recommended:

- append new records,
- preserve duplicates if the source emits them,
- track `record_hash` and `source_record_id`,
- perform canonical dedupe in Silver.

Clarification:

- append-only refers to the logical Bronze model,
- the runtime may still use idempotent batch rewrites such as `delete by batch_id then re-insert` for retry safety,
- this preserves append-oriented semantics at the batch level while keeping retries safe.

Exceptions:

- operational checkpoint tables,
- audit/control tables,
- rare source-specific cases where strict overwrite behavior is unavoidable.

### 4.2 Always keep the raw payload

Every Bronze record should retain the original source object in `payload_json`.

Why:

- replay after mapping changes,
- schema-drift debugging,
- late promotion of new fields,
- traceability back to the source.

### 4.3 Add a standard ingestion envelope

A Bronze row should always contain stable operational columns such as:

- `run_id`
- `source_system`
- `endpoint_name`
- `extracted_at_utc`
- `ingested_at_utc`
- `api_request_url`
- `api_page_number`
- `api_cursor`
- `http_status_code`
- `record_hash`
- `source_record_id`
- `event_time_utc`
- `partition_date`
- `load_date`
- `payload_json`

This is the minimum contract for replay, audit, and downstream observability.

### 4.4 Separate record selection from field promotion

Use two distinct concepts:

- `json_explode_rules` decides what one source record is.
- `selected_nodes` decides what fields are promoted into Bronze columns.

Do not conflate them.

This keeps ingestion flexible while preventing uncontrolled schema explosion.

### 4.5 Promote only stable and useful fields

Recommended promoted field classes:

- natural keys,
- event timestamps,
- join keys,
- resource identifiers,
- important status or category values.

Avoid promoting:

- highly nested transient blobs,
- low-value verbose text,
- every possible field from the response.

### 4.6 Keep type expectations conservative in Bronze

Bronze should tolerate variability.

Recommended default:

- preserve raw JSON as the truth,
- keep promoted fields tolerant,
- enforce business-ready typing in Silver.

BiTool’s current string-heavy Bronze promotion is acceptable for MVP and early production, especially when source APIs are not fully stable.

### 4.7 Make schema evolution non-fatal

Source APIs drift. Bronze should survive:

- new fields,
- missing optional fields,
- field order changes,
- nested-object changes that do not break record extraction.

Do not make Bronze brittle by requiring every promoted field to be present on every row.

### 4.8 Distinguish success from attempt

Checkpoint design should keep:

- last successful watermark/cursor,
- last attempted watermark/cursor,
- last successful run id,
- last status.

This prevents failed runs from corrupting recovery state.

### 4.9 Treat retries as first-class runtime behavior

Retry/backoff should be:

- explicit in config,
- observable in run detail,
- bounded,
- source-isolated.

Retries should never be hidden magic.

### 4.10 Quarantine bad records without poisoning the run

If one record cannot be serialized or mapped, do not fail the entire endpoint by default.

Write it to `bad_records` with:

- endpoint metadata,
- error message,
- payload preview or full payload,
- created timestamp,
- run id.

### 4.11 Partition for operational use, not idealized semantics

For Bronze, partition on ingestion-operational time unless there is a clear reason not to.

Recommended:

- `partition_date` from ingestion timestamp,
- optionally keep source event time separately as `event_time_utc`.

This makes ingestion support and backfill operations simpler.

### 4.12 Keep Bronze free of business logic

Do not turn Bronze into Silver.

Bronze should not contain:

- business KPI logic,
- multi-entity joins,
- final conformed dimensions,
- deduplicated latest-state business views.

Those belong downstream.

### 4.13 Make replay a product feature

Operators should be able to:

- rerun an endpoint,
- backfill a time window,
- reset checkpoint state,
- replay bad records,
- replay a historical run config.

If replay is hard, Bronze is not doing its job.

---

## 5. Recommended BiTool Product Behavior

For BiTool specifically, `API -> Bronze` should behave as follows:

1. Resolve source auth and endpoint config.
2. Apply scheduler/manual trigger context.
3. Read prior checkpoint state.
4. Build request and pagination state.
5. Fetch pages with bounded retry/backoff.
6. Identify logical records using `json_explode_rules`.
7. Write one Bronze row per logical record.
8. Preserve raw source object in `payload_json`.
9. Promote only configured or inferred stable fields.
10. Write bad records separately.
11. Update checkpoint and run detail only with safe semantics.
12. Trigger downstream Silver/Gold processing only after Bronze success criteria are met.

---

## 6. Anti-Patterns

Avoid these:

- flattening the full API response into hundreds of Bronze columns,
- letting failed runs overwrite good checkpoints,
- mixing business transformations into Bronze,
- silently retrying forever,
- coupling Bronze schema directly to every source response detail,
- treating Bronze as the final warehouse model,
- requiring manual SQL changes for every optional source field.

---

## 7. Recommended Next Improvements In BiTool

The most valuable next improvements for Bronze are:

1. schema inference for promoted columns,
2. optional type inference with safe defaults,
3. schema drift logging,
4. replay and checkpoint reset UX,
5. richer run monitoring UI.

These improve usability without violating Bronze best practices.

---

## 8. Acceptance Criteria For Good API to Bronze Design

An `API -> Bronze` flow should be considered well-designed when:

- raw payload is preserved,
- promoted schema is limited and stable,
- run metadata is complete,
- retries are bounded and visible,
- failed records are isolated,
- checkpoints are safe,
- schema drift is survivable,
- replay is possible,
- Silver can evolve without re-ingesting the source from scratch.
