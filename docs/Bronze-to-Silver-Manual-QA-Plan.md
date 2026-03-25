# Bronze to Silver Manual QA Plan

## Purpose

This runbook is for a full manual QA pass of the Bronze to Silver flow in Bitool.

It covers:

- Bronze ingestion prerequisites
- Modeling Console proposal creation
- schema and transform editing
- processing policy editing
- compile
- validate
- review / approve
- publish
- execute
- UI checks after each step
- backend/API checks after each step
- database checks after each step

This is written as an end-to-end QA plan, not a partial smoke test.

## Scope

This plan covers:

- Bronze source graph open and usable
- Bronze data present in the warehouse
- Silver proposal lifecycle
- Silver execution lifecycle
- persistence of lifecycle state across navigation and reload

This plan does not cover:

- Gold
- quarantine-table behavior unless specifically implemented
- warehouse-specific performance tuning

## Recommended Test Environment

Use a stable local environment:

- Bitool UI/backend running at `http://localhost:8080`
- PostgreSQL available at `postgresql://postgres:postgres@localhost/bitool`
- if validating API-to-Bronze first, local mock API at `http://localhost:3001`

Known reference graph used in prior validation:

- graph id: `2247`
- Bronze API source node id: `2`
- endpoint: `fleet/vehicles`
- Bronze table: `public.fleet_vehicles`
- Silver table: `public.silver_fleet_vehicles`

If your graph differs, replace ids/table names accordingly.

## Required Tools

- browser with DevTools Network tab
- SQL client or `psql`
- Bitool logs

Recommended terminal helpers:

```bash
psql postgresql://postgres:postgres@localhost/bitool
```

## Pre-QA Reset Guidance

Before starting a clean pass:

1. Start Bitool.
2. Open the target graph once and confirm the Modeling Console opens.
3. Decide whether you want to reuse an existing Silver proposal or create a new one.
4. If needed, note the current proposal rows:

```sql
select proposal_id, layer, target_model, status, created_at_utc
from model_proposal
where source_graph_id = 2247
order by proposal_id desc;
```

5. Note current run rows:

```sql
select model_run_id, layer, target_model, status, created_at_utc
from compiled_model_run
order by model_run_id desc
limit 20;
```

## Baseline Backend Queries

Run these before beginning:

```sql
select count(*) from public.fleet_vehicles;
select count(*) from public.silver_fleet_vehicles;
```

Also inspect latest Bronze load detail:

```sql
select run_id, endpoint_name, status, rows_extracted, rows_written, finished_at_utc
from audit.endpoint_run_detail
where endpoint_name = 'fleet/vehicles'
order by finished_at_utc desc
limit 5;
```

Expected:

- Bronze table exists
- Bronze table has rows
- latest Bronze endpoint run is `success`

If Bronze is not healthy, stop here. Silver QA is not meaningful without Bronze input data.

---

## Phase 1: Open Graph and Confirm Bronze Context

### UI actions

1. Open Bitool in browser.
2. Open the graph through the normal UI `Open` flow.
3. Confirm the graph canvas loads.
4. Click the Bronze source node.
5. Open the Modeling Console.
6. Select `Silver` layer.

### UI checks

- graph loads without `No graph selected`
- Modeling Console opens
- `Source Node ID` dropdown contains the Bronze source
- `Endpoint Name` dropdown contains the expected endpoint

### Backend/API checks

In browser network:

- `/graph` returns `200`
- `/getItem` for source node returns `200`
- no `500` errors in console/network during console open

### Pass criteria

- graph session is intact
- Modeling Console is bound to the opened graph
- source node and endpoint are selectable

---

## Phase 2: Create or Reopen Silver Proposal

### UI actions

1. In Modeling Console, go to `Proposals`.
2. Click `New Proposal`.
3. Select the Bronze source node.
4. Select endpoint `fleet/vehicles`.
5. Click `Create New Silver Proposal`.

### UI checks

- detail page opens for the created or reused Silver proposal
- proposal title shows a concrete proposal id
- status shows `draft` or `proposed`
- pipeline shows initial progression correctly
- `Compile` is available
- `Publish Release` and `Execute Release` are not yet visible

### Backend/API checks

Expected network:

- `POST /proposeSilverSchema` -> `200`
- if console reloads list, `GET /silverProposals` -> `200`
- detail load `GET /silverProposals/:id` -> `200`

Inspect DB:

```sql
select proposal_id, layer, target_model, status, source_graph_id, source_node_id, source_endpoint_name, created_at_utc
from model_proposal
where layer = 'silver'
  and source_graph_id = 2247
  and source_node_id = 2
  and source_endpoint_name = 'fleet/vehicles'
order by proposal_id desc
limit 5;
```

Expected:

- newest relevant Silver proposal exists
- status is `draft` or `proposed`
- proposal row points to correct graph/node/endpoint

### Additional DB inspection

```sql
select proposal_id, proposal_json::text
from model_proposal
where proposal_id = <silver_proposal_id>;
```

Check proposal JSON includes:

- `layer = silver`
- `source_layer = bronze`
- `target_model`
- `columns`
- `mappings`

### Pass criteria

- Silver proposal exists
- correct graph/node/endpoint binding
- detail view opens on the correct proposal id

---

## Phase 3: Edit Schema, Expression, and Processing Policy

### UI actions

1. In proposal detail, inspect `Silver Schema`.
2. Edit one safe field if needed:
   - example `Source Expression`:
   - `bronze.data_items_id`
3. In `Transformation Mapping`, click `Expression` for one mapping and confirm the editor opens.
4. Optionally apply a safe expression like:

```sql
UPPER(bronze."data_items_id")
```

5. If transform dropdown flow is enabled, click `Transform` on a mapping and apply a simple transform.
6. In `Processing Policy`, set:
   - `Business Keys`: `data_items_id`
   - `Ordering Strategy`: `latest_event_time_wins`
   - `Event Time Column`: choose timestamp-like column
   - `Late Data Mode`: `merge`
   - `Too-Late Behavior`: `quarantine` or current supported option
   - `Late Data Tolerance`: `10 minutes`
   - `Reprocess Window`: `24 hours`
7. Click `Save Changes`.

### UI checks

- `Expression` modal opens visibly
- `Transform` UI opens visibly if enabled
- `Save Changes` becomes enabled after edits
- after save, success message appears
- values remain visible in the form

### Backend/API checks

Expected network:

- `POST /updateSilverProposal` -> `200`

Reopen the same proposal from the list.

Expected after reopen:

- edited expression persists
- processing policy fields persist
- no fields silently revert

### Database checks

```sql
select proposal_json::text
from model_proposal
where proposal_id = <silver_proposal_id>;
```

Inspect `proposal_json`:

- `columns[].expression` updated if expression changed
- `mappings[].expression` updated if mapping expression changed
- `processing_policy.business_keys = ["data_items_id"]`
- `processing_policy.ordering_strategy = "latest_event_time_wins"`
- `processing_policy.event_time_column` is populated
- `processing_policy.reprocess_window` is present if configured

### Pass criteria

- edits persist through reopen
- proposal JSON matches the UI

---

## Phase 4: Compile Silver Proposal

### UI actions

1. From detail page, click `Compile`.

### UI checks

- status updates to `compiled`
- pipeline marks `Compile` done and `Validate` current
- `Validate` appears
- `Publish Release` does not appear yet
- compiled SQL card appears

### Backend/API checks

Expected network:

- `POST /compileSilverProposal` -> `200`
- detail reload `GET /silverProposals/:id` -> `200`

Inspect compile response in DevTools:

- `compiled_sql` present
- `sql_ir` present

What to look for in response payload:

- `sql_ir.processing_policy` exists
- ranked-before-merge logic exists if merge mode is in use

### Database checks

```sql
select status, compiled_sql
from model_proposal
where proposal_id = <silver_proposal_id>;
```

Expected:

- `status = compiled`
- `compiled_sql` is not null

If you want to inspect compile semantics:

- confirm SQL contains `ROW_NUMBER() OVER` when dedupe/ranking is expected
- confirm reprocess window filter appears if configured

### Pass criteria

- proposal compiles successfully
- compiled SQL is persisted
- compile stage persists after reopen

---

## Phase 5: Validate Silver Proposal

### What Validate Does

Validate is expected to:

- run static proposal checks
- compile proposal SQL if needed
- run sample execution or warehouse validation path
- persist a validation result
- move proposal to `validated` on success

### UI actions

1. Click `Validate`.

### UI checks

- status updates to `validated`
- pipeline keeps earlier steps blue
- `Review` becomes current
- `Go to Review` appears
- validation results card appears

### Backend/API checks

Expected network:

- `POST /validateSilverProposal` -> `200`
- detail reload `GET /silverProposals/:id` -> `200`

Inspect validate response payload:

- `status = valid`
- validation results/checks present when available

### Database checks

Validation table:

```sql
select validation_id, proposal_id, status, created_at_utc
from compiled_model_validation
where proposal_id = <silver_proposal_id>
order by validation_id desc
limit 5;
```

Proposal row:

```sql
select status
from model_proposal
where proposal_id = <silver_proposal_id>;
```

Expected:

- a new validation row exists
- proposal status is `validated`

### Persistence checks

1. Navigate back to `Proposals`.
2. Reopen the same proposal.
3. Confirm `validated` remains.
4. Hard reload browser page.
5. Reopen the same proposal again.

Expected:

- status still `validated`
- pipeline still shows `Compile` and `Validate` done

### Pass criteria

- validation result persisted
- validation state survives navigation and reload

---

## Phase 6: Review and Approve Silver Proposal

### UI actions

1. Click `Go to Review`.
2. In review tab, inspect:
   - schema summary
   - SQL preview
   - validation summary
3. In review decision dropdown, select `Approve`.
4. Enter notes if desired.
5. Click `Submit Review`.

### UI checks immediately after submit

- detail view returns or reloads cleanly
- detail header shows `Status: approved`
- success message appears:
  - `Review submitted: approved. Next step: Publish Release.`
- pipeline shows `Review` done and `Publish` current
- `Publish Release` button appears

### Backend/API checks

Expected network:

- `POST /reviewSilverProposal` -> `200`
- `GET /silverProposals/:id` -> `200`

Review response payload should include:

- `proposal_id`
- `status = approved`
- `review.state = approved`

### Database checks

```sql
select status, proposal_json::text
from model_proposal
where proposal_id = <silver_proposal_id>;
```

Inspect:

- top-level `status = approved`
- `proposal_json.review.state = approved`
- `proposal_json.review.reviewed_by` populated
- `proposal_json.review.reviewed_at` populated

### Persistence checks

These checks are mandatory because this area has regressed before.

1. Go back to `Proposals`.
2. Confirm the proposal row shows `approved`.
3. Reopen proposal.
4. Go to `Review` tab.
5. Confirm dropdown still shows `Approve` / `approved`.
6. Hard reload browser page.
7. Reopen the exact proposal id.
8. Go to `Review` tab again.

Expected:

- review dropdown still shows `approved`
- review tab does not revert to blank or `Review`
- pipeline still shows `Review` done and `Publish` current

### Pass criteria

- approval is persisted in DB
- approval survives navigation and hard reload

---

## Phase 7: Publish Silver Proposal

### UI actions

1. From detail view, click `Publish Release`.
2. Accept the confirmation dialog.

### UI checks

- proposal status becomes `published`
- pipeline shows `Publish` done and `Execute` current
- `Execute Release` appears
- `View Releases` appears

### Backend/API checks

Expected network:

- `POST /publishSilverProposal` -> `200`
- detail reload `GET /silverProposals/:id` -> `200`

Response should include:

- `release_id`
- published metadata

### Database checks

Proposal:

```sql
select status
from model_proposal
where proposal_id = <silver_proposal_id>;
```

Release:

```sql
select release_id, proposal_id, layer, target_model, status, active, published_at_utc
from compiled_model_release
where proposal_id = <silver_proposal_id>
order by release_id desc
limit 5;
```

Expected:

- proposal status is `published`
- a release row exists
- release is active

### Persistence checks

1. Go back to `Proposals`.
2. Reopen the same proposal.
3. Go to `Review` tab.
4. Confirm review dropdown still shows `approved`.
5. Confirm pipeline still shows publish complete.

### Pass criteria

- publish created a release row
- proposal remained published across reopen/reload
- review state remained intact after publish

---

## Phase 8: Execute Silver Release

### UI actions

1. Click `Execute Release`.
2. Accept confirmation.
3. UI should switch to `Releases & Execution`.
4. Watch execution monitor until terminal status.

### UI checks

- execution card becomes visible
- run id appears
- status moves through starting/running states if applicable
- terminal state becomes `SUCCEEDED`

### Backend/API checks

Expected network:

- `POST /executeSilverRelease` -> `200`
- `POST /pollSilverModelRun` -> repeated `200` until terminal

Execute response should include:

- `model_run_id`

Poll response should eventually indicate:

- `status = succeeded`

### Database checks

Run row:

```sql
select model_run_id, layer, target_model, status, execution_backend, created_at_utc, started_at_utc, finished_at_utc
from compiled_model_run
where model_run_id = <model_run_id>;
```

Expected:

- row exists
- `status = succeeded`
- `execution_backend` matches expected warehouse path, for example `postgresql_sql`

If execution links to a release:

```sql
select *
from compiled_model_run
where release_id = <release_id>
order by model_run_id desc;
```

### Warehouse checks

```sql
select count(*) from public.silver_fleet_vehicles;
```

Also inspect sample rows:

```sql
select *
from public.silver_fleet_vehicles
limit 10;
```

Expected:

- table exists
- row count is greater than zero
- data shape matches modeled Silver columns

If merge / late-data policy is in scope, check keys:

```sql
select data_items_id, count(*)
from public.silver_fleet_vehicles
group by 1
having count(*) > 1
limit 20;
```

Expected:

- no unexpected duplicate business keys if latest-state merge semantics are intended

### Pass criteria

- UI shows execution success
- run row persisted
- Silver target table populated

---

## Phase 9: Post-Execution Persistence Checks

These checks catch UI regressions that can otherwise slip through.

### UI actions

1. Navigate back to `Proposals`.
2. Reopen the executed proposal.
3. Confirm status remains `published`.
4. Go to `Review` tab.
5. Confirm review dropdown still shows `approved`.
6. Return to `Detail`.
7. Confirm `Execute Release` still appears for published proposal.
8. Hard reload browser page.
9. Repeat reopen checks.

### Backend/API checks

- `GET /silverProposals` shows row as `published`
- `GET /silverProposals/:id` shows:
  - `status = published`
  - `proposal.review.state = approved`
  - `active_release.release_id` present

### Pass criteria

- no loss of review state
- no loss of release visibility
- no UI regression after reload

---

## Optional Phase 10: Warehouse Validate Button

If the workflow uses `Warehouse Validate` separately:

### UI actions

1. Open a compiled proposal.
2. Click `Warehouse Validate`.

### UI checks

- clear status message appears
- no silent no-op

### Backend/API checks

- `POST /validateSilverProposalWarehouse` -> `200`

### Pass criteria

- route responds successfully
- UI communicates outcome clearly

---

## Failure Checklist

If any step fails, record all of the following:

- proposal id
- release id if present
- model run id if present
- current graph id
- current source node id
- current endpoint
- screenshot of UI state
- failing network request and response body
- relevant server log lines
- relevant DB rows from:
  - `model_proposal`
  - `compiled_model_validation`
  - `compiled_model_release`
  - `compiled_model_run`

## Minimum Defect Evidence Per Failure

For every failed check, capture:

1. Exact user action
2. Expected result
3. Actual result
4. Network request URL + status
5. Server-side error text if any
6. DB row state proving persistence or mismatch issue

---

## Final Pass/Fail Exit Criteria

The Bronze to Silver E2E QA pass is only `PASS` if all of the following are true:

- Bronze source table exists and contains data
- Silver proposal can be created or reopened correctly
- schema / mapping / processing policy edits persist
- compile succeeds and persists
- validate succeeds and persists
- review approval persists across navigation and hard reload
- publish creates a release and persists
- execute creates a succeeded model run
- Silver warehouse table is populated
- UI status, backend rows, and warehouse data all agree

The run is `FAIL` if any one of those is false.

## Suggested QA Evidence Bundle

At the end of a successful run, save:

- proposal id
- release id
- model run id
- screenshot of final published detail page
- screenshot of review tab showing persisted `approved`
- screenshot of execution status `SUCCEEDED`
- SQL output:

```sql
select proposal_id, status from model_proposal where proposal_id = <silver_proposal_id>;
select release_id, status from compiled_model_release where proposal_id = <silver_proposal_id> order by release_id desc limit 1;
select model_run_id, status, execution_backend from compiled_model_run where model_run_id = <model_run_id>;
select count(*) from public.silver_fleet_vehicles;
```

This is the minimum evidence set for closing Bronze to Silver QA as complete.
