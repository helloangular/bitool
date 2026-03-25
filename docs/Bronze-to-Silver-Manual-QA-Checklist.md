# Bronze to Silver Manual QA Checklist

## Purpose

This is the strict manual QA checklist for Bronze to Silver.

Use this when you want:

- every major click documented
- every Modeling Console dropdown called out
- expected defaults recorded
- allowed values recorded
- positive and negative coverage called out
- UI, API, DB verification after each step

This complements, not replaces, [Bronze-to-Silver-Manual-QA-Plan.md](/Users/aaryakulkarni/bitool/docs/Bronze-to-Silver-Manual-QA-Plan.md).

The other doc is the narrative runbook.
This doc is the strict operator checklist.

## Test Target

Reference environment:

- Bitool: `http://localhost:8080`
- Postgres: `postgresql://postgres:postgres@localhost/bitool`
- reference graph: `2247`
- Bronze source node: `2`
- endpoint: `fleet/vehicles`
- Bronze table: `public.fleet_vehicles`
- Silver table: `public.silver_fleet_vehicles`

Replace values if testing a different graph.

## Required Evidence for Each Failed Step

If any step fails, capture:

1. screenshot
2. exact click sequence
3. request URL and response body from DevTools
4. relevant server log lines
5. DB row state proving mismatch

## Global Pre-Checks

Before touching the UI, verify:

```sql
select count(*) from public.fleet_vehicles;
select count(*) from public.silver_fleet_vehicles;

select proposal_id, layer, target_model, status, created_at_utc
from model_proposal
where source_graph_id = 2247
order by proposal_id desc;
```

Expected:

- Bronze row count > 0
- prior Silver state known

---

## Section 1: Modeling Console Controls Inventory

This section is the control-by-control inventory for the current Bronze -> Silver UI.

### Header Controls

`Layer` dropdown: `#layerSelect`

Expected values:

- `silver`
- `gold`

Expected default:

- `silver`

Checks:

- changing to `gold` updates title copy and proposal form fields
- changing back to `silver` restores Silver fields

`Close` button: `#closeBtn`

Checks:

- closes console
- reopening console restores normal layout

### Proposal List Controls

`+ New Proposal` button: `#newProposalBtn`

Checks:

- reveals new proposal card

`Refresh` button: `#refreshProposalsBtn`

Checks:

- reloads proposal list without JS error

`Status Filter` dropdown: `#filterStatus`

Current values:

- blank / `All`
- `draft`
- `proposed`
- `compiled`
- `validated`
- `approved`
- `published`
- `rejected`

Checks:

- selecting each value reloads proposals
- row set changes accordingly
- clearing returns broader list

Note:

- backend statuses `reviewed` and `invalid` exist in modeling automation, but they are not currently exposed in the UI status filter dropdown
- if either status appears through direct API/DB inspection, record that as a UI/filter gap rather than a checklist failure

`Limit` dropdown: `#filterLimit`

Current values:

- `25`
- `50`
- `100`

Checks:

- selecting each value reloads proposals

### New Silver Proposal Controls

`Source Node ID` dropdown: `#propNodeId`

Checks:

- contains current open graph Bronze source nodes
- labels include visible node id
- selecting source enables endpoint dropdown

`Endpoint Name` dropdown: `#propEndpoint`

Checks:

- disabled before source node selection
- enabled after source node selection
- contains enabled endpoint names for selected source

`Generate Proposal` button: `#submitProposalBtn`

Checks:

- requires valid source + endpoint
- creates or reopens Silver proposal

`Cancel` button: `#cancelProposalBtn`

Checks:

- closes new proposal card

### Detail Controls

Schema card:

- `+ Add Column`: `#addColumnBtn`
- `Save Changes`: `#saveSchemaBtn`

Checks:

- `Save Changes` disabled on first load
- becomes enabled after editing any schema or processing policy field

### Workflow Steps

The detail pipeline has 7 steps, not 5:

- `Propose`
- `Edit`
- `Compile`
- `Validate`
- `Review`
- `Publish`
- `Execute`

### Processing Policy Controls

`Business Keys`: `#policyBusinessKeys`

Expected input format:

- comma-separated target columns
- example: `data_items_id`
- example: `trip_id, event_date`

`Ordering Strategy`: `#policyOrderingStrategy`

Current values:

- blank / `None`
- `latest_event_time_wins`
- `latest_sequence_wins`
- `event_time_then_sequence`
- `append_only`

`Event Time Column`: `#policyEventTime`

Expected:

- blank / `None`
- populated from available columns

`Sequence Column`: `#policySequence`

Expected:

- blank / `None`
- populated from available columns

`Late Data Mode`: `#policyLateMode`

Current values:

- blank / `Default`
- `merge`
- `append`

`Too-Late Behavior`: `#policyTooLate`

Current values:

- blank / `Default`
- `accept`
- `quarantine`
- `drop`

`Late Data Tolerance Value`: `#policyLateToleranceValue`

Expected:

- numeric
- min `0`

`Late Data Tolerance Unit`: `#policyLateToleranceUnit`

Current values:

- blank / `Unit`
- `minutes`
- `hours`
- `days`

`Reprocess Value`: `#policyReprocessValue`

Expected:

- numeric
- min `0`

`Reprocess Unit`: `#policyReprocessUnit`

Current values:

- blank / `Unit`
- `minutes`
- `hours`
- `days`

### Review Controls

`Review Decision`: `#reviewDecision`

Current values:

- blank / `-- Select --`
- `approved`
- `changes_requested`
- `rejected`

`Review Notes`: `#reviewNotes`

Checks:

- optional for `approved`
- required for `changes_requested`
- required for `rejected`

`Submit Review`: `#submitReviewBtn`

Checks:

- disabled until decision chosen
- enabled once decision selected

---

## Section 2: Exact Happy Path Click Checklist

Run every step in order and mark Pass/Fail.

### 2.1 Open graph and console

1. Open browser to `http://localhost:8080`
2. Use normal graph `Open` flow
3. Open graph `2247`
4. Confirm canvas renders
5. Open `Modeling Console`

Check:

- no `No graph selected`
- console visible
- tabs visible:
  - `Proposals`
  - `Proposal Detail`
  - `Review & Approve`
  - `Releases & Execution`
  - `SQL Preview`

### 2.2 Verify layer defaults

1. Inspect `Layer`

Check:

- default is `Silver`

2. Change to `Gold`
3. Change back to `Silver`

Check:

- no broken layout
- Silver proposal form uses source node + endpoint, not Silver Proposal ID

### 2.3 Open new proposal form

1. Click `+ New Proposal`

Check:

- new proposal card visible
- title is `Create New Silver Proposal`
- `Source Node ID` visible
- `Endpoint Name` visible
- `Generate Proposal` visible

### 2.4 Validate source node dropdown behavior

1. Inspect `Source Node ID`

Check:

- blank option exists
- source node options exist
- relevant source shows visible id like `#2`

2. Without selecting source node, inspect `Endpoint Name`

Check:

- disabled
- help text indicates source must be selected first

3. Select source node `2`

Check:

- endpoint dropdown becomes enabled
- endpoint list loads

### 2.5 Validate endpoint dropdown behavior

1. Open `Endpoint Name`
2. Select `fleet/vehicles`

Check:

- selected value sticks

### 2.6 Generate Silver proposal

1. Click `Generate Proposal`

Check in UI:

- proposal detail opens automatically
- proposal id visible in title
- status `draft` or `proposed`
- `Compile` visible
- `Synthesize Graph` visible
- `Validate` not visible
- `Warehouse Validate` not visible
- `Go to Review` not visible
- `Publish Release` not visible
- `Execute Release` not visible

Check in Network:

- `POST /proposeSilverSchema` -> `200`
- `GET /silverProposals/:id` -> `200`

Check in DB:

```sql
select proposal_id, status, source_graph_id, source_node_id, source_endpoint_name
from model_proposal
where proposal_id = <proposal_id>;
```

Expected:

- row exists
- layer `silver`
- graph id `2247`
- source node `2`
- endpoint `fleet/vehicles`

---

## Section 3: Detail Page Click-by-Click Checklist

### 3.1 Verify detail metadata

Check:

- title contains proposal id
- `Layer: Silver`
- status present
- source node present
- created timestamp present

### 3.2 Verify pipeline initial state

Check one of these:

- if `draft`, `Propose` current
- if `proposed`, `Propose` and `Edit` done, `Compile` current
- if `draft` and latest validation is `invalid`, `Validate` is red, `Propose/Edit/Compile` remain blue, and no current step is highlighted

Do not accept:

- random gray state with no current step

### 3.3 Verify schema table renders

Check:

- schema table has rows
- each row has:
  - Column Name
  - Data Type
  - Nullable
  - Primary Key
  - Description
  - Source Expression

### 3.4 Verify save button default state

Check:

- `Save Changes` is disabled on first load

### 3.5 Verify add column behavior

1. Click `+ Add Column`

Check:

- a new blank row appears
- `Save Changes` becomes enabled

2. Remove the new row with `×`

Check:

- row disappears

### 3.6 Verify mapping action buttons

For at least one mapping row:

1. Click `Expression`

Check:

- expression modal opens
- expression editor visible
- `Cancel` and `Apply Expression` visible

2. Click `Cancel`

Check:

- modal closes

3. If `Transform` is enabled, click `Transform`

Check:

- transform editor opens visibly
- not hidden behind panel

4. Close transform editor without saving

Check:

- main detail page remains intact

---

## Section 4: Processing Policy Exhaustive Checklist

This section validates every current Processing Policy control.

### 4.1 Business Keys

1. Click `Business Keys`
2. Enter `data_items_id`

Check:

- value remains in input
- `Save Changes` enabled

Negative case:

1. Clear it completely
2. Leave merge-oriented ordering selected
3. Try to save / validate later

Expected:

- validation should reject missing keys if ordering mode requires them

### 4.2 Ordering Strategy

Open dropdown and confirm all values exist:

- `None`
- `Latest event time wins`
- `Latest sequence wins`
- `Event time, then sequence`
- `Append only`

Positive case:

1. Select `Latest event time wins`

Check:

- selection persists visually

### 4.3 Event Time Column

Open dropdown and confirm:

- `None` exists
- column options are populated from proposal columns

Positive case:

1. Select a timestamp/date-like column

Check:

- selection persists visually

Negative case:

1. Leave ordering as `Latest event time wins`
2. set `Event Time Column` back to `None`
3. later validate

Expected:

- validation should fail

### 4.4 Sequence Column

Open dropdown and confirm:

- `None` exists
- column options are populated

Positive case:

1. Select a usable column if present
2. switch back to `None`

Check:

- both transitions work

### 4.5 Late Data Mode

Open dropdown and confirm values:

- `Default`
- `Merge`
- `Append`

Positive case:

1. Select `Merge`

Check:

- persists visually

### 4.6 Too-Late Behavior

Open dropdown and confirm values:

- `Default`
- `Accept`
- `Quarantine`
- `Drop`

Positive case:

1. Select `Quarantine`

Check:

- persists visually

### 4.7 Late Data Tolerance

Positive case:

1. Enter `10`
2. Select `minutes`

Check:

- both value and unit persist visually

Negative case:

1. Clear unit, keep value
2. later validate

Expected:

- validation should reject incomplete duration if current backend contract requires full pair

### 4.8 Reprocess Window

Positive case:

1. Enter `24`
2. Select `hours`

Check:

- both value and unit persist visually

Negative case:

1. set negative value if browser allows manual typing

Expected:

- input or validation rejects invalid value

---

## Section 5: Save Changes Checklist

### 5.1 Save policy changes

Use these final intended values:

- `Business Keys` = `data_items_id`
- `Ordering Strategy` = `latest_event_time_wins`
- `Event Time Column` = valid timestamp-like column
- `Sequence Column` = `None`
- `Late Data Mode` = `merge`
- `Too-Late Behavior` = `quarantine`
- `Late Data Tolerance` = `10 minutes`
- `Reprocess Window` = `24 hours`

1. Click `Save Changes`

Check in UI:

- save success message appears
- no forced tab switch
- values remain visible

Check in Network:

- `POST /updateSilverProposal` -> `200`

Check in DB:

```sql
select proposal_json::text
from model_proposal
where proposal_id = <proposal_id>;
```

Expected JSON contains:

- `processing_policy.business_keys = ["data_items_id"]`
- `processing_policy.ordering_strategy = "latest_event_time_wins"`
- `processing_policy.event_time_column` populated
- `processing_policy.late_data_mode = "merge"`
- `processing_policy.too_late_behavior = "quarantine"`
- `processing_policy.late_data_tolerance = {"value":10,"unit":"minutes"}`
- `processing_policy.reprocess_window = {"value":24,"unit":"hours"}`

### 5.2 Reopen persistence check

1. Navigate back to `Proposals`
2. Reopen same proposal

Check:

- every value above still displayed in UI

3. Hard reload page
4. Reopen same proposal again

Check:

- every value still displayed

---

## Section 6: Compile Checklist

### 6.1 Compile action

1. Click `Compile`

Check in UI:

- status becomes `compiled`
- `Compile` step blue
- `Validate` current
- `Validate` visible
- `Warehouse Validate` visible
- `Go to Review` visible
- `Publish Release` hidden

Check in Network:

- `POST /compileSilverProposal` -> `200`

Check response:

- `compiled_sql` present
- `sql_ir` present

### 6.2 Compiled SQL semantics

Inspect response payload or stored SQL and confirm:

- processing policy is represented in `sql_ir`
- ranking logic exists when merge semantics apply

Look for:

- `ROW_NUMBER() OVER`
- reprocess interval like `INTERVAL '24 hours'`

Check in DB:

```sql
select status, compiled_sql
from model_proposal
where proposal_id = <proposal_id>;
```

Expected:

- `status = compiled`
- compiled SQL persisted

### 6.3 Compile persistence

1. Back to `Proposals`
2. Reopen proposal
3. Hard reload
4. Reopen proposal

Check:

- still `compiled`
- `Validate` still visible
- earlier pipeline steps remain blue

### 6.4 Compiled review shortcut regression

At `compiled` status, the current UI also exposes `Go to Review`.

1. Click `Go to Review` before running `Validate`

Expected:

- review tab opens
- submitting review should fail with a `409` because the proposal is not actually reviewable yet
- record this as a product gap, not a checklist failure

### 6.5 Warehouse Validate

1. From compiled state, click `Warehouse Validate`

Check in Network:

- `POST /validateSilverProposalWarehouse` -> `200`

Check in UI:

- success or failure feedback is visible
- no silent no-op

Check in DB:

```sql
select validation_id, proposal_id, status, validation_kind, created_at_utc
from model_validation_result
where proposal_id = <proposal_id>
order by validation_id desc
limit 5;
```

Expected:

- newest row includes a warehouse validation kind for the Silver proposal

### 6.6 SQL Preview tab

1. Open `SQL Preview`
2. Inspect each available SQL sub-tab
3. Click copy if copy control is present

Check:

- SQL text renders
- tab switching works
- copy action does not throw a JS error

---

## Section 7: Validate Checklist

### 7.1 Validate success path

1. Click `Validate`

Check in UI:

- status becomes `validated`
- `Validate` step blue
- `Review` current
- `Go to Review` visible
- validation card visible

Check in Network:

- `POST /validateSilverProposal` -> `200`

Check response:

- `status = valid`

### 7.2 Validate persistence

1. Back to `Proposals`
2. Reopen proposal
3. Hard reload
4. Reopen proposal

Check:

- still `validated`
- previous steps remain blue
- `Go to Review` still visible

### 7.3 Validation DB checks

```sql
select validation_id, proposal_id, status, created_at_utc
from model_validation_result
where proposal_id = <proposal_id>
order by validation_id desc
limit 3;
```

Expected:

- latest row exists
- status indicates valid/pass

### 7.4 Validate failure path

Use an invalid processing policy combination, for example:

- `Ordering Strategy = latest_event_time_wins`
- `Event Time Column = None`

1. Save changes
2. Click `Compile` if required
3. Click `Validate`

Expected in UI:

- route returns `status = invalid`
- proposal top-level status returns to `draft`
- `Validate` step is red
- `Propose/Edit/Compile` stay blue
- no misleading success message

Expected in Network:

- `POST /validateSilverProposal` -> `200`
- response payload includes validation errors

Expected in DB:

```sql
select validation_id, proposal_id, status, validation_json
from model_validation_result
where proposal_id = <proposal_id>
order by validation_id desc
limit 3;
```

Expected:

- latest row has `status = invalid`
- `validation_json` contains schema and/or sample execution errors

---

## Section 8: Review & Approve Exhaustive Checklist

### 8.1 Open review tab

1. Click `Go to Review`

Check:

- review tab visible
- review pipeline rendered
- schema summary visible
- SQL preview visible
- validation summary visible

### 8.2 Review decision dropdown validation

Open `Review Decision` and confirm values:

- blank
- `Approve`
- `Request Changes`
- `Reject`

Note:

- backend also supports `reviewed`, but the current UI does not expose it in the dropdown
- if a proposal reaches `reviewed` through API or DB mutation, record that as backend-only state coverage

Check:

- `Submit Review` disabled before selection

### 8.3 Approve path

1. Select `Approve`

Check:

- `Submit Review` enabled
- notes still optional

2. Click `Submit Review`

Check in UI:

- detail tab returns or updates
- status becomes `approved`
- success text shown
- `Publish Release` visible
- pipeline shows `Review` blue and `Publish` current

Check in Network:

- `POST /reviewSilverProposal` -> `200`

Check response:

- `status = approved`

### 8.4 Approval persistence

Mandatory regression checks:

1. Back to `Proposals`
2. confirm proposal row status `approved`
3. reopen same proposal
4. go to `Review & Approve`
5. confirm dropdown shows `approved`
6. hard reload page
7. reopen same proposal
8. go to `Review & Approve`
9. confirm dropdown still shows `approved`

### 8.5 Review DB checks

```sql
select status, proposal_json::text
from model_proposal
where proposal_id = <proposal_id>;
```

Expected:

- top-level status `approved`
- `proposal_json.review.state = approved`
- `reviewed_by` present
- `reviewed_at` present

### 8.6 Negative review cases

`Request Changes`

1. On a separate proposal, select `Request Changes`
2. leave notes blank
3. click `Submit Review`

Expected:

- blocked by UI

4. add notes
5. submit again

Expected:

- route succeeds
- status becomes `changes_requested`

`Reject`

1. On a separate proposal, select `Reject`
2. leave notes blank
3. click `Submit Review`

Expected:

- blocked by UI

4. add notes
5. submit again

Expected:

- route succeeds
- status becomes `rejected`

### 8.7 Changes requested re-review loop

1. Start from a proposal in `changes_requested`
2. Confirm pipeline shows:
   - `Review` red
   - `Edit` current
3. Confirm actions visible:
   - `Compile`
   - `Synthesize Graph`
4. Edit schema, mapping, or policy
5. Save changes
6. Re-run:
   - `Compile`
   - `Validate`
   - `Review`
7. Approve

Expected:

- proposal can re-enter the lifecycle cleanly
- review step returns to blue after approval

### 8.8 Rejected lifecycle

1. Open a proposal in `rejected`

Check:

- pipeline shows `Review` red
- no detail actions are available

Then verify backend edit behavior:

```sql
select status
from model_proposal
where proposal_id = <proposal_id>;
```

Expected:

- proposal remains `rejected`
- editing in place is not available
- record this as a stuck-state product gap if no recovery path exists

### 8.9 Clone-on-edit behavior

For proposals in `validated`, `reviewed`, `approved`, or `published`:

1. Edit schema or processing policy
2. Click `Save Changes`

Expected:

- backend creates a new draft clone with a new proposal id
- original proposal keeps original id and status
- proposal list shows both original and clone

Check in DB:

```sql
select proposal_id, status, target_model, created_at_utc
from model_proposal
where source_graph_id = 2247
order by proposal_id desc
limit 10;
```

---

## Section 9: Publish Checklist

### 9.1 Publish action

1. From approved detail page, click `Publish Release`
2. accept confirmation dialog

Check in UI:

- status becomes `published`
- `Execute` becomes current
- `Execute Release` visible
- `View Releases` visible

Check in Network:

- `POST /publishSilverProposal` -> `200`

Check response:

- `release_id` present

### 9.2 Publish persistence

1. Back to `Proposals`
2. reopen proposal
3. open `Review & Approve`
4. confirm dropdown still `approved`
5. hard reload
6. reopen proposal

Check:

- status still `published`
- review decision still `approved`

### 9.3 Publish DB checks

```sql
select status
from model_proposal
where proposal_id = <proposal_id>;

select release_id, proposal_id, status, active, published_at_utc
from model_release
where proposal_id = <proposal_id>
order by release_id desc
limit 3;
```

Expected:

- proposal `published`
- release row exists
- active release present

---

## Section 10: Execute Checklist

### 10.1 Execute action

1. Click `Execute Release`
2. accept confirmation

Check in UI:

- console switches to `Releases & Execution`
- execution card visible
- run id visible
- status badge visible

### 10.2 Polling behavior

Check:

- poll request sent
- terminal status eventually appears
- status text shows `SUCCEEDED`

Network:

- `POST /executeSilverRelease` -> `200`
- `POST /pollSilverModelRun` -> repeated `200`

### 10.3 Execute DB checks

```sql
select model_run_id, status, execution_backend, started_at_utc, finished_at_utc
from compiled_model_run
where release_id = <release_id>
order by model_run_id desc
limit 3;
```

Expected:

- newest row exists
- `status = succeeded`
- correct backend value, for example `postgresql_sql`

### 10.5 Re-execution

1. On the same published proposal, click `Execute Release` again
2. Accept confirmation

Expected:

- a new `compiled_model_run` row is created
- previous runs remain intact
- UI again reaches terminal status

### 10.4 Warehouse checks

```sql
select count(*) from public.silver_fleet_vehicles;

select *
from public.silver_fleet_vehicles
limit 10;
```

Expected:

- table exists
- row count > 0

If latest-state semantics are expected:

```sql
select data_items_id, count(*)
from public.silver_fleet_vehicles
group by 1
having count(*) > 1
limit 20;
```

Expected:

- no unexpected duplicate keys

---

## Section 11: Post-Execution Reload Checklist

This section must be run even if everything else passes.

1. Hard reload page
2. Open graph again
3. Open Modeling Console
4. Select `Silver`
5. Open same proposal id

Check:

- status `published`
- `Execute Release` still visible
- `Review & Approve` tab shows dropdown value `approved`
- no UI regression to blank review state

## Section 11A: Concurrent / Conflicting Operations

### 11A.1 Two-tab edit conflict

1. Open the same proposal in two browser tabs
2. In tab A, change schema or policy and save
3. In tab B, make a different edit and save

Check:

- capture whether tab B overwrites silently, errors, or clones
- record the exact behavior because there is no explicit concurrency guard in the current UI checklist

### 11A.2 Double-click review submit

1. From review tab, choose a decision
2. Double-click `Submit Review`

Check:

- only one review request should be committed
- no duplicate state transitions or duplicate success banners

### 11A.3 Compile while previous compile is pending

1. Trigger `Compile`
2. Attempt to trigger `Compile` again before the first response returns

Check:

- button disabling or duplicate-request protection behavior is explicit
- no duplicated compile state or broken UI

---

## Section 12: Required Negative Cases Matrix

These are the minimum negative cases for Bronze -> Silver.

1. Create proposal with no source node selected
Expected:
- blocked

2. Create proposal with source node selected but no endpoint selected
Expected:
- blocked

3. Save processing policy with invalid required combination:
- `latest_event_time_wins` and no event time column
Expected:
- later validation fails

4. Save review with `changes_requested` and blank notes
Expected:
- blocked in UI

5. Save review with `rejected` and blank notes
Expected:
- blocked in UI

6. Reopen approved proposal after hard reload
Expected:
- dropdown still `approved`

7. Reopen published proposal after hard reload
Expected:
- dropdown still `approved`

---

## Section 13: Final Exit Criteria

The Bronze -> Silver manual QA pass is only complete if all items below are true:

- every required control is present
- every required dropdown contains the documented values
- happy path proposal creation succeeds
- schema and processing policy edits persist
- compile succeeds and persists
- validate succeeds and persists
- approve succeeds and persists
- publish succeeds and persists
- execute succeeds and persists
- Silver table contains data
- hard reload does not lose review state or lifecycle state

If any item above fails, the QA result is `FAIL`.

## Section 14: Final Evidence Bundle

Capture this at the end of a successful run:

1. proposal id
2. release id
3. model run id
4. screenshot of proposal detail showing `published`
5. screenshot of review tab showing `approved`
6. screenshot of execution monitor showing `SUCCEEDED`
7. SQL output:

```sql
select proposal_id, status from model_proposal where proposal_id = <proposal_id>;
select release_id, status from model_release where proposal_id = <proposal_id> order by release_id desc limit 1;
select model_run_id, status, execution_backend from compiled_model_run where model_run_id = <model_run_id>;
select count(*) from public.silver_fleet_vehicles;
```

This is the minimum close-out evidence for Bronze -> Silver manual QA.
