# Silver to Gold Manual QA Checklist

## Purpose

This is the strict manual QA checklist for Silver to Gold.

Use this when you want:

- every major click documented
- every Modeling Console control called out
- every relevant dropdown/value checked
- every lifecycle stage verified
- UI, API, DB verification after each step
- persistence checks after navigation and hard reload

This document is the Gold equivalent of the Silver checklist and is intended for full end-to-end manual verification.

## Test Target

Reference environment:

- Bitool: `http://localhost:8080`
- Postgres: `postgresql://postgres:postgres@localhost/bitool`
- reference graph: `2247`
- Bronze source node: `2`
- endpoint: `fleet/vehicles`
- Silver source model: `silver_fleet_vehicles`
- Gold target table: `public.gold_fleet_vehicles`

Replace values if testing a different graph or warehouse.

## Required Evidence for Each Failed Step

If any step fails, capture:

1. screenshot
2. exact click sequence
3. request URL and response body from DevTools
4. relevant server log lines
5. DB rows proving the mismatch

## Global Pre-Checks

Before touching the UI, verify:

```sql
select count(*) from public.silver_fleet_vehicles;
select count(*) from public.gold_fleet_vehicles;

select proposal_id, layer, target_model, status, created_at_utc
from model_proposal
where source_graph_id = 2247
order by proposal_id desc;
```

Expected:

- Silver row count > 0
- prior Gold state known

If Silver is not populated, stop here. Gold QA is not meaningful without Silver input data.

---

## Section 1: Modeling Console Controls Inventory for Gold

### Header Controls

`Layer` dropdown: `#layerSelect`

Expected values:

- `silver`
- `gold`

Checks:

- switching to `gold` updates the proposal copy and form fields
- switching back to `silver` restores Silver-specific fields

`Close` button: `#closeBtn`

Checks:

- closes console
- reopening restores layout cleanly

### Proposal List Controls

`+ New Proposal`: `#newProposalBtn`

Checks:

- opens the new proposal card

`Refresh`: `#refreshProposalsBtn`

Checks:

- reloads Gold proposal list without error

`Status Filter`: `#filterStatus`

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

- selecting each value reloads list
- results match chosen filter

Note:

- backend statuses `reviewed` and `invalid` exist, but the current UI filter does not expose them
- if either appears through API or DB inspection, record that as a UI/filter gap

`Limit`: `#filterLimit`

Current values:

- `25`
- `50`
- `100`

Checks:

- changing value reloads list

### New Gold Proposal Controls

`Silver Proposal ID`: `#propSilverProposalId`

Checks:

- visible only in Gold mode
- accepts numeric input
- required to generate a Gold proposal

`Generate Proposal`: `#submitProposalBtn`

Checks:

- blocked if Silver Proposal ID is blank
- generates or reopens a Gold proposal when valid

`Cancel`: `#cancelProposalBtn`

Checks:

- hides the new proposal card

### Detail Controls

Schema card:

- `+ Add Column`: `#addColumnBtn`
- `Save Changes`: `#saveSchemaBtn`

Checks:

- `Save Changes` disabled on first load
- becomes enabled after any schema/mapping/policy edit

### Workflow Steps

The detail pipeline has 7 steps:

- `Propose`
- `Edit`
- `Compile`
- `Validate`
- `Review`
- `Publish`
- `Execute`

### Processing Policy Controls

Gold currently uses the same shared detail UI, so the same controls exist:

- `#policyBusinessKeys`
- `#policyOrderingStrategy`
- `#policyEventTime`
- `#policySequence`
- `#policyLateMode`
- `#policyTooLate`
- `#policyLateToleranceValue`
- `#policyLateToleranceUnit`
- `#policyReprocessValue`
- `#policyReprocessUnit`

Even if not all are semantically meaningful for current Gold execution, the UI should still behave consistently.

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

- disabled until a decision is chosen
- enabled once a decision is selected

---

## Section 2: Exact Happy Path Click Checklist

### 2.1 Open graph and console

1. Open browser to `http://localhost:8080`
2. Use normal graph `Open` flow
3. Open graph `2247`
4. Confirm canvas renders
5. Open `Modeling Console`
6. Set `Layer` to `Gold`

Check:

- no `No graph selected`
- console visible
- tabs visible:
  - `Proposals`
  - `Proposal Detail`
  - `Review & Approve`
  - `Releases & Execution`
  - `SQL Preview`

### 2.2 Verify Gold layer copy

Check:

- `Layer` is `Gold`
- proposals title says `Gold Schema Proposals`
- new proposal title says `Create New Gold Proposal`
- help text mentions selecting an existing Silver proposal

### 2.3 Open new Gold proposal form

1. Click `+ New Proposal`

Check:

- new proposal card visible
- `Silver Proposal ID` visible
- Silver-specific source node/endpoint controls are hidden
- `Generate Proposal` visible

### 2.4 Negative create case

1. Leave `Silver Proposal ID` blank
2. Click `Generate Proposal`

Expected:

- blocked
- user sees an error such as `Silver Proposal ID is required.`

### 2.4A Negative create with non-existent Silver proposal id

1. Enter a non-existent `Silver Proposal ID`
2. Click `Generate Proposal`

Expected:

- request fails clearly
- response is a `404` or equivalent not-found error
- user sees a direct error instead of a silent no-op

### 2.4B Create from unpublished Silver proposal

Current backend behavior only requires the source proposal to exist and be a Silver proposal.

1. Use a valid Silver proposal id that is not yet published
2. Click `Generate Proposal`

Expected:

- Gold proposal generation still works
- if it fails, record the route response as a regression

### 2.5 Positive create case

1. Enter a valid Silver proposal id
2. Click `Generate Proposal`

Check in UI:

- Gold proposal detail opens
- proposal id visible
- status `draft` or `proposed`
- `Compile` visible
- `Synthesize Graph` visible
- `Validate` hidden
- `Warehouse Validate` hidden
- `Go to Review` hidden
- `Publish Release` hidden
- `Execute Release` hidden

Check in Network:

- `POST /proposeGoldSchema` -> `200`
- `GET /goldProposals/:id` -> `200`

Check in DB:

```sql
select proposal_id, status, source_graph_id, source_node_id, source_endpoint_name, target_model
from model_proposal
where proposal_id = <gold_proposal_id>;
```

Expected:

- row exists
- layer `gold`
- graph id `2247`
- source node points to the Silver source lineage

---

## Section 3: Detail Page Click-by-Click Checklist

### 3.1 Verify detail metadata

Check:

- title contains proposal id
- `Layer: Gold`
- status present
- source node present
- created timestamp present

### 3.2 Verify pipeline initial state

Check one of these:

- if `draft`, `Propose` current
- if `proposed`, `Propose` and `Edit` done, `Compile` current
- if `draft` and latest validation is `invalid`, `Validate` is red, `Propose/Edit/Compile` remain blue, and no current step is highlighted

Do not accept:

- all-gray pipeline with no current step

### 3.3 Verify schema table renders

Check:

- schema rows exist
- each row contains:
  - Column Name
  - Data Type
  - Nullable
  - Primary Key
  - Description
  - Source Expression

### 3.4 Verify save button default state

Check:

- `Save Changes` disabled on first load

### 3.5 Verify add/remove column behavior

1. Click `+ Add Column`

Check:

- blank row appears
- `Save Changes` becomes enabled

2. Remove the row using `×`

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
- not hidden behind the console

4. Close transform editor without saving

Check:

- detail page remains intact

---

## Section 4: Gold Mapping and Expression Editing Checklist

Gold needs explicit transform verification because this is where aggregation and derived expressions often regress.

### 4.1 Expression editing

1. Click `Expression` on one Gold mapping row
2. Enter a safe expression such as:

```sql
UPPER(silver."data_items_id")
```

3. Apply expression

Check:

- modal closes
- expression note in mapping row updates
- `Save Changes` becomes enabled

### 4.2 Transform editing

If the mapping row has `Transform` enabled:

1. Click `Transform`
2. Apply a transform like `UPPERCASE`
3. Save transform

Check:

- row transform summary updates
- resulting expression is reflected in mapping detail
- `Save Changes` enabled

### 4.3 Save edited mappings

1. Click `Save Changes`

Check in UI:

- success message appears
- changed expression remains visible

Check in Network:

- `POST /updateGoldProposal` -> `200`

Check in DB:

```sql
select proposal_json::text
from model_proposal
where proposal_id = <gold_proposal_id>;
```

Expected:

- changed `columns[].expression` or `mappings[].expression` persisted

### 4.4 Reopen persistence

1. Go back to `Proposals`
2. Reopen same proposal
3. Hard reload page
4. Reopen same proposal again

Check:

- expression edit persists
- transform edit persists if used

---

## Section 5: Processing Policy Checklist

Gold currently exposes the same policy controls. Validate the controls even if the current compiler uses only part of them.

### 5.1 Business Keys

1. Enter a valid value such as:

- `data_items_id,event_date`

Check:

- value stays in input
- `Save Changes` enabled

### 5.2 Ordering Strategy

Open dropdown and confirm values:

- `None`
- `Latest event time wins`
- `Latest sequence wins`
- `Event time, then sequence`
- `Append only`

### 5.3 Event Time / Sequence

Open `Event Time Column` and `Sequence Column`

Check:

- `None` exists
- available columns are populated

### 5.4 Late Data and Reprocess controls

Open and verify values for:

- `Late Data Mode`
- `Too-Late Behavior`
- `Late Data Tolerance Unit`
- `Reprocess Unit`

Confirm values exist exactly as expected:

- `merge`
- `append`
- `accept`
- `quarantine`
- `drop`
- `minutes`
- `hours`
- `days`

### 5.5 Save policy edits

1. Enter a valid set of policy values
2. Click `Save Changes`

Check in UI:

- success message appears
- values persist on reopen

Check in Network:

- `POST /updateGoldProposal` -> `200`

Check in DB:

- `proposal_json.processing_policy` reflects saved values

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

- `POST /compileGoldProposal` -> `200`

Check response:

- `compiled_sql` present
- `sql_ir` present

### 6.2 SQL semantics

Inspect response or SQL preview and confirm:

- compiled SQL exists
- Gold expressions appear correctly quoted for the warehouse
- aggregate expressions, if generated, appear correctly

Specific regression checks:

- mixed-case Silver source columns are quoted correctly, for example `silver."data_items_createdAtTime"`
- aggregate expressions such as `SUM(...)`, `COUNT(*)`, and matching `GROUP BY` clauses are syntactically consistent when present

### 6.3 Compile persistence

1. Back to `Proposals`
2. Reopen proposal
3. Hard reload
4. Reopen proposal

Check:

- status still `compiled`
- `Validate` still available

### 6.4 Compiled review shortcut regression

At `compiled` status the UI also exposes `Go to Review`.

1. Click `Go to Review` before validating

Expected:

- review tab opens
- review submit should fail with `409`
- record this as a product gap, not a checklist failure

### 6.5 Warehouse Validate

1. From compiled state, click `Warehouse Validate`

Check in Network:

- `POST /validateGoldProposalWarehouse` -> `200`

Check in DB:

```sql
select validation_id, proposal_id, status, validation_kind, created_at_utc
from model_validation_result
where proposal_id = <gold_proposal_id>
order by validation_id desc
limit 5;
```

### 6.6 SQL Preview tab

1. Open `SQL Preview`
2. Inspect each available SQL sub-tab
3. Use copy control if present

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
- `Validate` blue
- `Review` current
- `Go to Review` visible
- validation card appears

Check in Network:

- `POST /validateGoldProposal` -> `200`

Check response:

- `status = valid`

### 7.2 Validate persistence

1. Back to `Proposals`
2. Reopen proposal
3. Hard reload
4. Reopen proposal

Check:

- status still `validated`
- pipeline still advanced correctly

### 7.3 Validation DB checks

```sql
select validation_id, proposal_id, status, created_at_utc
from model_validation_result
where proposal_id = <gold_proposal_id>
order by validation_id desc
limit 3;
```

Expected:

- latest row exists
- status indicates valid/pass

### 7.4 Validate failure path

Use an invalid configuration, for example:

- `Ordering Strategy = latest_event_time_wins`
- `Event Time Column = None`

1. Save changes
2. Click `Compile` if required
3. Click `Validate`

Expected in UI:

- validation returns `status = invalid`
- proposal top-level status returns to `draft`
- `Validate` step becomes red
- `Propose/Edit/Compile` stay blue

Expected in DB:

```sql
select validation_id, proposal_id, status, validation_json
from model_validation_result
where proposal_id = <gold_proposal_id>
order by validation_id desc
limit 3;
```

---

## Section 8: Review and Approve Checklist

### 8.1 Open review tab

1. Click `Go to Review`

Check:

- review tab visible
- review pipeline rendered
- schema summary visible
- SQL preview visible
- validation summary visible

### 8.2 Review decision dropdown validation

Open `Review Decision`

Confirm values:

- blank
- `Approve`
- `Request Changes`
- `Reject`

Note:

- backend also supports `reviewed`, but the current UI does not expose it as a selectable decision

Check:

- `Submit Review` disabled before selection

### 8.3 Negative review cases

`Request Changes`

1. Select `Request Changes`
2. leave notes blank
3. click `Submit Review`

Expected:

- blocked in UI
- note-required message shown

`Reject`

1. Select `Reject`
2. leave notes blank
3. click `Submit Review`

Expected:

- blocked in UI
- note-required message shown

### 8.4 Approve path

1. Select `Approve`
2. click `Submit Review`

Check in UI:

- detail view returns or updates
- status becomes `approved`
- success text appears
- `Publish Release` visible
- review step blue
- publish current

Check in Network:

- `POST /reviewGoldProposal` -> `200`

Check response:

- `status = approved`

### 8.5 Approval persistence

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

### 8.6 Review DB checks

```sql
select status, proposal_json::text
from model_proposal
where proposal_id = <gold_proposal_id>;
```

Expected:

- top-level status `approved`
- `proposal_json.review.state = approved`
- `reviewed_by` present
- `reviewed_at` present

### 8.7 Changes requested re-review loop

1. Start from a proposal in `changes_requested`
2. Confirm pipeline shows:
   - `Review` red
   - `Edit` current
3. Confirm available actions:
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

- proposal can recover cleanly from `changes_requested`

### 8.8 Rejected lifecycle

1. Open a proposal in `rejected`

Check:

- pipeline shows `Review` red
- no detail actions are available

Expected:

- rejected proposal has no normal recovery path today
- record this as a product gap if editing is impossible

### 8.9 Clone-on-edit behavior

For proposals in `validated`, `reviewed`, `approved`, or `published`:

1. Edit schema or processing policy
2. Click `Save Changes`

Expected:

- a new draft clone is created with a new proposal id
- original proposal remains at its old status
- proposal list shows both rows

---

## Section 9: Publish Checklist

### 9.1 Publish action

1. Click `Publish Release`
2. accept confirmation dialog

Check in UI:

- status becomes `published`
- `Publish` step blue
- `Execute` current
- `Execute Release` visible
- `View Releases` visible

Check in Network:

- `POST /publishGoldProposal` -> `200`

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
where proposal_id = <gold_proposal_id>;

select release_id, proposal_id, status, active, published_at_utc
from model_release
where proposal_id = <gold_proposal_id>
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

- switches to `Releases & Execution`
- execution card visible
- run id visible
- status badge visible

### 10.2 Polling behavior

Check:

- poll request sent
- terminal status appears
- final status shows `SUCCEEDED`

Network:

- `POST /executeGoldRelease` -> `200`
- `POST /pollGoldModelRun` -> repeated `200`

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
- earlier runs remain intact
- terminal run status appears again in the UI

### 10.4 Warehouse checks

```sql
select count(*) from public.gold_fleet_vehicles;

select *
from public.gold_fleet_vehicles
limit 10;
```

Expected:

- table exists
- row count > 0

If Gold is aggregated:

- confirm row count is consistent with the intended grain
- confirm no obvious duplicate grain keys

Example:

```sql
select data_items_id, event_date, count(*)
from public.gold_fleet_vehicles
group by 1,2
having count(*) > 1
limit 20;
```

Expected:

- no unexpected duplicate Gold grain keys

---

## Section 11: Post-Execution Reload Checklist

1. Hard reload page
2. Open graph again
3. Open Modeling Console
4. Select `Gold`
5. Open the same proposal id

Check:

- status `published`
- `Execute Release` still visible
- `Review & Approve` tab still shows `approved`
- no regression to blank review state

## Section 11A: Concurrent / Conflicting Operations

### 11A.1 Two-tab edit conflict

1. Open the same Gold proposal in two browser tabs
2. In tab A, edit schema or policy and save
3. In tab B, make a different edit and save

Check:

- capture whether tab B overwrites silently, errors, or clones
- record exact behavior because the current checklist has no explicit concurrency guard

### 11A.2 Double-click review submit

1. In review tab, choose a decision
2. Double-click `Submit Review`

Check:

- only one review request should commit
- no duplicate success banners or duplicated status changes

### 11A.3 Compile while previous compile is pending

1. Trigger `Compile`
2. Attempt to trigger `Compile` again before the first response returns

Check:

- button disabling or duplicate-request protection behavior is explicit
- no duplicated compile state or broken UI

---

## Section 12: Required Negative Cases Matrix

These are the minimum negative cases for Silver -> Gold.

1. Create Gold proposal with blank Silver Proposal ID
Expected:
- blocked

2. Create Gold proposal with non-existent Silver Proposal ID
Expected:
- clear not-found error

3. Create Gold proposal from an unpublished Silver proposal
Expected:
- should still work with current backend behavior

4. Save review with `changes_requested` and blank notes
Expected:
- blocked in UI

5. Save review with `rejected` and blank notes
Expected:
- blocked in UI

6. Mixed-case column quoting in compiled SQL
Expected:
- mixed-case identifiers stay quoted

7. Aggregation SQL correctness
Expected:
- aggregate expressions and `GROUP BY` are consistent

8. Reopen approved Gold proposal after hard reload
Expected:
- review dropdown still `approved`

9. Reopen published Gold proposal after hard reload
Expected:
- review dropdown still `approved`

---

## Section 13: Final Exit Criteria

The Silver -> Gold manual QA pass is complete only if all items below are true:

- Gold proposal can be created or reopened from a Silver proposal
- mapping/expression edits persist
- compile succeeds and persists
- validate succeeds and persists
- approve succeeds and persists
- publish succeeds and persists
- execute succeeds and persists
- Gold table contains data
- hard reload does not lose review state or lifecycle state

If any item above fails, the QA result is `FAIL`.

## Section 14: Final Evidence Bundle

Capture this at the end of a successful run:

1. Gold proposal id
2. release id
3. model run id
4. screenshot of proposal detail showing `published`
5. screenshot of review tab showing `approved`
6. screenshot of execution monitor showing `SUCCEEDED`
7. SQL output:

```sql
select proposal_id, status from model_proposal where proposal_id = <gold_proposal_id>;
select release_id, status from model_release where proposal_id = <gold_proposal_id> order by release_id desc limit 1;
select model_run_id, status, execution_backend from compiled_model_run where model_run_id = <model_run_id>;
select count(*) from public.gold_fleet_vehicles;
```

This is the minimum close-out evidence for Silver -> Gold manual QA.
