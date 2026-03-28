# Current UI and AI Gap Matrix for Bronze, Silver, and Gold

## 1. Purpose

This document grounds the product discussion in the current Bitool codebase.

The goal is to answer a practical question:

- what is already present in the existing UI and backend,
- what is automated already,
- what is actually AI today versus heuristic automation,
- what gaps remain for a stronger `API -> Bronze -> Silver -> Gold` product.

This is intentionally not a greenfield product plan. It is a delta analysis against the current implementation.

---

## 2. Executive Summary

The short version is:

- `API -> Bronze` is already substantially covered.
- `Preview Schema` is real and already does useful inference and recommendation work.
- `Silver` and `Gold` proposal lifecycle is already present in the UI and backend.
- the biggest missing layer is not core flow coverage, but in-product AI assistance and explanation.

That means the next product step should not be to redesign Bronze/Silver/Gold from scratch.

It should be to:

1. keep the current API Configuration UI as the Bronze workbench,
2. keep the current Modeling Console as the Silver/Gold workbench,
3. add AI assistance inside those existing flows.

---

## 3. What Exists Today

### 3.1 Bronze / API authoring

The existing API UI already supports:

- connection configuration in [resources/public/apiComponent.js](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js),
- endpoint discovery from OpenAPI specs in [resources/public/apiComponent.js:1758](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L1758),
- per-endpoint load configuration and Bronze defaults in [resources/public/apiComponent.js:304](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L304),
- schema preview and sampling in [resources/public/apiComponent.js:1816](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L1816),
- inferred field editing, watermark marking, schema mode, schema evolution, Bronze table naming, and explode rules in [resources/public/apiComponent.js:1200](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L1200),
- backend schema preview execution in [src/clj/bitool/routes/home.clj:1086](/Users/aaryakulkarni/bitool/src/clj/bitool/routes/home.clj#L1086).

Conclusion:

- Bronze UI is not a major gap.
- Bronze product surface already exists and is meaningful.

### 3.2 Target / warehouse config

The current target editor already supports:

- target warehouse selection,
- Bronze target configuration,
- Silver and Gold execution settings,
- write mode selection,
- job ids and job params,
- options JSON,

in [resources/public/targetComponent.js](/Users/aaryakulkarni/bitool/resources/public/targetComponent.js).

Conclusion:

- target-side configuration is already present,
- although advanced settings still rely heavily on JSON textareas.

### 3.3 Silver / Gold modeling workflow

The Modeling Console already supports:

- Silver proposal generation from Bronze in [resources/public/modelingConsole.js:1039](/Users/aaryakulkarni/bitool/resources/public/modelingConsole.js#L1039),
- Gold proposal generation from Silver in [resources/public/modelingConsole.js:1018](/Users/aaryakulkarni/bitool/resources/public/modelingConsole.js#L1018),
- propose, compile, validate, review, publish, execute workflow in [resources/public/modelingConsole.js:325](/Users/aaryakulkarni/bitool/resources/public/modelingConsole.js#L325),
- backend proposal routes in [src/clj/bitool/routes/home.clj:1109](/Users/aaryakulkarni/bitool/src/clj/bitool/routes/home.clj#L1109) through [src/clj/bitool/routes/home.clj:1551](/Users/aaryakulkarni/bitool/src/clj/bitool/routes/home.clj#L1551),
- proposal generation and automation logic in [src/clj/bitool/modeling/automation.clj:2665](/Users/aaryakulkarni/bitool/src/clj/bitool/modeling/automation.clj#L2665) and [src/clj/bitool/modeling/automation.clj:2752](/Users/aaryakulkarni/bitool/src/clj/bitool/modeling/automation.clj#L2752).

Conclusion:

- Silver and Gold workflow is already productized at a lifecycle level.
- The main gap is not the workflow skeleton. It is smarter guidance inside that workflow.

---

## 4. Current State: Automation vs AI

The repo already contains useful automation, but most of it is deterministic or heuristic rather than true user-facing AI.

### 4.1 What is already automated

- OpenAPI endpoint discovery
- auth and pagination inference from spec
- schema sampling and inferred field generation
- watermark and PK recommendations
- Silver schema proposal generation
- Gold proposal generation
- SQL compilation
- validation, review, publish, and execute lifecycle

### 4.2 What is already AI-like, but not really AI product UX

There are recommendation behaviors in the API UI, especially around schema planning and inferred field handling, for example in [resources/public/apiComponent.js:1295](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L1295).

These are valuable, but they are:

- heuristic,
- local to the UI,
- not conversational,
- not explanatory in a user-facing AI sense.

More specifically, the existing API UI already includes:

- recommendation computation in [resources/public/apiComponent.js:1297](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L1297),
- a rendered recommendation card with confidence badges and `Show reasoning` in [resources/public/apiComponent.js:1384](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L1384),
- local heuristic watermark detection in [resources/public/apiComponent.js:460](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L460),
- local heuristic PK detection in [resources/public/apiComponent.js:541](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L541).

### 4.3 What is not really present yet

The current product does not yet expose a clear AI copilot experience for:

- BRD -> Silver or Gold model generation,
- explaining why a proposal was generated,
- explaining why a watermark or key was chosen,
- explaining validation failures in business terms,
- proposing schema drift remediations,
- proposing metric marts from business intent,
- root-cause analysis for KPI movement or pipeline failures.

Conclusion:

- we already have meaningful automation,
- we do not yet have a strong AI assistance layer exposed to the operator.

Important implementation note:

- the repo does already contain an LLM provider path in [src/clj/bitool/pipeline/intent.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/pipeline/intent.clj),
- new embedded AI features should reuse or extract that provider integration rather than introduce a second independent LLM stack.

### 4.4 Existing pipeline chat vs embedded AI

The repo already has a separate conversational surface in [resources/public/pipelineChatComponent.js](/Users/aaryakulkarni/bitool/resources/public/pipelineChatComponent.js).

That surface already covers:

- natural-language pipeline creation,
- iterative editing,
- assumptions,
- coverage feedback,
- preview-driven workflow.

The correct boundary should be:

- Pipeline Chat:
  - use for creating or reshaping pipelines across multiple layers from natural language
- Embedded AI in Bronze/Silver/Gold screens:
  - use for understanding, explaining, and refining the specific thing the user is already looking at

These should share the same provider layer, but they should not feel like two unrelated AI systems.

---

## 5. Grounded Gap Matrix

| Area | Existing UI / backend coverage | Current status | Main gap |
|---|---|---|---|
| API connection | API config screen, auth config, base URL, spec URL in [apiComponent.js](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js) | Covered | Mostly polish and explainability |
| Endpoint discovery | Spec-based endpoint discovery and inference in [apiComponent.js:1758](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L1758) | Covered | Better confidence/explanation for inferred auth and pagination |
| API -> Bronze load config | Per-endpoint load type, pagination, headers, query/body params, retries in [apiComponent.js:304](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L304) | Covered | Better guided defaults for domain-specific APIs |
| Preview Schema | Real preview workflow in [apiComponent.js:1816](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L1816) and backend route in [home.clj:1086](/Users/aaryakulkarni/bitool/src/clj/bitool/routes/home.clj#L1086) | Covered | Explanation of why inferred fields, explode rules, PKs, and watermarks were chosen |
| Bronze schema editing | Inferred field table, watermark marking, schema mode, schema evolution, Bronze table name in [apiComponent.js:1200](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L1200) | Covered | Richer UX than raw table editing; AI suggestions for field naming and canonicalization |
| Bronze target config | Warehouse target screen in [targetComponent.js](/Users/aaryakulkarni/bitool/resources/public/targetComponent.js) | Covered | Better form-based advanced config instead of JSON-heavy fields |
| Silver proposal generation | Proposal route and modeling console in [modelingConsole.js:1039](/Users/aaryakulkarni/bitool/resources/public/modelingConsole.js#L1039) and [home.clj:1109](/Users/aaryakulkarni/bitool/src/clj/bitool/routes/home.clj#L1109) | Covered | Better business-aware proposal explanation and in-place AI refinement |
| Gold proposal generation | Gold routes and console in [modelingConsole.js:1018](/Users/aaryakulkarni/bitool/resources/public/modelingConsole.js#L1018) and [home.clj:1348](/Users/aaryakulkarni/bitool/src/clj/bitool/routes/home.clj#L1348) | Covered | BRD-driven mart generation and metric semantics assistance |
| Compile / validate / publish / execute | End-to-end lifecycle exists in [modelingConsole.js:325](/Users/aaryakulkarni/bitool/resources/public/modelingConsole.js#L325) | Covered | Stronger failure explanation and recommendation UX |
| AI copilot | No first-class chat/copilot surface in these existing flows | Not covered | Major gap |
| BRD -> model | No explicit BRD intake in current UI | Not covered | Major gap |
| Explainability | Limited user-facing explanation for proposal decisions | Partially covered | Major gap |
| Drift remediation guidance | Drift handling infrastructure exists elsewhere, but not as an embedded AI/operator UX here | Partially covered | Major gap |

---

## 6. Screen-by-Screen Recommendation

### 6.1 API Configuration screen

Recommendation:

- keep this as the primary Bronze workbench,
- do not replace it with a new “wizard” product surface.

Add small AI features here:

- explain inferred pagination and auth,
- enrich the existing recommendation card rather than adding a second parallel explanation surface,
- give richer AI explanations for recommended watermark fields and primary keys,
- suggest better Bronze table names,
- suggest whether the endpoint is fact-like, dimension-like, or snapshot-like,
- summarize “what this endpoint will become downstream.”

### 6.2 Target screen

Recommendation:

- keep the current target screen,
- improve advanced fields rather than replacing the entire editor.

Add small AI features here:

- suggest target warehouse defaults,
- explain write mode choice,
- suggest merge keys,
- suggest clustering/partitioning,
- explain likely cost/performance tradeoffs.

### 6.3 Modeling Console

Recommendation:

- keep this as the primary Silver/Gold workbench,
- add AI inside the proposal detail flow instead of creating a parallel modeling UI.

Add small AI features here:

- “Explain this proposal”
- “Refine schema for analytics”
- “Suggest dimensions/measures”
- “Suggest transformations”
- “Why did validation fail?” layered on top of the existing deterministic validation display
- “What business questions does this model answer?”

### 6.4 New UI that is actually worth adding

If a new screen is added, the strongest candidate is not a replacement Bronze UI.

It is a lightweight BRD intake panel that feeds the existing Modeling Console:

- user pastes business requirement,
- system maps that to proposed Silver/Gold changes,
- user lands in the existing proposal editor with AI-generated draft content.

---

## 7. Product Conclusion

The correct product reading of the current repo is:

- Bronze is already substantially built.
- Silver and Gold lifecycle is already built.
- Bitool already has strong automation primitives for medallion modeling.
- the biggest missing product capability is an embedded AI guidance layer, not a replacement flow.

Therefore, the recommended next step is:

1. preserve current API Configuration as the Bronze surface,
2. preserve current Modeling Console as the Silver/Gold surface,
3. add AI assistance, BRD intake, and explanation inside those existing screens.

That is a much better fit for the current codebase than a clean-sheet UI plan.

---

## 8. Practical Build Order

### Phase 1

- Add “Explain Preview Schema” to API Configuration
- Add “Suggest watermark / PK / grain” improvements
- Add “Explain proposal” in Modeling Console
- Add “Explain validation failure” in Modeling Console

### Phase 2

- Add BRD intake for Silver/Gold proposal generation
- Add drift remediation suggestions

Dependency note:

- drift remediation should be sequenced after usable schema-drift review context is available in product code,
- it should not be treated as the very first AI-assisted workflow after Bronze and proposal explanations.

### Phase 3

- Add KPI and mart suggestion assistant for Gold
- Add business glossary and metric definition generation
- Add root-cause assistant for execution and KPI changes

---

## 9. Final Answer to the Original Question

If the question is:

“Are API to Bronze and Preview Schema already covered in our tool?”

The answer is:

Yes. Those are already meaningfully covered.

If the question is:

“What still needs to be built?”

The answer is:

Mostly the AI/operator assistance layer on top of the existing Bronze/Silver/Gold product surfaces.
