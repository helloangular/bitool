# Text to Transform Graph — Technical Design

**Status**: Draft
**Author**: Codex
**Date**: 2026-04-08
**Depends on**: `bitool.gil.*`, `graph2.clj`, `logic.clj`, `endpoint.clj`, existing ISL query pipeline

---

## 1. Purpose

This document defines how Bitool should support:

- natural language to transform graph generation,
- with Logic and Conditional nodes as first-class execution primitives,
- using a constrained intermediate language,
- then deterministically compiling that into a real Bitool graph.

The goal is to let a user say things like:

- "Create an API flow that normalizes customer input, assigns a score, branches on risk, and returns a decision payload."
- "Build an order transform that cleans fields, computes discounts, routes approvals, and shapes a final response."
- "Generate an Informatica-style transform graph without writing Python."

and have the system produce a valid graph that executes through the existing Bitool runtime.

This is the transform-side equivalent of the existing query pipeline:

- natural language -> ISL -> validation -> deterministic compilation -> execution

For transform generation, the corresponding architecture should be:

- natural language -> constrained transform GIL -> validation -> compile/apply graph

---

## 2. Executive Summary

Bitool is close to supporting text-to-transform graph generation, but not all the way there yet.

What already exists:

- a proven NL -> ISL -> validate -> compile -> execute pattern for query generation,
- a real GIL pipeline for graph construction,
- executable Logic and Conditional nodes in the graph runtime,
- graph compilation and apply endpoints.

What is missing:

- the NL translator for graph generation,
- grounding and repair loops specialized for transform graphs,
- a narrow semantic contract for Logic and Conditional graph generation.

Recommendation:

1. Treat the saved Bitool graph as the executable artifact.
2. Treat GIL as the canonical graph IR.
3. Use a constrained transform-oriented GIL subset directly for v1.
4. Define the expression and stage-scoping contract explicitly.
5. Start with a constrained transform subset before expanding to all graph types.

This gives Bitool the same architectural safety that ISL gives query generation:

- the LLM proposes,
- deterministic code validates and compiles,
- the runtime executes graph logic without relying on arbitrary generated source files.

---

## 3. Current State

### 3.1 Existing query architecture

Bitool already implements a clear query-generation pattern:

- `NL -> ISL -> Validate -> SQL -> Execute`

This is documented directly in the existing route implementation and provides the right precedent for transform generation.

### 3.2 Existing graph architecture

Bitool also already has:

- GIL normalization,
- GIL validation,
- GIL planning,
- GIL application to a real graph,
- HTTP endpoints for validate, compile, apply, and from-nl.

However, the `/gil/from-nl` path is currently a stub from the translator perspective. The graph compiler exists, but the natural-language-to-GIL translator is not implemented yet.

### 3.3 Existing transform execution capability

Bitool Logic (`Fu`) and Conditional (`C`) nodes are now usable as runtime execution primitives:

- Logic nodes support ordered assignments, expression evaluation, and multiple outputs.
- Conditional nodes support multiple forms such as `if-else`, `if-elif-else`, `case`, `cond`, and `pattern-match` style ordered guards.
- Execution runs through the existing Clojure runtime, not through generated source files.

That means the execution foundation is already strong enough to support meaningful text-generated transform graphs.

---

## 4. Problem Statement

Today, a user can manually build strong transform graphs in Bitool, but cannot reliably describe them in plain language and have the system synthesize them.

Without an intermediate language, direct NL -> graph generation would have several problems:

1. The LLM would need to invent raw graph shapes, node refs, edges, aliases, positions, and config formats.
2. The output would be too tied to storage and UI details rather than transform semantics.
3. Validation errors would be harder to repair cleanly.
4. Prompting would become fragile as graph types and config fields expand.

The system therefore needs a structured proposal layer between user text and graph mutation.

---

## 5. Design Principles

1. **LLM proposes, compiler decides.**
   The LLM never mutates the graph directly.

2. **The transform plan must be semantic before it is structural.**
   User intent is about normalization, branching, scoring, routing, and output shaping, not node ids and coordinates.

3. **Graph remains the executable artifact.**
   The final target is a real Bitool graph and existing runtime execution path.

4. **GIL remains the canonical graph IR.**
   Any transform-specific layer should lower into GIL rather than bypassing it.

5. **The translator contract should be narrower than full GIL.**
   It should capture transform meaning without exposing the whole graph surface area.

6. **Validation and repair must be first-class.**
   The model should be constrained, grounded, and able to self-correct with deterministic feedback.

7. **Phase by value, not completeness.**
   Start with the graph patterns that best prove "no Python transform node required."

---

## 6. Recommendation

### 6.1 Architectural stance

The long-term layering still makes sense conceptually:

```text
Natural language
  ->
semantic transform contract
  ->
GIL
  ->
graph
  ->
runtime
```

However, for v1 the system should **not** introduce a separate persisted Transform ISL format yet.

Instead, v1 should use a **constrained GIL subset directly** as the translator output format.

That gives Bitool:

- one less IR to build and maintain,
- reuse of the existing GIL validator and compiler path,
- a faster path to a working feature,
- fewer places where semantic drift can hide.

### 6.2 V1 recommendation

For the first release, the effective pipeline should be:

```text
Natural language
  ->
constrained transform GIL
  ->
validation / repair
  ->
GIL compile / apply
  ->
Bitool graph
  ->
existing runtime execution
```

### 6.3 Why constrained GIL first

GIL already handles:

- node typing,
- graph structure,
- planning,
- persistence,
- graph application.

The missing value is not another graph IR. The missing value is:

- a narrow transform-oriented schema subset,
- an explicit expression contract,
- explicit stage scoping rules,
- grounded validation,
- and a repair loop.

### 6.4 When to extract a separate Transform ISL later

A separate Transform ISL becomes worth introducing only when one or more of these become true:

1. the translator needs to express semantics that are not close to 1:1 with GIL,
2. multiple graph families need different semantic contracts,
3. users need a reusable, user-visible transform spec independent of graph compilation,
4. the GIL-facing translator becomes too prompt-heavy or too difficult to repair.

Until then, constrained GIL is the right implementation choice.

---

## 7. Scope

### 7.1 In scope for v1

The first release should focus on graphs that prove Bitool is already functionally complete for transform logic:

- `Endpoint -> Logic -> Conditional -> Logic -> Response Builder -> Output`
- `Endpoint -> Conditional -> Logic -> Response Builder -> Output`
- `Table -> Logic -> Conditional -> Logic -> Output`
- `Endpoint -> Logic -> Logic -> Response Builder -> Output`

Supported behaviors:

- field normalization,
- type conversion,
- derived variables,
- reuse of earlier derived variables,
- conditional branching,
- route or segment assignment,
- score and threshold logic,
- output payload shaping,
- multi-stage transforms.

### 7.2 Optional v1.5 additions

- `Db Execute` with grounded connection and SQL template intents,
- projections and basic mappings,
- table-source transforms using grounded schemas,
- patching an existing transform graph from text.

### 7.3 Out of scope for v1

- arbitrary graph generation across every node type,
- free-form graph layout generation,
- unrestricted join planning across unknown sources,
- complex medallion planning,
- full codegen to saved Clojure source files,
- opaque agentic graph mutation without preview.

---

## 8. Conceptual Architecture

```text
User request
  |
  v
[1] Intent classification
  |
  +--> query/reporting intent -> existing ISL pipeline
  |
  +--> transform-graph intent -> constrained transform GIL pipeline
                                  |
                                  v
                         [2] NL -> constrained transform GIL
                                  |
                                  v
                         [3] structural + expression validation
                                  |
                                  v
                         [4] grounding + stage ref resolution + repair
                                  |
                                  v
                         [5] GIL validation + plan
                                  |
                                  v
                         [6] Preview / apply
                                  |
                                  v
                         [7] Existing graph runtime
```

This allows Bitool to cleanly support both:

- text to query,
- text to transform graph,

without merging those concerns into one oversized translator.

---

## 9. Intermediate Representation Model

There are three practical layers in v1 and they should not be conflated.

### 9.1 User intent

This is natural language:

- "Clean order input, route high-value Indian orders to priority, and compute discount and score."

### 9.2 Constrained transform GIL

This is a restricted GIL document intended for transform generation. It is still GIL, but only a narrow subset is allowed:

- source node,
- ordered Logic and Conditional stages,
- response-builder and output,
- semantic stage references that are resolved before persist.

### 9.3 GIL

After normalization and reference resolution, the constrained transform GIL becomes ordinary GIL and flows through the normal validator/compiler path.

### 9.4 Saved graph

This is the executable artifact that runs through the existing Bitool runtime.

---

## 10. V1 Contract: Constrained Transform GIL

### 10.1 Goals

The v1 contract should:

- be easy for an LLM to produce correctly,
- stay close to existing GIL,
- constrain the translator to only valid transform shapes,
- express stage intent clearly enough for preview and repair,
- compile into a runtime-ready graph.

### 10.2 Top-level structure

The top-level shape is standard `:build` GIL:

```clojure
{:gil-version "1.0"
 :intent :build
 :graph-name "Order Approval Flow"
 :goal "Normalize input, route by policy, compute outputs"
 :nodes [...]
 :edges [...]
 :assumptions [...]
 :explanations [...]}
```

Translator constraints for v1:

- exactly one source node,
- exactly one output node,
- optional response-builder only before output,
- only linear edges,
- only the allowed node types listed below.

### 10.3 Allowed node types

Allowed source node types:

```clojure
{:type "endpoint"}
```

```clojure
{:type "table"}
```

Allowed transform node types:

- `function`
- `conditionals`
- `response-builder`
- `output`

Rejected in v1:

- join,
- aggregation,
- sorter,
- union,
- mapping,
- multi-source topologies,
- arbitrary branching topologies,
- graph patches from NL.

### 10.4 Topology rules

The translator may only emit graphs matching one of these shapes:

- `Endpoint -> Function -> Conditional -> Function -> Response Builder -> Output`
- `Endpoint -> Conditional -> Function -> Response Builder -> Output`
- `Endpoint -> Function -> Function -> Response Builder -> Output`
- `Table -> Function -> Conditional -> Function -> Output`

### 10.5 Source node conventions

For `endpoint`:

```clojure
{:type "endpoint"
 :config {:http_method "GET"
          :route_path "/demo/order-approval"
          :query_params [...]}}
```

For `table`:

```clojure
{:type "table"
 :config {}}
```

### 10.6 Function node conventions

Function nodes must use the existing `save-logic` config shape:

```clojure
{:type "function"
 :config {:fn_name "normalize_order"
          :fn_params [...]
          :fn_lets [...]
          :fn_outputs [...]}}
```

### 10.7 Conditional node conventions

Conditional nodes must use the existing `save-conditional` config shape:

```clojure
{:type "conditionals"
 :config {:cond_type "if-elif-else"
          :branches [...]
          :default_branch "standard"}}
```

### 10.8 Semantic stage references

To make downstream references readable to the translator, v1 allows semantic source references inside config values before final normalization.

Examples:

- `customer_segment.group`
- `customer_segment.matched`
- `normalize_order.amount_num`

These are translator-facing references only. Before graph persist, they are resolved into the actual runtime column names expected by the current engine.

---

## 11. Expression Language Contract

This section is the most important contract in the design. The translator may only emit syntax that the current Logic and Conditional runtime actually supports.

### 11.1 Identifiers

Identifiers must match:

```text
[A-Za-z_][A-Za-z0-9_]*
```

Examples:

- `amount_num`
- `country_norm`
- `vip_flag`

Not allowed:

- `customer-name`
- `order total`
- `a.b`

Stage-style references such as `customer_segment.group` are translator-facing only and must be resolved before expression parsing.

### 11.2 Literals

Supported literal types:

- strings using single or double quotes,
- integers,
- decimals,
- booleans: `true`, `false`,
- `null`.

Examples:

- `'INDIA'`
- `"priority"`
- `1000`
- `0.20`
- `true`
- `null`

### 11.3 Operators

Supported arithmetic operators:

- `+`
- `-`
- `*`
- `/`
- `%`

Supported comparison operators:

- `=`
- `==`
- `!=`
- `<>`
- `<`
- `<=`
- `>`
- `>=`

Supported boolean operators:

- `&&`
- `||`
- `!`
- `and`
- `or`
- `not`

Operator notes:

- `&&` and `||` short-circuit.
- `+` concatenates if either side is a string, otherwise performs numeric addition.
- comparison is numeric when both sides can be parsed as numbers, otherwise string comparison is used.

### 11.4 Built-in functions supported today

The current runtime supports these built-ins:

- `abs`
- `average`
- `ceil`
- `coalesce`
- `concat`
- `contains`
- `cos`
- `endswith`
- `equals`
- `exp`
- `floor`
- `if`
- `indexof`
- `isempty`
- `isnull`
- `length`
- `log`
- `log10`
- `lower`
- `max`
- `matches`
- `min`
- `not`
- `or`
- `and`
- `parsefloat`
- `parseint`
- `pow`
- `replace`
- `round`
- `sin`
- `sqrt`
- `startswith`
- `substring`
- `sum`
- `tan`
- `tofixed`
- `toboolean`
- `tolowercase`
- `tonumber`
- `tostring`
- `touppercase`
- `trim`
- `upper`

### 11.5 Recommended subset for v1 generation

Even though the runtime supports many functions, the translator should stay within a smaller recommended subset for reliability:

- `trim`
- `upper`
- `lower`
- `concat`
- `tonumber`
- `toboolean`
- `tostring`
- `coalesce`
- `contains`
- `startswith`
- `endswith`
- `replace`
- `round`
- `length`
- `if`
- `isnull`
- `isempty`
- `min`
- `max`
- `sum`

### 11.6 Unsupported examples for v1 generation

The translator should not emit:

- dotted field access inside expressions such as `customer_segment.group`,
- user-defined functions,
- lambdas,
- loops,
- collections or maps,
- arbitrary Clojure syntax,
- SQL snippets,
- regex literals outside the `matches()` function.

---

## 12. Stage Scope and Conditional Output Contract

### 12.1 Global data flow model

The runtime carries a flat value map through the graph.

Each node:

- receives the current flat map,
- computes new outputs,
- merges those outputs back into the flat map,
- passes the merged map downstream.

This means downstream nodes can read values produced by earlier nodes.

### 12.2 Function node scope

Function node execution has three phases:

1. `fn_params`
   Param bindings are created first from upstream flat-map values.

2. `fn_lets`
   Assignment rows execute in order. Each assignment may reference:
   - all bound params,
   - all upstream flat-map values with valid identifiers,
   - earlier assignments in the same node.

3. `fn_outputs`
   Outputs execute in order. Each output may reference:
   - params,
   - assignments,
   - earlier outputs in the same node.

This is the effective scoping model already used by the runtime.

### 12.3 Function naming rules

Within a single function node, parameter names, assignment variable names, and output names must be unique.

For generated graphs, the translator should also avoid intentional shadowing of upstream names unless there is a strong reason.

### 12.4 Conditional node outputs

Conditional nodes do not emit arbitrary branch-local assignment maps in v1.

Instead, each conditional emits a fixed output contract:

- `<conditional>.group`
- `<conditional>.matched`
- `<conditional>.used_default`
- `<conditional>.branch_index`
- `<conditional>.condition`
- `<conditional>.value`

At runtime, these are lowered to the existing flat-map columns:

- `cond_<node-id>_group`
- `cond_<node-id>_matched`
- `cond_<node-id>_used_default`
- `cond_<node-id>_branch_index`
- `cond_<node-id>_condition`
- `cond_<node-id>_value`

### 12.5 Conditional semantics

For predicate-style conditionals such as `if-else`, `if-elif-else`, `multi-if`, `cond`, and `pattern-match`:

- branches are evaluated in order,
- the first truthy branch wins,
- `group` becomes the selected group output,
- `matched` is true for branch matches and false when only the default applies.

For `case`:

- each branch compares a subject expression to a branch value,
- the first equal branch wins,
- `value` is the resolved branch match value.

### 12.6 Stage reference rules

Downstream stages may reference:

- source fields directly by name,
- outputs from any earlier function node using semantic stage refs,
- fixed conditional outputs from any earlier conditional node.

Examples:

- `normalize_input.amount_num`
- `customer_segment.group`
- `customer_segment.matched`

Resolution order for semantic refs:

1. exact stage output match,
2. exact source field match,
3. validation failure if unresolved.

### 12.7 Collision rules

Generated stage aliases must be unique across the graph.

If two stages would normalize to the same alias, the compiler should rename deterministically, for example:

- `normalize_input`
- `normalize_input_2`

The translator should never rely on implicit collision resolution.

---

## 13. Example Constrained Transform GIL

```json
{
  "gil_version": "1.0",
  "intent": "build",
  "graph_name": "Conditional Logic Demo",
  "goal": "Normalize request data, classify the customer, and compute outputs",
  "nodes": [
    {
      "node_ref": "src",
      "type": "endpoint",
      "alias": "demo_request",
      "config": {
        "http_method": "GET",
        "route_path": "/demo/conditional-logic",
        "query_params": [
          {"param_name": "name", "data_type": "string"},
          {"param_name": "country", "data_type": "string"},
          {"param_name": "amount", "data_type": "number"},
          {"param_name": "vip", "data_type": "boolean"}
        ]
      }
    },
    {
      "node_ref": "n1",
      "type": "function",
      "alias": "normalize_input",
      "config": {
        "fn_name": "normalize_input",
        "fn_params": [
          {"param_name": "customer_name_raw", "source_column": "name"},
          {"param_name": "country_raw", "source_column": "country"},
          {"param_name": "amount_raw", "source_column": "amount"},
          {"param_name": "vip_raw", "source_column": "vip"}
        ],
        "fn_lets": [
          {"variable": "customer_name", "expression": "trim(customer_name_raw)"},
          {"variable": "country_norm", "expression": "upper(country_raw)"},
          {"variable": "amount_num", "expression": "tonumber(amount_raw)"},
          {"variable": "vip_flag", "expression": "toboolean(vip_raw)"}
        ],
        "fn_outputs": [
          {"output_name": "customer_name", "expression": "customer_name"},
          {"output_name": "country_norm", "expression": "country_norm"},
          {"output_name": "amount_num", "expression": "amount_num"},
          {"output_name": "vip_flag", "expression": "vip_flag"}
        ]
      }
    },
    {
      "node_ref": "n2",
      "type": "conditionals",
      "alias": "customer_segment",
      "config": {
        "cond_type": "if-elif-else",
        "branches": [
          {
            "condition": "country_norm = 'INDIA' && amount_num >= 1000",
            "group": "priority_india"
          },
          {
            "condition": "vip_flag",
            "group": "vip_customer"
          }
        ],
        "default_branch": "standard"
      }
    },
    {
      "node_ref": "n3",
      "type": "function",
      "alias": "offer_logic",
      "config": {
        "fn_name": "offer_logic",
        "fn_params": [
          {"param_name": "customer_name", "source_column": "normalize_input.customer_name"},
          {"param_name": "amount_num", "source_column": "normalize_input.amount_num"},
          {"param_name": "segment", "source_column": "customer_segment.group"}
        ],
        "fn_lets": [
          {
            "variable": "discount",
            "expression": "if(segment = 'priority_india', 0.20, if(segment = 'vip_customer', 0.15, 0.05))"
          },
          {
            "variable": "score",
            "expression": "if(segment = 'priority_india', amount_num * 2, if(segment = 'vip_customer', amount_num * 1.5, amount_num))"
          }
        ],
        "fn_outputs": [
          {"output_name": "greeting", "expression": "concat('Hello ', customer_name)"},
          {"output_name": "segment", "expression": "segment"},
          {"output_name": "discount", "expression": "discount"},
          {"output_name": "score", "expression": "score"}
        ]
      }
    },
    {
      "node_ref": "rb",
      "type": "response-builder",
      "alias": "response_builder",
      "config": {
        "template": [
          {"output_key": "greeting", "source_column": "greeting"},
          {"output_key": "segment", "source_column": "segment"},
          {"output_key": "discountRate", "source_column": "discount"},
          {"output_key": "priorityScore", "source_column": "score"}
        ]
      }
    },
    {
      "node_ref": "out",
      "type": "output",
      "alias": "Output"
    }
  ],
  "edges": [
    ["src", "n1"],
    ["n1", "n2"],
    ["n2", "n3"],
    ["n3", "rb"],
    ["rb", "out"]
  ]
}
```

---

## 14. Validation and Repair Model

Validation should occur at multiple layers.

### 14.1 Structural validation

Checks:

- required top-level fields,
- exactly one allowed source node,
- only allowed node types,
- only allowed topology shapes,
- required config fields,
- unique graph node refs,
- unique stage aliases.

### 14.2 Expression-level validation

Checks:

- assignment variables are unique within a function node,
- output expressions reference known variables in scope,
- branch expressions use only supported syntax,
- function names are from the supported expression registry,
- unsupported syntax is rejected clearly.

### 14.3 Stage reference validation

Checks:

- `normalize_input.amount_num` resolves to a known earlier function output,
- `customer_segment.group` resolves to a valid conditional export,
- no stage may reference a later stage,
- no unresolved semantic refs survive into persist/apply.

### 14.4 Grounding validation

Checks:

- endpoint query params are well-formed,
- table sources refer to real known tables when grounding is enabled,
- `db-execute` connection/table references exist,
- referenced columns exist in grounded source schemas.

### 14.5 GIL validation

After semantic references are resolved, the output must pass the existing GIL validator.

This keeps graph legality in one place instead of reimplementing it in the transform planner.

### 14.6 Repair loop

The repair loop should be explicit and deterministic:

1. generate constrained transform GIL,
2. run structural validation,
3. run expression validation,
4. run stage reference resolution,
5. run grounding validation if applicable,
6. if any step fails, return machine-readable errors,
7. feed those errors back into the translator with the previous output,
8. retry with a bounded attempt count,
9. if still invalid, return preview-mode failure rather than applying anything.

Recommended retry policy:

- max 2 repair attempts after the initial generation,
- stop immediately on unsupported-intent or unsafe-intent failures,
- persist nothing until validation is fully green.

### 14.7 Example validation failure

Example invalid translator output:

```json
{
  "code": "unknown_stage_output",
  "message": "Stage reference 'customer_segment.segment' is invalid. Allowed outputs are group, matched, used_default, branch_index, condition, value.",
  "path": ["nodes", 3, "config", "fn_params", 2, "source_column"]
}
```

Another example:

```json
{
  "code": "unknown_logic_function",
  "message": "Function 'titlecase' is not supported. Use one of: trim, upper, lower, concat, tonumber, toboolean, ...",
  "path": ["nodes", 1, "config", "fn_lets", 0, "expression"]
}
```

### 14.8 Example repair prompt

Repair prompts should be concrete and bounded:

```text
Your previous transform graph proposal was invalid.

Errors:
1. Stage reference 'customer_segment.segment' is invalid. Use one of:
   customer_segment.group
   customer_segment.matched
   customer_segment.used_default
   customer_segment.branch_index
   customer_segment.condition
   customer_segment.value
2. Function 'titlecase' is not supported.

Return a corrected constrained transform GIL document only.
Do not change the graph goal.
Do not introduce new node types.
```

### 14.9 Example refusal

Unsupported requests should fail clearly.

Example:

```text
Request: "Build a transform with two parallel branches that later join and aggregate by region."

Response:
This request is outside v1 transform generation scope. Supported v1 graphs are linear chains centered on Endpoint/Table, Logic, Conditional, Response Builder, and Output nodes.
```

---

## 15. Grounding

Grounding is the main reason text-to-graph can be reliable instead of decorative.

### 15.1 V1 grounding inputs

The translator should have access to:

- supported node types,
- supported conditional forms,
- supported expression functions,
- currently selected graph context when patching,
- available connections,
- available tables and columns for grounded sources,
- endpoint route and parameter conventions.

### 15.2 Grounding modes

#### Ungrounded mode

Useful for demos and sandboxed graph creation:

- endpoint source,
- logic,
- conditional,
- response-builder,
- output.

#### Grounded mode

Useful for production graph generation:

- table-backed transforms,
- db-execute usage,
- schema-aware output mapping,
- connection-aware graph creation.

### 15.3 Repair loop

Repair behavior is defined in Section 14 and should not be reimplemented differently here.

Grounding-specific repair should only add source and schema errors to the same shared repair loop, for example:

- unknown table,
- unknown source column,
- unsupported connection,
- invalid grounded `db-execute` target.

---

## 16. Suggested API Shape

The cleanest external API for v1 should reuse existing GIL routes where possible and add only one translator-facing route.

### 16.1 Validate and plan via existing GIL routes

```text
POST /gil/validate
POST /gil/compile
```

Input:

- constrained transform GIL

Output:

- normalized GIL,
- validation results,
- optional plan preview.

### 16.2 Natural language to transform graph

```text
POST /transform/from-nl
```

Input:

- user text,
- optional graph/session context,
- optional grounding mode,
- optional `do_apply` flag.

Output:

- generated constrained transform GIL,
- validation results,
- graph plan preview,
- optional applied graph result.

### 16.3 Why no separate transform validate route in v1

The translator route should call the same validation/planning pipeline internally.

That avoids:

- duplicate validators,
- route sprawl,
- and a fake API boundary before the contract is mature.

### 16.4 Relationship to existing GIL routes

This new layer should sit above current GIL routes rather than replacing them.

The pipeline should reuse:

- GIL normalize,
- GIL validate,
- GIL plan,
- GIL apply.

---

## 17. UI and Product Experience

### 17.1 Minimal v1 experience

The primary entry point should be the existing graph-generation chat/intention surface rather than a buried standalone screen.

Recommended v1 entry points:

- the existing pipeline-chat or graph-chat entry point,
- a `Generate Graph from Text` action near the canvas toolbar,
- optionally the existing intent bar if that is already where users start graph creation.

The user enters:

- "Build a transform that normalizes customer name, classifies by amount and country, and returns segment and score."

The system returns:

- a graph preview drawer or modal attached to the current graph workflow,
- a preview of the generated stages,
- assumptions,
- validation status,
- the graph plan,
- an apply button.

### 17.2 Preview requirements

Before apply, the user should be able to inspect:

- source node type,
- ordered stages,
- logic assignments,
- conditional rules,
- response fields,
- semantic stage references after resolution,
- assumptions,
- generated graph plan.

### 17.3 Explanation requirements

The system should explain in plain language:

- what the graph will do,
- where assumptions were made,
- which fields are derived,
- how the branching behaves.

This matters because transform graphs are business logic, not just plumbing.

### 17.4 Apply behavior

After the user clicks Apply:

- the graph should be created or updated,
- the canvas should focus on the newly created graph,
- generated nodes should be selected or highlighted,
- the preview should remain available for inspection until dismissed.

---

## 18. Example User Requests

The following prompts should be in scope for v1:

- "Create an API graph that cleans name and amount fields, classifies VIP customers, and returns a discount."
- "Build a transform for order approvals with two decision stages and a final response payload."
- "Create a graph like a PL/SQL procedure that normalizes invoice fields, routes exceptions, and computes payment instructions."
- "Generate a short executive demo flow that scores leads and routes them to account executives."

These prompts are valuable because they align directly with the existing demo graphs and current runtime strengths.

---

## 19. Example Reference Resolution Output

The translator-facing constrained GIL may contain semantic refs such as:

- `normalize_input.amount_num`
- `customer_segment.group`

Before persist, these should be resolved to the concrete runtime columns that the existing engine uses.

Example resolved form:

```clojure
{:gil-version "1.0"
 :intent :build
 :graph-name "Conditional Logic Demo"
 :nodes [{:node-ref "src"
          :type "endpoint"
          :alias "demo_request"
          :config {...}}
         {:node-ref "s1"
          :type "function"
          :alias "normalize_input"
          :config {...}}
         {:node-ref "s2"
          :type "conditionals"
          :alias "customer_segment"
          :config {...}}
         {:node-ref "s3"
          :type "function"
          :alias "offer_logic"
          :config {:fn_name "offer_logic"
                   :fn_params [{:param_name "customer_name"
                                :source_column "customer_name"}
                               {:param_name "amount_num"
                                :source_column "amount_num"}
                               {:param_name "segment"
                                :source_column "cond_3_group"}]
                   :fn_lets [...]
                   :fn_outputs [...]}}
         {:node-ref "rb"
          :type "response-builder"
          :alias "response_builder"
          :config {...}}
         {:node-ref "out"
          :type "output"
          :alias "Output"}]
 :edges [["src" "s1"]
         ["s1" "s2"]
         ["s2" "s3"]
         ["s3" "rb"]
         ["rb" "out"]]}
```

Here `customer_segment.group` has been resolved to `cond_3_group`, which matches the current runtime output contract for the conditional node once node ids are known.

---

## 20. Relationship to Existing Runtime

This design does not require a new execution engine.

That is an important product point.

The generated graph executes through the current runtime:

- request params or source values flow into the graph,
- Logic nodes evaluate assignments and outputs,
- Conditional nodes evaluate route conditions,
- Response Builder shapes the final payload,
- Output completes the graph.

So the system is not "pretending" to generate transforms. It generates artifacts that run through the actual Bitool Clojure runtime already in place.

---

## 21. Why This Supports the "No Python Transform Node" Story

This architecture is a strong proof point that Bitool does not require Python transform nodes to be expressive.

The reasons:

1. Logic nodes already support variable binding and expression evaluation.
2. Conditional nodes already support branching semantics.
3. Graph execution already runs through the runtime engine.
4. Text generation only proposes transform structure; execution stays in the platform.
5. Complex transform demos can already be modeled as graphs without custom script steps.

This means the product story becomes:

- "You can describe procedural transform logic in text."
- "Bitool converts it into a graph."
- "That graph executes natively in the platform runtime."

---

## 22. Risks

### 22.1 Over-broad translator scope

If the first version tries to support every graph pattern, it will become brittle.

Mitigation:

- constrain v1 to transform graphs centered on Logic and Conditional nodes.

### 22.2 Ambiguous user intent

A prompt like "build a customer flow" could mean API, ETL, dashboard query, or medallion pipeline.

Mitigation:

- add intent classification first,
- route ambiguous prompts to clarification or preview mode.

### 22.3 Weak grounding

Without schema/context grounding, table or DB-oriented graph generation may drift.

Mitigation:

- start with endpoint-backed transform graphs,
- only enable grounded DB modes when metadata is available.

### 22.4 Expression mismatch

The LLM may invent syntax not supported by the current expression engine.

Mitigation:

- publish an expression registry into the prompt,
- validate early,
- retry with exact error feedback.

### 22.5 User trust

Automatically applying a generated graph without preview may feel unsafe.

Mitigation:

- default to preview,
- show assumptions,
- make apply explicit.

---

## 23. Rollout Plan

### Phase 1: Internal prototype

- add constrained transform GIL validator extensions,
- add stage reference resolver,
- support `endpoint + logic + conditional + response-builder + output`,
- build example prompts and golden outputs.

### Phase 2: Demo-ready flow

- add `from-nl` translator,
- add preview UI,
- support apply path,
- support repair loop,
- ship with demo prompt library.

### Phase 3: Grounded transform generation

- add table source grounding,
- add connection-aware `db-execute`,
- add graph patching from text,
- add richer transform families.

### Phase 4: Broader planning

- connect to medallion-specific planning,
- connect to graph-to-SQL or Databricks compilation layers,
- support multi-source and materialization-aware graph synthesis.

---

## 24. Concrete Implementation Plan

### 24.1 New namespaces

Suggested additions:

- `bitool.transform`
- `bitool.transform.nl`
- optional `bitool.transform.api` if route logic becomes too large for `home.clj`

### 24.2 Reused namespaces

- `bitool.gil.normalize`
- `bitool.gil.validator`
- `bitool.gil.compiler`
- `bitool.graph2`
- `bitool.logic`
- `bitool.endpoint`

### 24.3 Initial feature flags

Suggested flags:

- `:allow-text-transform-graph`
- `:allow-text-transform-graph-apply`
- `:allow-grounded-text-transform-graph`

---

## 25. Suggested Prompting Strategy

The translator prompt should follow the same safety model as the existing query ISL flow:

1. never emit raw graph storage form,
2. never emit raw Clojure source,
3. always emit constrained transform GIL only,
4. use only allowed stage kinds,
5. use only allowed expression functions,
6. state assumptions explicitly,
7. retry on validation failure with structured feedback.

The prompt should also include:

- node capabilities,
- supported conditional types,
- supported expression functions,
- stage output contract,
- example transform graphs,
- examples of good and bad outputs,
- rules for when to refuse unsupported requests.

---

## 26. Open Questions

1. Should the semantic stage-ref syntax stay inside constrained GIL long-term, or later move into a cleaner standalone Transform ISL?
   Recommendation: keep it in constrained GIL for v1 and revisit only after real usage.

2. Should the first translator support patching existing graphs?
   Recommendation: no for first milestone.

3. Should `db-execute` be part of v1?
   Recommendation: only if source grounding is available.

4. Should multi-branch graph topologies be generated in v1?
   Recommendation: no, keep v1 linear and stage-oriented.

5. Should generated graphs be auto-executed?
   Recommendation: no, preview and apply should stay explicit.

---

## 27. Final Recommendation

Bitool should treat **GIL as the graph compiler IR** and use a **constrained transform-oriented GIL subset** as the v1 translator contract for text-to-transform generation.

That gives the product a clean and defensible architecture:

- query generation uses ISL,
- transform graph generation uses constrained GIL,
- graph construction uses GIL,
- graph execution uses the existing runtime.

This is the right balance of:

- safety,
- explainability,
- lower implementation cost,
- compiler reuse,
- product speed,
- and a strong story that Bitool can express real transform logic without Python script nodes.

---

## 28. Related Documents

- [GIL_Design_v1_0.md](./GIL_Design_v1_0.md)
- [GIL-for-Medallion-Modeling-Tech-Design.md](./GIL-for-Medallion-Modeling-Tech-Design.md)
- [Intent-to-Pipeline-Tech-Design.md](./Intent-to-Pipeline-Tech-Design.md)
- [Bitool Graph -> SQL Compiler -> Databricks Job Execution Design.md](./Bitool%20Graph%20-%3E%20SQL%20Compiler%20-%3E%20Databricks%20Job%20Execution%20Design.md)
