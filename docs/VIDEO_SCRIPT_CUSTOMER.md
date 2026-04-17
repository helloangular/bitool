# Bitool Customer Video Script
### Goal: Drive demo requests / customer meeting calls
### Runtime: ~2:45 (tight, no filler)
### Tone: Confident, direct, peer-to-peer. Not salesy.

---

## COLD OPEN (0:00 - 0:15)
> *Show: Screen recording — someone scrolling through a 400-line SQL file with 6 JOINs, 4 subqueries, nested CASE statements. Slow zoom. Then cut to: a Slack message "hey when is the fleet dashboard going to be ready?" followed by "it's been 3 weeks."*

**NARRATOR:**
"Your business asked for a fleet analytics dashboard three weeks ago. You're still debugging a 400-line SQL query — six joins, four subqueries, a CASE statement nobody remembers writing. And when the upstream API changes a field name, this entire thing breaks silently."

---

## THE REAL PROBLEM (0:15 - 0:40)
> *Show: Quick montage — (1) wall of dbt SQL files, (2) Informatica canvas with dozens of manual nodes, (3) ChatGPT generating SQL with a hallucinated column name highlighted in red.*

**NARRATOR:**
"You've got three options today. Write SQL by hand — it works, but it takes weeks and only one person understands it. Use a visual ETL tool — faster to see, but you're still wiring every node manually. Or ask an AI to write SQL — and then spend just as long validating whether it's actually correct."

*Beat.*

"None of these solve the real problem: getting from what the business needs to a correct, running, governed pipeline — fast."

---

## THE PRODUCT — VISUAL TRANSFORMATION GRAPH (0:40 - 1:00)
> *Show: Bitool UI. Clean. A graph with clearly visible nodes: two Table sources feeding into a Join node, then a Filter, then an Aggregate, then a Sort, flowing to an Output. Each node is labeled and color-coded by type.*

**NARRATOR:**
"This is Bitool. Instead of writing SQL, you build with visual nodes — Joins, Filters, Aggregations, Sorts, Unions, Conditionals. Every transformation is a visible, clickable block on a graph."

> *Show: Click the Join node — see the join config (left table, right table, join key, join type). Then click the Filter node — see the filter expression. Then click the Aggregate — see group-by columns and measures.*

"Click a Join — see exactly which tables, which keys, what type. Click a Filter — see the condition. Click an Aggregate — see your group-by and measures. No more reading 200 lines of SQL to understand what a query does."

> *Show: SQL panel on the right updates as the graph is shown. Highlight: same pipeline, two representations.*

"Behind every graph, there's compiled SQL — deterministic, auditable. Edit either side. They stay in sync."

---

## AI BOOTSTRAPS THE GRAPH (1:00 - 1:20)
> *Show: Type a plain-English request into the intent bar: "Connect to Samsara's fleet API. Pull vehicle locations, driver activity, and fuel transactions. Build daily fleet utilization and a driver safety scorecard."*

**NARRATOR:**
"You can build graphs by hand. Or you can describe what you need in plain English — and AI builds the graph for you."

> *Show: Graph populating node by node — API source nodes appearing, Join nodes connecting them, Filter and Aggregate nodes forming the business logic, Output nodes at the end. Smooth animation, 4 seconds.*

"AI generates the full structure — sources, joins, filters, aggregations, business models. Not a SQL dump you have to trust blindly. A visual graph you can inspect node by node."

> *Show: User clicks a node, types "only include active vehicles." Node updates, SQL recompiles.*

"See something wrong? Click it, fix it. Tell the AI what to change. The graph updates, SQL recompiles. AI proposes. You decide."

---

## MEDALLION ARCHITECTURE (1:20 - 1:35)
> *Show: Full pipeline view — left side: Bronze nodes (API endpoints, raw ingestion). Middle: Silver nodes (cleaned dimensions, facts). Right: Gold nodes (KPIs, marts, aggregations). Color-coded layers with labeled tags.*

**NARRATOR:**
"For teams building data platforms, Bitool supports full medallion architecture out of the box. Bronze — raw API ingestion with auto-configured auth, pagination, and watermarks. Silver — cleaned, typed, deduplicated dimension and fact tables. Gold — business-ready KPIs, aggregations, and marts."

> *Show: Click a Bronze node, see the API config. Click a Silver node, see merge logic and primary keys. Click a Gold node, see the aggregation feeding a dashboard.*

"Every layer is visible on the graph. When a Gold report is wrong, trace it back through Silver to Bronze in seconds — not hours."

---

## GOVERNANCE (1:35 - 1:50)
> *Show: Governance flow — Propose button clicked, diff view appears showing exactly what changed, validation checkmarks run, reviewer approval, Publish button.*

**NARRATOR:**
"Every change follows the same path: propose, compile, validate, review, publish. Full diff visibility. No cowboy deploys. No unreviewed changes reaching production."

> *Show: Version history sidebar — list of immutable published versions with timestamps and authors.*

"Every version is immutable and auditable. Rollback anytime. Built for compliance."

---

## PRODUCTION OPS (1:50 - 2:10)
> *Show: Ops Console — freshness dashboard with green/yellow/red indicators per source. Schema drift alert pops up. Bad records panel. Pipeline health metrics.*

**NARRATOR:**
"Most tools stop at 'it runs.' Bitool shows you *how* it's running. Per-source freshness SLAs. Automatic schema drift detection — when an API changes a field, you know before your dashboards break. AI explains the impact and suggests a fix."

> *Show: Bad records panel — records with error class, message, original payload. Then: chain monitoring view showing Bronze → Silver → Gold freshness.*

"Bad records captured with full context — inspect, replay, or ignore. Chain monitoring across your entire pipeline — when a report is stale, you see exactly which upstream stage is the bottleneck."

"When something breaks at 2am, you know what happened and why."

---

## PROOF: SHEETZ FLEET INTELLIGENCE (2:10 - 2:25)
> *Show: Sheetz dashboard — executive summary with KPIs (miles driven, fuel efficiency, safety scores). Quick scroll through report categories. AI Q&A panel: someone types "top 5 vehicles by fuel consumption this quarter" and gets a formatted answer.*

**NARRATOR:**
"Here's what this looks like in production. Sheetz runs fleet intelligence on Bitool — 15 Samsara API endpoints flowing through a full medallion pipeline. 37 live reports. 18 KPIs. AI-powered Q&A anyone on the team can use — constrained to the governed schema, not hallucinating from the internet."

"Governed, monitored, and running today."

---

## CTA (2:25 - 2:45)
> *Show: Split screen — left side: dbt + Fivetran + Monte Carlo + Airflow logos (4 tools). Right side: Bitool logo (1 tool). Animated merge. Then: Bitool logo centered. Clean background. "Request a Demo" button animates in.*

**NARRATOR:**
"Today your pipeline stack is four or five tools stitched together. Bitool is one platform — transform, ingest, govern, monitor."

*Beat.*

"Visual graphs anyone can read. AI that helps you build. A compiler that keeps it correct. Governance that keeps it safe."

"See it live. Book a demo."

> *Show: bitool.io / hello@bitool.io — hold for 3 seconds.*

---

# Production Notes

## Key Visuals Needed (Priority Order)
1. **Transformation graph demo** (HERO SHOT — most important):
   - Graph with Join, Filter, Aggregate, Sort nodes clearly visible
   - Click into each node — show config panel
   - Edit a node visually → SQL recompiles in real time
2. **AI intent → graph generation**:
   - Type plain English → graph populates node by node
   - Click generated node, modify it, see SQL update
3. **Medallion pipeline view**:
   - Bronze → Silver → Gold layers, color-coded
   - Click through each layer
4. **Governance flow**:
   - Propose → diff → validate → approve → publish
5. **Ops Console**:
   - Freshness SLAs, schema drift, bad records, chain monitoring
6. **Sheetz dashboard + AI Q&A**
7. **Pain montage** (can be stock/recreated):
   - Long SQL file scroll with complex JOINs highlighted
   - Slack "when is it ready" message
   - AI hallucinating a column name

## Script Strategy: Why This Order
- **Lead with the transformation graph** — this is universal. Everyone who writes SQL has felt this pain. Joins, filters, aggregations as visible nodes is the instant "I want that" moment.
- **Then show AI bootstrapping** — now that they understand the graph, show how AI generates it. This avoids the "just another AI tool" dismissal.
- **Then medallion** — for data platform buyers, this shows depth. For others, it shows the tool scales beyond simple transforms.
- **Governance + Ops last** — these close the deal for enterprise buyers. The "wow" already happened; this is the "yes, we can actually use this."

## Narration Style
- Calm and authoritative — think Stripe or Linear product videos
- Not hyped, not monotone. Confident peer explaining something they built
- Pacing: deliberate. Let the product visuals breathe
- **Pause on the graph.** The graph generation moment and the node click-through are the two moments that sell. Give them air.

## Music
- Lo-fi electronic, minimal. Builds slightly during product section, drops for CTA
- Reference: Linear launch videos, Vercel product videos

## What NOT to Do
- No "imagine a world where..." opening
- No stock footage of people in meetings
- No feature list rapid-fire — let the demo sell itself
- No claims we can't show on screen (if we say it, we show it)
- Don't say "revolutionary" or "game-changing"
- Don't lead with Bronze/Silver/Gold — earn it after showing the universal graph

## Recommended Cuts
- **Full 2:45** — website landing page, YouTube
- **60-second** (Cold Open + Transform Graph + AI Bootstrap + CTA) — LinkedIn, Twitter
- **30-second** (Transform Graph + AI generates it + CTA) — targeted ads, data eng communities
- **15-second** (graph generation moment only) — paid social ads
