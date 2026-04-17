# Sheetz Samsara Demo Script — Graph 2524

**Audience:** Sheetz leadership / decision-makers
**Target Length:** 7:00 to 7:45
**Structure:** Lead with business value (dashboards, AI), then show the engine behind it

---

## 0:00 - 0:25 — Opening: Lead With the Outcome

**Show:** Open the Reporting Dashboard (http://localhost:8080/reporting) — Executive Summary loads by default

Before I show you how the data gets here, let me show you what your team actually gets.This is a live Fleet Intelligence dashboard running on your Samsara data.
At a glance your ops team sees 18 key performance indicators across the entire fleet — total fuel consumed, average MPG, idle hours, active vehicles, dispatch jobs, driver safety events, cold chain breaches, asset health alerts, and more. Trend arrows show whether you're improving or declining compared to the prior period.

---

## 1:00 - 2:00 — Dashboard Drill-Downs (9 Categories, 37 Reports)

**Show:** Click through one report in each category — spend ~8 seconds per view, just enough to register the visual

Here are some reports avaialble.

Fleet Operations gives you six views — utilization rates, vehicle activity,route performance, dispatch throughput, and a full vehicle master list.

Fuel is one of your biggest controllable costs. This section breaks down fuel consumption, idling hours, cost opportunities, and emissions — by vehicle and over time.

Driver Safety ranks your drivers by events and severity. Your safety manager knows exactly who to coach this week — not next month.

Hours of Service, IFTA reporting, driver compliance status — all in one place.Compliance issues surface here before they become violations.

For a company selling food and beverages, cold chain is non-negotiable. Temperature breaches by sensor by day — a daily compliance scorecard, not a monthly surprise.

Asset Health covers maintenance queues, DVIR status, trailer and equipment health.You see what needs attention before it becomes a breakdown on the road.

Alerts and exceptions are centralized — every operational incident, its resolution time, and the trend over time.

Sensor telemetry goes beyond vehicles — facility sensors, industrial equipment, all flowing through the same pipeline.

In total, there are 37 reports across 9 categories — all connected to live data.
---

## 2:00 - 2:15 — Time Range Picker

**Show:** Click the time picker in the top-right corner, show the preset options

> "Every report supports time filtering. Your team can look at the last hour, last 7 days,
> 90 days, year to date — or set a custom date range. Same data, any time window."

Click "Last 30 days" to demonstrate, then move on.

---

## 2:15 - 3:15 — AI Assistant (The Wow Moment)

**Show:** Click the lightning bolt button (bottom-right) to open the AI chat

Now here's the part I'm most excited about. Anyone on your team can ask a business question in plain English — no special tools, no training, no IT ticket.The AI translates the question into a validated structured query and generates a dynamic report.

### Query 1

**Click chip:** "Top 5 vehicles by fuel consumption this quarter"

Wait for the report to render in the main dashboard.


**Type:** "What was our average MPG in February vs March?"

Let me show you another report.
What was our average MPG in February vs March?
Now think about this for a second. Nobody built this report. There's no ticket, no waiting two weeks for engineering. A fleet manager just typed a question over coffee and got an answer in five seconds. That's what changes when your data is clean and AI-ready — the entire organization gets self-service analytics, not just the people who know SQL.

---

## 3:15 - 3:35 — Transition to the Engine

**Show:** Navigate to graph `2524`, show the graph view

So that's what the business gets — 37 reports, 18 KPIs, 9 categories, and an AI assistant that can answer anything in the data. Now let me show you the engine behind it — how Samsara data goes from raw API calls to those dashboards.

---

## 3:35 - 3:55 — API Coverage

**Show:** Open the API node, scroll the endpoint list steadily

Samsara has a massive API — roughly 80 endpoints covering everything from fuel and GPS to driver logs and cold chain sensors. We've already mapped all of them. What you're looking at is the subset powering your 37 reports — but scaling to the full API is just configuration, not a rebuild.

Do not pause too long here.

---

## 3:55 - 4:25 — Bronze: Connecting to Samsara

**Show:** Show 2 endpoint configs: `fleet/vehicles` and `fleet/dispatch/jobs`

This is the Bronze layer — where raw data first lands from Samsara. This one brings in your vehicle registry — every truck, van, and trailer in your fleet. This one pulls dispatch jobs — every delivery, every route, every assignment. The system figures out the best way to keep each one fresh automatically. No programming required so its much faster to develop.

---

## 4:25 - 4:45 — Bronze: Data Preview

**Show:** Click `Preview Schema` on `fleet/dispatch/jobs`, show inferred fields and key recommendations

Still in Bronze — before we pull a single record, the system shows us exactly what we're going to get — vehicle IDs, timestamps, status fields — all auto-detected. Normally this means a data engineer spending weeks reading API docs, writing pagination logic, building retry handling, error recovery — real programming work. Here all of that is built in. One click and the schema is ready.

---

## 4:45 - 5:00 — Bronze: Live Data Landing

**Action:** Click the **Run** button in the API panel. Wait for the success status.

> ""

**Action:** Switch to the **Databricks console** in your browser. Run a quick query on the Bronze table.

```sql
SELECT vehicle_id, vehicle_name, make, model, vehicle_year FROM main.bronze.samsara_fleet_vehicles_raw LIMIT 10
```

Let me run this live. This is pulling real data from the Samsara API right now. And here it is in Databricks. vehicle records — landed, queryable, real. This is the Bronze layer in your warehouse. Everything else builds on top of this — Silver, Gold, and those 37 reports you just saw.

---

## 5:00 - 5:25 — Silver Models

**Show:** Open Modeling Console, click into `silver_vehicle_master` proposal

Now we move from Bronze to Silver. Think of Bronze as the raw ingredients — Silver is where they become usable. That messy JSON payload with nested tags and timestamps? Silver turns it into a clean vehicle record with make, model, year, VIN — one row per truck, no duplicates, no guesswork.
And look at the workflow here — this isn't just a transformation. There's a full governance pipeline. Every Silver model goes through proposal, compilation, validation, review, and publishing before it touches production. Nobody ships bad data because there's no shortcut around the process.
The system proposed this schema automatically from the Bronze source — column names, data types, which fields are primary keys — all with confidence scores. A data engineer can review and tweak, but 90% of the work is already done.
And if the mappings aren't obvious — like converting a nested JSON array into a flat column — you can ask AI to suggest the transformation. It writes the expression, you approve it.

---

## 5:25 - 5:55 — Gold Models (Powering the 37 Reports)

**Show:** Click into `gold_fuel_efficiency_daily`, then quickly show `gold_fleet_utilization_daily` and `gold_driver_safety_daily`

Gold is the final layer — this is what your reports and your AI assistant actually read from. Every one of those 37 dashboards you saw at the beginning? They're powered by these 17 Gold tables. Each one answers a specific business question — how much fuel are we burning, which drivers need coaching, are we staying compliant.
And here's what makes this different. When the business wants a new metric — say, cost per mile by region — you don't file a ticket and wait. You click "Suggest mart design" and AI analyzes your schema and proposes the right dimensions, aggregations, and grain. A human reviews it, approves it, and it's live. AI accelerates. Humans decide.

---

## 6:10 - 6:40 — Ops Console

**Show:** Open Ops Console → Pipeline Overview. Point to KPI tiles (Active Sources, Running Jobs, Avg Freshness, Bad Records). Scroll to Source Status table showing Samsara API source with last run, throughput, freshness.

 This is your real-time ops console — every source, every layer, one screen. You can see the Samsara API is healthy, data is fresh, and exactly how many records flowed through the last run.

**Show:** Click Source Health → API Sources tab. Point to the fleet/vehicles endpoint row showing last run time, duration, rows ingested, freshness status.

Drill into source health and you see every API endpoint individually — when it last ran, how long it took, how many rows it pulled, and whether it's on schedule. If something falls behind, you know instantly.

**Show:** Click Schema Drift. Point to a drift event with severity. Click "Explain drift with AI" button.

This is where it gets powerful. If Samsara changes their API tomorrow — adds a field, removes one, changes a data type — we detect it automatically before it silently breaks anything downstream. AI explains exactly what changed and suggests how to handle it. You're not finding out about a data issue from a wrong number in a quarterly report.

**Show:** Click Schema & Medallion. Point to the Bronze → Silver → Gold freshness chain visual.

And here's the full picture — Bronze, Silver, Gold — you can see the freshness at every layer of the pipeline. The data you saw in those 37 reports? You can trace it all the way back to the API call that brought it in.

---

## 6:40 - 7:00 — Close

**Show:** Return to graph view, show `API -> Output -> Target`

So to bring it together — you saw 37 reports across 9 categories, 18 live KPIs, and an AI assistant that can answer any question in the data. Behind it: automated API ingestion, clean data modeling, 17 Gold marts, and operational monitoring.The full pipeline from Samsara to business decisions, already in place.

---

## 7:00 - 7:30 — Commercial Close

> "My approach would be to get the Samsara foundation in place quickly, stand up the
> first Gold marts and reports with the business, and do it with less discovery drag
> and lower delivery risk."

Optional:
> "If needed, I can also be more competitive on price. But the main value is speed
> to usable analytics."

---

## Recommended Click Path

1. **Open /reporting dashboard**
2. **Walk through Executive Summary — 18 KPIs**
3. **Quick-click one report per category (9 categories):**
   - Fleet Utilization
   - Fuel Efficiency
   - Driver Scorecard
   - HOS Compliance
   - Cold Chain Compliance
   - Maintenance Queue
   - Alerts Dashboard
   - Sensor Monitoring
   - Fleet Reference *(optional, skip if short on time)*
4. **Show time picker — click Last 30 days**
5. **Open AI chat, run 2-3 queries**
6. Navigate to graph `2524`
7. Open API node, scroll endpoint list
8. Show `fleet/vehicles` config
9. Show `fleet/dispatch/jobs` config
10. Click `Preview Schema`
11. Show Bronze run result
12. Open Modeling Console
13. Show `silver_vehicle_master`
14. Show `silver_dispatch_job`
15. Show `gold_fuel_efficiency_daily`
16. Show `gold_fleet_utilization_daily`
17. Show `gold_driver_safety_daily`
18. Click `Suggest mart design`
19. Open Ops Console
20. Close on graph view

---

## Report Catalog Quick Reference (for Q&A)

| Category | Reports | Key Gold Tables |
|---|---|---|
| Fleet Operations | Executive Summary, Fleet Utilization, Vehicle Activity, Route Performance, Dispatch Throughput, Vehicle Master | gold_fleet_utilization_daily, gold_dispatch_jobs, gold_fleet_vehicles_fuel_energy |
| Fuel & Efficiency | Fuel Efficiency, Idling Analysis, Fuel Cost Opportunity, Emissions | gold_fuel_efficiency_daily, gold_fleet_vehicles_fuel_energy |
| Driver Safety | Safety Dashboard, Driver Scorecard, Coaching Priority, Safety Trend | gold_driver_safety_daily |
| Compliance | HOS Compliance, IFTA Summary, Driver Compliance, Compliance Trend | gold_hos_daily, gold_driver_safety_daily |
| Cold Chain & Temp | Cold Chain Compliance, Temp Excursion, Reefer Performance, Door Monitoring | gold_cold_chain_daily, gold_door_events |
| Asset Health | Asset Health, Maintenance Queue, DVIR Dashboard, Trailer Health, Equipment Health | gold_asset_health_daily, gold_trailer_stats, gold_equipment_stats |
| Alerts & Exceptions | Alerts Dashboard, Exception Management, Incidents, Resolution Performance | gold_alert_events, gold_asset_health_daily |
| Sensor & Industrial | Sensor Monitoring, Industrial Telemetry, Facility Sensor | gold_industrial_telemetry |
| Reference & Admin | Fleet Reference, Coverage Report, Document Coverage | gold_fleet_vehicles_fuel_energy |

---

## Safe Phrases To Use

- "live Fleet Intelligence dashboard running on your Samsara data"
- "37 reports across 9 categories — all connected to live data"
- "18 key performance indicators updated automatically"
- "anyone on your team can ask questions in plain English"
- "validated query — the AI can't hallucinate or make up data"
- "daily compliance scorecard, not a monthly surprise"
- "roughly 80 endpoints available for onboarding"
- "17 Gold tables powering every report and the AI assistant"
- "AI accelerates. Humans decide."
- "the full pipeline from Samsara to business decisions"
- "designed to reduce manual data-engineering setup"

## Avoid

- "all 80 are in this graph"
- "all reports are already fully built" unless you can open them
- "AI builds everything automatically"
- Technical jargon with this audience: HTTP, SQL, REST, JSON, middleware, REPL
- Saying "ISL" — just say "validation layer" or "safety net"
- Leading with architecture before showing the payoff

---
---

# BiTool Marketing Video — "What If You Could See Your Data?"

**Format:** 90-second sales video (emotion-first, not feature-first)
**Tagline:** *The visual is the truth.*
**Audience:** Data leaders, analytics engineers, ops teams — any industry, any source

---

## 0:00–0:08 — The Universal Pain

*Dark screen. Typing sounds. Quiet tension.*

**Voiceover:**
"Every company runs on data. But nobody can actually see how it works."

**Screen:** A blinking cursor in a SQL editor. Lines and lines of code. Someone scrolling, squinting.

---

## 0:08–0:18 — The Trust Problem

**Voiceover:**
"Your team writes queries they can't fully explain. Builds pipelines they can't easily change. And makes decisions based on numbers... they hope are right."

**Screen:** A Slack message — "Can someone verify these numbers before the board meeting?" Left on read.

---

## 0:18–0:28 — The Turn

*Music shifts. Light comes in.*

**Voiceover:**
"What if you could just... see it?"

**Screen:** A DAG appears. Clean. Visual. Nodes lighting up — API, Bronze, Silver, Gold. The data flow is obvious.

---

## 0:28–0:50 — The Magic Loop (music only, no narration)

Quick cinematic cuts, 2-3 seconds each:

1. Someone drags a node on the DAG — SQL updates live in a side panel
2. An API node connects to a source — schema previews instantly
3. Preview Schema clicked — fields, types, keys auto-detected
4. A Silver model shows the 7-step pipeline bar — all green checkmarks
5. "Explain this pipeline" clicked — plain English summary appears
6. AI chat opens — someone types "What was our average MPG in February vs March?"
7. The answer renders as a chart in the main dashboard — not the chat, the actual dashboard
8. Executive Summary with 18 KPIs — trend arrows, live numbers
9. Quick click through reports — Fuel, Safety, Cold Chain, Compliance
10. Databricks console — real data, real tables

*The feeling: everything is connected, everything is visible, everything just works.*

---

## 0:50–0:58 — The AI Moment (voiceover returns)

**Voiceover:**
"A question nobody pre-built a report for. Asked over coffee. Answered in five seconds. That's what happens when your data is clean, governed, and AI-ready."

**Screen:** The fleet manager looking at the AI-generated report. Slight nod.

---

## 0:58–1:08 — The Platform (credibility, fast)

**Voiceover:**
"APIs. Databases. Kafka. Streaming. Files. Every source flows through the same visual pipeline — ingestion, transformation, modeling, reporting — governed at every step."

**Screen:** The graph zoomed out — dozens of nodes. Then zoom into one node. Then the Modeling Console with 49 Silver models. Then Gold tables in Databricks.

---

## 1:08–1:18 — The Identity Close

**Voiceover:**
"Other tools make you choose: write code you can't see, or use drag-and-drop you can't trust."

*Beat.*

"BiTool is the first platform where the visual is the truth. AI builds it. You verify it. Your team trusts it."

**Screen:** Someone closes the laptop. Walks into a meeting. Confident.

---

## 1:18–1:25 — End Card

**Screen:** Clean. Dark background.

> **BiTool**
> *Intent to Insight*
>
> Book a demo → bitool.io

---

## Voiceover Script (paste into ElevenLabs / Descript)

```
Every company runs on data. But nobody can actually see how it works.

Your team writes queries they can't fully explain. Builds pipelines they can't easily change. And makes decisions based on numbers... they hope are right.

What if you could just... see it?

[pause 15 seconds for montage]

A question nobody pre-built a report for. Asked over coffee. Answered in five seconds. That's what happens when your data is clean, governed, and AI-ready.

APIs. Databases. Kafka. Streaming. Files. Every source flows through the same visual pipeline — ingestion, transformation, modeling, reporting — governed at every step.

Other tools make you choose: write code you can't see, or use drag-and-drop you can't trust.

BiTool is the first platform where the visual is the truth. AI builds it. You verify it. Your team trusts it.

BiTool. Intent to Insight.
```

---

## Screen Recordings Needed

| # | What to Record | Duration | Notes |
|---|---|---|---|
| 1 | SQL editor with long query, scrolling | 3 sec | The "pain" shot — any complex SQL |
| 2 | Fake Slack message | 2 sec | Screenshot, not real Slack |
| 3 | DAG appearing / loading in graph view | 3 sec | The "turn" — satisfying reveal |
| 4 | Dragging a node, SQL panel updating | 3 sec | Dual-representation magic |
| 5 | API node → Preview Schema click | 3 sec | Fields auto-detecting |
| 6 | Silver model with pipeline bar (all green) | 2 sec | Governance visual |
| 7 | "Explain this pipeline" → English text | 3 sec | AI assist |
| 8 | AI chat: type question, answer renders in dashboard | 4 sec | The wow moment |
| 9 | Executive Summary — 18 KPIs | 2 sec | Breadth |
| 10 | Quick report montage (3 reports) | 3 sec | Fast cuts |
| 11 | Databricks console with real query result | 2 sec | Proof it's real |
| 12 | Graph zoomed out — full pipeline visible | 2 sec | Scale |
| 13 | Modeling Console — 49 Silver models listed | 2 sec | Depth |

---

## Production Notes

- **Music:** Search "minimal cinematic corporate" on Artlist or Epidemic Sound. Quiet 0:00-0:18, builds at the turn, peaks 0:28-0:50, resolves softly at close.
- **Transitions:** Simple crossfades for the pain section. Hard cuts for the montage. Slow fade to black at end.
- **Text overlays:** None during montage — let the product speak. Only the tagline at the end card.
- **Voice tone:** Warm, confident, conversational. Not announcer-voice. Think "smart colleague explaining over coffee."
- **Total screen recordings:** ~35 seconds of product footage. The rest is voiceover + music + the Slack screenshot + end card.
