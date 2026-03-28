# BRD: Sheetz Fleet, Delivery, and Cold-Chain Operations Reporting

## 1. Business Context

Sheetz operates a large convenience retail and foodservice business with fuel, made-to-order food, store replenishment, field operations, and delivery-related logistics. Operations leadership wants a reporting solution that gives a single operational view of fleet movement, driver behavior, route execution, fuel efficiency, cold-chain integrity, and asset health.

Today, teams rely on fragmented operational screens and ad hoc investigation. Leadership wants a business-facing reporting layer that helps operations managers answer questions quickly without needing to understand the underlying telematics platform.

The reporting solution should support day-to-day operational management, regional performance reviews, exception handling, and proactive intervention.

## 2. Business Goal

Create a reporting layer that helps Sheetz:

- improve on-time execution for store replenishment and delivery-related operations
- reduce fuel waste and excess idling
- identify driver safety risk early and prioritize coaching
- monitor refrigerated and temperature-sensitive assets to reduce product-loss risk
- improve maintenance planning and reduce downtime from unresolved defects
- give regional and corporate operations leaders a consistent operational scorecard

## 3. Intended Users

- VP / Director of Operations
- Fleet Operations Manager
- Transportation Manager
- Regional Operations Manager
- Safety Manager
- Maintenance Manager
- Cold-chain / Quality Manager
- Dispatch and route-planning teams

## 4. Business Questions To Answer

### 4.1 Fleet Utilization and Route Execution

- Which vehicles are underutilized by region, market, or operating group?
- Which routes are consistently late or inefficient?
- Which stores or operating regions are experiencing the most route exceptions?
- How many jobs, runs, or route segments were completed on time versus late?
- Which assets are active but producing low operational value?

### 4.2 Driver Safety and Coaching

- Which drivers have the highest current safety risk?
- Which drivers have deteriorating safety trends over the last 7, 30, and 90 days?
- What types of safety events are increasing most quickly?
- Which drivers should be prioritized for coaching this week?
- Which regions or operating groups show the highest concentration of safety incidents?

### 4.3 Fuel, Idling, and Efficiency

- Which vehicles and routes generate the most fuel waste?
- Where is excessive idling occurring?
- Which asset classes have the worst fuel efficiency?
- How is fuel efficiency trending over time?
- What is the estimated weekly and monthly cost impact of idling and inefficient routing?

### 4.4 Cold Chain and Temperature Protection

- Which refrigerated assets experienced temperature excursions?
- Which temperature-sensitive deliveries or store replenishment trips are at highest spoilage risk?
- How often are trailer or asset doors opened outside expected workflow?
- Which routes, assets, or regions have the highest cold-chain compliance risk?
- Which temperature incidents require immediate operational follow-up?

### 4.5 Maintenance, DVIR, and Defects

- Which vehicles, trailers, or assets are at highest risk of downtime?
- Which unresolved defects are oldest or most operationally severe?
- Which assets show repeated defects or recurring inspection issues?
- Which DVIR issues remain unresolved beyond target SLA?
- Which maintenance issues are affecting route readiness and service reliability?

## 5. Required Business Reports

The initial release should include the following reports.

### 5.1 Fleet Utilization Dashboard

Purpose:
Provide a daily and weekly operating view of fleet usage and route productivity.

Required measures:

- active vehicles
- inactive vehicles
- utilization hours
- trips or jobs completed
- route completion rate
- on-time route rate
- average route duration
- average stop count or route complexity indicator

Required filters:

- date
- region
- operating group
- store cluster
- vehicle type / asset class

Required views:

- daily summary
- weekly trend
- underutilized assets list
- route exceptions list

### 5.2 Driver Safety Dashboard

Purpose:
Help safety and operations teams identify risk, trends, and coaching priorities.

Required measures:

- safety score
- count of safety events
- events by severity
- events by type
- driver ranking by risk
- safety trend over time

Required filters:

- date range
- region
- manager / supervisor
- driver
- vehicle
- event type

Required views:

- driver leaderboard by risk
- safety trend by week
- event-type breakdown
- coaching priority queue

### 5.3 Fuel and Idling Dashboard

Purpose:
Identify cost reduction opportunities and operating inefficiencies.

Required measures:

- fuel consumed
- estimated fuel cost
- idle hours
- idle percentage
- fuel efficiency / MPG or equivalent efficiency measure
- estimated idle cost
- route efficiency score or proxy

Required filters:

- date
- region
- route
- driver
- vehicle
- asset class

Required views:

- top idling vehicles
- top idling routes
- trend of fuel efficiency
- weekly cost opportunity summary

### 5.4 Cold-Chain Compliance Dashboard

Purpose:
Protect product quality and reduce temperature-related operational loss.

Required measures:

- count of temperature excursions
- count of severe temperature excursions
- duration outside safe range
- count of door-open incidents
- assets or trailers at risk
- route/store impact counts

Required filters:

- date
- region
- route
- trailer / refrigerated asset
- store / destination
- temperature event severity

Required views:

- cold-chain incident summary
- high-risk assets
- route/store impact list
- temperature trend over time

### 5.5 Maintenance and Asset Health Dashboard

Purpose:
Give maintenance and operations teams a clear view of readiness and failure risk.

Required measures:

- unresolved defects
- repeated defects
- open DVIR issues
- age of open issues
- assets not ready for dispatch
- downtime-risk ranking

Required filters:

- date
- region
- asset / vehicle / trailer
- defect severity
- maintenance status

Required views:

- unresolved issue queue
- recurring defect summary
- maintenance-risk leaderboard
- readiness summary by region

## 6. Alerts and Exception Monitoring

The business wants exception-focused reporting and alerting, not only historical dashboards.

Priority exceptions:

- route or job running materially late
- severe safety event
- repeated safety events for the same driver within a short period
- excessive idling over threshold
- temperature excursion outside approved range
- refrigerated asset door-open anomaly
- unresolved defect or DVIR beyond SLA
- asset not ready for dispatch at start of operating window

## 7. Time Grain and Refresh Expectations

The business needs both near-real-time operational visibility and historical trend reporting.

Expected refresh expectations:

- operational dashboards refreshed at least hourly
- exception and alert-oriented views refreshed more frequently where possible
- daily summary reporting available by start of next business day
- weekly and monthly trend reporting available for leadership reviews

## 8. Required Slicing Dimensions

The reporting layer should support analysis by:

- date / week / month
- region
- operating group
- market
- store or destination
- route
- driver
- vehicle
- trailer / refrigerated asset
- asset class / vehicle type
- event type
- severity

## 9. Data Quality Expectations

Business users expect the reporting layer to be trusted for operational decision-making.

Minimum expectations:

- no duplicate operational events in dashboard totals
- consistent daily totals when reports are refreshed
- ability to trace critical metrics back to operational source records
- visible data freshness indicators
- clear handling of missing or delayed data

## 10. Adoption Expectations

Success will be measured by:

- reduced time to identify route, safety, fuel, and cold-chain issues
- improved operational review quality at regional and corporate levels
- more proactive coaching and maintenance action
- reduction in spoilage-risk and avoidable operational waste

## 11. Out of Scope for Initial Release

- full financial P&L reporting
- customer-facing delivery experience analytics
- labor scheduling optimization
- store sales and merchandising analytics
- predictive optimization beyond initial risk and exception scoring

## 12. Open Business Questions

The following items require business confirmation:

- what constitutes an "on-time" route or job
- what threshold defines "excessive" idling
- what safety event types should be considered severe
- what temperature bands define compliant versus non-compliant cold-chain operation
- what SLA should apply to unresolved DVIR and defect issues
- how regions, markets, and operating groups should be defined for reporting
- whether store replenishment, customer delivery, and field operations should be reported separately

## 13. Summary

The business needs an operations-focused reporting layer that turns telematics and operational data into five usable report domains:

- fleet utilization and route execution
- driver safety and coaching
- fuel and idling efficiency
- cold-chain compliance
- maintenance and asset health

The reporting solution should prioritize timely operational visibility, exception management, and regional performance analysis, while remaining simple enough for business users to consume directly.
