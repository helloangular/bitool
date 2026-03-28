# Samsara Analytics Gold Catalog

## 1. Base URL

Local mock API base URL:

- `http://localhost:3001`

All mock endpoints in this repo hang off that base URL.

Examples:

- `http://localhost:3001/fleet/vehicles`
- `http://localhost:3001/fleet/drivers`
- `http://localhost:3001/fleet/safety/events`

## 2. Coverage Summary

The local Samsara mock in this repo covers the major analytics domains needed for a sellable fleet analytics product:

- vehicles, vehicle stats, vehicle locations, fuel-energy, vehicle safety scores
- drivers, driver safety scores, HOS logs, HOS violations, daily logs, tachograph files
- trailers, assets, reefers, equipment, equipment locations, equipment stats
- DVIRs, defects, maintenance history
- routes, dispatch routes, dispatch jobs
- fuel and IFTA
- addresses, geofences, tags
- documents and document types
- alerts and alert configurations
- users, contacts, user roles, webhooks
- gateways, industrial assets, industrial data, industrial inputs, sensors
- driver-vehicle and vehicle-driver assignments

This means the product can support fleet operations, safety, compliance, maintenance, dispatch, utilization, geospatial, and industrial/IoT reporting from one connector family.

## 3. Gold Design Principles

For Samsara analytics, Gold should be organized into:

- executive daily scorecards
- current-state operational marts
- daily/weekly performance marts
- compliance and exception marts
- domain-specific scorecards by vehicle, driver, asset, site, and route

Gold tables should be report-ready:

- business-friendly names
- stable dimensions and measures
- one clear business grain per table
- support for BI dashboards, scheduled reports, and customer-facing analytics packs

## 4. Recommended Core Gold Tables

These are the highest-value Gold tables for a Samsara analytics product.

| Gold table | Grain | Primary source endpoints | Analytics provided |
| --- | --- | --- | --- |
| `fleet_overview_daily` | day | `fleet/vehicles`, `fleet/drivers`, `fleet/dispatch/jobs`, `fleet/safety/events`, `alerts` | Daily executive fleet KPIs: active vehicles, active drivers, jobs completed, safety events, alerts triggered. |
| `fleet_exception_daily` | day | `fleet/hos/violations`, `fleet/maintenance/dvirs`, `fleet/defects`, `alerts`, `fleet/safety/events` | Daily operational exception summary across compliance, safety, and maintenance. |
| `fleet_asset_status_current` | current snapshot | `fleet/vehicles`, `fleet/trailers`, `fleet/assets`, `fleet/equipment`, `fleet/vehicles/locations`, `fleet/trailers/locations`, `fleet/equipment/locations` | Current location and status of all mobile and non-mobile fleet assets in one operational mart. |
| `fleet_dispatch_performance_daily` | day | `fleet/routes`, `fleet/dispatch/routes`, `fleet/dispatch/jobs` | Route and dispatch productivity: jobs assigned, jobs completed, routes active, route completion rate. |
| `fleet_energy_cost_daily` | day | `fleet/vehicles/fuel-energy`, `fleet/fuel-energy/vehicle-report`, `fleet/ifta/summary` | Daily fuel consumption, economy, emissions proxy, and fuel-tax reporting readiness. |

## 5. Vehicle Analytics Gold Tables

| Gold table | Grain | Source endpoints | Analytics provided |
| --- | --- | --- | --- |
| `vehicle_master_current` | current snapshot | `fleet/vehicles`, `tags` | Current vehicle roster with make/model/year/VIN/tag segmentation for BI slicing. |
| `vehicle_utilization_daily` | vehicle, day | `fleet/vehicles`, `fleet/vehicles/stats`, `fleet/vehicles/locations`, `fleet/dispatch/jobs` | Vehicle usage by day: active status, movement, jobs serviced, operating intensity. |
| `vehicle_utilization_weekly` | vehicle, week | `fleet/vehicles/stats`, `fleet/dispatch/jobs` | Weekly trend view for vehicle utilization, used for capacity planning and underused asset detection. |
| `vehicle_location_current` | vehicle | `fleet/vehicles/locations` | Last known location, speed, heading, and freshness for live fleet maps and dispatch boards. |
| `vehicle_location_history_daily` | vehicle, day | `fleet/vehicles/locations/history` | Daily rollups of location movement, last stop, motion patterns, and route adherence. |
| `vehicle_performance_daily` | vehicle, day | `fleet/vehicles/stats`, `fleet/vehicles/:id/stats/history` | Operating statistics per vehicle per day for health/performance trend reporting. |
| `vehicle_fuel_efficiency_daily` | vehicle, day | `fleet/vehicles/fuel-energy`, `fleet/fuel-energy/vehicle-report` | MPG/energy efficiency reporting, fuel consumed, cost proxy, and emissions proxy by vehicle. |
| `vehicle_safety_score_daily` | vehicle, day | `fleet/safety/events`, `fleet/safety/scores/vehicles`, `fleet/vehicles/:id/safety/score` | Daily safety scorecard by vehicle with event count, severity mix, and risk ranking. |
| `vehicle_driver_assignment_current` | vehicle | `fleet/vehicles/driver-assignments` | Current assigned driver by vehicle for dispatch, accountability, and utilization reporting. |
| `vehicle_driver_assignment_history` | vehicle, assignment | `fleet/vehicles/driver-assignments`, `fleet/drivers/vehicle-assignments` | Historical assignment lineage between vehicles and drivers for operational audit and shift analytics. |
| `vehicle_alert_activity_daily` | vehicle, day | `alerts`, `alerts/configurations` | Vehicle-specific alert volume, alert types, threshold hits, and alert coverage by day. |

## 6. Driver Analytics Gold Tables

| Gold table | Grain | Source endpoints | Analytics provided |
| --- | --- | --- | --- |
| `driver_master_current` | current snapshot | `fleet/drivers`, `fleet/users`, `user-roles`, `tags` | Current driver dimension with employment status, tags, user mapping, and role segmentation. |
| `driver_productivity_daily` | driver, day | `fleet/drivers`, `fleet/dispatch/jobs`, `fleet/routes` | Daily driver output: assigned work, completed jobs, route participation, and activity trends. |
| `driver_safety_daily` | driver, day | `fleet/safety/events`, `fleet/safety/scores/drivers`, `fleet/drivers/:id/safety/score` | Driver risk monitoring: event counts, severity mix, safety score trends, coachable events. |
| `driver_hos_compliance_daily` | driver, day | `fleet/hos/logs`, `fleet/hos/daily-logs`, `fleet/hos/clocks`, `fleet/hos/violations` | Daily HOS compliance dashboard: duty status, remaining clocks, violations, and audit readiness. |
| `driver_hos_violation_detail` | violation | `fleet/hos/violations`, `fleet/drivers/:id/hos/violations` | Detailed violation reporting for compliance teams and remediation workflows. |
| `driver_dvir_activity_daily` | driver, day | `fleet/maintenance/dvirs` | Inspection submission and defect reporting activity by driver. |
| `driver_vehicle_assignment_current` | driver | `fleet/drivers/vehicle-assignments` | Current assigned vehicle by driver for operations and dispatch. |
| `driver_risk_scorecard_monthly` | driver, month | `fleet/safety/events`, `fleet/safety/scores/drivers`, `fleet/hos/violations`, `alerts` | Sellable monthly driver scorecard combining safety, compliance, and alert burden. |

## 7. Trailer, Asset, Reefer, and Equipment Gold Tables

| Gold table | Grain | Source endpoints | Analytics provided |
| --- | --- | --- | --- |
| `trailer_master_current` | current snapshot | `fleet/trailers` | Current trailer inventory and segmentation by trailer type, status, and tags. |
| `trailer_location_current` | trailer | `fleet/trailers/locations` | Live trailer visibility for yard management and lost-asset prevention. |
| `trailer_utilization_daily` | trailer, day | `fleet/trailers`, `fleet/trailers/locations`, `fleet/trailers/stats` | Trailer usage, movement, idle assets, and rotation efficiency. |
| `asset_master_current` | current snapshot | `fleet/assets` | Current tracked asset inventory for non-vehicle field assets. |
| `asset_reefer_health_daily` | asset, day | `fleet/assets/reefers/stats` | Reefer unit temperature/health reporting and cold-chain monitoring. |
| `equipment_master_current` | current snapshot | `fleet/equipment` | Equipment inventory and classification for operations and maintenance reporting. |
| `equipment_location_current` | equipment | `fleet/equipment/locations` | Last-known location of equipment for site logistics and asset recovery. |
| `equipment_usage_daily` | equipment, day | `fleet/equipment/stats`, `fleet/equipment/stats/history` | Engine hours, state, fuel, and utilization trends for equipment-heavy fleets. |
| `equipment_location_history_daily` | equipment, day | `fleet/equipment/locations/history`, `fleet/equipment/locations/feed` | Historical movement and location trail for equipment deployment analytics. |

## 8. Safety and Incident Gold Tables

| Gold table | Grain | Source endpoints | Analytics provided |
| --- | --- | --- | --- |
| `safety_event_detail` | event | `fleet/safety/events`, `fleet/safety/events/:id`, `fleet/safety-events` | Detailed safety incident mart for event review, coaching, and root-cause analysis. |
| `safety_event_trends_daily` | day | `fleet/safety/events` | Event counts, severity trends, and behavior categories over time. |
| `safety_event_trends_by_driver_daily` | driver, day | `fleet/safety/events`, `fleet/drivers` | Driver-level event burden and improvement trend analysis. |
| `safety_event_trends_by_vehicle_daily` | vehicle, day | `fleet/safety/events`, `fleet/vehicles` | Vehicle-level safety event patterns and high-risk unit detection. |
| `safety_coaching_queue_current` | current snapshot | `fleet/safety/events`, `fleet/safety/events/:id` | Current queue of coachable events and unresolved safety review items. |

## 9. Compliance and Maintenance Gold Tables

| Gold table | Grain | Source endpoints | Analytics provided |
| --- | --- | --- | --- |
| `dvir_submission_daily` | day | `fleet/maintenance/dvirs`, `fleet/dvirs/history` | Inspection volume, completion rate, and submission timeliness by day. |
| `dvir_defect_open_current` | defect | `fleet/maintenance/dvirs`, `fleet/defects`, `fleet/defects/history` | Open unresolved defects with age, severity, responsible asset, and resolution status. |
| `maintenance_compliance_daily` | day | `fleet/maintenance/dvirs`, `fleet/defects`, `fleet/defects/history` | Daily maintenance compliance KPIs: inspections done, open defects, aging backlog. |
| `vehicle_defect_history_daily` | vehicle, day | `fleet/defects`, `fleet/defects/history`, `fleet/maintenance/dvirs` | Vehicle-specific defect trends and repeat issue detection. |
| `driver_compliance_scorecard_monthly` | driver, month | `fleet/hos/violations`, `fleet/hos/daily-logs`, `fleet/maintenance/dvirs` | Monthly driver compliance scorecard combining HOS and inspection behaviors. |

## 10. Dispatch, Route, and Service Gold Tables

| Gold table | Grain | Source endpoints | Analytics provided |
| --- | --- | --- | --- |
| `route_master_current` | current snapshot | `fleet/routes`, `fleet/dispatch/routes`, `addresses`, `fleet/geofences` | Current route catalog with route structure and served locations. |
| `route_execution_daily` | route, day | `fleet/routes`, `fleet/dispatch/routes`, `fleet/dispatch/jobs` | Route productivity, completion status, route counts, and service output per day. |
| `dispatch_job_detail` | job | `fleet/dispatch/jobs` | Detailed delivery/service job mart for workload, status, and SLA analytics. |
| `dispatch_job_execution_daily` | day | `fleet/dispatch/jobs` | Jobs created, assigned, completed, failed, late, and in-progress by day. |
| `dispatch_productivity_by_driver_daily` | driver, day | `fleet/dispatch/jobs`, `fleet/drivers` | Driver delivery productivity and work allocation by day. |
| `dispatch_productivity_by_vehicle_daily` | vehicle, day | `fleet/dispatch/jobs`, `fleet/vehicles` | Vehicle utilization in service execution and dispatch throughput. |
| `route_on_time_performance_daily` | route, day | `fleet/dispatch/routes`, `fleet/dispatch/jobs`, `addresses`, `fleet/geofences` | Planned vs actual execution and route SLA reporting. |
| `customer_site_service_daily` | address/geofence, day | `fleet/dispatch/jobs`, `addresses`, `fleet/geofences` | Customer-site service volume, dwell, and service coverage reporting. |

## 11. Fuel, Tax, and Sustainability Gold Tables

| Gold table | Grain | Source endpoints | Analytics provided |
| --- | --- | --- | --- |
| `fuel_consumption_daily` | vehicle, day | `fleet/vehicles/fuel-energy`, `fleet/fuel-energy/vehicle-report` | Gallons consumed, distance covered, and fuel-use trend by day. |
| `fuel_efficiency_daily` | vehicle, day | `fleet/vehicles/fuel-energy`, `fleet/fuel-energy/vehicle-report`, `fleet/vehicles/stats` | MPG/efficiency comparisons across vehicles, classes, and periods. |
| `carbon_emissions_daily` | vehicle, day | `fleet/vehicles/fuel-energy`, `fleet/fuel-energy/vehicle-report` | Estimated CO2 by vehicle/day for ESG and sustainability dashboards. |
| `ifta_summary_quarterly` | vehicle, jurisdiction, quarter | `fleet/ifta/summary` | Fuel tax reporting mart for IFTA preparation and audit support. |
| `energy_exception_daily` | day | `fleet/vehicles/fuel-energy`, `alerts` | Low-fuel, abnormal-consumption, and efficiency exception reporting. |

## 12. Geofence, Address, and Network Gold Tables

| Gold table | Grain | Source endpoints | Analytics provided |
| --- | --- | --- | --- |
| `address_master_current` | current snapshot | `addresses` | Customer sites, depots, yards, terminals, and service locations. |
| `geofence_master_current` | current snapshot | `fleet/geofences` | Geofence inventory and operational tagging for location analytics. |
| `geofence_activity_daily` | geofence, day | `fleet/geofences`, `fleet/vehicles/locations`, `alerts` | Entry/exit activity and site traffic trends. |
| `yard_entry_exit_daily` | yard, day | `fleet/geofences`, `fleet/vehicles/locations`, `alerts` | Yard departures, arrivals, throughput, and congestion proxy metrics. |
| `site_dwell_proxy_daily` | site, day | `fleet/vehicles/locations/history`, `addresses`, `fleet/geofences` | Dwell-time proxy analytics for customer and yard operations. |

## 13. Document, Alert, and Workflow Gold Tables

| Gold table | Grain | Source endpoints | Analytics provided |
| --- | --- | --- | --- |
| `document_submission_daily` | day | `fleet/documents`, `fleet/document-types` | Document completion and submission throughput by day and document type. |
| `document_compliance_current` | current snapshot | `fleet/documents`, `fleet/document-types`, `fleet/drivers`, `fleet/vehicles` | Missing, late, or incomplete operational documentation. |
| `alert_activity_daily` | day | `alerts` | Alert volume, alert type trends, and operational exception spikes. |
| `alert_activity_by_entity_daily` | entity, day | `alerts`, `fleet/vehicles`, `fleet/drivers`, `fleet/geofences` | Alert burden by vehicle, driver, or geofence for targeted action. |
| `alert_configuration_coverage` | configuration | `alerts/configurations` | Which alert policies exist, who they notify, and where operational monitoring is weak. |
| `webhook_configuration_current` | current snapshot | `webhooks` | Webhook inventory and downstream integration footprint for platform governance. |

## 14. User, Org, and Admin Gold Tables

| Gold table | Grain | Source endpoints | Analytics provided |
| --- | --- | --- | --- |
| `user_master_current` | current snapshot | `fleet/users`, `users`, `user-roles`, `me` | Current user inventory, role mix, and workspace identity mapping. |
| `user_role_distribution_current` | role | `fleet/users`, `users`, `user-roles` | Admin/security reporting on role distribution and access footprint. |
| `contact_master_current` | current snapshot | `contacts` | Shared contact directory for dispatch, maintenance, and operational workflows. |
| `tag_usage_current` | tag | `tags`, `fleet/vehicles`, `fleet/drivers`, `fleet/assets`, `fleet/equipment` | How tags are used across entities for segment reporting and governance. |

## 15. Industrial, Sensor, and Gateway Gold Tables

| Gold table | Grain | Source endpoints | Analytics provided |
| --- | --- | --- | --- |
| `gateway_health_current` | gateway | `gateways` | Current IoT gateway status, last-seen freshness, and offline monitoring. |
| `industrial_asset_master_current` | current snapshot | `industrial/assets` | Industrial asset inventory for site operations analytics. |
| `industrial_input_master_current` | current snapshot | `industrial/data-inputs` | Sensor/data-input registry for industrial telemetry reporting. |
| `industrial_sensor_health_daily` | sensor/input, day | `industrial/data`, `sensors/list`, `sensors/temperature`, `sensors/humidity`, `sensors/door` | Sensor freshness, signal quality, and monitored condition trends. |
| `industrial_asset_condition_daily` | asset, day | `industrial/data`, `industrial/assets`, `industrial/data-inputs` | Asset condition and telemetry trends for facilities and industrial operations. |
| `door_event_activity_daily` | asset/site, day | `sensors/door` | Door-open/close and access-event reporting for secure facilities. |
| `temperature_compliance_daily` | asset/site, day | `sensors/temperature`, `industrial/data` | Cold-chain and facility temperature compliance reporting. |
| `humidity_condition_daily` | asset/site, day | `sensors/humidity`, `industrial/data` | Environmental condition monitoring for storage and facilities. |

## 16. Sellable Analytics Packages

These Gold tables can be packaged into client-facing analytics products.

### Fleet Operations Pack

- `fleet_overview_daily`
- `vehicle_utilization_daily`
- `dispatch_job_execution_daily`
- `route_execution_daily`
- `fleet_asset_status_current`

Use case:

- daily fleet command center dashboards
- route efficiency and dispatch productivity
- live asset visibility

### Safety and Compliance Pack

- `driver_safety_daily`
- `vehicle_safety_score_daily`
- `driver_hos_compliance_daily`
- `driver_hos_violation_detail`
- `dvir_defect_open_current`
- `maintenance_compliance_daily`

Use case:

- safety coaching
- insurance/risk reviews
- compliance monitoring
- inspection and defect follow-up

### Fuel and Sustainability Pack

- `fuel_consumption_daily`
- `fuel_efficiency_daily`
- `carbon_emissions_daily`
- `ifta_summary_quarterly`

Use case:

- fuel optimization
- route cost reduction
- ESG dashboards
- tax reporting support

### Asset and Equipment Pack

- `trailer_utilization_daily`
- `equipment_usage_daily`
- `equipment_location_current`
- `asset_reefer_health_daily`

Use case:

- trailer pooling
- heavy equipment utilization
- cold-chain operations
- asset recovery and site logistics

### Industrial and Facility Pack

- `gateway_health_current`
- `industrial_sensor_health_daily`
- `industrial_asset_condition_daily`
- `temperature_compliance_daily`
- `door_event_activity_daily`

Use case:

- warehouse/facility monitoring
- industrial equipment telemetry
- environmental compliance
- secure-site operations

## 17. Important Modeling Note

The existing mock and local metrics package already cover core Samsara-style operational analytics, but the mock does not include a first-class `fleet/trips` endpoint. For this Samsara product, Gold should therefore lean on:

- `fleet/vehicles/locations`
- `fleet/dispatch/jobs`
- `fleet/routes`
- `fleet/vehicles/stats`

instead of assuming a trip ledger exists.

That gives a more realistic sellable analytics story for Samsara API clients:

- fleet utilization
- dispatch throughput
- safety/compliance
- asset visibility
- fuel/tax
- industrial telemetry

without depending on endpoints the mock does not expose.
