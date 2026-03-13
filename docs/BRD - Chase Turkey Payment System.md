# Business Requirements Document (BRD)
# Chase Bank Turkey Payment System — Microservices Platform

**Project Name:** Chase Turkey Payment System Extension
**Client:** JPMorgan Chase — Turkey Operations
**Document Version:** 1.0
**Date:** February 23, 2026
**Status:** Draft
**Prepared By:** BiTool Engineering

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Business Context](#2-business-context)
3. [Problem Statement](#3-problem-statement)
4. [Proposed Solution](#4-proposed-solution)
5. [Product 1: Microservices Builder](#5-product-1-microservices-builder)
6. [Product 2: Microservices Orchestrator](#6-product-2-microservices-orchestrator)
7. [Turkey-Specific Payment Requirements](#7-turkey-specific-payment-requirements)
8. [Functional Requirements](#8-functional-requirements)
9. [Non-Functional Requirements](#9-non-functional-requirements)
10. [Integration Requirements](#10-integration-requirements)
11. [Binary Build Plan](#11-binary-build-plan)
12. [Risk Assessment](#12-risk-assessment)
13. [Success Criteria](#13-success-criteria)
14. [Appendix](#14-appendix)

---

## 1. Executive Summary

Chase Bank operates a multi-country payment processing system that must now be extended to Turkey. Turkey's financial regulatory environment (BRSA/BDDK, TCMB, MASAK) imposes country-specific requirements for payment processing, fraud detection, tax compliance, and reporting that differ significantly from other markets.

This BRD proposes two products built on the **BiTool platform**:

1. **Microservices Builder** — A visual, no-code/low-code platform for designing, building, and deploying payment microservices as directed graphs. Business analysts and developers drag-and-drop processing nodes (authentication, validation, routing, transformation) to compose complete microservice pipelines.

2. **Microservices Orchestrator** — A runtime engine that executes the graph-based microservices, handles service mesh coordination, manages transaction boundaries, and provides observability across the payment processing chain.

Together, these products allow Chase Turkey to:
- Rapidly implement Turkey-specific payment rules without modifying the global codebase
- Visually design payment flows that are auditable and compliance-friendly
- Deploy, version, and rollback payment services independently
- Reuse existing global payment components while adding country-specific logic

---

## 2. Business Context

### 2.1 Chase Global Payment System (Existing)

Chase operates payment processing across 60+ countries. The existing system provides:
- Core payment rails (SWIFT, SEPA, ACH, Wire)
- Multi-currency settlement
- Global compliance (AML/KYC/Sanctions screening)
- Fraud detection and prevention
- Regulatory reporting (per-country)

### 2.2 Turkey Market Entry

Turkey's payment ecosystem is governed by:
- **BRSA/BDDK** (Banking Regulation and Supervision Agency) — banking licenses and operational requirements
- **TCMB** (Central Bank of Turkey) — monetary policy, FX regulations, IBAN validation
- **MASAK** (Financial Crimes Investigation Board) — AML/CTF requirements
- **FAST (Fonlar Arasi Siparis Transfer)** — Turkey's instant payment system (analogous to FedNow/UPI)
- **EFT (Electronic Fund Transfer)** — Turkey's domestic wire transfer system
- **BTrans** — Central Bank transfer system for high-value payments

### 2.3 Business Drivers

| Driver | Description |
|--------|-------------|
| **Regulatory compliance** | Turkey mandates specific transaction reporting, FX controls, and AML thresholds different from other markets |
| **Speed to market** | Visual microservice builder reduces time from months to weeks for new payment flows |
| **Auditability** | Graph-based design provides visual audit trail for regulators |
| **Isolation** | Turkey-specific rules deployed as independent services, not changes to global code |
| **Reusability** | Global payment components (auth, sanctions screening) reused as graph nodes |

---

## 3. Problem Statement

### 3.1 Current Challenges

1. **Monolithic coupling**: Adding Turkey-specific rules to the global payment system risks regressions in 60+ existing markets
2. **Long release cycles**: Code changes require full regression testing across all markets (8-12 week cycles)
3. **Compliance complexity**: Turkish MASAK reporting rules, FAST integration, and TCMB FX controls require specialized logic that doesn't fit cleanly into the global framework
4. **Visibility gap**: Payment processing logic is buried in code — compliance teams cannot audit or verify rules without developer assistance
5. **Vendor lock-in**: Current integration middleware is proprietary and expensive to extend

### 3.2 What This Solves

| Problem | Solution |
|---------|----------|
| Monolithic coupling | Independent microservices per payment flow, isolated deployment |
| Long release cycles | Visual builder → deploy in hours, not months |
| Compliance complexity | Turkey-specific nodes, drag-and-drop composition |
| Visibility gap | Graph = visual audit trail, readable by compliance officers |
| Vendor lock-in | Open IGL (Intermediate Graph Language) format, Clojure/JVM runtime |

---

## 4. Proposed Solution

### 4.1 Solution Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    MICROSERVICES BUILDER                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │  Visual   │  │  Node    │  │  Graph   │  │ OpenAPI  │      │
│  │  Canvas   │  │  Library │  │  Storage │  │  Import  │      │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘      │
│                         ↓ IGL (Graph)                           │
├─────────────────────────────────────────────────────────────────┤
│                   MICROSERVICES ORCHESTRATOR                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │  Graph   │  │ Service  │  │  Tx      │  │ Observ-  │      │
│  │ Executor │  │  Mesh    │  │  Manager │  │  ability  │      │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘      │
│                         ↓ Runtime                               │
├─────────────────────────────────────────────────────────────────┤
│                     PAYMENT INFRASTRUCTURE                       │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐      │
│  │ FAST │ │ EFT  │ │SWIFT │ │ TCMB │ │MASAK │ │ Core │      │
│  │      │ │      │ │      │ │  FX  │ │ AML  │ │  DB  │      │
│  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘ └──────┘      │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 IGL — Intermediate Graph Language

The graph IS the microservice. Each payment flow is stored as an IGL graph:

```
graph "turkey-fast-payment" {
  ep1 = Ep(POST, "/payments/fast", body=[amount, sender_iban, receiver_iban, description])
  au1 = Auth(JWT, claims=[bank_id, user_role])
  vd1 = Validator(amount: {required, min:0.01, max:500000}, sender_iban: {pattern: "TR[0-9]{24}"})
  fx1 = FXCheck(source:TRY, threshold:15000)
  ml1 = AMLScreen(provider:MASAK, threshold:75000)
  dx1 = DbExecute(INSERT INTO transactions ...)
  hc1 = HttpCall(FAST_API, "/transfers", method:POST)
  rb1 = ResponseBuilder(201, json)

  ep1 -> au1 -> vd1 -> fx1 -> ml1 -> dx1 -> hc1 -> rb1 -> Output
}
```

---

## 5. Product 1: Microservices Builder

### 5.1 Overview

The Microservices Builder is the **design-time** tool. It provides a visual canvas where users compose microservice pipelines by connecting processing nodes.

### 5.2 Node Library

#### 5.2.1 Existing Nodes (Available Now)

| Node | Code | Function | Payment Use Case |
|------|------|----------|------------------|
| Endpoint | Ep | HTTP entry point | Receive payment requests |
| Filter | Fi | Conditional filtering | Route by payment type/amount |
| Projection | P | Field selection/rename | Shape API responses |
| Join | J | Combine data sources | Enrich with customer data |
| Union | U | Merge datasets | Combine multi-source results |
| Aggregation | A | Group + aggregate | Daily settlement totals |
| Sort | S | Order results | Priority queue ordering |
| Function | Fu | Column transforms | Format IBAN, currency conversion |
| Output | O | Terminal node | HTTP response / message publish |

#### 5.2.2 Phase 1 Nodes — Core Request/Response

| Node | Code | Function | Payment Use Case |
|------|------|----------|------------------|
| Response Builder | Rb | Set HTTP status, headers, format | Return 201 Created with payment ID |
| Validator | Vd | Schema + rule validation | Validate IBAN format, amount limits, required fields |
| DB Execute | Dx | Parameterized SQL | Insert/query transaction records |
| Error Handler | Eh | Catch + format errors | Return structured error responses to callers |

#### 5.2.3 Phase 2 Nodes — Security & Middleware

| Node | Code | Function | Payment Use Case |
|------|------|----------|------------------|
| Auth | Au | JWT/API Key/OAuth | Verify bank identity, extract claims |
| Rate Limiter | Rl | Throttle requests | Protect FAST endpoints from abuse |
| CORS | Cr | Cross-origin config | Enable partner bank integrations |
| Logger | Lg | Structured logging | Audit trail for every payment step |

#### 5.2.4 Phase 3 Nodes — Data Integration

| Node | Code | Function | Payment Use Case |
|------|------|----------|------------------|
| HTTP Call | Hc | Outbound API calls | Call FAST, EFT, SWIFT, MASAK APIs |
| Cache | Ca | Response caching | Cache FX rates, institution lookups |
| Transformer | Tr | Complex data reshape | Map between ISO 20022 and domestic formats |
| Conditional Branch | Cb | If/else routing | Route by amount (FAST vs EFT vs BTrans) |
| Loop/Iterator | Li | Batch processing | Process bulk payment files |

#### 5.2.5 Phase 4 Nodes — Turkey-Specific

| Node | Code | Function | Payment Use Case |
|------|------|----------|------------------|
| IBAN Validator | Iv | Turkey IBAN (TR + 24 digits) | Validate TR IBANs before processing |
| FX Control | Fx | TCMB foreign exchange rules | Enforce FX conversion limits, reporting thresholds |
| MASAK Screen | Ms | AML/CTF screening | Check against MASAK watchlists, report STRs |
| FAST Connector | Fc | FAST instant payment API | Submit/receive FAST transfers |
| EFT Connector | Ec | EFT wire transfer API | Submit domestic wire transfers |
| Tax Withholding | Tw | BSMV/KKDF calculation | Calculate banking & insurance transaction taxes |
| KPS Identity | Ki | Turkish national ID verification | Verify TC Kimlik No via KPS/Mernis |

### 5.3 Builder Capabilities

| Capability | Description |
|------------|-------------|
| **Drag-and-drop canvas** | Visual graph editor for composing microservices |
| **Node configuration panels** | Context-specific forms for each node type |
| **Live column propagation** | Upstream column changes automatically flow downstream |
| **Graph versioning** | Every save creates a new version; rollback to any point |
| **OpenAPI import** | Auto-generate graphs from OpenAPI specs |
| **Graph templates** | Pre-built templates for common payment patterns (CRUD, Gateway, Event) |
| **Validation** | Design-time validation of graph completeness and correctness |
| **Test mode** | Send test requests through the graph without deploying |
| **Export** | Generate OpenAPI spec from graph (IGL → OpenAPI) |

### 5.4 User Personas

| Persona | Role | Use of Builder |
|---------|------|----------------|
| **Payment Developer** | Designs payment flows | Builds graphs, configures nodes, tests pipelines |
| **Business Analyst** | Defines requirements | Reviews graphs as visual specs, validates logic |
| **Compliance Officer** | Ensures regulatory compliance | Audits graphs for required nodes (AML, validation, logging) |
| **DevOps Engineer** | Deploys and monitors | Promotes graphs through environments, monitors runtime |

---

## 6. Product 2: Microservices Orchestrator

### 6.1 Overview

The Microservices Orchestrator is the **runtime** engine. It takes IGL graphs from the Builder and executes them as live microservices.

### 6.2 Core Components

#### 6.2.1 Graph Executor

```
Incoming HTTP Request
        ↓
  Route Matching (Ep node)
        ↓
  Graph Traversal Engine
    ├── Execute node N
    ├── Pass tcols to next node
    ├── Handle errors (Eh nodes)
    ├── Branch conditionally (Cb nodes)
    └── Merge parallel results (Pl nodes)
        ↓
  Response Assembly (Rb → Output)
        ↓
  HTTP Response
```

**Execution Model:**
- **Sequential**: Default — nodes execute in edge order
- **Parallel**: Pl node forks execution into parallel branches
- **Conditional**: Cb node evaluates condition and routes to one of two paths
- **Transactional**: Tx boundary nodes group operations into atomic units

#### 6.2.2 Service Mesh Integration

| Feature | Description |
|---------|-------------|
| **Service discovery** | Each deployed graph registers as a named service |
| **Load balancing** | Round-robin / weighted distribution across instances |
| **Health checks** | Heartbeat and deep health probes per service |
| **Circuit breaking** | Automatic failover when downstream services are degraded |
| **mTLS** | Mutual TLS between all services (zero-trust) |

#### 6.2.3 Transaction Manager

| Feature | Description |
|---------|-------------|
| **ACID transactions** | DB Execute nodes within a transaction boundary share a connection |
| **Saga pattern** | Long-running transactions across multiple services with compensating actions |
| **Idempotency** | Built-in idempotency key tracking for payment retry safety |
| **Two-phase commit** | For cross-database operations (e.g., debit account A, credit account B) |

#### 6.2.4 Observability Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| **Metrics** | Prometheus / Grafana | Request rate, latency (p50/p95/p99), error rate, throughput |
| **Logging** | ELK Stack (Elasticsearch + Logstash + Kibana) | Structured logs per node execution |
| **Tracing** | Jaeger / OpenTelemetry | Distributed trace across graph nodes and external calls |
| **Alerting** | PagerDuty / OpsGenie | Alert on SLA breach, error spike, payment failure |

### 6.3 Deployment Model

```
┌─────────────────────────────────────────┐
│              Kubernetes Cluster          │
│                                         │
│  ┌─────────────────────────────────┐   │
│  │      Orchestrator Pods          │   │
│  │  ┌────────┐  ┌────────┐       │   │
│  │  │ Graph  │  │ Graph  │  ...  │   │
│  │  │ Exec-1 │  │ Exec-2 │       │   │
│  │  └────────┘  └────────┘       │   │
│  └─────────────────────────────────┘   │
│                                         │
│  ┌─────────────────────────────────┐   │
│  │      Graph Registry (DB)        │   │
│  │  version-controlled IGL graphs  │   │
│  └─────────────────────────────────┘   │
│                                         │
│  ┌──────┐ ┌──────┐ ┌──────┐          │
│  │Istio │ │Vault │ │Redis │          │
│  │ Mesh │ │Secrets│ │Cache │          │
│  └──────┘ └──────┘ └──────┘          │
└─────────────────────────────────────────┘
```

### 6.4 Orchestrator Capabilities

| Capability | Description |
|------------|-------------|
| **Hot deployment** | Deploy new graph versions without downtime (blue-green) |
| **Canary releases** | Route % of traffic to new version before full rollout |
| **Auto-scaling** | Scale graph executor pods based on request volume |
| **Graph registry** | Central store of all graph versions with promotion workflow |
| **Environment promotion** | DEV → QA → UAT → PROD pipeline per graph |
| **Rollback** | Instant rollback to any previous graph version |
| **Feature flags** | Enable/disable graph sections at runtime |
| **Multi-tenancy** | Isolate graphs per business unit or partner bank |

---

## 7. Turkey-Specific Payment Requirements

### 7.1 FAST (Instant Payments)

| Requirement | Detail |
|-------------|--------|
| **TR-FAST-001** | Support FAST instant transfers (7x24, 365 days) |
| **TR-FAST-002** | Maximum single transaction: 500,000 TRY |
| **TR-FAST-003** | Settlement within 10 seconds (SLA) |
| **TR-FAST-004** | IBAN validation: TR + 2 check digits + 5-digit bank code + 17-digit account (24 digits total after TR) |
| **TR-FAST-005** | Support FAST QR code payments |
| **TR-FAST-006** | Real-time notification to sender and receiver |
| **TR-FAST-007** | Integrate with TCMB FAST hub API |

### 7.2 EFT (Electronic Fund Transfer)

| Requirement | Detail |
|-------------|--------|
| **TR-EFT-001** | Support EFT domestic wire transfers |
| **TR-EFT-002** | Settlement cycles: 5 batches per business day |
| **TR-EFT-003** | No maximum amount limit (institution-level limits apply) |
| **TR-EFT-004** | Support EFT return/reversal messages |
| **TR-EFT-005** | Integration with TCMB EFT system |

### 7.3 FX Controls (TCMB)

| Requirement | Detail |
|-------------|--------|
| **TR-FX-001** | Enforce TCMB FX conversion regulations |
| **TR-FX-002** | Report FX transactions above 50,000 USD equivalent to TCMB |
| **TR-FX-003** | Apply BSMV tax on FX transactions (currently 1%) |
| **TR-FX-004** | Validate against TCMB published FX rates |
| **TR-FX-005** | Support TRY/USD, TRY/EUR, TRY/GBP primary pairs |
| **TR-FX-006** | Enforce surrender requirements for export proceeds |

### 7.4 AML/CTF (MASAK)

| Requirement | Detail |
|-------------|--------|
| **TR-AML-001** | Screen all transactions against MASAK watchlists |
| **TR-AML-002** | STR (Suspicious Transaction Report) filing via MASAK portal |
| **TR-AML-003** | CTR (Cash Transaction Report) for transactions > 75,000 TRY |
| **TR-AML-004** | Enhanced due diligence for transactions > 350,000 TRY |
| **TR-AML-005** | PEP (Politically Exposed Person) screening |
| **TR-AML-006** | 10-year transaction record retention |
| **TR-AML-007** | Real-time sanctions screening (UN, EU, OFAC, MASAK domestic) |

### 7.5 Tax & Regulatory

| Requirement | Detail |
|-------------|--------|
| **TR-TAX-001** | BSMV (Banking and Insurance Transaction Tax) — 5% on interest, 1% on FX |
| **TR-TAX-002** | KKDF (Resource Utilization Support Fund) — varies by product |
| **TR-TAX-003** | Stamp duty on promissory notes and guarantees |
| **TR-TAX-004** | Withholding tax on interest payments |
| **TR-TAX-005** | Monthly regulatory reporting to BRSA/BDDK |
| **TR-TAX-006** | Real-time reporting to Revenue Administration (GIB) for e-invoicing |

### 7.6 Identity Verification

| Requirement | Detail |
|-------------|--------|
| **TR-ID-001** | TC Kimlik No (11-digit national ID) validation via KPS/Mernis |
| **TR-ID-002** | Tax ID (VKN - 10 digits) validation for corporate accounts |
| **TR-ID-003** | e-Devlet (e-Government) integration for identity verification |
| **TR-ID-004** | Address verification via Mernis (Central Population Admin System) |

---

## 8. Functional Requirements

### 8.1 Microservices Builder — Functional Requirements

| ID | Requirement | Priority | MoSCoW |
|----|-------------|----------|--------|
| **BLD-001** | Visual graph canvas with drag-and-drop node placement | Critical | Must |
| **BLD-002** | Node library with all Phase 1-4 nodes available | Critical | Must |
| **BLD-003** | Node configuration panels with context-sensitive forms | Critical | Must |
| **BLD-004** | Live column propagation from source to downstream nodes | Critical | Must |
| **BLD-005** | Graph versioning with full version history | High | Must |
| **BLD-006** | OpenAPI spec import to auto-generate graphs | High | Should |
| **BLD-007** | Design-time graph validation (completeness, type safety) | High | Must |
| **BLD-008** | Test mode — send test requests through the graph | High | Should |
| **BLD-009** | Graph templates for common payment patterns | Medium | Should |
| **BLD-010** | Export graph as OpenAPI spec | Medium | Could |
| **BLD-011** | Multi-user concurrent editing with conflict resolution | Low | Won't (v1) |
| **BLD-012** | Graph diff — visual comparison of two versions | Medium | Should |
| **BLD-013** | Role-based access control (who can edit/deploy/view) | High | Must |
| **BLD-014** | Audit log of all graph modifications | High | Must |

### 8.2 Microservices Orchestrator — Functional Requirements

| ID | Requirement | Priority | MoSCoW |
|----|-------------|----------|--------|
| **ORC-001** | Execute IGL graphs as live HTTP microservices | Critical | Must |
| **ORC-002** | Sequential node execution with tcol propagation | Critical | Must |
| **ORC-003** | Conditional branching (route by expression) | High | Must |
| **ORC-004** | Parallel execution with merge strategies | High | Should |
| **ORC-005** | Transaction boundaries (ACID for grouped DB operations) | Critical | Must |
| **ORC-006** | Saga pattern for distributed transactions | High | Should |
| **ORC-007** | Hot deployment — zero-downtime graph updates | Critical | Must |
| **ORC-008** | Canary releases — gradual traffic shift | Medium | Should |
| **ORC-009** | Auto-scaling based on request volume | High | Must |
| **ORC-010** | Service registry and discovery | High | Must |
| **ORC-011** | Circuit breaker for external service calls | High | Must |
| **ORC-012** | Idempotency key tracking | Critical | Must |
| **ORC-013** | Distributed tracing (OpenTelemetry) | High | Must |
| **ORC-014** | Structured logging per node execution | High | Must |
| **ORC-015** | Metrics export (Prometheus format) | High | Must |
| **ORC-016** | Environment promotion pipeline (DEV→QA→UAT→PROD) | High | Must |
| **ORC-017** | Instant rollback to previous graph version | Critical | Must |
| **ORC-018** | Rate limiting per endpoint | High | Must |

### 8.3 Turkey Payment — Functional Requirements

| ID | Requirement | Priority | MoSCoW |
|----|-------------|----------|--------|
| **TRK-001** | FAST instant payment initiation and receipt | Critical | Must |
| **TRK-002** | EFT domestic wire transfer submission | Critical | Must |
| **TRK-003** | IBAN validation (TR format) | Critical | Must |
| **TRK-004** | MASAK AML/CTF screening on all transactions | Critical | Must |
| **TRK-005** | TCMB FX rate validation and reporting | Critical | Must |
| **TRK-006** | BSMV tax calculation and withholding | High | Must |
| **TRK-007** | TC Kimlik / VKN identity verification | High | Must |
| **TRK-008** | STR/CTR reporting to MASAK | Critical | Must |
| **TRK-009** | BRSA regulatory reporting (monthly) | High | Must |
| **TRK-010** | 10-year transaction archival | High | Must |

---

## 9. Non-Functional Requirements

### 9.1 Performance

| Metric | Target | Notes |
|--------|--------|-------|
| FAST payment end-to-end | < 3 seconds | From API receipt to FAST hub submission |
| Graph execution latency (p95) | < 200ms | For a 6-node graph (typical payment flow) |
| Graph execution latency (p99) | < 500ms | Including external API calls |
| Throughput | 5,000 TPS | Peak load during salary payment days |
| Builder response time | < 1 second | For any UI operation (save, load, add node) |

### 9.2 Availability

| Metric | Target |
|--------|--------|
| Orchestrator uptime | 99.99% (52 min downtime/year) |
| Builder uptime | 99.9% (8.7 hrs downtime/year) |
| FAST availability | 99.99% (7x24) |
| Recovery Time Objective (RTO) | < 5 minutes |
| Recovery Point Objective (RPO) | 0 (no transaction loss) |

### 9.3 Security

| Requirement | Detail |
|-------------|--------|
| Encryption at rest | AES-256 for all data stores |
| Encryption in transit | TLS 1.3 for all communications |
| mTLS | Between all internal services |
| Secret management | HashiCorp Vault for keys, credentials |
| Access control | RBAC with LDAP/AD integration |
| PCI DSS | Level 1 compliance for card payment data |
| KVKK | Turkish data protection law compliance (Turkey's GDPR) |
| Audit logging | Immutable audit trail for all operations |
| Penetration testing | Annual third-party pen test required |

### 9.4 Scalability

| Dimension | Target |
|-----------|--------|
| Concurrent graphs | 500+ deployed simultaneously |
| Graph complexity | Up to 50 nodes per graph |
| Horizontal scaling | Stateless executor pods, scale to 100+ instances |
| Database connections | Pool per graph, max 1000 total |
| Storage | 10-year retention → ~50TB estimated |

### 9.5 Compliance

| Standard | Applicability |
|----------|--------------|
| PCI DSS Level 1 | Card payment processing |
| ISO 27001 | Information security management |
| SOC 2 Type II | Service organization controls |
| KVKK | Turkish personal data protection |
| BRSA regulations | Banking operational requirements |
| MASAK regulations | AML/CTF requirements |

---

## 10. Integration Requirements

### 10.1 Internal Systems

| System | Integration Type | Direction | Purpose |
|--------|-----------------|-----------|---------|
| Chase Global Payment Hub | REST API / Message Queue | Bidirectional | Route payments to/from global rails |
| Core Banking System | JDBC / REST API | Bidirectional | Account balance, debit/credit |
| Risk Engine | REST API | Outbound | Real-time risk scoring |
| Customer Master | REST API | Outbound | Customer data enrichment |
| Notification Service | Message Queue | Outbound | SMS/email/push notifications |
| Reconciliation System | Batch File / API | Outbound | End-of-day settlement matching |

### 10.2 External Systems (Turkey)

| System | Provider | Protocol | Purpose |
|--------|----------|----------|---------|
| FAST Hub | TCMB | ISO 20022 / API | Instant payment submission/receipt |
| EFT System | TCMB | ISO 8583 / Batch | Domestic wire transfers |
| MASAK Portal | MASAK | Web Service / SFTP | AML reporting and watchlist |
| KPS / Mernis | NVI | SOAP Web Service | National identity verification |
| GIB (Revenue Admin) | GIB | REST API | e-Invoice, tax reporting |
| TCMB FX | TCMB | REST API | Official FX rates |
| SWIFT | SWIFT | FIN / ISO 20022 | International transfers |
| Interbank Card Center (BKM) | BKM | ISO 8583 | Card payment processing |

### 10.3 Data Formats

| Format | Use Case |
|--------|----------|
| ISO 20022 (XML) | FAST, SWIFT, international payments |
| ISO 8583 | Card transactions, EFT |
| JSON | Internal APIs, Builder ↔ Orchestrator |
| EDN | IGL graph storage |
| CSV/Fixed-width | Batch regulatory reports |

---

## 11. Binary Build Plan

### 11.1 What is a Binary Plan?

A binary plan defines clear **go/no-go** milestones. At each checkpoint, the decision is binary: either all criteria are met (GO) or they aren't (NO-GO, with specific remediation actions).

### 11.2 Phase Overview

```
Phase 0    Phase 1      Phase 2      Phase 3     Phase 4     Phase 5
Foundation  Core Nodes   Security +   Turkey       Integration  Production
            + Executor   Middleware   Specifics    Testing      Launch
[4 weeks]  [8 weeks]    [6 weeks]   [8 weeks]    [6 weeks]    [4 weeks]
    ↓          ↓            ↓           ↓            ↓            ↓
  GO/NOGO   GO/NOGO     GO/NOGO     GO/NOGO      GO/NOGO      LAUNCH
```

**Total Timeline: 36 weeks (9 months)**

---

### 11.3 Phase 0: Foundation (Weeks 1-4)

**Objective:** Platform infrastructure and existing node stabilization

| ID | Deliverable | Owner | Binary Criteria |
|----|-------------|-------|-----------------|
| P0-1 | Development environment setup (K8s, CI/CD, Vault) | DevOps | All devs can deploy to DEV in < 5 min |
| P0-2 | Existing nodes stabilized (Ep, Fi, P, J, U, A, S, Fu, O) | Backend | All existing nodes pass integration tests |
| P0-3 | Graph versioning with rollback | Backend | Can save, load, and rollback to any version |
| P0-4 | IGL schema formalized | Architect | Written spec document, validated by team |
| P0-5 | Security baseline (mTLS, Vault, RBAC skeleton) | Security | mTLS between all services, Vault operational |
| P0-6 | Turkey regulatory requirements sign-off | Compliance | Written sign-off from compliance team on TR-* requirements |

**GO/NO-GO Checkpoint:**
- [ ] All 9 existing nodes have passing integration tests
- [ ] CI/CD pipeline deploys to DEV automatically
- [ ] IGL schema document reviewed and approved
- [ ] Turkey compliance requirements signed off

---

### 11.4 Phase 1: Core Nodes + Executor (Weeks 5-12)

**Objective:** Build the four core Phase 1 nodes and the graph execution engine

#### Sprint 1-2 (Weeks 5-6): Response Builder + Validator

| ID | Deliverable | Binary Criteria |
|----|-------------|-----------------|
| P1-1 | Response Builder (Rb) node — backend | Rb node sets status code, headers, format; tests pass |
| P1-2 | Response Builder (Rb) node — frontend | Config panel renders, saves, loads correctly |
| P1-3 | Validator (Vd) node — backend | Validates required, type, min/max, regex, enum; returns 400 on failure |
| P1-4 | Validator (Vd) node — frontend | Rule builder UI, error preview |

#### Sprint 3-4 (Weeks 7-8): DB Execute + Error Handler

| ID | Deliverable | Binary Criteria |
|----|-------------|-----------------|
| P1-5 | DB Execute (Dx) node — backend | Executes parameterized SQL, results become tcols |
| P1-6 | DB Execute (Dx) node — frontend | SQL template editor with column placeholder insertion |
| P1-7 | Error Handler (Eh) node — backend | Catches upstream errors, maps to status codes |
| P1-8 | Error Handler (Eh) node — frontend | Error mapping configuration UI |

#### Sprint 5-6 (Weeks 9-10): Graph Executor v1

| ID | Deliverable | Binary Criteria |
|----|-------------|-----------------|
| P1-9 | Sequential graph executor | Traverses Ep→...→Output, executes each node, passes tcols |
| P1-10 | Request routing | HTTP request matched to deployed graph by path + method |
| P1-11 | Node execution framework | Pluggable execute() per btype, standardized input/output |
| P1-12 | Error propagation | Errors bubble to nearest Eh node or return 500 |

#### Sprint 7-8 (Weeks 11-12): Integration + Hardening

| ID | Deliverable | Binary Criteria |
|----|-------------|-----------------|
| P1-13 | End-to-end test: Ep → Vd → Dx → P → Rb → O | Complete payment-like flow executes successfully |
| P1-14 | Graph hot deployment | Deploy new graph version with zero downtime |
| P1-15 | Basic observability (structured logs + request tracing) | Every node execution logged with trace ID |

**GO/NO-GO Checkpoint:**
- [ ] 4 new nodes (Rb, Vd, Dx, Eh) fully functional with tests
- [ ] Graph executor runs a 6-node graph end-to-end
- [ ] Hot deployment works without dropping requests
- [ ] Latency < 200ms for 6-node graph (no external calls)

---

### 11.5 Phase 2: Security & Middleware (Weeks 13-18)

**Objective:** Add security nodes and operational middleware

#### Sprint 9-10 (Weeks 13-14): Auth + Rate Limiter

| ID | Deliverable | Binary Criteria |
|----|-------------|-----------------|
| P2-1 | Auth (Au) node — JWT validation | Validates JWT, extracts claims as columns, returns 401 on failure |
| P2-2 | Auth (Au) node — API Key + Basic | Multiple auth schemes configurable per node |
| P2-3 | Rate Limiter (Rl) node | Enforces rate limits, returns 429, supports IP/key/user keys |

#### Sprint 11-12 (Weeks 15-16): Logger + HTTP Call

| ID | Deliverable | Binary Criteria |
|----|-------------|-----------------|
| P2-4 | Logger (Lg) node | Logs configurable fields at configurable levels per node |
| P2-5 | HTTP Call (Hc) node | Makes outbound HTTP with column-based URL/body templates |
| P2-6 | HTTP Call timeout + retry | Configurable timeout, retry with exponential backoff |

#### Sprint 13 (Weeks 17-18): Conditional Branch + Cache

| ID | Deliverable | Binary Criteria |
|----|-------------|-----------------|
| P2-7 | Conditional Branch (Cb) node | Routes to branch A or B based on expression evaluation |
| P2-8 | Cache (Ca) node | Caches by key expression, TTL-based eviction, cache hit returns immediately |
| P2-9 | OpenTelemetry integration | Distributed traces exported, viewable in Jaeger |

**GO/NO-GO Checkpoint:**
- [ ] Auth node blocks unauthorized requests (JWT, API Key, Basic)
- [ ] Rate limiter correctly throttles above threshold
- [ ] HTTP Call node successfully calls external API and maps response to columns
- [ ] Conditional branch routes traffic correctly in both paths
- [ ] OpenTelemetry traces visible in Jaeger

---

### 11.6 Phase 3: Turkey-Specific Nodes (Weeks 19-26)

**Objective:** Build all Turkey-specific payment processing nodes

#### Sprint 14-15 (Weeks 19-20): IBAN + Identity

| ID | Deliverable | Binary Criteria |
|----|-------------|-----------------|
| P3-1 | IBAN Validator (Iv) node | Validates TR IBAN format (TR + 24 digits), check digit verification |
| P3-2 | KPS Identity (Ki) node | Calls KPS/Mernis SOAP service, validates TC Kimlik No |
| P3-3 | VKN (Tax ID) validation | Validates 10-digit corporate tax IDs |

#### Sprint 16-17 (Weeks 21-22): AML + FX

| ID | Deliverable | Binary Criteria |
|----|-------------|-----------------|
| P3-4 | MASAK Screen (Ms) node | Screens against MASAK watchlist, flags matches |
| P3-5 | STR/CTR reporting | Generates and submits STR/CTR reports to MASAK |
| P3-6 | FX Control (Fx) node | Validates against TCMB rates, enforces thresholds, calculates BSMV |

#### Sprint 18-19 (Weeks 23-24): FAST + EFT Connectors

| ID | Deliverable | Binary Criteria |
|----|-------------|-----------------|
| P3-7 | FAST Connector (Fc) node | Submits instant payment to FAST hub, handles response |
| P3-8 | FAST receipt handling | Receives incoming FAST payments, routes through graph |
| P3-9 | EFT Connector (Ec) node | Submits EFT batch entries, handles settlement responses |

#### Sprint 20 (Weeks 25-26): Tax + Reporting

| ID | Deliverable | Binary Criteria |
|----|-------------|-----------------|
| P3-10 | Tax Withholding (Tw) node | Calculates BSMV, KKDF, stamp duty per transaction type |
| P3-11 | BRSA reporting node | Generates monthly regulatory reports in required format |
| P3-12 | Transaction archival | 10-year retention with indexed search |

**GO/NO-GO Checkpoint:**
- [ ] FAST payment round-trip works end-to-end (submit + receive)
- [ ] EFT submission works with settlement batch handling
- [ ] MASAK screening blocks sanctioned entities
- [ ] STR/CTR reports generate correctly
- [ ] TCMB FX validation enforces limits
- [ ] Tax calculations match manual verification (5 test cases)
- [ ] TC Kimlik verification works via KPS

---

### 11.7 Phase 4: Integration Testing (Weeks 27-32)

**Objective:** End-to-end testing of all Turkey payment flows

#### Sprint 21-22 (Weeks 27-28): Payment Flow Testing

| ID | Deliverable | Binary Criteria |
|----|-------------|-----------------|
| P4-1 | FAST payment graph (full chain) | Ep→Au→Vd→Iv→Ms→Fx→Dx→Fc→Lg→Rb→O executes end-to-end |
| P4-2 | EFT payment graph (full chain) | Ep→Au→Vd→Iv→Ms→Dx→Ec→Lg→Rb→O executes end-to-end |
| P4-3 | International payment graph | Ep→Au→Vd→Ms→Fx→Dx→Hc(SWIFT)→Lg→Rb→O executes end-to-end |
| P4-4 | Conditional routing graph | Amount-based routing: < 500K → FAST, >= 500K → EFT |

#### Sprint 23-24 (Weeks 29-30): Non-Functional Testing

| ID | Deliverable | Binary Criteria |
|----|-------------|-----------------|
| P4-5 | Performance testing | p95 < 200ms (no external), p95 < 3s (with FAST) at 5000 TPS |
| P4-6 | Chaos testing | Kill any single pod → no failed transactions, no data loss |
| P4-7 | Security penetration testing | Third-party pen test, all Critical/High findings remediated |
| P4-8 | Failover testing | DB failover, FAST hub timeout → graceful degradation |

#### Sprint 25 (Weeks 31-32): UAT + Compliance

| ID | Deliverable | Binary Criteria |
|----|-------------|-----------------|
| P4-9 | UAT with Turkey operations team | All 10 TRK-* requirements demonstrated and signed off |
| P4-10 | Compliance audit | Graph-based audit trail reviewed and approved by compliance |
| P4-11 | BRSA submission package | Regulatory documentation package complete |
| P4-12 | Disaster recovery drill | Full DR failover and recovery within RTO (5 min) |

**GO/NO-GO Checkpoint:**
- [ ] All payment flows pass end-to-end testing
- [ ] Performance meets SLAs under load
- [ ] Pen test findings remediated
- [ ] UAT sign-off from Turkey operations
- [ ] Compliance audit passed
- [ ] DR drill successful

---

### 11.8 Phase 5: Production Launch (Weeks 33-36)

**Objective:** Controlled production rollout

#### Sprint 26 (Weeks 33-34): Soft Launch

| ID | Deliverable | Binary Criteria |
|----|-------------|-----------------|
| P5-1 | Production environment provisioned | K8s, Vault, monitoring, alerting all operational |
| P5-2 | Canary deployment (5% traffic) | 5% of FAST payments routed through new system, 0 errors for 48 hrs |
| P5-3 | Production monitoring dashboards | All SLA metrics visible in Grafana |
| P5-4 | Runbook and on-call rotation | Operations team trained, runbook reviewed |

#### Sprint 27 (Weeks 35-36): Full Launch

| ID | Deliverable | Binary Criteria |
|----|-------------|-----------------|
| P5-5 | Ramp to 25% → 50% → 100% traffic | Each ramp held for 24 hrs, rollback if error rate > 0.1% |
| P5-6 | Hypercare support (2 weeks) | 24/7 engineering support during initial production period |
| P5-7 | Post-launch review | Lessons learned documented, backlog prioritized |
| P5-8 | Handover to BAU team | Operations, monitoring, and on-call fully transitioned |

**LAUNCH Checkpoint:**
- [ ] 100% traffic running through new system for 7+ days
- [ ] Error rate < 0.01%
- [ ] All SLAs met
- [ ] On-call team operational
- [ ] Regulatory reporting verified in production
- [ ] Stakeholder sign-off

---

### 11.9 Binary Decision Matrix

| Phase | GO Criteria | NO-GO Action |
|-------|-------------|--------------|
| **Phase 0** | All infra + compliance sign-offs | Extend by 2 weeks, escalate blockers |
| **Phase 1** | Core nodes + executor pass all tests | Fix failures, no Phase 2 work until resolved |
| **Phase 2** | Security nodes block unauthorized access | Security review, remediate before Turkey nodes |
| **Phase 3** | Turkey payment flows work end-to-end | Regulatory consultation, fix gaps |
| **Phase 4** | UAT + compliance + pen test pass | Remediate findings, re-test (add 2-4 weeks) |
| **Phase 5** | Canary at 0 errors for 48 hrs | Rollback, root cause analysis, re-attempt |

---

## 12. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| TCMB FAST API changes during development | Medium | High | Build FAST connector behind abstraction layer; version API contracts |
| MASAK watchlist format changes | Medium | Medium | Pluggable screening adapter, not hardcoded format |
| Performance SLA missed at scale | Medium | High | Load test early (Phase 1), not just Phase 4 |
| Regulatory requirements change mid-project | High | High | Bi-weekly compliance sync, modular node design allows rapid changes |
| Key person dependency on BiTool platform | Medium | High | Cross-training, detailed IGL documentation, pair programming |
| Integration environment unavailability | High | Medium | Mock/stub external systems for dev/test; real integration only in UAT |
| Data residency requirements (KVKK) | Low | Critical | All data stored in Turkey data center, no cross-border replication |
| Graph complexity limits (50+ nodes) | Low | Medium | Profile executor performance, optimize hot paths, add graph splitting |

---

## 13. Success Criteria

### 13.1 Business Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Time to build new payment flow | < 2 weeks (vs. 8-12 weeks today) | From requirements to UAT |
| Payment processing success rate | > 99.95% | Monthly average |
| FAST settlement time | < 3 seconds (95th percentile) | Real-time monitoring |
| Regulatory compliance | 0 findings in annual audit | External audit report |
| System availability | 99.99% | Annual uptime calculation |
| Developer productivity | 3x improvement in service creation | Measured by flow creation time |

### 13.2 Technical Success Metrics

| Metric | Target |
|--------|--------|
| Graph executor latency (p95) | < 200ms (internal only) |
| Deployment frequency | Multiple times per day (per graph) |
| Mean time to recovery (MTTR) | < 5 minutes |
| Test coverage | > 80% for all node implementations |
| Zero-downtime deployments | 100% of deployments |

---

## 14. Appendix

### 14.1 Glossary

| Term | Definition |
|------|-----------|
| **IGL** | Intermediate Graph Language — BiTool's graph representation of a microservice |
| **FAST** | Fonlar Arasi Siparis Transfer — Turkey's instant payment system |
| **EFT** | Electronic Fund Transfer — Turkey's domestic wire system |
| **MASAK** | Mali Suclari Arastirma Kurulu — Financial Crimes Investigation Board |
| **TCMB** | Turkiye Cumhuriyet Merkez Bankasi — Central Bank of Turkey |
| **BRSA/BDDK** | Banking Regulation and Supervision Agency |
| **BSMV** | Banking and Insurance Transaction Tax |
| **KKDF** | Resource Utilization Support Fund |
| **KVKK** | Kisisel Verilerin Korunmasi Kanunu — Personal Data Protection Law |
| **KPS/Mernis** | Central Population Administration System — identity verification |
| **TC Kimlik No** | 11-digit Turkish national identification number |
| **VKN** | Vergi Kimlik Numarasi — 10-digit corporate tax ID |
| **STR** | Suspicious Transaction Report |
| **CTR** | Cash Transaction Report |
| **PEP** | Politically Exposed Person |

### 14.2 Reference Documents

| Document | Location |
|----------|----------|
| BiTool Design Document | `docs/DESIGN.md` |
| Microservices Builder Plan | `docs/Microservices builder.md` |
| Microservice Nodes Roadmap | `docs/Microservice Nodes Roadmap.md` |
| TCMB FAST Technical Specification | (External — TCMB) |
| MASAK Reporting Guidelines | (External — MASAK) |
| BRSA Regulatory Requirements | (External — BDDK) |

### 14.3 Approval

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Project Sponsor | | | |
| Business Owner (Turkey Ops) | | | |
| Chief Architect | | | |
| Compliance Officer | | | |
| CISO (Security) | | | |
| Head of Engineering | | | |

---

**Document Version:** 1.0
**Date:** February 23, 2026
**Classification:** Confidential — JPMorgan Chase Internal
