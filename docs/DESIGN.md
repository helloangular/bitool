# BiTool - Design Document

## Project Overview

BiTool is a visual data integration and ETL (Extract, Transform, Load) platform that enables users to build complex data pipelines through an intuitive graph-based interface. The application allows users to connect to multiple data sources (databases, REST APIs), transform data through various operations (joins, filters, aggregations), and output results to target destinations.

**Version:** 1.0
**Framework:** Luminus 4.50 (Clojure web framework)
**Primary Language:** Clojure

---

## 1. Functional Design

### 1.1 Core Capabilities

#### 1.1.1 Visual Data Pipeline Builder
- **Graph-Based Interface**: Users create data transformation pipelines as directed graphs where:
  - **Nodes** represent data operations (tables, joins, filters, aggregations, etc.)
  - **Edges** represent data flow between operations
- **Interactive Canvas**: Drag-and-drop interface for adding and connecting nodes
- **Auto-Layout**: Automatic coordinate calculation for visual representation
- **Real-time Updates**: Changes to the pipeline are immediately reflected in the UI

#### 1.1.2 Data Source Connectivity

**Database Connectors:**
- Primary support for PostgreSQL
- Extensible to MySQL and other JDBC-compatible databases
- Connection management with pooling via HikariCP

**API Connectors:**
- REST API integration via OpenAPI/Swagger specifications
- Automatic endpoint discovery from OpenAPI specs
- Support for various authentication methods:
  - Basic Auth
  - Bearer Token
  - API Key
  - OAuth 2.0 (with token refresh)
- Built-in pagination handling (cursor-based, offset-based, page-based)

#### 1.1.3 Data Transformation Operations

**Supported Node Types:**

1. **Table (T)**: Source database tables
   - Column selection
   - Data preview

2. **Join (J)**: Combine data from multiple sources
   - Join types: INNER, LEFT, RIGHT, FULL OUTER
   - Multi-column join conditions
   - Visual join configuration

3. **Union (U)**: Combine results from similar sources
   - UNION and UNION ALL support

4. **Projection (P)**: Column selection and transformation
   - Select specific columns
   - Column renaming
   - Calculated columns

5. **Filter (Fi)**: Data filtering
   - WHERE clause conditions
   - HAVING clause for aggregations
   - Support for complex conditions

6. **Aggregation (A)**: Group and aggregate data
   - GROUP BY operations
   - Aggregate functions (SUM, AVG, COUNT, MIN, MAX)
   - HAVING clauses

7. **Sorter (S)**: Sort data
   - Multi-column sorting
   - ASC/DESC ordering

8. **Function (Fu)**: Apply functions to columns
   - String functions: CONCAT, SUBSTRING, REPLACE, CHARINDEX
   - Null handling: COALESCE, NULLIF
   - Custom column functions

9. **Mapping (Mp)**: Field mapping for transformations
   - Source to target field mapping

10. **Target (Tg)**: Output destination
    - Target table configuration
    - Insert/Update operations

11. **API Connection (Ap)**: REST API as data source
    - Endpoint configuration
    - Parameter mapping

12. **Output (O)**: Final output node
    - Required terminal node for all pipelines

### 1.2 User Workflows

#### Workflow 1: Create New Data Pipeline
1. User creates a new graph with a name
2. System initializes graph with default Output node
3. User adds source nodes (tables or API connections)
4. User adds transformation nodes (joins, filters, etc.)
5. User connects nodes to define data flow
6. User configures each node with specific parameters
7. System validates the pipeline
8. User saves the configuration
9. User executes the pipeline to process data

#### Workflow 2: Connect to REST API
1. User provides OpenAPI specification URL
2. System fetches and parses the OpenAPI spec
3. System displays available endpoints
4. User selects endpoint and HTTP method
5. System generates schema from endpoint definition
6. User configures authentication
7. User sets pagination parameters (if applicable)
8. System creates API connection node in the graph

#### Workflow 3: Configure Join Operation
1. User adds join node to graph
2. User connects source tables to join node
3. System displays available columns from both tables
4. User selects join type (INNER, LEFT, etc.)
5. User defines join conditions (column pairs)
6. User optionally filters joined data
7. System saves join configuration

---

## 2. Technical Design

### 2.1 Architecture Overview

BiTool follows a **multi-tier web application architecture**:

```
┌─────────────────────────────────────────────┐
│         Presentation Layer (Web UI)         │
│     HTML Templates + JavaScript/React       │
└─────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────┐
│          Application Layer (Clojure)        │
│                                             │
│  ┌──────────────┐  ┌──────────────┐       │
│  │   Routes     │  │  Middleware  │       │
│  │  (Reitit)    │  │   (Ring)     │       │
│  └──────────────┘  └──────────────┘       │
│         ↓                  ↓                │
│  ┌──────────────┐  ┌──────────────┐       │
│  │  Handlers    │  │  Graph Ops   │       │
│  │ (home.clj)   │  │ (graph2.clj) │       │
│  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────┐
│         Data Access Layer (Database)        │
│                                             │
│  ┌──────────────┐  ┌──────────────┐       │
│  │   Metadata   │  │    User DB   │       │
│  │  PostgreSQL  │  │   Connectors │       │
│  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────┘
```

### 2.2 Technology Stack

#### 2.2.1 Backend Technologies
- **Language**: Clojure 1.12.0
- **Web Framework**: Luminus (based on Ring/Reitit)
- **Web Server**: Luminus-Undertow 0.1.18
- **Routing**: Reitit 0.5.18
- **Middleware**: Ring (core, defaults, JSON)
- **Template Engine**: Selmer 1.12.55 (logic-less templates)
- **HTTP Client**: clj-http 3.12.3
- **JSON Processing**: Cheshire 5.10.0
- **State Management**: Mount 0.1.16 (component lifecycle)

#### 2.2.2 Data Layer
- **ORM**: Toucan2 1.0.556 (lightweight ORM)
- **JDBC**: next.jdbc 1.3.955
- **SQL DSL**: HoneySQL 2.3.928
- **Connection Pooling**: HikariCP 6.2.1
- **Database**: PostgreSQL 42.2.10 (primary metadata store)

#### 2.2.3 Graph Processing
- **Graph Library**:
  - Loom 1.0.2 (graph data structures and algorithms)
  - Ubergraph 0.8.2 (enhanced graph capabilities)
- **Data Transformation**: Specter 1.1.4 (powerful data navigation/transformation)

#### 2.2.4 Security & Authentication
- **Crypto**: Buddy 1.12.0 (encryption, hashing, authentication)
- **Encoding**: commons-codec 1.15

#### 2.2.5 Development Tools
- **REPL**: nREPL 1.0.0
- **Logging**:
  - Timbre 6.5.0
  - Telemere 1.1.0
  - Logback 1.4.4
- **Configuration**: cprop 0.1.19
- **Hot Reload**: tools.namespace 1.5.0

### 2.3 Core Components

#### 2.3.1 Graph Data Structure

**Graph Schema:**
```clojure
{:a {:name "graph-name"    ;; Graph attributes
     :v 1                   ;; Version number
     :id 123}               ;; Graph ID
 :n {1 {:na {:name "Output" ;; Node 1 attributes
             :btype "O"      ;; Block type
             :tcols {}       ;; Table columns
             :x 250          ;; X coordinate
             :y 250}}        ;; Y coordinate
     2 {:na {...}           ;; Node 2 attributes
        :e {3 {}}}}}        ;; Edges: node 2 -> node 3
```

**Key Concepts:**
- **`:a`**: Graph-level attributes (name, version, ID)
- **`:n`**: Map of nodes (keyed by node ID)
- **`:na`**: Node attributes (name, type, columns, position)
- **`:e`**: Edges (map of target node IDs)
- **`:btype`**: Block type code (T, J, U, P, Fi, Fu, A, S, Tg, O, Mp, Ap)

**Graph Operations:**
- `createGraph`: Initialize new graph with Output node
- `add-node`: Add new node to graph
- `add-edge`: Create connection between nodes
- `delete-edge`: Remove connection
- `update-node`: Modify node attributes
- `getData`: Retrieve node data for UI display
- `save-*`: Persist node configuration (join, filter, aggregation, etc.)

#### 2.3.2 Database Layer

**Primary Database (PostgreSQL - bitool):**
- **Tables:**
  - `graph`: Stores graph definitions
    - `id`: Graph identifier
    - `version`: Version number (incremented on each save)
    - `name`: Graph name
    - `definition`: Serialized graph structure (compressed, base64-encoded)
  - `connection`: Database connection configurations
  - `api_connection`: REST API connection configurations

**Data Persistence:**
- Graphs are stored as EDN (Extensible Data Notation) strings
- Compression using GZIP reduces storage size
- Base64 encoding for safe text storage
- Version history maintained for all graph changes

**Connection Pooling:**
```clojure
{:dbtype "postgresql"
 :dbname "bitool"
 :host "localhost"
 :user "postgres"
 :password "postgres"}
```

#### 2.3.3 API Connector

**OpenAPI Integration:**
1. **Specification Loading**:
   - Fetch OpenAPI spec (YAML/JSON) from URL
   - Parse and validate specification
   - Extract endpoint definitions

2. **Schema Resolution**:
   - Resolve `$ref` references
   - Handle `allOf`, `oneOf`, `anyOf` combinators
   - Flatten nested object schemas
   - Generate column definitions from schemas

3. **Authentication**:
   - Support for multiple auth types
   - Token refresh mechanism for OAuth
   - Cookie store for session management

4. **Pagination**:
   - Cursor-based pagination
   - Offset-based pagination
   - Page number-based pagination
   - Automatic next-page detection

**HTTP Request Flow:**
```
User Request → Auth Resolution → Header Merge →
HTTP GET (pooled connection) → Response Parse →
Pagination Logic → Data Transform → Return to Graph
```

#### 2.3.4 Routing & HTTP Handlers

**Route Structure** (`home-routes`):
```clojure
[""
 ["/graph" {:post graph-page}]              ;; Load graph
 ["/newgraph" {:post new-graph}]            ;; Create new graph
 ["/addtable" {:post add-table}]            ;; Add table node
 ["/addSingle" {:post add-single}]          ;; Add single node
 ["/connectSingle" {:post connect-single}]  ;; Connect nodes
 ["/getItem" {:get get-item}]               ;; Get node config
 ["/saveJoin" {:post save-join}]            ;; Save join config
 ["/saveFilter" {:post save-filter}]        ;; Save filter config
 ["/saveAggregation" {:post save-aggregation}]
 ["/saveMapping" {:post save-mapping}]
 ["/saveTarget" {:post save-target}]
 ["/getEndpoints" {:get get-endpoints}]     ;; API discovery
 ["/getEndpointSchema" {:get get-endpoint-schema}]
 ;; ... more routes
]
```

**Session Management:**
- Graph ID (`gid`) stored in session
- Version number (`ver`) tracked per session
- User context maintained across requests

#### 2.3.5 Middleware Stack

**Applied Middleware** (from innermost to outermost):
1. **Format Middleware**: JSON/Transit request/response handling
2. **Session Middleware**: Session state management
3. **Wrap-Base**: Security, logging, error handling
4. **Content-Type**: Auto-detection of response types
5. **Webjars**: Serve frontend dependencies

### 2.4 Data Flow

#### 2.4.1 Graph Creation Flow
```
POST /newgraph
  ↓
Handler: new-graph
  ↓
g2/createGraph → Initialize graph with Output node
  ↓
db/insertGraph → Persist to PostgreSQL
  ↓
Return graph ID + version
  ↓
Store in session (:gid, :ver)
  ↓
Return Output node coordinates to UI
```

#### 2.4.2 Node Addition Flow
```
POST /addtable (or /addSingle)
  ↓
Handler: add-table
  ↓
Extract params (table, conn_id, gid)
  ↓
db/get-table → Fetch table metadata
  ↓
g2/processAddTable → Add node to graph
  ↓
db/insertGraph → Save new version
  ↓
g2/mapCoordinates → Calculate visual layout
  ↓
Return updated coordinate array
```

#### 2.4.3 API Discovery Flow
```
GET /getEndpoints?url=<openapi-url>
  ↓
Handler: get-endpoints
  ↓
sc/fetch-openapi! → Download OpenAPI spec
  ↓
sc/list-endpoints-from-url → Extract endpoints
  ↓
Return JSON array of {endpoint, method}
  ↓
UI displays endpoint list
```

#### 2.4.4 Data Transformation Flow
```
User configures pipeline graph
  ↓
Graph stored in database
  ↓
User executes pipeline (runTarget)
  ↓
g2/run-target → Traverse graph from target node
  ↓
Build SQL query or API calls from graph
  ↓
Execute against data sources
  ↓
Transform data through pipeline
  ↓
Write to target destination
```

### 2.5 Security Considerations

#### 2.5.1 Implemented Security
- **Encryption**: AES-128-CBC with HMAC-SHA256 for sensitive data
- **Session Management**: TTL-based sessions with secure cookies
- **SQL Injection Prevention**: Parameterized queries via HoneySQL
- **CSRF Protection**: Available (commented out in routes, can be enabled)

#### 2.5.2 Authentication & Authorization
- **API Authentication**: Support for multiple auth schemes
- **Token Management**: Automatic refresh for OAuth tokens
- **Credential Storage**: Encrypted storage of API keys and passwords

### 2.6 Performance Optimizations

#### 2.6.1 HTTP Connection Pooling
- Reusable connection manager (64 max threads, 16 per route)
- GZIP/Deflate compression enabled
- Keep-alive connections

#### 2.6.2 Database Optimization
- HikariCP for connection pooling
- Versioned graph storage (immutable history)
- Base64 + GZIP compression for graph serialization

#### 2.6.3 Async Processing
- Core.async for concurrent operations
- Pipeline parallelism for data transformations
- Non-blocking HTTP requests

### 2.7 Extensibility Points

#### 2.7.1 Adding New Node Types
1. Define new block type code in `btype-codes` map
2. Create node creation function (`add-<type>`)
3. Implement `getData` multimethod for node type
4. Add save handler (`save-<type>`)
5. Create UI template in resources/html

#### 2.7.2 Adding New Data Connectors
1. Implement connector in `bitool.connector.*` namespace
2. Define connection configuration schema
3. Add connection type to database
4. Implement data fetching logic
5. Register in routing layer

#### 2.7.3 Adding New Transformations
1. Define transformation in `bitool.graph2`
2. Add to `rectangles` map (allowed connections)
3. Implement transformation logic
4. Add UI configuration panel

### 2.8 Deployment Architecture

**Development Mode:**
```
lein run
  ↓
Mount starts components
  ↓
HTTP Server (port from config)
nREPL Server (port from config)
  ↓
Hot reload enabled
Development middleware active
```

**Production Mode:**
```
lein uberjar
  ↓
Generates bitool.jar
  ↓
java -jar bitool.jar -p 8080
  ↓
Production configuration
Optimized performance
No development middleware
```

### 2.9 Configuration Management

**Environment-Based Config:**
- `dev-config.edn`: Development settings
- `test-config.edn`: Test settings
- `env/prod/resources/config.edn`: Production settings

**Configuration via cprop:**
- Environment variables
- JVM system properties
- EDN config files
- Command-line arguments

---

## 3. Key Design Patterns

### 3.1 Immutable Data Structures
- All graph operations return new graph versions
- Functional transformations using Specter
- No in-place mutations

### 3.2 Multimethod Dispatch
- `getData` uses multimethods for polymorphic behavior
- Extensible for new node types without modifying core logic

### 3.3 Component Lifecycle (Mount)
- HTTP server lifecycle management
- Database connection lifecycle
- REPL server management

### 3.4 Data-Driven Configuration
- Routes defined as data structures
- Node types defined in configuration maps
- Transformation rules as data

### 3.5 Middleware Composition
- Layered middleware for cross-cutting concerns
- Ring middleware for HTTP handling
- Custom middleware for domain logic

---

## 4. Future Enhancements

### 4.1 Planned Features
1. **Real-time Data Preview**: Live data sampling at each node
2. **Error Handling**: Comprehensive error tracking and recovery
3. **Performance Monitoring**: Pipeline execution metrics
4. **Scheduling**: Automated pipeline execution
5. **Data Lineage**: Track data transformations and dependencies
6. **Collaboration**: Multi-user graph editing
7. **Export/Import**: Pipeline templates and sharing
8. **GraphQL Support**: Native GraphQL connector
9. **Data Quality**: Built-in validation and profiling
10. **Cloud Deployment**: Kubernetes/Docker orchestration

### 4.2 Technical Debt
1. Incomplete API target execution
2. Limited error handling in HTTP layer
3. Missing comprehensive test coverage
4. UI needs modernization (current HTML/JS to React)
5. Documentation needs expansion

---

## 5. File Structure

```
bitool/
├── project.clj                 # Leiningen project configuration
├── README.md                   # Project readme
├── DESIGN.md                   # This design document
├── dev-config.edn              # Development configuration
├── test-config.edn             # Test configuration
│
├── src/clj/bitool/
│   ├── core.clj                # Application entry point
│   ├── handler.clj             # Route handler setup
│   ├── config.clj              # Configuration loading
│   ├── middleware.clj          # Custom middleware
│   ├── layout.clj              # Template rendering
│   ├── graph.clj               # Legacy graph operations
│   ├── graph2.clj              # Main graph operations
│   ├── db.clj                  # Database operations
│   ├── utils.clj               # Utility functions
│   ├── macros.clj              # Macro definitions
│   ├── exceptions.clj          # Exception types
│   ├── exception_handler.clj   # Exception handling
│   ├── nrepl.clj               # REPL server
│   │
│   ├── routes/
│   │   └── home.clj            # Main HTTP routes and handlers
│   │
│   ├── connector/
│   │   ├── api.clj             # REST API connector
│   │   ├── auth.clj            # Authentication handlers
│   │   ├── pagination.clj      # Pagination logic
│   │   ├── paginate.clj        # Alternative pagination
│   │   ├── config.clj          # Connector configuration
│   │   ├── schema.clj          # Schema handling
│   │   ├── schema_tree.clj     # Schema tree operations
│   │   ├── schema_converter.clj# Schema conversion
│   │   └── api_discovery.clj   # API endpoint discovery
│   │
│   ├── api/
│   │   ├── schema.clj          # OpenAPI schema parsing
│   │   ├── gschema.clj         # GraphQL schema
│   │   ├── conn.clj            # API connections
│   │   ├── jsontf.clj          # JSON transformation
│   │   └── jsontx.clj          # JSON transformation (alt)
│   │
│   └── middleware/
│       └── formats.clj         # Request/response formats
│
├── env/
│   ├── dev/
│   │   ├── clj/bitool/
│   │   │   ├── env.clj         # Dev environment
│   │   │   └── dev_middleware.clj # Dev middleware
│   │   └── resources/
│   │       └── config.edn      # Dev config
│   │
│   ├── prod/
│   │   ├── clj/bitool/
│   │   │   └── env.clj         # Prod environment
│   │   └── resources/
│   │       └── config.edn      # Prod config
│   │
│   └── test/
│       └── resources/
│           └── config.edn      # Test config
│
├── resources/
│   ├── sql.edn                 # SQL queries
│   ├── html/                   # HTML templates
│   ├── public/                 # Static assets
│   └── docs/
│       └── docs.md             # Additional documentation
│
└── target/                     # Build artifacts
```

---

## 6. API Endpoints Reference

### Graph Operations
- `POST /newgraph` - Create new data pipeline graph
- `POST /graph` - Load existing graph
- `GET /getItem?id=<node-id>` - Get node configuration
- `POST /save/:fn` - Generic save handler (dynamic function dispatch)

### Node Operations
- `POST /addtable` - Add database table node
- `POST /addSingle` - Add single transformation node
- `POST /connectSingle` - Connect two nodes
- `POST /deleteTable` - Remove node from graph

### Transformation Configuration
- `POST /saveJoin` - Configure join operation
- `POST /saveFilter` - Configure filter conditions
- `POST /saveAggregation` - Configure aggregation
- `POST /saveFunction` - Configure function transformation
- `POST /saveSorter` - Configure sorting
- `POST /saveMapping` - Configure field mapping
- `POST /saveTarget` - Configure target destination
- `POST /saveUnion` - Configure union operation

### API Connector
- `GET /getEndpoints?url=<spec-url>` - Discover API endpoints
- `GET /getEndpointSchema?url=<endpoint>&spec=<spec-url>&method=<method>` - Get endpoint schema
- `POST /saveApi` - Save API connection configuration

### Utilities
- `GET /getConn?conn-id=<id>` - Get database connection tree
- `POST /runTarget` - Execute data pipeline
- `GET /about` - About page (session test)

---

## Glossary

- **BiTool**: Business Intelligence Tool - the name of this data integration platform
- **Graph**: A data structure representing a data pipeline as nodes and edges
- **Node**: A single operation in the data pipeline (table, join, filter, etc.)
- **Edge**: A connection between nodes representing data flow
- **Block Type (btype)**: The type/category of a node (T, J, U, P, Fi, Fu, etc.)
- **tcols**: Table columns - the columns available in a node's output
- **ETL**: Extract, Transform, Load - the process of moving and transforming data
- **OpenAPI**: A specification format for describing REST APIs
- **EDN**: Extensible Data Notation - Clojure's data format
- **Mount**: Component lifecycle management library
- **Reitit**: Routing library for Clojure web applications
- **Specter**: Data navigation and transformation library

---

**Document Version:** 1.0
**Last Updated:** February 15, 2026
**Author:** Generated by Claude Code
