# database-framework

Server-side database engine built on FoundationDB, implementing index maintenance for [database-kit](https://github.com/1amageek/database-kit) models.

## Overview

database-framework is the **server execution layer** that provides:

- **FDBContainer / FDBContext**: Change-tracking data access with ACID transactions
- **12 index maintainers**: Scalar, Vector, FullText, Spatial, Graph, Aggregation, and more
- **Schema Registry**: pg_catalog-style metadata persistence
- **Migration System**: Online index building and schema evolution
- **DatabaseServer**: WebSocket server for [database-client](https://github.com/1amageek/database-client) connections
- **DatabaseCLI**: Interactive REPL for data inspection

```
┌──────────────────────────────────────────────────────────┐
│                      database-kit                        │
│  @Persistable models, IndexKind protocols, QueryIR       │
└──────────┬───────────────────────────────┬───────────────┘
           │                               │
           ▼                               ▼
┌─────────────────────┐       ┌─────────────────────────┐
│  database-framework │       │    database-client       │
│  FDBContainer       │◄─────│    DatabaseContext        │
│  Index Maintainers  │  WS  │    KeyPath queries       │
│  FoundationDB       │       │    iOS / macOS           │
└─────────────────────┘       └─────────────────────────┘
```

## Installation

### Prerequisites

- **FoundationDB 7.3+** installed and running
- **Swift 6.2+**
- **macOS 15+** or Linux

```bash
# macOS
brew install foundationdb
brew services start foundationdb
```

### Swift Package Manager

```swift
dependencies: [
    .package(url: "https://github.com/1amageek/database-framework.git", from: "26.0411.1")
]
```

## Quick Start

```swift
import Database
import Core

// 1. Define models (from database-kit)
@Persistable
struct User {
    #Directory<User>("app", "users")
    #Index(ScalarIndexKind<User>(fields: [\.email]), unique: true)

    var email: String
    var name: String
}

// 2. Create container
let schema = Schema([User.self])
let container = try await FDBContainer(for: schema)

// 3. Insert data
let context = container.newContext()
context.insert(User(email: "alice@example.com", name: "Alice"))
context.insert(User(email: "bob@example.com", name: "Bob"))
try await context.save()

// 4. Query
let users = try await context.fetch(User.self)
    .where(\.name == "Alice")
    .execute()
```

## Modules

### Core

| Module | Description |
|--------|-------------|
| `DatabaseEngine` | FDBContainer, FDBContext, IndexMaintainer protocol, Schema Registry |
| `Database` | All-in-one re-export of all index modules |
| `QueryAST` | SQL/SPARQL parser, AST builder, serializer |
| `DatabaseServer` | WebSocket server for database-client connections |
| `DatabaseCLICore` | Interactive REPL library for data inspection |

### Index Maintainers

| Module | IndexKind | Features |
|--------|-----------|----------|
| `ScalarIndex` | `ScalarIndexKind` | Range queries, sorting, unique constraints |
| `VectorIndex` | `VectorIndexKind` | HNSW and flat similarity search |
| `FullTextIndex` | `FullTextIndexKind` | Tokenization, inverted index |
| `SpatialIndex` | `SpatialIndexKind` | S2 / Geohash / Morton encoding |
| `RankIndex` | `RankIndexKind` | Skip-list based rankings |
| `GraphIndex` | `GraphIndexKind` | Graph traversal, SPARQL, OWL DL reasoning |
| `AggregationIndex` | `Count/Sum/Min/Max/AverageIndexKind` | Atomic aggregations |
| `VersionIndex` | `VersionIndexKind` | FDB versionstamp history |
| `BitmapIndex` | `BitmapIndexKind` | Roaring bitmap for categorical data |
| `LeaderboardIndex` | `TimeWindowLeaderboardIndexKind` | Time-windowed leaderboards |
| `PermutedIndex` | `PermutedIndexKind` | Alternative field orderings |
| `RelationshipIndex` | `RelationshipIndexKind` | Cross-type relationship queries |

## Architecture

### Container / Context Pattern

| Component | Role | Transactions |
|-----------|------|-------------|
| `FDBContainer` | Resource manager (DB connection, Schema, Directory) | Does NOT create |
| `FDBContext` | User-facing API, change tracking, batch operations | Creates |
| `FDBDataStore` | Low-level operations within transactions | Receives |

### Lifecycle Management

```
FDBContainer (owns FDBDatabase)
  └── newContext() → FDBContext (references container, does NOT own database)

Startup:  FDBClient.initialize() → FDBContainer.init → container.newContext()
Teardown: context = nil → container = nil → FDBClient.shutdown() [optional]
```

| Component | Owns Database? | Cleanup |
|-----------|---------------|---------|
| `FDBContainer` | Yes (strong ref) | ARC — releases database on dealloc |
| `FDBContext` | No (references container) | ARC — discards pending changes |

**Process exit**: Safe without explicit cleanup. OS reclaims all resources.

**Explicit shutdown** (servers, tests):

```swift
// 1. Release contexts (discard pending changes)
context = nil

// 2. Release container (triggers fdb_database_destroy via ARC)
container = nil

// 3. Stop network (optional — OS handles this at process exit)
FDBClient.shutdown()
```

**Server pattern**: One container per process, one context per request.
**CLI pattern**: One container, one context, `Foundation.exit(0)` is safe.
**Test pattern**: One container per suite, `FDBClient.shutdown()` in teardown.

### Change Tracking

```swift
let context = container.newContext()

// Stage changes
context.insert(user1)
context.insert(user2)
context.delete(user3)

// Commit all in one ACID transaction
try await context.save()
```

### Protocol Bridge

database-kit defines **what** to index. database-framework defines **how** to maintain it.

```swift
// database-kit: metadata only
public struct ScalarIndexKind: IndexKind {
    public static let identifier = "scalar"
}

// database-framework: FDB-dependent implementation
extension ScalarIndexKind: IndexKindMaintainable {
    public func makeIndexMaintainer<Item: Persistable>(...) -> any IndexMaintainer<Item> {
        return ScalarIndexMaintainer(...)
    }
}
```

## Features

### Security

```swift
// Declarative access control
extension Post: SecurityPolicy {
    static func allowGet(resource: Post, auth: (any AuthContext)?) -> Bool {
        resource.isPublic || resource.authorID == auth?.userID
    }
}

// Encryption at rest (AES-256-GCM)
let config = TransformConfiguration(
    encryptionEnabled: true,
    keyProvider: RotatingKeyProvider(...)
)
let container = try FDBContainer(for: schema, transformConfiguration: config)
```

### Hybrid SQL/SPARQL

```swift
let sql = """
SELECT * FROM User
WHERE id IN (SPARQL(Connection, 'SELECT ?from WHERE { ?from "follows" "bob" }'))
  AND age > 18
"""
let users = try await context.executeSQL(sql, as: User.self)
```

### Migration

```swift
let migration = Migration(
    fromVersion: Schema.Version(1, 0, 0),
    toVersion: Schema.Version(2, 0, 0),
    description: "Add email index"
) { ctx in
    try await ctx.addIndex(emailIndexDescriptor)
}
```

### Ontology Integration

Link Persistable types to OWL ontology classes with `@OWLClass` and `@OWLDataProperty` / `@OWLObjectProperty` macros (defined in [database-kit](https://github.com/1amageek/database-kit) Graph module). `@Persistable` handles pure persistence; `@OWLClass` handles OWL class mapping independently.

```swift
import Database
import Core
import Graph

// 1. Define ontology-aware types
@Persistable
@OWLClass("http://example.org/onto#Employee")
struct Employee {
    #Directory<Employee>("app", "employees")

    @OWLDataProperty("http://example.org/onto#name")
    var name: String

    @OWLDataProperty("http://example.org/onto#worksFor", to: \Department.id)
    var departmentID: String?
}

@Persistable
@OWLClass("http://example.org/onto#Department")
struct Department {
    #Directory<Department>("app", "departments")
    var name: String
}

// 2. Define object property (edge type) with @OWLObjectProperty
@Persistable
@OWLObjectProperty("http://example.org/onto#worksFor", from: "employeeID", to: "departmentID")
struct WorksFor {
    #Directory<WorksFor>("app", "assignments")
    var employeeID: String = ""
    var departmentID: String = ""

    @OWLDataProperty("http://example.org/onto#since")
    var startDate: Date = Date()
}

// 3. Define RDF triple store
@Persistable
struct RDFTriple {
    #Directory<RDFTriple>("app", "knowledge")
    var subject: String = ""
    var predicate: String = ""
    var object: String = ""

    #Index(GraphIndexKind<RDFTriple>(
        from: \.subject, edge: \.predicate, to: \.object,
        strategy: .hexastore
    ))
}

// 4. Create container
let schema = Schema([Employee.self, Department.self, WorksFor.self, RDFTriple.self])
let container = try await FDBContainer(for: schema)

// 5. Load ontology independently via OntologyStore
let ontology = OWLOntology(iri: "http://example.org/onto") {
    Class("ex:Employee", subClassOf: "ex:Person")
    Class("ex:Department")
    ObjectProperty("ex:worksFor", domain: "ex:Employee", range: "ex:Department")
}

let context = container.newContext()
try await context.ontology.load(ontology)
```

**What happens automatically**:
- `@OWLClass` generates `OWLClassEntity` conformance with `ontologyClassIRI`
- `@OWLClass` scans `@OWLDataProperty` annotations to build `ontologyPropertyDescriptors`
- `@OWLObjectProperty` generates `OWLObjectPropertyEntity` conformance with `objectPropertyIRI`, `fromFieldName`, `toFieldName`, and auto-generates a `GraphIndexKind` for the edge type
- `@OWLDataProperty(to:)` generates a reverse index on `departmentID` for efficient lookups
- `context.ontology.load()` persists the ontology to `/_ontology/` via OntologyStore
- GraphIndex uses the ontology for OWL 2 RL materialization on triple writes

See [GraphIndex README](Sources/GraphIndex/README.md) for SPARQL, OWL reasoning, and graph algorithms.

### Polymorphable

```swift
@Polymorphable
protocol Document: Polymorphable {
    var id: String { get }
    var title: String { get }
}

// Query all conforming types from a shared directory
let docs = try await context.fetchPolymorphic(Article.self)
```

## Data Layout

```
[directory]/R/[type]/[id]              → Protobuf-encoded item
[directory]/I/[indexName]/[values]/[id] → Index entry
[directory]/state/[indexName]           → Index state (readable/writeOnly/disabled)
/_catalog/[typeName]                    → JSON-encoded TypeCatalog
```

## Build and Test

```bash
swift build                                    # Debug build
swift build -c release                         # Release build
swift test                                     # All tests (requires FoundationDB)
swift test --filter DatabaseEngineTests        # Engine tests only
```

## Benchmarks

Performance benchmarks live in the `PerformanceBenchmarks` test target and require a healthy FoundationDB cluster.

```bash
swift test --filter 'PerformanceBenchmarks.CoveringIndexBenchmark'
swift test --filter 'PerformanceBenchmarks.FDBFrameworkCRUDBenchmarkTests'
swift test --filter 'PerformanceBenchmarks.IndexedQueryAndWriteBenchmarkTests'
swift test --filter 'PerformanceBenchmarks.MinMaxBatchBenchmark'
swift test --filter 'PerformanceBenchmarks.RangeTreeBenchmark'
swift test --filter 'PerformanceBenchmarks.SerializationBenchmark'
```

The `DatabaseEngine` benchmarks use `.serialized` suites, `FDBTestSetup.shared.withSerializedAccess`, unique benchmark IDs, and explicit directory cleanup so each run stays isolated and does not deadlock on shared FoundationDB state.

### Latest Snapshot (2026-04-11)

**Environment**: macOS 26.3, Apple M4 Max, local Docker FoundationDB cluster

| Module | Scenario | Result |
|--------|----------|--------|
| `ScalarIndex` | Fetch all users (300 records) | p95 `3.60ms`, `293 ops/s` |
| `ScalarIndex` | Scan scalability (200 records) | p95 `6.45ms`, `184 ops/s` |
| `AggregationIndex` | MIN/MAX grouped query | p95 `2.64ms`, `377 ops/s` |
| `AggregationIndex` | Combined MIN+MAX vs separate queries | p95 `2.33ms`, throughput `+29.05%` |
| `RankIndex` | Top-100 from 1,000 players | p95 `10.61ms`, `97 ops/s` |
| `RankIndex` | Top-K scaling | p95 stayed near `10.72-11.02ms` |
| `BitmapIndex` | JSON serialization | p95 `0.07ms`, `14,910 ops/s` |
| `BitmapIndex` | JSON round-trip (10,000 values) | p95 `1148.92ms` |

Detailed benchmark reports:
- `ScalarIndex`: `Sources/ScalarIndex/README.md`
- `AggregationIndex`: `Sources/AggregationIndex/README.md`
- `RankIndex`: `Sources/RankIndex/README.md`
- `BitmapIndex`: `Sources/BitmapIndex/README.md`

## Platform Support

| Platform | Support |
|----------|---------|
| macOS | 15.0+ |
| Linux | Swift 6.2+ |

> iOS/watchOS/tvOS/visionOS are not supported (requires FoundationDB).

## Related Packages

| Package | Role | Platform |
|---------|------|----------|
| **[database-kit](https://github.com/1amageek/database-kit)** | Model definitions, IndexKind protocols, QueryIR | iOS, macOS, Linux |
| **[database-client](https://github.com/1amageek/database-client)** | Client SDK with KeyPath queries and WebSocket transport | iOS, macOS |

## License

MIT License
