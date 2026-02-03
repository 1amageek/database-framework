# Changelog

All notable changes to database-framework will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-02-03

### Added

#### Core Engine
- **FDBContainer**: Application resource manager (database connection, schema, directory resolution)
- **FDBContext**: Transaction manager with change tracking and batch operations
- **FDBDataStore**: Low-level data operations within transactions
- **IndexMaintenanceService**: Centralized index maintenance with uniqueness checking
- **SchemaRegistry**: PostgreSQL pg_catalog equivalent for schema metadata persistence
- **TypeCatalog**: Codable schema metadata (fields, indexes, directory structure)
- **DynamicProtobufDecoder/Encoder**: Runtime codec for TypeCatalog-based data access

#### Index Types (13 modules)
- **ScalarIndex**: Single/composite field indexing with uniqueness constraints
- **VectorIndex**: High-dimensional vector similarity search (HNSW, Flat)
- **FullTextIndex**: Text search with tokenization and BM25 ranking
- **SpatialIndex**: Geographic queries (R-tree, geohash)
- **RankIndex**: Leaderboard and ranking queries
- **PermutedIndex**: Multi-field permutation indexing
- **GraphIndex**: RDF triple store with SPARQL support
- **AggregationIndex**: Pre-computed aggregates (COUNT, SUM, MIN, MAX)
- **VersionIndex**: Temporal versioning with point-in-time queries
- **BitmapIndex**: Low-cardinality categorical queries
- **LeaderboardIndex**: Real-time ranking with score updates
- **RelationshipIndex**: One-to-many/many-to-many relationship queries

#### Query Engine
- **QueryAST**: Abstract Syntax Tree for SQL and SPARQL
  - SQLParser: ISO SQL query parsing
  - SPARQLParser: W3C SPARQL 1.1 query parsing
  - SQL/PGQ support: Graph pattern matching (ISO/IEC 9075-16:2023)
  - Query builder: Programmatic query construction
  - SQL injection prevention: Identifier and string escaping
- **QueryIR**: Intermediate representation for query optimization
- **Query Planner**: Cost-based optimization
  - DNF conversion (Disjunctive Normal Form)
  - Predicate pushdown
  - Index selection
  - Join optimization

#### DatabaseCLI
- **DatabaseREPL**: Interactive database shell (PostgreSQL psql equivalent)
- **Schema Commands**: `schema list`, `schema show <Type>`
- **Data Commands**: `insert`, `get`, `update`, `delete`
- **Query Commands**: `find` with filter/sort/limit
- **Graph Commands**: `graph`, `sparql`
- **Partition Support**: `--partition` for multi-tenant types
- **Destructive Commands**: `clear` with safety gates
- **Raw FDB Access**: `raw get/set/delete/range`
- **LocalCluster**: Local FoundationDB cluster management

#### Infrastructure
- **ItemEnvelope**: Magic-number-based data format (prevents raw reads)
- **TransactionRunner**: Automatic retry with exponential backoff
- **ReadVersionCache**: Optimistic concurrency control
- **DirectoryLayer**: Hierarchical namespace management
- **FieldReader**: Zero-copy field access with dynamic member lookup
- **TypeConversion**: Unified type conversion utilities
- **TupleEncoder/Decoder**: FDB Tuple Layer integration

#### Value Access Architecture (3-layer)
- **Layer 1**: Zero-copy evaluation (KeyPath-based, no existential overhead)
- **Layer 2**: FieldReader fallback (type-erased after DNF conversion)
- **Layer 3**: DataAccess (storage layer with error propagation)

#### Migration System
- **Schema Versioning**: Major/minor/patch version tracking
- **OnlineIndexer**: Non-blocking index rebuilds
- **AdminContext**: Schema migration utilities

### Technical Highlights

#### Protocol-Driven Extensibility
- `IndexKind` protocol (database-kit): Defines WHAT to index
- `IndexMaintainer` protocol (database-framework): Defines HOW to maintain
- `IndexKindMaintainable` bridge: Connects metadata to runtime

#### Two-Package Architecture
- **database-kit**: Platform-independent model definitions (iOS-compatible)
- **database-framework**: FoundationDB-dependent execution (server-only)
- Shared `@Persistable` types across client and server

#### Concurrency Model
- `final class + Mutex<T>` pattern (high-throughput)
- No actor isolation (avoid serialization bottlenecks)
- `nonisolated(unsafe)` for DatabaseProtocol

### Standards Compliance
- ISO/IEC 9075:2023 (SQL)
- ISO/IEC 9075-16:2023 (SQL/PGQ - Property Graph Queries)
- W3C SPARQL 1.1 Query Language
- W3C RDF 1.1 (Named Graphs)
- IEEE 754 (Double encoding with FDB Tuple Layer)

### Requirements
- Swift 6.2+
- macOS 15+
- FoundationDB 7.3+
- fdb-swift-bindings (feature/directory-layer branch)
- database-kit (main branch)

### Known Limitations
- CLI writes do NOT update indexes (debug/development only)
- WatchManager is stub implementation (requires fdb-swift-bindings extension)
- Index rebuild uses simplified implementation (use OnlineIndexer for production)

### References
- [database-kit](https://github.com/1amageek/database-kit) - Model definitions and index types
- [fdb-swift-bindings](https://github.com/1amageek/fdb-swift-bindings) - FoundationDB Swift bindings
- [swift-hnsw](https://github.com/1amageek/swift-hnsw) - HNSW vector index

[0.1.0]: https://github.com/1amageek/database-framework/releases/tag/v0.1.0
