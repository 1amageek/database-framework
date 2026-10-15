# GraphIndex

## Purpose and Scope

`GraphIndex` owns physical graph execution: property-graph indexes, RDF
datasets and stores, SPARQL compilation and execution, graph-table access,
property paths, graph algorithms, ontology projection, and SHACL execution.
It consumes DatabaseEngine's admitted read boundary and the canonical RDF
term-byte contract supplied by the DatabaseEngine Serialization/RDF component.

- Parent: [database-framework](../../DESIGN.md).
- Children: none in this task. Existing source subdirectories organize this
  module; no separate design authority is introduced for them here.

## Responsibilities and Boundaries

GraphIndex owns:

- property-graph index declaration resolution, maintenance, physical layouts,
  scanners, and graph edge reads;
- canonical RDF quad indexes, named-graph catalog state, RDF graph mutation,
  dataset source planning, and RDF identity storage;
- SPARQL algebra/compiler/optimizer, transaction-bound query execution,
  retained result projection, and runtime graph-table/SPARQL sources;
- property-path evaluation, graph algorithms, ontology projection/storage, and
  SHACL validation execution;
- physical graph key/range codecs and the mapping between logical graph
  results and retained/canonical output rows.

It does not own SQL parsing or SQL string entry points (`Database`), generic
session creation or authorization policy (`DatabaseEngine`), storage backend
behavior (`StorageKit`), schema/query declarations (`DatabaseKit`), or the
canonical RDF term-byte format
([Serialization/RDF](../DatabaseEngine/Serialization/RDF/DESIGN.md)).

The primary physical paths are [GraphQuery](GraphQuery.swift),
[SPARQLEntryPoint](SPARQL/SPARQLEntryPoint.swift),
[SPARQLQueryExecutor](SPARQL/SPARQLQueryExecutor.swift),
[RDFDatasetScanner](RDFDatasetScanner.swift), and
[RDFQuadIndexPhysicalCodec](RDFQuadIndexPhysicalCodec.swift).

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [database-framework](../../DESIGN.md) | parent | Package composition and dependency direction | Places graph execution beside the DatabaseEngine and specialized features. | Keep graph physical details out of the package index. |
| [DatabaseEngine](../DatabaseEngine/DESIGN.md) | depends on | Session-owned read transaction, authorization, snapshot, meter, and retained output | Supplies the canonical admission and lifecycle boundary. | GraphIndex must not open a nested transaction on a session path. |
| [Base Composition](../DatabaseEngine/Base/DESIGN.md) | depends on | Prebound columns, positional member pages and provenance | Supplies MultiBase global merging for SPARQL. | Bind primitive RDF metadata before rows and retain it on empty pages; SQL typing does not redefine RDF identity. |
| [Serialization/RDF](../DatabaseEngine/Serialization/RDF/DESIGN.md) | depends on | Role-aware canonical RDF term bytes, validation, limits, and scoped views | Supplies the term representation used in graph keys and identity storage. | GraphIndex owns key layout and graph meaning, not term encoding. |
| [Database](../Database/DESIGN.md) | used by | SQL-facing SPARQL entry adaptation into transaction-bound execution | Routes SQL-embedded calls to this module's graph-facing executor. | SQL expression rewriting remains a Database responsibility. |

### S03 shared semantic model adaptation

Consume [Query's S03 distinctions](../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s03-semantic-carriage-and-occurrence-addresses)
without acquiring SQL inference. SQL unknown strings and queryValue literals
with an invalid/unresolved explicit declaration fail existing typed SPARQL
literal/compile gates; schema-qualified calls fail the SQL-qualified-function
gate. Existing valid typed primitives retain RDF meaning. SPARQL rewriters
preserve function schema rather than erasing it. S03-C04/C12 and the existing
SPARQL/graph suites verify these direct shared-model adaptations; no graph
algorithm, transaction or authority change is introduced.

GRAPH_TABLE retains its established primitive string meaning for quoted
lexical strings in its limited scalar and property-filter language. Its own
literal adapter explicitly resolves SQL unknown strings to that text contract
before calling primitive conversion; the general literal converter continues
to reject unbound SQL strings. This preserves the existing product behavior
without introducing PostgreSQL type-inference claims for GRAPH_TABLE. Numeric
properties retain their existing typed representations. The public
GraphTableSQLExecutionContractTests covers parser-to-executor string filters
alongside the existing numeric-property behavior.

## Architecture

```text
DatabaseEngine session / explicit low-level engine
    -> GraphIndex resolver and admission boundary
       |-> PropertyGraphIndexResolver -> physical graph index scanner
       |-> RDFDatasetSourcePlanner -> IndexedRDFDatasetScanner
       |                              -> canonical RDF graph store/catalog
       `-> SPARQLQueryExecutor / RuntimeSPARQLSourceExecutor
             -> algebra, paths, algorithms, SHACL, and GraphTable execution
    -> retained/canonical graph rows or public graph result

Serialization/RDF -> canonical term bytes -> RDF quad/catalog codecs
StorageKit        -> transaction, range cursor, and bounded read behavior
```

Canonical DatabaseContext and builder paths use a session-owned
`DatabaseReadTransaction`. GraphIndex also exposes explicit low-level
`GraphQueryExecutor`/top-level SPARQL entry points over a caller-supplied
`StorageEngine`; those paths are separate physical execution adapters and do
not provide DatabaseEngine authorization guarantees.

## Contracts and Invariants

The typed direct GRAPH_TABLE adapter projects descriptor-owned raw columns by
their unqualified names. A property graph name resolves the index; it does not
create a SQL relation qualifier for those raw columns.

- Canonical session execution validates sealed authorization before readable
  index resolution or storage disclosure and uses the supplied session
  transaction, snapshot, and work meter for the complete operation.
- A transaction-bound `SPARQLQueryExecutor` never creates a storage
  transaction. The low-level top-level executor may use its explicit supplied
  engine only at its own entry boundary.
- Graph catalogs and index storage are rooted at a caller-supplied subspace
  derived from the Partition's Framework root. GraphIndex owns only the
  component name below that root, never a database-wide reserved prefix.
- Property-graph and RDF index physical keys preserve their declared roles,
  ordering, prefixes, and range boundaries. The six RDF quad orderings use
  canonical term bytes and typed validation; they do not substitute strings
  or descriptive text for stored identity.
- RDF dataset scans distinguish logical source composition from physical
  index traversal, preserve named-graph and blank-node semantics, deduplicate
  according to the dataset contract, and admit bounded retained output before
  exposing bytes or rows.
- Canonical graph-store mutations update quad indexes and the explicit named
  graph catalog atomically in one writable transaction. Missing, malformed,
  ambiguous, unauthorized, over-budget, cancelled, and cleanup failures stay
  typed and are not returned as synthetic success.
- SPARQL projection, ASK, CONSTRUCT, DESCRIBE, property paths, algorithms,
  ontology, SHACL, and GraphTable paths preserve their declared result and
  ordering semantics. Retained rows remain owned until the consuming output
  boundary completes.
- A CONSTRUCT template omits a quad whose terms are not all bound by the
  solution, and that omission is local to the omitted quad. A reifying term
  expands into an independent `rdf:reifies` quad whose own terms are bound, so
  that quad is still produced, under its own retained-footprint reservation,
  when the quad enclosing the reifier is omitted.
- `GraphQueryBuilder` and transaction-bound SPARQL builders reuse the caller's
  read transaction. The explicit low-level executor is not an authorization
  shortcut for a canonical session path.
- GRAPH_TABLE derives raw binding columns from validated graph index field
  descriptors and MATCH variables before reading an index. Endpoints, node IDs
  and labels are primitive strings; included properties retain their declared
  primitive storage type and optionality. An empty result carries the same
  columns. Graph-owned match bindings remain internal until the final admitted
  positional `QueryRow` boundary; SQL COLUMNS expressions remain execution-owner
  work rather than graph row type inference.
- A `Literal.queryValue(.primitive(value))` delegates through DatabaseKit's
  lossless `FieldValue.queryLiteral` conversion into the existing RDF literal
  validation, evaluation, and CONSTRUCT footprint paths. Unsupported primitive
  conversions retain their typed conversion error.
- SQL-specific numeric, JSON, JSONB, array, and interval query values have no
  implicit RDF representation. SPARQL compilation rejects them as unsupported
  expressions, and literal/CONSTRUCT conversion reports
  `SPARQLLiteralConversionError.queryLiteralConversion(.unsupportedSPARQLLiteral)`.
  No primitive surrogate or descriptive text is returned as a successful term.
- GraphIndex has no dependency on the `Database` entry module, so SQL
  adaptation cannot flow back into graph physical execution.

## Runtime Flows

```text
admitted session request
    -> resolve graph/RDF descriptor
    -> select physical scanner or executor
    -> bounded cursor/range reads and term validation
    -> admit/retain logical rows
    -> execute graph/SPARQL/path/SHACL operation
    -> promote canonical rows or explicit public result
    -> finish cursors and release all owners
```

Graph mutation follows the same physical codec and catalog rules inside one
writable transaction. A scan or executor failure waits for cursor cleanup and
releases partial retained state before propagating the error.

## State, Ownership, and Lifecycle

The session owns authorization, transaction, snapshot, and meter identity.
GraphIndex owns operation-local scanners, physical index descriptors,
retained graph matches, and canonical graph-store state. Storage cursors and
backend buffers remain owned by StorageKit contracts; borrowed term bytes are
used only within their validated scope. Cache state is immutable or protected
by its existing synchronization owner and is never registered globally by a
request.

## Failure, Concurrency, and Constraints

Graph reads are serialized by the supplied StorageKit transaction where the
backend requires it; asynchronous I/O never occurs under a mutex. Range and
point reads are bounded by the session work contract. Cursor completion,
cancellation, transaction failure, malformed RDF bytes, catalog inconsistency,
and later-row failure all preserve typed errors and release prior claims.
The low-level explicit engine adapters remain opt-in and must not be confused
with authenticated DatabaseEngine session execution.

## Verification and Change Impact

| Contract | Evidence |
|---|---|
| Builder read and graph property behavior | [GraphQueryBuilderTests](../../Tests/GraphIndexTests/GraphQueryBuilderTests.swift), [GraphQueryPropertyFilterTests](../../Tests/GraphIndexTests/GraphQueryPropertyFilterTests.swift) |
| Authorization order and read budget | [GraphDirectReadAuthorizationTests](../../Tests/GraphIndexTests/GraphDirectReadAuthorizationTests.swift), [GraphPhysicalReadBudgetTests](../../Tests/GraphIndexTests/GraphPhysicalReadBudgetTests.swift) |
| RDF physical keys, ranges, and store/catalog mutation | [RDFQuadIndexPhysicalCodecTests](../../Tests/GraphIndexTests/RDFQuadIndexPhysicalCodecTests.swift), [CanonicalRDFGraphStoreTests](../../Tests/GraphIndexTests/CanonicalRDFGraphStoreTests.swift), [RDFQuadIndexMaintainerTests](../../Tests/GraphIndexTests/RDFQuadIndexMaintainerTests.swift) |
| Dataset source, reservation, malformed, and identity behavior | [RDFDatasetReadModeTests](../../Tests/GraphIndexTests/RDFDatasetReadModeTests.swift), [RDFDatasetScanReservationTests](../../Tests/GraphIndexTests/RDFDatasetScanReservationTests.swift), [MalformedRDFGraphIndexTests](../../Tests/GraphIndexTests/MalformedRDFGraphIndexTests.swift), [RDFGraphIdentityStorageSharingTests](../../Tests/GraphIndexTests/RDFGraphIdentityStorageSharingTests.swift) |
| Query literal RDF conversion, compilation, evaluation, and CONSTRUCT footprint | [LiteralSPARQLTests](../../Tests/GraphIndexTests/LiteralSPARQLTests.swift) |
| SPARQL, named graphs, and property paths | [SPARQLIntegrationTests](../../Tests/GraphIndexTests/SPARQLIntegrationTests.swift), [NamedGraphSPARQLTests](../../Tests/GraphIndexTests/NamedGraphSPARQLTests.swift), [SPARQLPropertyPathRuntimeSemanticsTests](../../Tests/GraphIndexTests/SPARQLPropertyPathRuntimeSemanticsTests.swift) |
| Composition authorization and retained event lifetime | [CompositionSPARQLAuthorizationTests](../../Tests/GraphIndexTests/CompositionSPARQLAuthorizationTests.swift) requires live claims while a consumer retains result events, then zero rows and bytes after its final owner releases them. |
| SHACL, algorithms, ontology, and GraphTable | [SHACLValidationTests](../../Tests/GraphIndexTests/SHACLValidationTests.swift), [GraphAlgorithmTests](../../Tests/GraphIndexTests/GraphAlgorithmTests.swift), [OntologyPersistenceTests](../../Tests/GraphIndexTests/OntologyPersistenceTests.swift), [GraphTableCanonicalContractTests](../../Tests/GraphIndexTests/GraphTableCanonicalContractTests.swift) |

Changes to the DatabaseEngine session or retained-row contract require a
focused GraphIndex caller review. Changes to RDF term serialization require
the Serialization/RDF evidence and the affected physical codec evidence.
