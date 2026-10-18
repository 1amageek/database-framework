# Database

## Purpose and Scope

`Database` is the package's public entry and adaptation module. It exposes
SQL and SQL-facing SPARQL `DatabaseContext` calls, binds input into the
canonical execution contracts, and composes the selected runtime feature
products.

- Parent: [database-framework](../../DESIGN.md).
- Children: none. This module has no nested design authority; parser and AST
  semantics remain owned by the `QueryAST`/`DatabaseKit` dependencies.
  PostgreSQL compatibility additions below are planned S00 contracts.

## Responsibilities and Boundaries

S07 public SQL consumes [Engine's selected collection composition](../DatabaseEngine/QueryExecution/DESIGN.md#s07-selected-collections-and-relational-identity).
The facade preserves selected metadata, complete payload leases, source
failures and SQL versus SPARQL language applicability. It does not implement
array/JSON algorithms. S08 composes ROW and ARRAY(subquery) through the same
selected Engine contract; their implementation and behavioral proof remain
required before declaring support. General SRF relation composition remains
the S10 gate with explicit pre-read rejection until then.

`Database` owns:

- the public SQL entry points, parameter binding, SELECT admission, and
  canonical SQL response adaptation;
- the SQL-facing SPARQL string/function entry and adaptation boundary;
- preserving one parent read transaction and work-meter identity when a SQL
  expression invokes SPARQL;
- exporting and composing the package's runtime feature targets.

`Database` does not own SQL grammar or AST meaning, graph/RDF physical
scanning, index maintenance algorithms, session authorization, storage
transactions, backend implementations, or model/schema declarations. It
delegates those contracts to `QueryAST`, `GraphIndex`, `DatabaseEngine`,
`StorageKit`, and `DatabaseKit` respectively.

The concrete entry/adaptation paths are [DatabaseContext+SQL](DatabaseContext+SQL.swift),
[SPARQLFunctionRewriter](SPARQLFunctionRewriter.swift), and
[SPARQLStringExecutor](SPARQLStringExecutor.swift).

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [database-framework](../../DESIGN.md) | parent | Package composition and dependency direction | Places this module at the public entry boundary. | Keep child contracts linked rather than duplicated here. |
| [DatabaseEngine](../DatabaseEngine/DESIGN.md) | depends on | Read session, transaction-bound preparation, canonical rows, and typed failures | Supplies the session and persistence execution boundary. | `Database` must not replace session authority with ambient state. |
| [SQLBlock](../DatabaseEngine/SQLBlock/DESIGN.md) | depends on | Semantic statement/block admission and post-closure result transfer | Owns the S12 aggregate execution lifecycle. | First error rolls back the request and throws; SQL control text and remote operations belong to S16/S17. |
| [QueryAST](../QueryAST/DESIGN.md) | depends on | Strict parser and canonical SQL serializer | Adapts text without executing it. | SELECT and statement entry points must agree on shared syntax. |
| [DatabaseKit SQL](../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md) | depends on | SQL profile, statement/block, positional typed result | Owns semantic values and compatibility inventory. | Public adaptation must preserve column order, duplicate names, types, and failures. |
| [GraphIndex](../GraphIndex/DESIGN.md) | depends on | Transaction-bound graph-facing SPARQL and graph physical execution | Receives graph work after SQL entry/adaptation and session admission. | Graph layout, RDF bytes, and graph-facing SPARQL semantics remain in GraphIndex. |

The `DatabaseKit`, `QueryAST`, and `StorageKit` dependencies are package-owned
contracts without a local design child in this module; their APIs are
consumed, not redefined.

## Architecture

```text
SQL text + parameters
    -> SQLParser / QueryParameterBinder
    -> SELECT validation and ReadExecutionContext
       |-> plain query -> DatabaseEngine query execution
       `-> SPARQL function
             -> session-bound SPARQLFunctionRewriter
             -> GraphIndex transaction-bound executor
             -> rewritten SQL plan -> canonical rows
    -> QueryResponse
    -> typed model decode only at the requested output boundary
```

The public top-level SPARQL string path uses GraphIndex's explicit
`StorageEngine`-backed executor entry. A SQL-embedded SPARQL function always
uses the parent `DatabaseReadTransaction`; it does not open a nested
transaction.

## Contracts and Invariants

- S08 consumes the [canonical relation query contract](../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s08-frozen-relational-query-carriage)
  through QueryAST and Engine. Entry adaptation preserves all ordered
  projection items, source/set scopes, quantifiers and pagination expressions
  during parameter admission and SPARQL rewriting. The facade never executes
  its own CTE cache, composite conversion or ordering algorithm. Engine must
  close every demand producer and session before this facade exposes a ready
  positional response. Empty responses retain bound descriptors, and a
  composite remains distinct from a primitive object or SQL NULL.

- S05 concatenation and dynamic SQL pattern operands participate in SPARQL
  detection and rewriting under the [QueryExecution traversal contract](../DatabaseEngine/QueryExecution/DESIGN.md#s05-selected-string-pattern-binary-and-cast-execution).
  Rewriting retains every child occurrence and the same parent transaction,
  meter and Mutex-owned ledger; pattern language execution stays below this module.

- SQL parsing, parameter binding, unsupported-statement rejection, and
  execution failures remain typed failures; malformed input is not converted
  to an empty response.
- `executeSQL` accepts only the statements supported by its public contract.
  A non-SELECT statement is rejected before execution.
- A SQL SPARQL function is detected and rewritten before the parent plan is
  executed. The rewriter receives the exact parent read transaction and work
  meter, and GraphIndex receives that transaction-bound capability.
- SPARQL function index resolution and execution do not use TaskLocal or a
  second authorization decision. The sealed session admission remains the
  authority.
- `DatabasePreparedSQLSelect` retains its literal storage and meter identity
  until execution or explicit release. It rejects a foreign work meter.
- Canonical query rows are the inter-module result boundary. Model decoding is
  performed only by the explicit typed output API, not by SQL adaptation.
- The module does not silently coerce RDF terms, missing graph/index state,
  cancellation, or backend failures into synthetic SQL success.

### S02 exact static SQL namespace

Static schema entity and field names are exact SQL logical names, equivalent
to quoted identifiers. The parser folds unquoted ASCII names and preserves
quoted names; runtime resolution still uses exact admitted registration keys.
SQLQueryBuilder inputs are exact names and canonical serialization quotes
those names. Existing camelCase SQL fixtures must quote their registered names
rather than renaming registries, adding case-insensitive lookup or silently
falling back after a missing name. Directory/Partition, authorization and
stored field identity do not change, and S11's dynamic SQL catalog is not
introduced early to implement this rule.

The public single-statement SELECT entry continues to reject non-SELECT and
multiple statements. Parser.parseStatements is a syntax capability only;
S12/S16/S17 own transaction blocks, control syntax and their public execution.
S02 preserves syntax errors with zero-based UTF-8 token-start positions.
S03 owns propagation of parsed source annotations into binding failures under
the contract below; no S02 success claim includes that boundary. Typed ARRAY/ROW and
unimplemented temporal conversion paths return explicit marked unsupported
failures rather than successful synthetic values.

### S03 SQL binding and local diagnostic bridge

Database composes
[Parser's annotated semantic result](../QueryAST/Parser/DESIGN.md#s03-occurrence-source-sidecar),
[Kit parameter identity](../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s03-frozen-semantic-binding-boundary)
and [executable Engine binding](../DatabaseEngine/QueryExecution/DESIGN.md#s03-executable-binding-contract).
SQL admission preserves the original QueryIR parameter references and supplied
declarations; it no longer performs context-free literal substitution before
Framework inference. Existing SPARQL/GraphTerm substitution retains its own
language contract. Parameter state is operation-owned and explicitly passed,
never ambient. Prepared SQL owns immutable resolved parameters, the exact
meter, rewritten literal storage and semantic provenance until execution or
release; a foreign meter remains an error.

```text
SQL text + supplied parameters
  -> parse result(statement, semantic arena/root, sparse source ranges)
  -> boundQueryParameters preflight -> immutable resolved parameters
  -> optional admitted product SPARQL producer in parent transaction
  -> rewrite with origin provenance -> full bind of actual produced values
  -> canonical SQL relation execution -> existing authoritative result closure
typed Engine failure(address, cause) -> optional local source annotation
```

The product SPARQL producer may have data-dependent output types. Preflight
checks its exact unqualified signature, raw SPARQL syntax and all determinable
SQL names/types/parameter contexts before graph reads. Deferred producer output
is an explicit admission state, not a fabricated executable value. After the
existing transaction-bound rewrite, full binding checks actual produced values
before SQL relation scans. Deterministically invalid surrounding SQL must
start zero graph reads; the contract does not claim that data-dependent output
failures are knowable before the producer runs. Existing graph reservation,
parent transaction and retained prepared-storage lifetimes are preserved.

Engine receives only Kit occurrence arena/root, never QueryAST/source-span
types. Database retains the parse result and maps a decisive typed Engine
failure address to zero-based UTF-8 source position, preserving the typed
underlying cause. Runtime scalar failures carry their decisive optional occurrence
in CanonicalReadError.expressionEvaluation; executeSQL maps that address into
the existing SQLBindingSourceFailure without treating execution as binding.
Prepared rewrites remap runtime occurrences through the same explicit origin
dictionary before source annotation. Repeated identical expressions, parameter uses, normalization
and alias/ordinal/SPARQL rewrites require explicit provenance; direct semantic
IR has no invented position. When a SPARQL rewrite changes child structure,
the existing rewriter explicitly walks original field/index addresses while
constructing the rewritten semantic arena/root and a new-ID-to-original-ID
origin dictionary. Every generated literal in a producer expansion maps to
that producer's original occurrence. Prepared storage retains both arena/root
lifetimes and the origin map; reusing the old arena on the changed tree is
invalid. Prepared execution remaps a rewritten binding failure's exact address
and root through that origin dictionary before Database consults the original
source map. An absent origin yields an absent source position. This avoids
copying source ranges into another map. Existing rewriter child knowledge is
sufficient; no generic Kit AST visitor or equality-based source search is
required. Admission precedes arena/map/lookup growth, and
the existing runtime meter retains the corresponding actual-owner claim.
S16 owns final remote SQLSTATE/details and CLI
formatting. S03-C02/C03/C09–C11 prove the public production parameter path,
preflight ordering, actual coercion, local positions and retained-owner cleanup.

## Runtime Flows

For a SQL request, parsing and binding happen before plan execution. A plain
SELECT enters DatabaseEngine directly. A query containing `SPARQL(...)`
creates a session-bound rewriter, resolves the graph index through the parent
transaction, executes the retained SPARQL values, rewrites the expression,
and executes the resulting SQL plan in the same read transaction.

## State, Ownership, and Lifecycle

`DatabasePreparedSQLSelect` owns the immutable select syntax, request meter,
and any retained literal storage for the prepared operation. The rewriter
borrows the parent transaction and does not retain it beyond the operation.
`Database` owns no global authorization or backend registry; runtime
composition is injected by `DBContainer`.

## Failure, Concurrency, and Constraints

SQL and SPARQL adaptation is asynchronous where parsing, storage, or graph
execution suspends. No mutex is held across `await`. Parent transaction,
cursor, retained-literal, meter, and graph cleanup failures propagate through
the typed contract. The module creates no nested transaction for an embedded
SPARQL function.

## Verification and Change Impact

| Contract | Evidence |
|---|---|
| SQL parsing, binding, NULL semantics, grouping, ordering, and failures | [SQLRowExecutionTests](../../Tests/DatabaseTests/SQLRowExecutionTests.swift) |
| Prepared literal ownership and meter identity | [DatabasePreparedSQLSelectTests](../../Tests/DatabaseTests/DatabasePreparedSQLSelectTests.swift) |
| Canonical SQL retained ownership | [CanonicalSQLRetainedOwnershipTests](../../Tests/DatabaseTests/CanonicalSQLRetainedOwnershipTests.swift) |
| Parent transaction for SQL SPARQL functions | [SPARQLFunctionTransactionTests](../../Tests/DatabaseTests/Integration/SPARQLFunctionTransactionTests.swift) |
| Index admission and explicit missing/ambiguous failures | [SPARQLFunctionIndexAdmissionTests](../../Tests/DatabaseTests/Integration/SPARQLFunctionIndexAdmissionTests.swift) |
| Real SPARQL function behavior | [SPARQLFunctionIntegrationTests](../../Tests/DatabaseTests/Integration/SPARQLFunctionIntegrationTests.swift) |
| Graph-table SQL adaptation | [GraphTableSQLExecutionContractTests](../../Tests/DatabaseTests/GraphTableSQLExecutionContractTests.swift) |

The user-approved [S03-C12 verification schedule](../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s03-falsifiable-case-ids)
places graph-enabled Database standard WASM/Embedded compile/link/runtime in
S18. `OccurrenceRewrite` and `InliningStructureMeter` use the common Mutex
storage and Native behavior tests; their graph-enabled portable runtime path
remains unverified until that lane passes. Two stopped GraphIndex builds
executed zero runtime cases and are not passing evidence. The pinned release
compiler concentrated in async-variable debug-information propagation even
with debug information disabled; a separately identified debug-pass option is
only a diagnostic candidate, not accepted runtime evidence.

Changes to SQL/SPARQL entry contracts require rechecking DatabaseEngine's
session design and GraphIndex's transaction-bound executor evidence. Changes
to parser semantics belong to the owning dependency and are not expanded into
this module's design.

### S12 Semantic Entry Contract

The proposed semantic overloads are
`executeStatement(_:parameters:options:)` for an existing `QueryStatement`
and `executeSQLBlock(_:parameters:options:)` for Kit's `SQLBlock`. The first
uses the same block execution path with one statement and returns that
statement's outcome. `SQLBlockExecutionOptions` is a Framework-owned immutable
configuration with required StorageKit checkpoint limits and existing request
budget, structural-limit, search-path and transaction defaults. Its mutation
limit derivation and exact single-count accounting are owned only by
[SQLBlock](../DatabaseEngine/SQLBlock/DESIGN.md#api-and-result-owner).
The first statement
or checkpoint error stops the immutable block, rolls back all effects and
throws the typed failure after mandatory cleanup; later ROLLBACK TO steps
are not automatically executed. Failed requests publish no earlier results.

`Database` performs bounded structural/parameter admission and adapts the
Engine-owned result only after its complete producing lifecycle closes. It
does not loop over public `executeSQL`, open a transaction per statement,
create a work meter per statement, or implement checkpoint/name semantics.
The [SQLBlock component](../DatabaseEngine/SQLBlock/DESIGN.md) owns the execution
subset, results, readiness and checkpoint coordination. Kit owns block input
and command meaning; Framework owns `rows(QueryResponse)` and other results
created by execution. Neither a third row type nor Kit-to-Framework dependency
is required.

The fixed Engine entry uses
`withCheckpointTransaction(configuration:checkpointLimits:workMeter:executionDeadline:_:)`
with required StorageKit checkpoint limits and the exact request meter. Its
copyable callback result retains the existing `T: Sendable` signature;
SQLBlock bridges its linear collecting owner using the existing internal
`DatabaseReadResultBox` and finalizes only after authoritative runner return.
The facade exposes neither that bridge nor a new transaction result factory.

```text
semantic QueryStatement / SQLBlock + parameters + immutable options
    -> one cumulative admission -> SQLBlock execution owner
        -> one meter + one transaction + transaction-bound sessions
        -> authoritative close -> cancellation gate -> Ready result
    -> one consuming caller-owned output transfer
```

The existing `executeSQL` text SELECT facade and its single-statement rejection
contract remain unchanged. SQL control syntax is S16; new remote statement and
block descriptors, transport invocation and SQL-file calls are S17. A trusted
catalog-candidate test SPI is not part of public SQL input or parser grammar.

Public S12 tests must observe read-your-writes, ordered typed/empty results,
successful-result retention after rollback-to, catalog-subset rejection,
exact/one-past cumulative limits, full failure/cancellation cleanup and no
completion while commit/session/cleanup is suspended. They must enter these
facades and perform real backend readback. Engine tests own the underlying
state-machine and checkpoint cases instead of duplicating them here.

Database's GraphIndex and OntologyIndex dependencies are selected only by the
GraphIndexes trait; the semantic block facade and ordinary collecting seam do
not intrinsically require that trait. S12 records native public-block evidence
separately from formal Engine/Kit WASM/Embedded checkpoint/control/declaration
evidence. A portable fixture proves the graph-free orchestrator or facade only
when that exact path executes. The existing Graph-enabled Database portable
obligation above remains SQL-S18.02.03, and lower Engine/Kit evidence does not
close it or introduce a new deferral.

### Aggregate Entry Assumptions

The current `executeSQL` implementation parses one statement, binds values,
and rejects non-SELECT statements. That read-only API remains read-only.
S01–S17 add `executeStatement` and `executeSQLBlock` public entry points;
names here describe accepted proposed APIs, not existing declarations.
Their DatabaseKit input/output types and wire shapes follow the linked SQL
design. Database owns parsing and admission, not execution-policy decisions.

```text
SQL text + parameters + execution configuration
    -> QueryAST strict parse
    -> DatabaseKit structural validation and parameter contract
    -> DatabaseEngine scope/name/type binding
    -> Framework-owned statement or block lifecycle
    -> purpose-specific Ready result
    -> public positional result / command / schema-change adaptation
```

- A statement result distinguishes rows, affected-row command, and schema
  change. A block returns results in statement order only after its complete
  producing lifecycle ends. It never exposes an intermediate result while a
  transaction, sequence recovery operation, or schema publication is active.
- Parameters retain their semantic types, typmods, and owned bytes. An empty
  relation still returns its bound column schema; duplicate labels and
  projection order survive adaptation without a dictionary conversion.
- Block state, prepared statements, temporary relations, and SET values are
  request-scoped. They do not establish an ambient cross-request session or
  transport-owned transaction. SELECT admission does not widen write or DDL
  authority, and SQL schema selection does not select a Partition or Tenant.
- Existing SQL-embedded SPARQL work continues to borrow the exact admitted
  transaction and meter; it cannot open an independent transaction to obtain
  compatibility behavior.
- Syntax, type, cardinality, constraint, budget, cancellation, stale schema,
  cleanup, and commit-unknown failures remain typed through response
  adaptation. SQLSTATE classification is a semantic contract, not message
  parsing. No failed block is represented as an empty successful result.

Verification extends the linked SQL acceptance IDs with public-entry tests:
WITH accepted identically by SELECT and statement entries; non-SELECT
rejected by the read-only entry; same-name columns and empty typed results
retained; DML RETURNING held through commit; blocks publish nothing early;
SQL-embedded SPARQL retains parent authority. Tests run through the real
Framework backend path, with wire/CLI publication verified by their owners.

### S01 typed SELECT response adaptation

S01 keeps the read-only SELECT entry and adds typed carriage for its existing
syntax rather than exposing later statement/block capabilities early.
Database passes QueryResponse.columns with every positional QueryRow through
its caller-facing response. Typed executeSQL(as:) explicitly calls the
Read-owned codec with those columns; duplicate-name or incompatible rich-value
model projection throws. It never recreates schema from the first row or
accesses an implicit fields dictionary. Rich parameters become bound
Literal.queryValue with their optional explicit QueryColumnType preserved;
public SQL projection of a rich parameter does not serialize it back to text.

[Read](../DatabaseEngine/Read/DESIGN.md#s01-frozen-public-result-and-projection-boundary)
owns the exact API and final public transfer boundary.
[QueryExecution](../DatabaseEngine/QueryExecution/DESIGN.md#s01-frozen-positional-execution-contract)
owns relation slots and metadata binding.
DatabaseTests owns real public-entry S01-C01–C08 and C10 evidence; failures
must retain typed causes. Existing SQL/SPARQL parameter meaning and the parent
transaction/meter path remain intact (C13).

SQL parameters used by the product SPARQL producer retain their SQL occurrence
identity through preflight. The producer's string argument contexts resolve
parameter declarations before graph work. Only that producer's argument tree
uses the Kit substitution boundary after declarations are frozen; surrounding
SQL expressions keep parameter references for canonical binding. Explicit
string QueryValue carriers are accepted, and NULL or non-string arguments fail
before graph reads.

Preparation admits the frozen parameter-declaration array before preflight
materializes it. Its reservation remains with the prepared carrier. The same
reservation grows before each rewritten occurrence and origin-map entry is
retained; arena and origin ownership therefore remain request-accounted until
the prepared carrier is released. Parameter value payloads and the original
parse arena retain their existing owners without copying those payloads.


### S06 SQL statement boundary

executeSQL captures the [Core temporal environment](../DatabaseEngine/Core/DESIGN.md#s06-clock-and-configuration-ownership)
before parsing and passes it through ReadExecutionContext and the existing
canonical/SPARQL preparation path. Internal reentry retains that statement
time. The public facade does not implement temporal conversion, acquire zone
data or read a process default. Binding/source failure mapping still uses the
original occurrence arena; injected clocks and zones grant no authority.


### S09 grouping and window adaptation assumption

**PLANNED; not implementation or verification evidence.** SQL source mapping
and SPARQL-function inlining consume Kit's S09 grouping/window children without
flattening grouping sets or dropping named WINDOW metadata. Inlining traverses
each represented expression with the original occurrence role, and query
rebuilds preserve every window definition. The canonical Engine path owns
SQL grouping/aggregate/window execution and legality; this facade adds no
alternate aggregate evaluator. QueryAST's S09-SYN05 and public Database SQL
tests own provenance and inlining regression witnesses.


### S11 SQL entry and prepared search path

**Implementation contract; not completion evidence.** Both public `executeSQL`
overloads and `prepareSQLSelectForCanonicalExecution` accept
`searchPath: SQLSearchPath = .default`. SQL entry captures one Read-owned
immutable reference resolution inside `withDataOperation` (which enters
`withOperationSchemaLease`), from that admitted catalog generation, selected path and
`AuthorizationContext.principal.identifier` (anonymous has no current-user
schema), then passes it explicitly through parameter binding, SPARQL rewrite,
prepared execution and canonical execution. This explicit SQL entry selection
does not change typed QueryIR or graph-facing SPARQL physical semantics.

```text
SQL entry + path -> withDataOperation / withOperationSchemaLease
  -> capture admitted SQL resolution -> bind parameters/source names
     -> same-lease optional SPARQL rewrite -> retained prepared SQL
        -> same-lease authorization and scan -> source-map failure
```

Prepared SQL retains its resolution owner and search path with the existing
literal/metadata reservations and meter. Both ordinary execute and complete
staging use that captured policy; caller-supplied execution options cannot
silently switch it to another catalog generation or relation. External SPI
preparation captures and retains its actual session policy/lease. Execution in
a different session generation throws a typed prepared-resolution mismatch;
it cannot reacquire the current snapshot or silently rebind. Nested ordinary
SQL, preflight, rewrite, authorization and scan inherit the enclosing lease,
including when publication happens between those steps. The package
parameter-binding seam accepts explicit resolution while the existing generic
binding entry retains physical semantics. SPARQL function work continues to
borrow the admitted parent transaction and authority; its physical graph
namespace is not reinterpreted as SQL schemas.

[QueryExecution](../DatabaseEngine/QueryExecution/DESIGN.md#s11-frozen-catalog-bound-select-execution)
owns missing catalog/runtime source failures. This entry preserves their typed
cause and deterministic occurrence through the existing SQLBindingSourceFailure
source-map adapter; it never converts them to an empty successful result.
The reference owner and metadata are preadmitted under the supplied work meter.
The normative [S11-C01–C12 matrix](../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s11-frozen-logical-catalog-and-identity)
owns completion IDs. Prepared tests cover default/custom path, current-user/anonymous behavior,
rewrite/staging carriage, retained owner release, same-lease execution during
concurrent catalog publication and typed cross-session generation mismatch. Public
SQL tests cover located qualified failures and stale continuation rejection;
existing meter, literal, graph parent-transaction and typed-output behavior
remain regressions. S11 does not add DDL, DML, constraints or SQL blocks.


### S13 text sequence effects

The SELECT text facade classifies sequence functions with the Engine-owned bounded
expression walker. An effectful SELECT enters the same single-statement SQLBlock
orchestrator as semantic requests, preserving the facade's work meter, absolute
deadline and captured temporal scope. It returns rows only after data closure,
nonrollback allocation and mandatory recovery closure. Session-only functions keep
the existing read path. Sequence requests use the profile's serializable collecting
block contract; an external page or continuation is rejected before effects because
its lifetime cannot preserve this request-local effect history. Pure SELECT paging
is unchanged. General text DML/DDL and remote dispatch keep their later sprint owners.

### S14 semantic mutation results

The existing semantic `executeStatement` and `executeSQLBlock` facade consumes
[SQLBlock's S14 composition](../DatabaseEngine/SQLBlock/DESIGN.md#s14-mutation-bearing-statement-composition).
A DML RETURNING result remains `.rows(QueryResponse)` and preserves its direct
`affectedRows`, even for an empty relation. The facade neither reexecutes a CTE
nor materializes a result before the Engine's transaction, read session, required
schema/sequence work and request leases have authoritatively closed. Textual DML
admission and remote operation publication retain their separate later owners.


### S16 text block and diagnostic adaptation

`executeSQLBlock(String,parameters:options:)` parses one bounded text request with
QueryAST's SQLParsedBlock, then invokes Engine's existing block executor once.
The same request meter and monotonic deadline include parsing and execution.
Aligned step roots preserve absolute occurrence addresses; no per-statement public
execution or independent transaction is introduced. Existing SELECT and semantic
entry contracts remain available. The text overload returns Engine's typed ordered
block outcome, including request-contained COPY data after authoritative close.

The adapter owns PostgreSQL-style one-based Unicode scalar source positions.
QueryAST/Engine continue to report zero-based UTF-8 ranges. A byte offset must be
within the original UTF-8 source and on a scalar boundary; otherwise position is
absent. Direct semantic input never acquires a fabricated text position. Parser
cases map by typed case, Engine causes use SQLDiagnostics, and SQLExecutionFailure
retains the original typed error, including mandatory-cleanup composites. No
localized message parsing or runtime reflection decides SQLSTATE.

Tests execute multi-statement text, explicit rollback, prepared values, inline
COPY, own-DDL catalog reads and EXPLAIN through this facade. Separate tests cover
multibyte/CRLF/batch source positions, original constraint identity after rollback,
syntax/unsupported/resource classification and unchanged semantic error behavior.
Standard and Embedded source graphs retain the same immutable parsed input and
Engine isolation contracts; target proof remains local to the exact graph run.
