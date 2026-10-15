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
S03 later owns propagation of parsed source annotations into binding failures;
no S02 success claim includes that deferred boundary. Typed ARRAY/ROW and
unimplemented temporal conversion paths return explicit marked unsupported
failures rather than successful synthetic values.

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

Changes to SQL/SPARQL entry contracts require rechecking DatabaseEngine's
session design and GraphIndex's transaction-bound executor evidence. Changes
to parser semantics belong to the owning dependency and are not expanded into
this module's design.

### Planned statement and block entry contracts

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
