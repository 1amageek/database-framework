# Database

## Purpose and Scope

`Database` is the package's public entry and adaptation module. It exposes
SQL and SQL-facing SPARQL `DatabaseContext` calls, binds input into the
canonical execution contracts, and composes the selected runtime feature
products.

- Parent: [database-framework](../../DESIGN.md).
- Children: none. This module has no nested design authority; parser and AST
  semantics remain owned by the `QueryAST`/`DatabaseKit` dependencies.

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
