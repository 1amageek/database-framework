# Query Execution

The proposed [SDK local adapter](../../DatabaseEmbedded/DESIGN.md)
consumes this public connection contract. Named access remains an upper
application composition concern; global runtime registration and new query
interpretation do not enter Framework. This adapter is currently design-only.

## Purpose and Scope

This component owns the common in-process connection surface and final result
adaptation. Parent: [Database](../DESIGN.md). Children: none. SQL, QueryStatement
and SQLBlock use one execute family and Kit's canonical result algebra.

## Responsibilities and Boundaries

QueryConnection.execute is the general public execution surface. Language-specific
DatabaseContext collecting kernels (executeSQL, executeStatement and
executeSQLBlock) remain DatabaseExecution SPI for execution composition and owner
behavior tests. They are not alternate application execution interfaces. Their
working SQL, model-adaptation and transaction behavior is retained under that
explicit boundary.

A connection attenuates one existing DatabaseContext; it never substitutes a
principal, owns the container's engine, or makes a successful open into execution
authority. The Database umbrella reexports the Wire-owned retained carriers so
callers need only import Database, including Embedded specializations.
SQL execution remains owned by Engine SQLBlock. This component projects
its provisional output through the bounded Wire writer and exposes decoded,
frame-retaining values only after the concrete Ready owner proves closure.
The existing query profile's SPARQL read forms use admitted read sessions.
Existing typed mutation preconditions/idempotency and external RDF LOAD remain
owned by their mutation contracts; this change does not advertise new IR forms.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Database](../DESIGN.md) | parent | Public adaptation and runtime composition | Owns the entry surface | No second interpreter |
| [Kit execution](../../../../database-kit/Sources/DatabaseKit/Query/Execution/DESIGN.md) | depends on | SQL, QueryInput, QueryResult and QueryRow | Common semantic values | Interpolated values are bound; no execute(String) overload |
| [Wire](../../../../database-kit/Sources/DatabaseWire/DESIGN.md) | depends on | Scoped projection and retained pages | Canonical final representation | Resource limits apply before effects become publishable |
| [SQLBlock](../../DatabaseEngine/SQLBlock/DESIGN.md) | depends on | Exact meter, original authority, transaction and Ready owner | Executes SQL text/IR/blocks | First statement failure rolls back the entire request |
| [Read](../../DatabaseEngine/Read/DESIGN.md) | depends on | Admitted snapshot/session and runtime rows | Executes SPARQL reads | Drain session before final values escape |
| [Client](../../../../database-client/DESIGN.md) | coordinates with | Matching execute and connection lifetime | Remote implementation | Transport failure never automatically retries effects |

## Architecture

```text
DatabaseContext.openConnection() -> QueryConnection (actor)
    -> SQL / QueryStatement / SQLBlock
    -> same Engine admission, meter and transaction
    -> provisional canonical projection, including metadata and version bytes
    -> commit/rollback + session/transaction/schema closure
    -> concrete Ready owner -> retained QueryResult -> result.rows
```

## Contracts and Invariants

- SQL text is parsed execution-side with source diagnostics; IR is neither trusted
  nor converted back into text. Both enter the same statement/block executor.
- Complete execution is the default. An explicit page size requests the existing
  read-only query profile; sequences, writable CTEs, blocks and mutations are
  rejected before effects rather than replayed by continuation. The original
  QueryInput.block is rejected at this entry boundary even when it contains one
  SELECT; Engine's single-statement check separately admits parsed SQL and IR
  read requests. This preserves the remote Server admission contract before the
  block is lowered into an Engine execution unit. In-process rows
  already reside in memory, so this retrieval preference does not alter LIMIT,
  aggregate semantics or result values.
- Columns, duplicate labels, exact QueryValue, annotations, opaque version,
  metadata, affected counts, COPY and ordered block outcomes survive projection.
- Final result encoding failures remain pre-commit failures on SQL paths. The
  original meter owns preparation, result bytes and temporary conversion claims.
- Connection.close rejects new calls, cancels and awaits every admitted call, and
  never shuts down an engine owned by DBContainer. Complete immutable results
  transfer to the caller and do not expose result.close().
- A logical connection defaults to one admitted execution slot. Explicit
  multiplexing has a positive caller-selected limit; overflow rejects immediately
  rather than retaining an unbounded queue. SQL request settings remain scoped to
  SQLBlock. Execution inside an enclosing transaction is rejected so an inner
  callback cannot masquerade as a committed result.

## State, Ownership, and Lifecycle

| State | Native / WASM / Embedded storage and isolation | Terminal transition |
|---|---|---|
| Connection phase and active tasks | Same actor, same declarations and entry points | open -> closing -> closed; one shared close task |
| SQL collecting/Ready result | Existing Engine concrete owner and reservations | Discard on failure; promote once after closure |
| Row/metadata conversion scratch | Same synchronous scoped borrow and meter claim | Released after projection callback |
| Final frame and row iterator | Immutable ByteString owner and exclusive value iterator | Caller ownership; no backend transaction |

## Failure, Concurrency, and Constraints

Execute cancellation forwards to its owned operation task and waits for cleanup.
Close is idempotent; cancellation of the closing caller does not cancel the drain.
The drain awaits each Task.value on every target. Execute retains the individual
success/failure contract; close consumes those outcomes only to prove termination.
Typed SQL diagnostics retain their underlying failure; IR positions remain absent.
SQL and graph projections share one classification of Wire response limits.
Canonical Graph namespace emission unwraps its encoding/destination envelope at
that boundary: encoding limits use the same resource classification, while the
original meter/cancellation failure propagates unchanged. A
graph projection raises QueryResponseEncodingError.resourceLimit after data
cleanup; SQL retains its SQL diagnostic around the same underlying typed limit.
Malformed representations remain Wire failures and are never reclassified as a
resource limit. No synthetic SQL diagnostic is added to a graph result.
The row adapter does not silently invent NULL, numeric conversions or read points.
No actor/mutex/Sendable contract varies with Embedded mode. Source parsing,
structural limits and checkpoint ceilings remain bounded by the selected policy.

## Verification and Change Impact

Focused behavior compares text/IR/block rows, columns, rich values, commands,
RETURNING and COPY. Explicit paged single/multiple-statement blocks must fail
before opening a transaction, while equivalent SQL/IR reads and complete blocks
retain their results. It checks default-deny/field denial, parameter injection,
work/row/memory exhaustion, late-error rollback, cancellation and concurrent close,
new-call rejection and container usability after connection closure. A bounded
Graph frame must fail as a typed response resource limit and the same connection
must remain usable after the failed read is closed. Native plus
standard/Embedded tests exercise the actual connection and projection. Server and
Client differential tests validate the same final contract over real transport.
