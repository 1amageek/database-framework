# DatabaseEmbedded

## Purpose and Scope

Status: implemented optional SwiftPM module. Parent: [Framework package](../../DESIGN.md).
Children: none. Owns in-process composition for application access. Dependencies
are Client-owned DatabaseAccess and Framework Database/DatabaseEngine targets;
selected runtime/backend traits stay explicit. No remote Client invocation or Server target dependency.

## Responsibilities and Boundaries

The adapter implements DatabaseOpening/DatabaseExecuting using public
DBContainer, DatabaseContext and QueryConnection contracts. A configured owning
factory creates a fresh container with the application's schema, runtime,
security, engine and clocks, then a dedicated authorization-bound context and
one connection with the configured positive concurrency limit.

A separate explicitly borrowing factory may use an externally owned context.
Its SDK close drains the logical connection but never shuts down that context's
container/engine. Ownership is visible in factory names/configuration; it is not
inferred from the number of aliases. Factories transferring ownership are
single-use; the same owner is not transferred to two aliases.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Framework package](../../DESIGN.md) | parent | Optional module composition | Owns dependency direction | Not a replacement execution engine |
| [Common access](../../../database-client/Sources/DatabaseAccess/DESIGN.md) | depends on | DatabaseOpening/DatabaseExecuting and lazy results | Supplies application access contracts | No transport dependency |
| [NamedAccess](../../../database-client/Sources/DatabaseAccess/NamedAccess/DESIGN.md) | used by | Opener and access lifetime | Publishes prepared access | No partial publication |
| [Results](../../../database-client/Sources/DatabaseAccess/Results/DESIGN.md) | depends on | Lazy result adaptation | Wraps final Framework pages | No extra encoding |
| [Framework connection](../Database/QueryExecution/DESIGN.md) | depends on | Original-authority execution/close | Performs all query semantics | Container still exclusively owns engine |
| [Engine core](../DatabaseEngine/Core/DESIGN.md) | depends on | Container/context lifecycle | Owns storage admission and shutdown | No raw-engine operational bypass |
| [Storage operations](../../../storage-kit/Sources/StorageKit/Storage/DESIGN.md#compound-operation-scheduling) | depends on | Joined compound operation and inherited authority | Bounds immediate-resume continuation stacks | No new transaction or budget owner |

## Architecture

```text
configuration opener -> DBContainer.open -> newContext(authorization:)
    -> context.openConnection(configured limit)
    -> execute(SQL / statement / block) -> final page -> common results
close -> connection.close -> owning container.shutdown
```

## Contracts and Invariants

Configuration carries explicit application schema/runtime/security and storage
composition. A named alias never changes SQL database identity, policy factory,
schema, root Directory, Base/Composition target or principal. The local factory
does not disable security for convenience. A borrowed context does not imply
that its pending model changes are committed by canonical query execution.

Local execute delegates to the existing QueryConnection exactly once, preserving
options, parameters, checkpoint/structural/frame limits and original authority.
Nonempty unsupported OperationRequestMetadata is rejected before execution;
server idempotency is never silently invented. A metadata-free SQL/IR/block
request uses the existing path without a Wire request dispatch layer.

The adapter consumes only authoritatively closed final results. It never calls
SPI executeQueryEncoded directly, decodes a frame a second time, fabricates
closure evidence or runs a second interpreter. Page-size read-only admission
and complete local retention remain unchanged.

## Runtime Flows

Validate configuration before creating owned resources. An opener owns cleanup
of anything acquired before it throws. DBContainer.open already awaits its
claimed-engine cleanup on failure; if connection creation fails after a
successful container open, this adapter awaits container.shutdown before throw.
Success transfers the owner once to NamedAccess. Close drains the connection
before shutting down the owned container. Borrowed close stops after the drain.

## State, Ownership, and Lifecycle

One immutable endpoint holds the container/context/connection references and an
explicit owning/borrowing mode. Connection actor state and container storage
lifecycle remain their existing owners. If a factory's single-use claim needs
mutable state, one identical Mutex<State> protects it on all three targets.
No additional principal, transaction, plan or meter cache is added to the SDK.

The adapter does not reexport Framework's Database umbrella into application
imports. Public SDK Database and the existing Framework module named Database
must coexist in a mixed-import compilation test; do not rename an existing
module or add compatibility aliases merely to avoid resolving that spelling.

## Failure, Concurrency, and Constraints

Existing typed authorization, SQL diagnostics, resource limits and cancellation
pass through unchanged. Close after a committed write does not prove rollback;
the SDK must not retry to resolve an ambiguous result. Server users create
request-bound contexts over shared containers as described by the package;
the app's shared authorization-bound endpoint is not reused across users.

## Verification and Change Impact

N02 uses real SQLite for owned/borrowed cleanup, denied entity/field access,
request budgets, SQL/IR/block equality, first-error rollback, restart and close.
N03 adds named generations and concurrent use. The ordinary/Embedded gates
exercise actual local invocation with the selected traits, not host-only
declarations. A backend/trait not executed remains explicitly unverified.

### N02 concrete composition

DatabaseConfiguration.owningEmbedded receives a fresh-container factory and an
explicit async context selector. This makes principal/target selection visible
and supports ordinary/MultiBase composition without a default privileged
context. borrowingEmbedded retains a caller-owned context. Existing frame,
structural and checkpoint limits are forwarded to openConnection.
EmbeddedOpening serializes its single-use acquisition claim. EmbeddedExecution
forwards execution without another admission queue or meter, rejecting any set
traceID/idempotencyKey before invoking the local connection. Its cleanup actor
coalesces close: drain the connection, then await shutdown of an owned container.
Borrowed close leaves the external container usable. These actors and Sendable
contracts are identical on Native/ordinary WASM/Embedded.

The module-owned `scripts/access-runtime-harness` pins Swift 6.4.0 release and
its matching standard/Embedded WASI SDKs for the new adapter runtime suite.
This does not replace or change the older Framework release-gate pin in
`docs/production-readiness.md`. The suite executes ScalarIndexes SQL/IR/block
results, work-budget rollback, retained rows and owned/borrowed closure through
DatabaseExecuting witnesses. Its InMemoryEngine and deterministic clocks prove
adapter/executor behavior, not durable storage or elapsed-time limits. SQLite
restart and actual HTTP remain separate Native evidence.

The portable fixture links the matching SDK's `libc++abi`, as the existing
Framework consumers do for Swift Concurrency TaskGroup support. It supplies no
replacement runtime symbols. Its WASI executable uses the existing Framework
portable stack budget of 8 MiB in both debug and release configurations;
production library targets do not select a consumer's stack limit.

N03 adds named two-database isolation, typed projection/predicate parity,
100-row insertion and stale-generation rejection to the portable fixture.
Final optimized integration is owned by N05. Native shared-container tests additionally prove separate original
request authorities and independently owned per-call budgets.

N05 composes the same named API with authenticated Native HTTP, verifies
parameterized SQL/IR/block results and denied request scopes, and compares
direct connection execution with the facade. The stable Swift 6.4.0 ordinary
and Embedded release fixtures also execute a fluent CTE/grouped aggregate with
LIMIT over the inserted data. Snapshot-only Kit evidence is recorded separately.
Final consumers resolve the committed URL graph without local overrides.

Immediately completing nested storage operations use the StorageKit-owned
[compound scheduling boundary](../../../storage-kit/Sources/StorageKit/Storage/DESIGN.md#compound-operation-scheduling).
This adapter forwards the original operation and does not add per-query yields,
a second budget meter or a transaction boundary. Standalone WASI and hosted
execution are separate runtime evidence; the common API never installs or
replaces a global executor.
