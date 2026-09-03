# database-framework

## Purpose and Scope

Package-level design authority and design index for the in-process
`database-framework` package. This document owns package composition,
dependency direction, and the direct-child index. It does not copy the
contracts owned by a child design.

- Parent: the workspace system design at [../DESIGN.md](../DESIGN.md).
- Direct children: [Database](Sources/Database/DESIGN.md),
  [DatabaseEngine](Sources/DatabaseEngine/DESIGN.md),
  [GraphIndex](Sources/GraphIndex/DESIGN.md),
  [BitmapIndex](Sources/BitmapIndex/DESIGN.md),
  [FullTextIndex](Sources/FullTextIndex/DESIGN.md),
  [RankIndex](Sources/RankIndex/DESIGN.md),
  [VectorIndex](Sources/VectorIndex/DESIGN.md), and
  [VersionIndex](Sources/VersionIndex/DESIGN.md).

The child list is an index of design authorities that exist for the current
material. A source target with only caller-signature adaptation and no
independent design contract is not promoted to a new design child.

## Responsibilities and Boundaries

The package owns in-process composition and the dependency boundary between
the database entry module, execution engine, graph execution, and specialized
index modules. `DBContainer` composition selects the runtime features and
injects the storage engine; the child modules own their domain behavior.

The package does not own primitive values, schema/query declarations, storage
backend implementations, wire dispatch, transport, server hosting, or
application schemas. Those contracts remain in their owning packages or
products.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Database](Sources/Database/DESIGN.md) | child | SQL and SQL-facing SPARQL entry/adaptation | Routes public database calls into the execution and graph contracts. | It must not absorb graph-facing SPARQL physical execution or session authority. |
| [DatabaseEngine](Sources/DatabaseEngine/DESIGN.md) | child | Session, authorization, transaction, persistence, and resource contracts | Owns the in-process execution boundary consumed by feature modules. | Changes to session or retained ownership require every feature child to revalidate its caller path. |
| [GraphIndex](Sources/GraphIndex/DESIGN.md) | child | RDF, SPARQL, graph, and graph-table physical execution | Owns physical graph execution above the DatabaseEngine boundary. | SQL entry/adaptation remains in Database. |
| [BitmapIndex](Sources/BitmapIndex/DESIGN.md) | child | Bitmap read and retained-result contract | Consumes the DatabaseEngine session and result boundary. | Bitmap-specific behavior must not become a package-wide resource contract. |
| [FullTextIndex](Sources/FullTextIndex/DESIGN.md) | child | Full-text read, scoring, facet, and autocomplete contract | Owns full-text execution and its child designs. | Authorization and session creation remain in DatabaseEngine. |
| [RankIndex](Sources/RankIndex/DESIGN.md) | child | Ordered rank read and retained-key contract | Owns rank-specific ordering and result transfer. | It must preserve the DatabaseEngine ownership boundary. |
| [VectorIndex](Sources/VectorIndex/DESIGN.md) | child | Vector algorithm and retained-match contract | Owns vector maintenance and search algorithms. | Session and final row ownership remain in DatabaseEngine. |
| [VersionIndex](Sources/VersionIndex/DESIGN.md) | child | Version-history read and retained-result contract | Owns version ordering and annotations. | It must not define generic transaction or authorization policy. |

External package APIs are dependencies of these children and are not
duplicated in this package index.

## Architecture

Arrows point from a consumer to the contract it consumes:

```text
Database (public entry and adaptation)
    -> GraphIndex (graph/RDF/SPARQL physical execution)
    -> DatabaseEngine (session, authorization, transaction, persistence)
        -> DatabaseKit + DatabaseTypes + StorageKit

BitmapIndex / FullTextIndex / RankIndex / VectorIndex / VersionIndex
    -> DatabaseEngine

DatabaseEngine -X-> specialized feature modules
```

`DatabaseEngine` remains independent of specialized index implementations;
feature targets are composed by the `Database` entry target or an explicit
runtime configuration.

## Contracts and Invariants

- A child design is the sole authority for the responsibility named by its
  scope; parent documents link to it instead of restating its contract.
- Database owns SQL and SQL-facing SPARQL entry/adaptation, GraphIndex owns
  graph-facing SPARQL and RDF physical execution, DatabaseEngine owns
  authenticated session execution, and specialized indexes own their
  algorithm-specific execution.
- Dependency arrows do not reverse through an implementation detail: the
  execution engine cannot import a feature module merely because an entry
  point consumes that feature.
- Canonical session paths preserve authorization, transaction, meter,
  ownership, cancellation, and typed-failure guarantees across module
  boundaries.
- The default composition uses one injected storage engine and one ordinary
  database root. Base/placement and persisted-grant composition are enabled
  only by the explicit `MultiBase` trait.
- Runtime capabilities are injected at composition; global mutable
  registration is not a package contract.

## Runtime Flows

```text
public SQL/graph/index request
    -> Database entry/adaptation or feature entry
    -> DatabaseEngine session and admission
    -> graph or specialized physical executor
    -> retained/canonical result boundary
    -> caller-visible output
```

The exact execution and failure transitions are owned by the corresponding
child design. This package only guarantees that the composition preserves
their declared dependency direction.

## State, Ownership, and Lifecycle

`DBContainer` owns runtime composition, schema state, and its injected storage
engine. DatabaseEngine owns session-scoped authority, transaction, snapshot,
and work-meter lifetimes. GraphIndex and specialized modules own only their
algorithm or physical-operation state; they do not create a second authority
for a canonical session path.

## Failure, Concurrency, and Constraints

Typed authorization, storage, decode, budget, cancellation, conflict, and
cleanup failures cross package boundaries unchanged. Shared mutable runtime
state follows the owning module's actor or mutex contract; package composition
does not weaken synchronization or ownership for a target or feature.

## Verification and Change Impact

Each child design owns its focused behavioral evidence. A package-level change
is complete only after the affected child contracts are reviewed and the
integrated package path is verified once at the converged boundary. A change
to dependency direction, runtime composition, or a child public contract
invalidates the corresponding child and package integration evidence.

### Serialized integration-scenario lifecycle

Backend integration evidence is produced under one serialized scenario gate.
The next scenario begins by clearing the whole key range, so an operation that
outlives its scenario would reach the service while that reset, or the next
scenario's own work, is in flight. The gate therefore releases on one
condition:

```text
gate release iff admission is closed and no admitted operation is running
```

Surviving objects are not part of that condition. An idle transaction or cursor
the scenario leaked holds no gate, because a sealed engine admits it nothing
further; a running backend operation holds the gate whatever owns the object
that issued it. What must be terminal is the service work, not the reference
graph.

```text
scenario body
    -> every container and raw transaction owner shuts its engine down
        -> scenario admission closes
        -> the ledger reaches quiescence
        -> base engine shutdown completes
    -> the scenario owner returns: authoritative terminal confirmation
    -> serialized gate releases
    -> next scenario resets the full key range
```

Terminality is owned by the engine the scenario hands out, not by per-test
discipline. The scenario engine decorates the real backend engine and counts
every storage operation it forwards in a balanced pair, so an operation is
outstanding exactly while it runs and no count depends on when a reference is
released. `createTransaction()` is the seam a container and a raw transaction
holder both reach, because `createOwnedTransaction` defaults through it, and it
is refused after sealing without being counted: creation returns before the
caller issues anything, so counting the moment of creation would report
quiescence for a transaction that had not yet read or written.

`executeTransaction` is forwarded to the base engine rather than decorated,
because a base engine may own that lifecycle itself and a decorator that
supplied the protocol default instead would silently replace it. The forwarded
call creates, commits, or cancels its own transaction before returning, so
admitting the call as a whole bounds it more tightly than counting its
individual operations would: it cannot outlive the scenario that started it.

| Operation class | Scenario admission |
|---|---|
| Commit | Admitted and counted: a commit that landed after its scenario would write into the next scenario's keyspace |
| Owned-transaction execution | Admitted and counted as one operation for the whole call, which the base engine owns end to end |
| Read, key select, read version, range metadata, cursor advance, buffered mutation, Directory operation | Admitted and counted |
| Versionstamp resolution | Admitted and counted: the value is answered only once the commit that produces it has been applied, so awaiting it is service work |
| Transaction creation | Refused after sealing, not counted: creation is local to the base engine and issues nothing, and what the transaction goes on to do is admitted operation by operation |
| Cancel, cursor finish | Forwarded past a closed admission and not counted: this is the work that makes a leaked transaction or cursor terminal, and it releases backend state instead of reaching the service. Any advance a finish waits on is already counted |
| Local transaction state: capabilities, compaction, storage failure, mutation byte limit, options, conflict ranges, versionstamp request, committed version | Forwarded: no service work and no state the next scenario can observe |

Sealing belongs to `endScenario()`, which only the scenario resource owner
calls, after every container and raw transaction holder the scenario body
created has been shut down. The `StorageEngine` shutdown methods only forward,
because a container storage lifecycle reaches both of them for the single
container it owns: `requestShutdown()` from `deinit`, and `waitUntilShutdown()`
from `shutdown()`. Sealing in either one would refuse the work of a scenario
that shut one container down and went on using the engine, turning a running
scenario into a false failure at a boundary that is not the scenario's own.

The decorated engine guarantees that operations in flight when the scenario
seals reach zero before the base engine shuts down, and that an operation
started after the seal never reaches the service. It does not guarantee
attribution: a rejection can be raised after the gate has released, so a
rejection the scenario observes fails that scenario, and a later one is a
blocked operation whose scenario is no longer identifiable. Each count carries
the operation kinds behind it, so one integration run identifies what it caught
without being repeated.

The wait for quiescence has no bound and no cancellation exit. A bound would
release the gate with backend work still running, which is the state the gate
exists to prevent, and a cancellation exit would do the same on every failing
scenario. The external test timeout owns the case where an admitted operation
never ends; in practice the backend bounds it first, because the coordinator
configures a FoundationDB transaction timeout, so an operation that cannot
finish fails rather than running forever. A wait that has to block names the
operations it is waiting on once, as a report that nothing reads.

The boundary check runs on the failing path as well as the succeeding one, so a
scenario that failed and also reached the service after sealing reports both:
the refusal failure carries the scenario's own failure rather than replacing
it.

Owners: [scenario admission quiescence tests](Tests/DatabaseEngineTests/ScenarioAdmissionQuiescenceTests.swift)
prove the release condition itself, including that neither cancellation nor an
open admission releases it, and
[FoundationDB scenario lifecycle tests](Tests/DatabaseEngineTests/FoundationDBScenarioLifecycleTests.swift)
prove the boundary over
[the scenario engine](Tests/Shared/ScenarioStorageEngine.swift) and
[the scenario coordinator](Tests/Shared/FoundationDBScenarioCoordinator.swift).
