# database-framework

The approved [common access API](../database-client/Sources/DatabaseAccess/DESIGN.md)
is consumed by the optional [DatabaseEmbedded adapter](Sources/DatabaseEmbedded/DESIGN.md).
Only that adapter depends on the Client-owned DatabaseAccess product; Database,
DatabaseEngine and storage execution targets retain their current dependencies.
Existing context/container/connection APIs remain independently available.

## Purpose and Scope

Package-level design authority and design index for the in-process
`database-framework` package. This document owns package composition,
dependency direction, and the direct-child index. It does not copy the
contracts owned by a child design.

- Parent: the workspace system design at [../DESIGN.md](../DESIGN.md).
- Direct children: [DatabaseEmbedded](Sources/DatabaseEmbedded/DESIGN.md), [Database](Sources/Database/DESIGN.md),
  [DatabaseEngine](Sources/DatabaseEngine/DESIGN.md),
  [DatabaseRuntime](Sources/DatabaseRuntime/DESIGN.md),
  [DatabaseMath](Sources/DatabaseMath/DESIGN.md),
  [DecimalFormatting](Sources/DecimalFormatting/DESIGN.md),
  [QueryAST](Sources/QueryAST/DESIGN.md),
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
| [DatabaseRuntime](Sources/DatabaseRuntime/DESIGN.md) | child | Explicit runtime capability and policy composition | Supplies schema-driven entity and authorization factories. | Execution and authority decisions remain with Engine and the application. |
| [Full-text measurements](Benchmarks/FullTextQuery/DESIGN.md) | child | Independent Native measurement composition | Owns host compatibility and fixture gates. | Framework deployment support remains separate. |
| [Access measurements](Benchmarks/Access/DESIGN.md) | child | Independent Native measurement composition | Owns fixtures and observer outside the production graph. | Native evidence does not prove portable runtime behavior. |
| [DatabaseMath](Sources/DatabaseMath/DESIGN.md) | child | Platform-neutral scalar math and bounded decimal formatting | Adapts numeric runtime operations consumed by execution and feature modules. | SQL overload and failure policy remain in QueryExecution. |
| [DecimalFormatting](Sources/DecimalFormatting/DESIGN.md) | child | Fixed-function bounded C formatting | Owns buffer validation and locale restoration at the C runtime boundary. | It has no external product or SQL policy. |
| [QueryAST](Sources/QueryAST/DESIGN.md) | child | SQL parsing, building, and canonical serialization | Adapts SQL syntax to DatabaseKit semantic contracts. | Grammar acceptance never proves executable SQL compatibility. |
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
    -> QueryAST (parser, builder, canonical serializer) -> DatabaseKit
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

Mutable expected test counts are owned by the executable configuration:
[backend lanes](scripts/docker/versions.env) and
[GraphIndexes target lanes](.github/workflows/ci.yml). Verification guides link
to those owners; intentional test additions update the corresponding owner.

The Docker harness's `postgresql-run` entry executes reference SQL against its
pinned isolated server and owns identity checks, logs, authoritative stop,
negative readiness, and state removal. Its external timeout keeps a supervisor
outside the command's process group: deadline expiry sends TERM, allows at most
five seconds to exit, then sends KILL and reaps the command before returning
124. This remains effective when a command changes its own signal handlers.
Command failure remains failure after cleanup; cleanup failure also rejects an
otherwise successful run.

The FoundationDB package lane admits one Swift Testing case at a time through
`swift test --no-parallel`. All cases share one isolated server; unbounded
case admission produced backend GRV rate-limit errors (1051) and transaction
timeouts (1031), while the same affected cases passed in isolation. This is a
harness scheduling contract, not a production retry or error-conversion policy.
Compilation remains parallel and each case's explicit concurrent tasks still
run concurrently. The pinned [SwiftPM command](https://github.com/swiftlang/swift-package-manager/blob/83ce3b6fc4f7c8ed23d60fcba598e9103a08a44e/Sources/Commands/SwiftTestCommand.swift)
forwards the flag to the pinned [Testing entry point](https://github.com/swiftlang/swift-testing/blob/f5f1b3154107af3f188ab513a375cc796f8e8c8f/Sources/Testing/ABI/EntryPoints/EntryPoint.swift),
whose [runner](https://github.com/swiftlang/swift-testing/blob/f5f1b3154107af3f188ab513a375cc796f8e8c8f/Sources/Testing/Running/Runner.swift)
serializes case-body admission. Changes to this scheduling rule require the
complete exact-count real-service lane, including explicit concurrency tests,
zero backend failures and authoritative teardown. Other backend lanes retain
their existing case scheduling.

The service traces of the 2026-10-14 full-lane runs show ratekeeper holding
TPSLimit at 0 with `WorstFreeSpaceTLog` 0: the transaction log shared the
Docker VM filesystem, which had about 5.6% free space, below the ratekeeper
free-space ratio. Every GRV then waited or failed (1051, 1031) regardless of
case admission. The disposable service therefore keeps `/var/fdb/data` on a
run-owned tmpfs bounded by `FOUNDATIONDB_DATA_TMPFS_SIZE`, so free-space
throttling depends on that bound rather than on the host disk's fill level.

The user deferred the full FoundationDB regression to S18. The serial-case
lane's first startup executed zero cases before the Docker runtime stalled;
its scheduling change has no successful full-lane runtime evidence yet.
Previously affected cases passed in the focused real-service lane. Native,
SQLite and PostgreSQL evidence remains local to its recorded snapshots.
The deferred lane must still satisfy exact counts and authoritative teardown
before the overall SQL task is complete.

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

### PostgreSQL basic SQL compatibility program

The accepted S00–S18 program extends the in-process engine, not a backend
SQL forwarding path. The feature inventory, PostgreSQL reference version,
explicit product differences, and stable acceptance IDs are owned by
[DatabaseKit SQL](../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md).
That inventory includes lexical syntax, queries, functions, type semantics,
DML, transactional DDL, SQL blocks, and auxiliary operations; it is not
limited to repairing SELECT. The acceptance inventory is a planned contract,
not a claim that the current source implements it.

| Layer | Sole responsibility in this program | Design authority |
|---|---|---|
| DatabaseKit | SQL semantic values, statements, results, constraints, and profile | Linked SQL design |
| QueryAST | Strict text adaptation and canonical serialization | [QueryAST](Sources/QueryAST/DESIGN.md) |
| Database | Public statement/block admission and response adaptation | [Database](Sources/Database/DESIGN.md) |
| DatabaseEngine | Binding, planning, execution, catalog, and operation lifecycle | [DatabaseEngine](Sources/DatabaseEngine/DESIGN.md) |
| StorageKit | Physical transaction checkpoints, ordered journals, and backend guarantees | [StorageKit](../storage-kit/Sources/StorageKit/DESIGN.md) |
| Server/client/host | Dispatch, transport, and publication after readiness | [System](../DESIGN.md) |

S00 establishes these responsibilities and executable acceptance evidence.
Later sprints supply implementation and invalidate evidence only where a
changed contract requires it. Every SQL path preserves [SPEC](../SPEC.md)
transaction, Directory, Partition, authority, and post-closure publication
contracts. PostgreSQL-backed storage is not PostgreSQL SQL compatibility
evidence.

S07 composes selected arrays and JSON through the
[DatabaseEngine collection boundary](Sources/DatabaseEngine/DESIGN.md#s07-collection-composition).
Existing stored value formats and transport value codecs remain authoritative;
only expression carriage and in-process semantics change. S08 composes the
[Query canonical relation contract](../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s08-frozen-relational-query-carriage)
with QueryAST's parser/emitter and DatabaseEngine's selected relation, source
presence, CTE lifecycle and optional Base merge contracts. Public Database
results and downstream Wire consumers must preserve that composition.
General set-function relation invocation remains required S10 composition.
Each implementation gate retains explicit unsupported failures until its
actual execution proof exists. Full FoundationDB, graph-enabled Database
portable and MultiBase portable integration, final URL verification and push
remain the user's S18 verification boundary.

S14 extends the existing semantic statement/block path through
[Engine DML composition](Sources/DatabaseEngine/DESIGN.md#s14-dml-composition)
and [QueryAST](Sources/QueryAST/DESIGN.md). Their children own declaration
adaptation, immutable statement reads, mutation outcomes, ordinary authorization
and transactional table/identity changes. The package facade retains its
post-closure result contract; no new storage or transport owner is introduced.


### S16 module composition

DatabaseEngine now consumes the pure QueryAST Builder contract only for canonical
SQL text returned by information_schema and EXPLAIN. QueryAST remains below that
use with dependencies only on Kit/Types; it has no Engine or storage dependency.
This does not reverse specialized feature ownership or move parsing, authorization
or execution into the serializer. The corresponding module contract is
[QueryAST](Sources/QueryAST/DESIGN.md#s16-canonical-catalog-text-consumption).

## IR-centered Execution Integration

[Database facade](Sources/Database/DESIGN.md) owns the directly consumed contract for the accepted QueryStatement/SQLBlock unification. Existing authorization, exact work budgets, first-error rollback, retained ownership and authoritative shutdown remain required. Runtime state and publication do not move into Kit declarations.

## Native Access Performance Verification

The independent [Access measurement package](Benchmarks/Access/DESIGN.md) owns
the Native executable and observation boundary outside this package manifest and
correctness test graph. Normal/Embedded functional runtime gates remain separate
evidence. CI keeps exact test-count contracts and committed benchmark dependency
locks. The bitmap/full-text lane retains its August 14 compiler and reviewed
median bounds; the unit/parity lane retains its separate September 4 compiler.

The CI workflow composes independent Bitmap, FullText, general benchmark-build,
and Access jobs. Each job owns one cold package build and its evidence; a failed
or timed-out build cannot consume a later package's measurement lifecycle.
`fail-fast: false` preserves each independent result. The existing
`Benchmark verification` status is an always-evaluated aggregate that succeeds
only when every matrix job succeeds, preserving the external check contract.

```text
Bitmap ────────────────────┐
FullText ──────────────────┤
General benchmark build ───┼─> Benchmark verification (all must succeed)
Access build + quick run ──┘
```

CI owns 1,800-second build guards and a 45-minute outer job limit. The observed
1,200-second expiry occurred while Bitmap's last test target was compiling,
after continued compiler progress; it was not a runtime median failure.
These build/lifecycle operating bounds do not change any request budget,
reviewed latency maximum, expected test count, or warning gate. Runtime tests
retain their existing separate timeout owners. Changing a lane's compiler,
SDK, package graph, executable or measurement inputs invalidates its evidence.


### AS25 integrated acceptance (2026-10-25)

The URL Client reference `89f03891f9d6700e7eb6d10fd04aaa1a50bf0d5f`
includes shared generation admission for named execution and continuation.
The [Embedded adapter contract](Sources/DatabaseEmbedded/DESIGN.md) remains
under the original context's authority, budgets, rollback and lifecycle.
Native focused3 and consolidated2633 passed with zero failures, skips,
expected failures and runtime warnings. Exact Swift 6.4.0 release standard and
Embedded SDKs each built, linked and executed all four adapter cases; the
Embedded compiler command explicitly enables Embedded for wasm32-unknown-wasip1.
Client's own Native100/MultiBase103, JavaScript77 and portable12-per-target
proofs remain source-matched. Node emits its WASI ExperimentalWarning; no
portable trap or case failure occurred. Existing connection/container source
files and initial user files retain their recorded hashes.

[Integrated evidence](../.codex-tools/access-shutdown-20261025/integration-verification.json)
links the runtime records, unchanged-source guards and
[complete Native performance acceptance](Benchmarks/Access/Sources/DatabaseAccessPerformanceContract/DESIGN.md).


### RX25 execution refactoring scope

TAIL25 closes the remaining scalar-allocation and concurrent-CTE timing gaps
under [SQLScalar](Sources/DatabaseEngine/QueryExecution/SQLScalar/DESIGN.md#tail25-control-and-value-ownership)
and the [performance owner](Benchmarks/Access/Sources/DatabaseAccessPerformanceContract/DESIGN.md). It keeps
the original single-meter, authority, rollback and lifecycle boundaries; SQLite
transaction capacity remains a StorageKit contract.

TAIL25 source`f0286841` passes Native2650 and release standard/Embedded four-group
runtimes. The [matched measured results](Benchmarks/Access/Sources/DatabaseAccessPerformanceContract/DESIGN.md#tail25-measured-results-2026-10-25)
retain both allocation baselines, common-origin/rank timing, identical-binary and
WAL capacity controls, and the original tail events with the fixed pooled audit.

SPEED25 reopens performance acceptance and removes measured repeated frame,
single-row projection and ORDER fingerprint work under the
[QueryExecution contract](Sources/DatabaseEngine/QueryExecution/DESIGN.md#speed25-repeated-execution-costs).
Existing correctness evidence remains scoped to its source; latency and
throughput require paired measurement and are not inferred from passing tests.
The [measured acceptance](Benchmarks/Access/Sources/DatabaseAccessPerformanceContract/DESIGN.md#speed25-measured-results-2026-10-25)
records actual speed gains, the concurrent small-workload p50 variation and
the remaining historical allocation-volume cost separately.
SPEED25 source at`c2e5630c` passed Native2649 and exact Swift6.4.0 release
standard/Embedded four-case runtimes, including100-row/eight-column computed
projection and canonical fingerprint ties. Native and portable evidence is
source-hash attributed; the portable checks are functional, not speed claims.

This task preserves public invocation/result APIs, original authorization,
request budgets, rollback and final publication while refining four execution
responsibilities. [QueryExecution](Sources/DatabaseEngine/QueryExecution/DESIGN.md)
and its SQLRelations child own positional transfer, ordering and scalar control;
[SQLBlock](Sources/DatabaseEngine/SQLBlock/DESIGN.md) coordinates SQL admission
with the sequence, transaction and schema owners. Lower owner designs are
updated before their implementations. The workspace PROGRESS owns sprint order
and completion evidence; unrelated user changes remain outside task commits.


Historical RX25 verification at5ef46212 used Native2647 (Engine1437, Database233, SQLite152, QueryAST825)
and exact Swift6.4.0 release standard/Embedded four-case runtime fixtures.
The async scalar adapter retains one necessary suspension while synchronous
instructions and positional reads avoid per-node scheduling. Sequence metering
has one request owner. Authorization, typed failures, original budgets,
rollback, publication and connection lifetime remain the existing contracts.
[Performance acceptance](Benchmarks/Access/Sources/DatabaseAccessPerformanceContract/DESIGN.md#rx25-matched-refactoring-measurements-2026-10-25)
records measured allocation reduction and the unresolved elapsed-time variation;
it does not claim a speed gain from the shared-desktop timing pair.
