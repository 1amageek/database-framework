# Transaction Coordination

## Purpose and Scope

This component owns attempt-scoped DatabaseTransaction semantics, operation
serialization, mutation preparation/validation and the active transaction
binding. Parent: [DatabaseEngine](../DESIGN.md). Children: none.
The S06 change adds logical transaction time without moving storage commit,
retry, cancellation or backend authority into query values.

## Responsibilities and Boundaries

DatabaseTransaction owns its open/executing/preparing/closed attempt state,
operation gate and mutation journal. The existing TransactionRunner/storage
executor owns retry and authoritative commit/cancel. Core owns the public
logical operation and captures transaction time before entering retry.
Read attenuation owns narrowed access, while this binding preserves its
admitted root, schema lease, authorization and immutable transaction time.
SQLTemporal owns precision and clock-result interpretation.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Transaction and mutation composition | Owns cross-component invariants. | No clock data widens storage authority. |
| [Core](../Core/DESIGN.md) | used by | Logical entry and retry boundary | Captures the stable transaction instant. | Attempt construction must receive it explicitly. |
| [Read](../Read/DESIGN.md) | coordinates with | Scope and attenuation | Preserves clock data when actor access is removed. | Clock state cannot replace admission. |
| [Mutation](../Mutation/DESIGN.md) | coordinates with | Prepared change and retained payload | Performs selected assignment/predicate semantics. | One mutation statement gets one fresh statement environment. |
| [SQLTemporal](../QueryExecution/SQLTemporal/DESIGN.md) | depends on | Immutable environment | Supplies selected clock values and zone data. | Monotonic deadline time is separate. |
| [Base](../Base/DESIGN.md) | coordinates with | Member and decision bindings | Rebinds admitted roots while retaining logical time. | Member reads must not capture a new transaction instant. |
| [StorageKit Storage](../../../../storage-kit/Sources/StorageKit/Storage/DESIGN.md) | depends on | Configured opaque transaction checkpoints | Restores physical writes, overlay and Directory metadata. | The backend owns journal meaning; Framework never reconstructs backend keys. |
| [Schema](../Schema/DESIGN.md) | coordinates with | Immutable staged aggregate and authoritative publication | Owns candidate meaning and construction. | A candidate is request-local until the enclosing commit succeeds. |
| [SQLBlock](../SQLBlock/DESIGN.md) | used by | Ordered statements over one admitted attempt | Owns names, block state and ordered collecting results. | SQL control syntax and remote invocation belong to later sprint owners. |

## Architecture

```text
Core logical operation: capture transaction instant outside retry
    -> TransactionRunner attempt
        -> DatabaseTransaction + active binding
            -> serialized statement/mutation
                -> prepareForCommit or invalidate
                    -> runner authoritative terminal action
```

## Contracts and Invariants

- A production attempt receives the same logical transaction instant as every
  retry of that operation. Nested bindings and read attenuation copy it.
- Direct SPI construction represents a new caller-owned logical attempt and
  may capture its injected container clock once; Core production callers pass
  their already-captured instant rather than relying on that default.
- Each public Mutation statement derives a fresh statement instant while
  retaining this actor's transaction instant and container configuration.
- Active binding validation still checks container, roots/domain, schema,
  authorization and MultiBase resource/access. Time is not authority.
- Removing databaseTransaction in admittingRead removes mutation reachability;
  it does not remove or re-sample immutable transaction time.
- Actor state, the existing operation gate and journal remain the same on all
  targets. No new raw shared mutable state or conditional isolation is added.

## Runtime Flows

```text
explicit transaction
  -> repeated public statements: same transaction time, fresh statement time
    -> nested/subquery/session: same statement environment

retry
  -> new storage attempt/actor/binding, same logical transaction instant

failure/cancel
  -> existing invalidate and runner cleanup, release environment references
```

## State, Ownership, and Lifecycle

The actor owns one transaction attempt. Its immutable transactionInstant is
retained alongside the existing container and read policy. TaskLocal binding
is a scoped immutable bridge, not a global mutable registry. The container-
bound statement environment ends with its operation; no temporal context is
persisted or emitted as a storage token. Existing actor shutdown/invalidation
and authoritative runner cleanup remain required before success escapes.

## Failure, Concurrency, and Constraints

Overlapping entry, closed attempts, invalid context, mutation/validation,
resource, storage and cancellation failures retain their existing typed
contracts. CLOCK_TIMESTAMP can advance or move backwards with the injected
wall clock without affecting monotonic retry/deadline behavior. Environment
propagation performs no I/O or callback under a Mutex. A missing environment
at a temporal scalar boundary is a typed failure rather than a fabricated time.

## Verification and Change Impact

DatabaseEngineTests and DatabaseTests own transaction/statement clock tests
using an advancing Mutex-protected WallClock: multiple statements, retry,
attenuation, nested reads, Mutation atomic failure and Composition members.
Unchanged transaction lifecycle regression remains required. Exact target
portable tests prove applicable Core/Read/Transaction propagation; Graph-
enabled Database and MultiBase portable integration remain S18 evidence.
Changes revalidate Core entry, Read session copies, Mutation scalar admission,
Base bindings and selected SQL execution together.

## S12 Checkpoint Contract

This section fixes the lower execution boundary for S12. Implementation and
behavioral verification remain task evidence rather than implications of
these interface declarations. The public immutable SQL block stops at its first
statement error, rolls back the whole request, and throws the typed error;
later rollback steps are not executed. Its separate lower callback capability
can explicitly recover a failed attempt with rollback-to. That capability
does not alter the public block's failure policy.

### Confirmed implementation and required delta

| Current implementation | Required checkpoint delta |
|---|---|
| `DatabaseTransaction.performOperation` closes the actor after an operation error. | Keep that behavior for ordinary transactions; a separately admitted checkpoint-capable attempt retains rollback-only failure state. |
| `TransactionMutationJournal` keeps first-seen order, original existence and current values until final validation. | Restore these logical effects alongside the physical checkpoint, including ownership of superseded values. |
| `subspaceCache` keeps successful Directory resolutions for an attempt. | Clear resolved entries after rollback so a removed or restored Directory is resolved again. |
| `TransactionOperationGate.closeAndWait` permanently closes admission. | Add a checkpoint barrier phase that blocks new root admission, drains admitted work and scoped cleanup, and reopens only after a successful nonterminal barrier. Terminal close remains permanent. |
| Scoped reads attenuate storage and await cursor/scope invalidation before returning. | Register admitted bound read scopes with the checkpoint attempt so an independently entered read cannot race physical restore. |
| Core creates one new actor for each runner attempt. | Keep checkpoint tokens, undo owners, read-scope registrations and candidates attempt-local; none survive a retry. |

### Responsibilities and API

`DatabaseTransaction` owns the joined physical/logical checkpoint. StorageKit
owns `TransactionCheckpoint`, `TransactionCheckpointAccess`, its configured
limits and physical journal. The actor consumes that capability only when the
transaction lifecycle owner configured it before the first storage access.
Core privately injects checkpoint control separately from attenuated data
access. A read-authorized callback receives read-only storage plus the
actor's bounded logical checkpoint operations; the read wrapper continues
to expose no checkpoint capability and rejects every data/Directory write.
The actor never returns its private physical control and clears it at
terminal preparation/invalidation. The runner alone retains commit and
cancellation authority. A read-only block does not require a write grant
merely to create, roll back or release a checkpoint.
Absence of the capability is an explicit unsupported failure, not an emulated
Framework key journal.

The actor-facing surface is:

```swift
public protocol DatabaseTransactionCheckpointing: Sendable {
    func createCheckpoint() async throws -> DatabaseTransactionCheckpoint
    func rollback(to checkpoint: DatabaseTransactionCheckpoint) async throws
    func release(_ checkpoint: DatabaseTransactionCheckpoint) async throws
}
```

Core admits the new explicit callback entry below. Its public default access
matches ordinary explicit transactions; the existing package entry carries
the selected `requiredAccess` without widening principal or Partition rights.
An existing ambient transaction cannot become a separately committed callback;
its owner uses the already admitted actor instead.

```swift
public func withCheckpointTransaction<Result: Sendable>(
    configuration: TransactionConfiguration = .default,
    checkpointLimits: TransactionCheckpointLimits,
    workMeter: DatabaseWorkMeter,
    executionDeadline: TransactionExecutionDeadline? = nil,
    _ operation: @Sendable @escaping (DatabaseTransaction) async throws -> Result
) async throws -> Result
```

The runner receives optional checkpoint limits and configures its newly owned
transaction before the first storage option, read-version selection,
Directory admission or callback. A fresh actor binds the exact request meter
on every automatic attempt. The entry returns only after authoritative close,
required Schema publication and the applicable cancellation check. Existing
normal callback entries keep their admission, retry and failure behavior.

SQLBlock uses the existing exactly-once `DatabaseReadResultBox` to carry a
noncopyable collecting result through this Copyable callback boundary. The
box transports ownership; only authoritative completion establishes closure.
SQLBlock consumes the returned collecting owner and alone constructs its
purpose-specific Ready result. No new generic transfer abstraction or change
to the runner's generic result contract is required.

`DatabaseTransactionCheckpoint` is an opaque, immutable, attempt-bound token.
Only the actor creates it. The token carries no commit/cancel authority, SQL
name, storage address, schema definition or caller-selectable ordinal. An
actor record associates it with one StorageKit token, one logical undo
position and the optional immutable Schema-owned staging state. The actor
conforms to the protocol; Core and SQLBlock choose whether to admit the
checkpoint-capable execution path. Ordinary `withTransaction` callers retain
their existing failure contract.

SQLBlock owns exact name comparison, duplicate-name shadowing and lookup. The
actor owns token validity: rollback retains the target marker and invalidates
later markers; release invalidates the target and later markers while keeping
their mutations. Foreign, released, superseded and terminal-attempt tokens
fail explicitly before physical work. Operation identifiers and token identity
counters never rewind. The StorageKit and actor marker transitions must agree.

The lower callback may catch a recoverable statement failure and explicitly
roll back to a valid checkpoint. While failed, it can only request rollback or
terminal invalidation; reads, mutations, checkpoint creation, release and
commit preparation reject the failed state. A successful rollback clears that
state only after both physical and logical restoration. Cancellation,
deadline expiry, a failed restoration/cleanup, and an unknown commit outcome
are terminal and cannot be converted into recoverable statement failures.
Retryable backend failures leave the attempt and reach the runner's existing
retry policy; savepoint recovery does not reset or replay a backend attempt.
The public block never converts its first statement failure into a later
command result by using this lower recovery capability.

All actor operations, including package operations admitted by an operation
identifier and derived model maintenance, use the actor's single failure
classification. These nested operations must not close a checkpoint attempt
before its recoverable statement error reaches the callback. Ordinary
transactions still close, and terminal failures keep the same classification
as root operations.

### Barrier and state flow

```text
admitted checkpoint attempt
    -> ordered statement/read/mutation scopes
        -> reject new root admission at checkpoint barrier
            -> drain admitted operations and derived maintenance
            -> invalidate and await registered read sessions/cursors
                -> create / physical rollback / release
                    -> restore logical undo position and Schema candidate
                    -> discard stale subspace cache and abandoned owners
                        -> reopen admission or remain rollback-only

final success
    -> permanent admission close and the same scope drain
        -> final validation of restored current models
            -> detach prepared Schema publication, if any
                -> runner authoritative commit
                    -> nonthrow Schema publication -> Ready values

failure / cancellation / commit-unknown
    -> permanent close, drain and invalidate
        -> discard checkpoint/journal/candidate owners
            -> runner authoritative outcome, preserving cleanup failures
```

The barrier is not quiescence inferred from actor isolation. Reentrant actors
can have suspended operations, and ambient SELECT uses a separate read owner.
The existing binding therefore connects checkpoint-capable reads to an
attempt-owned scope registration. Registration occurs before any backend
read, closes new admission when the barrier begins, and retains the scope
until its awaited invalidation completes. The barrier detaches its cleanup
registrations before awaiting them, breaking retention cycles. Callbacks and
I/O execute outside Mutex critical sections. A checkpoint requested from a
scope it would need to drain is rejected explicitly instead of waiting for
itself. No arbitrary parallel statement scheduler is introduced.

SQLBlock's sequential collecting statement path already awaits each read
session's invalidation and each mutation context's `closeAndWait`; that is its
normal barrier entry. The registration also covers an independently admitted
bound read, rather than relying on every caller being sequential. An escaped
read or derived capability remains revoked after the barrier. Successful
rollback does not resurrect a cursor, continuation, producer or old scope.

SQLBlock enters each read or mutation statement through the actor's package
boundary:

```swift
package func withStatementScope<Result: Sendable>(
    _ operation: @Sendable () async throws -> Result
) async throws -> Result
```

It admits one statement, binds the current candidate lease (or original
attempt lease) to both `DatabaseSchemaExecutionScope` and a copied active
transaction binding, then awaits the closure and its scoped cleanup. The
closure opens the existing `DatabaseReadSession.withSession` once for SELECT,
or invokes the existing mutation executor on this actor. The request meter
is already bound; temporal environment capture stays with the statement
owner. A noncopyable statement carrier can use the existing result box at
this Copyable boundary. Read or mutation failure enters the same rollback-only
state for a lower callback, while SQLBlock propagates it as whole-request
failure. The bound policy is not changed by rewriting an ancestor TaskLocal.

### Logical state and accounting

| State | Checkpoint or terminal rule |
|---|---|
| Mutation journal entries, first-seen order, original-existence bit and current models | Append bounded undo information before changing the owned entry; reverse to the marker after physical rollback. Final validation/effects see only restored current state. |
| Superseded retained payloads and undo slots | A concrete undo owner retains and accounts for them until the final marker needing them is released or invalidated. |
| Journal lookup and reserved capacity | Capacity retained for restoration stays accounted; undo never relies on an unreserved replacement allocation. |
| Scheduled writes/deletions and active derived mutation identities | Scope-local sets drain to empty before a checkpoint; they are not copied while maintenance is in flight. |
| Resolved Directory/subspace cache | Invalidate after rollback; rebuild through the admitted Directory contract on demand. |
| Schema staging state | Retain the immutable Schema-owned value at the marker; restore it only after physical rollback succeeds. |
| Work, elapsed deadline, cancellation, admission/token counters | Never restore or refund. Control and undo processing consume the request's existing work meter. |
| Collecting result values and reservations | Remain owned by their actual result owners. A checkpoint marker never refunds claims for a surviving result. |
| Idempotency/preconditions, primary/index/relationship/security storage effects | Restore through the same physical journal and canonical mutation pipeline; do not add a second Framework effect store. |

A shallow copy of `TransactionMutationJournal` is not a checkpoint: its
mutable reservation accounting would still alias the live journal. The undo
owner must reserve structural and retained-value growth before allocation or
mutation, release only abandoned claims, and preserve prior state if admission
fails. Immutable candidate/result owners may be shared by reference because
their reservation lifetime follows the retained owner, not a copied counter.
Every rollback/release/full close includes the claims of all invalidated
markers. Backend checkpoint limits and the request meter bound these owners;
no guessed SQL-specific checkpoint count is introduced.

Undo accounting reuses `DatabaseIntermediateReservation.reserveChild`,
`absorbGuaranteedPartial` and `absorbAll` to move already admitted claims
between current-journal and undo owners. Restoration does not release a claim
and then reserve it again: a limit failure between those actions would prevent
recovery of state the checkpoint already retained. No second reservation
registry is introduced.

### Schema lifecycle coordination

Schema owns the immutable `DatabaseSchemaStagingState` and the meaning
of its candidate lease and prepared publication. The transaction actor owns
one optional current state and includes it in checkpoint records. Schema's
catalog-only staging extension validates and writes the aggregate through this actor's
caller-owned storage access before installing the candidate value. It does
not invoke the current independent-commit `publishSQLCatalog` path.
The physical schema, runtime, mutation maintainers and index layout stay fixed
in S12; physical schema staging and dynamic runtime replacement belong to S15.
The current candidate changes the read policy/catalog lease used by subsequent
statements, and rollback restores that policy together with the candidate.

The coordination uses `prepareForCommit()` for existing permanent
drain/final validation, followed by package-only
`takePreparedSchemaPublication()` for the checkpoint-aware Core path. The
transfer removes publication ownership from the closed actor and returns an
optional Schema-owned prepared publication. Core carries it beside the
collecting value across the runner's commit and invokes Schema's nonthrow
publication hook only after authoritative success. Failure and commit-unknown
discard it; neither calls the publication hook. Candidate construction and
all fallible preparation happen before commit dispatch. The existing
`prepareForCommit()` Void contract remains unchanged for other callers.

Rollback assigns the saved immutable staging state after restoring storage;
discarding the abandoned candidate releases only claims whose last owner has
gone. Root/domain/Partition/principal admission remains the original attempt's
authority. Schema lease rebinding uses the Schema owner's candidate contract;
the transaction owner does not independently reinterpret catalog mappings.
S12 introduces no SQL DDL grammar, constraints, sequence allocation or remote
transaction lifecycle through these hooks.

The actor's trusted execution SPI is
`stageSQLCatalog(_:expectedGeneration:idempotencyKey:) async throws -> DatabaseSchemaPublicationResult`.
It derives authorization, current source lease and the already bound meter
from its admitted attempt and calls the Schema-owned helper. A nonnil staging
outcome state replaces the actor's current state and read policy; an old
idempotent replay returns its publication result without replacing the latest
candidate. ReadPolicy exposes its retained immutable lease and authorization
only to the package execution owner for this derivation; caller input cannot
replace them. `takePreparedSchemaPublication()` returns the Schema-owned
`DatabasePreparedSchemaPublication?` after successful preparation. Schema owns
that value's construction and the nonthrow publication hook in DBContainer;
Transaction and Core only coordinate transfer and ordering.

### Read-wrapper propagation

StorageKit's result-free `TransactionReadAccess.performOperation` is a
requirement, with a direct-body default for transactions without checkpoints.
Every Framework wrapper forwards it to the underlying admitted access rather
than inheriting that default. `TransactionAccess.checkpoints` remains a
mutation capability and is never forwarded by a read-authorized wrapper.

| Concrete wrapper or boundary | Required propagation |
|---|---|
| Core `ContainerTransaction` | Forward `performOperation` and checkpoint configuration/capability to the owned backend transaction while retaining the container operation lease. |
| Core `ContainerTransactionAccess` | Forward `performOperation` and the configured checkpoint capability to the borrowed access; it acquires no lifecycle authority. |
| `ReadAuthorizedTransactionAccess` | Acquire/validate a read-scope operation, resolve the parent, forward `performOperation`, validate completion, and end the scope operation on success/failure. Return no checkpoint capability. |
| `DatabaseReadTransaction` | Forward the read-only operation scope to its retained `ReadAuthorizedTransactionAccess`; expose no mutation/checkpoint API. |
| Directory `ContainerDirectoryAccess` | Keep the full outer compound scope while borrowing the raw backend transaction for each node. Unwrapping preserves the backend's concrete type and does not replace the outer operation admission. |

The concrete path is container engine -> container transaction wrapper ->
optional direct/scoped read authorization -> `DatabaseReadTransaction`.
Scoped authorization can resolve another parent authorization wrapper, so
each level retains and validates its read-scope lease until the forwarded
operation completes. InstrumentedTransaction and CompositionDecisionTransaction
do not conform to the StorageKit access protocols; their existing retained
access follows the same concrete chain without adding a default witness.

An absolute Directory path walk and Partition generation validation consist
of multiple node reads. If a container/read wrapper inherits the direct-body
default, its outer compound admission never reaches the journal: each node
may finish its own backend scope, allowing rollback between parent and child
resolution. Forwarding prevents that counterexample. Framework-owned relative
Directory walks must also bracket their complete loop through the same typed
StorageKit operation executor. Per-node raw borrowing alone cannot establish
a stable compound boundary.

### Falsifiable completion evidence

The following case IDs are planned behavioral obligations, not test results.
The owner is [DatabaseEngineTests](../../../Tests/DatabaseEngineTests);
[DatabaseTests](../../../Tests/DatabaseTests) and the production backend suites
own public block and concrete backend composition evidence.

| ID | Required counterexample-sensitive evidence |
|---|---|
| S12-T01 | Normal operation failure still closes an ordinary transaction; an admitted checkpoint attempt rejects nonrollback operations after failure and accepts them only after successful rollback. `DatabaseTransactionCheckpointTests.statementMutationFailureAllowsRollback` drives a typed maintainer failure through the real statement mutation executor and its operation-ID apply path, restores the original row and effects, then commits a subsequent statement. |
| S12-T02 | Same identity is inserted/updated/deleted on both sides of nested markers; rollback restores net effect kind, order, current model, final-validation inputs and persisted data together. |
| S12-T03 | Duplicate-name policy is exercised above this component; actor tests reject foreign/released/later/old-attempt tokens and prove target retention, release-without-undo and repeated rollback. |
| S12-T04 | Suspend a real mutation maintainer and an actual bound read/cursor; the barrier blocks fresh admission and exposes no restored state until derived/read cleanup completes. Reentrant checkpoint requests fail rather than deadlock. |
| S12-T05 | Create/remove/move Directory state after a marker; rollback plus later fetch/recreate uses the restored Directory and never the abandoned subspace cache. Primary/index/relationship/security and precondition/idempotency effects match. |
| S12-T06 | Retained old/new payloads, marker growth and failed admission have exact/one-past meter witnesses. Repeated rollback does not refund work/deadline; surviving result claims persist, abandoned claims release, terminal retention reaches zero and same-meter follow-up works. |
| S12-T07 | Stage Schema-owned candidates before/after a marker. Rollback restores local lookup and aggregate persistence together; final commit publishes only the surviving candidate, while failed/cancelled/unknown outcomes never publish. |
| S12-T08 | Inject a retryable failure and capture old actor/token/scope handles. A new attempt owns fresh state; old handles reject access, work/deadline remain request-owned, and commit-unknown is neither replayed nor reported as rollback success. |
| S12-T09 | Suspend final validation, backend commit and required cleanup. No Ready result escapes early; operation plus cleanup failures retain both causes, and publication is transferred/invoked at most once after authoritative success. |

Native, standard WASM and Embedded use the same actor, logical storage,
admission and Sendable contracts. Target-specific verification proves the
selected compiled and runtime path; host tests do not establish portable
synchronization behavior. The existing lifecycle/journal/clock tests remain
regression owners. StorageKit conformance and each selected production backend
must additionally exercise the physical journal; an InMemory-only result does
not close S12's backend evidence.
