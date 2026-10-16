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
