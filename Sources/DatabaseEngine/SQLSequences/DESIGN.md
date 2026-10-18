# SQLSequences

## Purpose and Scope

This component owns durable nonrollback sequence allocation and request-local
CURRVAL/LASTVAL for S13. Parent: [DatabaseEngine](../DESIGN.md). Children: none.

## Responsibilities and Boundaries

Sequence declaration and identity belong to Kit; catalog generation belongs to
SQLCatalog/Schema; data-attempt rollback and authoritative close belong to
Transaction. This owner neither opens a nested allocator transaction nor alters
StorageKit retry classification. A container-owned actor serializes admitted SQL
requests; durable allocator transactions serialize allocation across processes.
The bounded profile does not promise PostgreSQL nonblocking concurrency.

## Related Designs

| Design | Relationship | Contract used | Summary | Cautions |
|---|---|---|---|---|
| [Engine](../DESIGN.md) | parent | sequence lifecycle | Composes request effects. | No readiness before recovery closes. |
| [SQLBlock](../SQLBlock/DESIGN.md) | used by | replay after closed data attempt | Owns outer allocation loop. | Cleanup failure is terminal. |
| [Kit](../../../../database-kit/Sources/DatabaseKit/Schema/SQLCatalog/DESIGN.md) | depends on | SQLSequence | Owns numeric bounds and stable IDs. | No mutable cursor in declarations. |
| [SQLCatalog](../SQLCatalog/DESIGN.md) | coordinates with | immutable constant-default bindings | Admission fixes literal targets before runtime effects and dependency checks. | Rebuild only from canonical declarations, never the caller path. |
| [Transaction](../Transaction/DESIGN.md) | depends on | runner/Partition lease | Authoritative independent commit/close. | Allocation is outside the data attempt. |

## Architecture

```text
SQLBlock request -> container FIFO permit -> data attempt
    -> sequence effect -> cached occurrence value
                       -> private allocation demand
    -> rollback + authoritative close
    -> independent allocator transaction + durable recovery marker
    -> replay same request, meter, deadline, temporal timeline
    -> data commit + close -> remove recovery markers + close -> Ready
```

## Contracts and Invariants

Each effect uses the complete expression or an exact identity-column occurrence,
statement index and dynamic ordinal. A replay mismatch in target/arguments fails
rather than reusing an unrelated value. Per-request cached effects are charged
before retaining them; work/deadline never rewind. NEXTVAL and SETVAL persist an
allocator cell and request-event recovery marker atomically. Replay reuses that
marker/value; allocation gaps after failed requests are legal. CURRVAL/LASTVAL
are rebuilt by replay and survive SQL savepoint rollback. SETVAL(false) does not
initialize CURRVAL; SETVAL(true) does, without changing LASTVAL's sequence.
Checked addition enforces bounds; CYCLE and explicit rewinds may intentionally
repeat values. No automatic retry or restart repeats a consumed allocator value.

Sequence CREATE/ALTER/DROP stage catalog generation inside the data transaction.
CREATE's stable declaration ID permits an allocation cell to exist before catalog
commit; rollback leaves an unreachable cell, never a visible sequence. ALTER
without restart keeps the cursor; transactional RESTART is staged in the data
transaction and becomes visible only on commit. Allocation after a staged restart
must preserve that restart's transactional rollback semantics.

## State, Ownership, and Lifecycle

| State | Owner | Lifetime |
|---|---|---|
| FIFO admission | DBContainer actor | Request, including mandatory close |
| Event cache/current values | SQLSequenceRequest actor | One request, all attempts |
| Allocator cell | selected Partition system root | Durable across restart |
| Recovery marker | same transaction as allocation | Until authoritative request cleanup; interrupted markers remain completed durable evidence, never pending work |
| Data changes/catalog | DatabaseTransaction | Checkpoint/attempt lifecycle |

Native, WASM and Embedded use identical actor/Sendable state. No target branch
removes isolation. No external callback runs under a memory lock.

## Failure, Concurrency, and Constraints

Admission uses the same `Task.isCancelled` guard and `CancellationError` on all targets, including before queuing and after permit delivery. The actor returns a raced permit before propagating cancellation. This avoids the standard library checkCancellation overload that is unavailable in Embedded without weakening cancellation or isolation. Sequence administration explicitly imports StorageKit because Embedded specialization requires the storage protocol owner at its call site.


Unknown sequence, uninitialized CURRVAL/LASTVAL, exhausted bounds, malformed
persisted cells, invalid arguments and replay divergence remain typed failures.
Only the private demand is intercepted, after cleanup succeeds. A composite
operation/cleanup failure cannot enter allocation. Commit-unknown propagates;
its durable marker prevents reallocation when the committed value is observable.
Request termination never rolls back already committed consumption.

## Verification and Change Impact

SQLSequenceExecutionTests owns NEXTVAL/SETVAL boundaries, request history,
rollback/retry stability and durable restart. SQLBlock tests own public readiness,
cancellation and first-error rollback. Real SQLite evidence must execute the
independent transaction after the original transaction closes; savepoint release
is not independent durability. Changes recheck selected/raw evaluators, Mutation
identity adaptation, catalog staging and Transaction closure.


### Interrupted requests and recovery retention

The allocator atomically commits its complete cell and recovery value; there is
no pending replay or repair operation after that commit. A process interruption
may leave a completed marker or an unreachable CREATE/RESTART incarnation. Those
are safe retained durable metadata, with at most one marker/cell per admitted
request effect and no scan required for readiness. They do not become a second
allocation source. Normal request close removes its marker range in one bounded
transaction; a failed close exposes no Ready result. This component does not
claim global garbage collection of interrupted-request metadata. SQLSequenceSQLiteTests'
interrupted-marker fixture is a deterministic durable-state fixture, not a
separate-process crash or PostgreSQL crash-parity test.

The FIFO wait owns the same absolute operation deadline and cancellation. A
queued request is removed and fails without waiting for the current owner. A
grant racing cancellation/deadline is returned before the queued request fails.


### Numeric ALTER and constant dependencies

SQLSequence persists its declared smallint/integer/bigint type. ALTER without
AS preserves that type; NO MINVALUE/NO MAXVALUE uses its signed range and
increment direction. ALTER AS replaces prior default bounds with the new type
bounds and validates retained explicit bounds. START changes declaration metadata;
RESTART alone changes the transactional head.

ALTER preserves its stable sequence identity while numeric options, ownership,
logical name and namespace may change. S15 persists direct literal default
bindings in SQLColumn.sequenceBindings, so rename never re-resolves an existing
default through its original spelling. Protected identity policies retain their
sequence and owner contracts through logical rename.
At catalog admission, direct string and SQL unknown-string literal arguments to
NEXTVAL/CURRVAL/SETVAL bind to stable sequence IDs. The publication API has no
search path: unqualified names resolve only in the declaring relation namespace;
qualified names resolve the named namespace. A missing constant target rejects
publication. Explicit text casts/dynamic arguments remain late-bound.
SQLCatalogSnapshot retains immutable per-column literal-to-ID maps and restores
them from canonical persisted SQLSequenceBinding declarations. A declaration
without retained bindings is resolved once on admission; stale or unused
persisted bindings reject rather than retarget. Unchanged policies cannot
retarget a different ID during catalog replacement. During default evaluation
(including referential SET DEFAULT), task-local binding selects those IDs only
for direct literal occurrences. Ordinary statement expressions continue to use
the request search path. DROP compares the admitted IDs without re-resolving
any default through its caller search path.
RESTRICT preserves dependent identity/default/ownership declarations. CASCADE
removes only affected policies in the same catalog generation transaction,
retaining the physical column and unrelated constraints. Checkpoint rollback
restores those policies with the sequence declaration.

### Exact builtin identity

FunctionCall carries the exact semantic identifier. Selected binding, effect
admission and sequence dispatch recognize lowercase `nextval`, `currval`,
`lastval` and `setval`, either unqualified or under `pg_catalog`. SQL lexical
folding belongs to the parser; a quoted uppercase name is not a builtin alias.
Semantic test fixtures construct those canonical identifiers directly. The
existing selected binder's argument coercion and occurrence/arena forwarding
remain authoritative before request effect dispatch.

## S14 TRUNCATE Identity Composition

SQLBlock resolves TRUNCATE targets and Transaction computes the complete
foreign-key table closure. This component consumes only the resulting exact
relation IDs and the admitted caller transaction. The package request seam is
`restartOwnedIdentities(relationIDs:step:transaction:) async throws`.

```text
TRUNCATE effective table set
  -> exact owned identity sequence IDs, deduplicated in catalog order
  -> request-stable forkHead(step:start:)
  -> stage head keys in the same caller transaction as row removals
  -> commit exposes new incarnations / rollback restores old heads
```

RESTART selects sequence declarations whose `ownedBy` identifies an identity
column of an effective relation and whose column identity policy references
that same sequence. A default expression's NEXTVAL reference alone does not
confer ownership. Unowned sequences, identity columns outside the effective
set and CONTINUE mode receive no head write. Multiple columns referring to
one sequence stage its head once. Restart uses each declaration's configured
start; it changes no declaration, schema generation or catalog publication.

The existing nonce/step incarnation mechanism keeps head selection stable
through allocation-demand replay and storage retry. Head writes participate
in the caller's physical checkpoint journal. NEXTVAL and SETVAL continue to
allocate through independently committed cells and recovery markers after
the original data attempt has authoritatively closed. Consumption on a staged
incarnation can remain unreachable after rollback; it never changes the
restored prior incarnation. CURRVAL/LASTVAL preserve the existing request
history contract across SQL rollback. No new sequence cache or authority
boundary is introduced.

This is identity-sequence restart only. Hidden physical SQL row identity is
owned by Mutation's logical request timeline and is independent of SQL
sequence allocation. Compiled-model declared identifiers retain their model
adaptation contract.

SQL TRUNCATE tests must prove rollback of rows and owned heads together,
CONTINUE behavior, CASCADE ownership closure, non-owned/default-only exclusion,
restart with a nondefault start, and allocation-demand replay after restart.
Existing sequence durability and nonrollback tests remain authoritative for
ordinary NEXTVAL/SETVAL; this section does not reinterpret those effects as
rollbackable allocations.
