# SQLBlock

## Purpose and Scope

This S12 component owns single-request semantic SQL block execution
and the aggregate result lifecycle. Parent: [DatabaseEngine](../DESIGN.md).
Children: none. This document freezes the component boundary and public
first-error policy; no production implementation or behavioral evidence is
implied by the design alone.

## Responsibilities and Boundaries

- Execute Kit block steps in order under one admitted database transaction,
  exact work meter, schema generation and authority.
- Coordinate named checkpoints with Transaction's opaque checkpoint token
  and logical-state restoration; do not implement backend mutation semantics.
- Retain ordered collecting statement results through authoritative close,
  cancellation admission and a consuming public-output transfer.
- Leave SQL grammar to QueryAST, declaration meaning to Kit, physical journals
  to StorageKit, transaction state to Transaction, selected query evaluation
  to QueryExecution, mutation maintenance to Mutation and generation staging
  to Schema. New Wire operations and transport dispatch remain S17.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Operation authority and purpose-specific readiness | Composes this request owner. | A callback return alone is not closure proof. |
| [Kit SQL](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s12-semantic-block-contract) | depends on | SQLBlock/SQLBlockStep and command meaning | Supplies immutable semantic input. | First error aborts the immutable request. |
| [Database](../../Database/DESIGN.md#s12-semantic-entry-contract) | used by | Semantic entry and final output adaptation | Supplies one admitted request. | Do not invoke public executeSQL once per step. |
| [Transaction](../Transaction/DESIGN.md) | depends on | Attempt lifecycle and checkpoint/logical restoration | Owns the single transaction and restoration barrier. | Tokens are opaque and attempt-bound; SQL names remain here. |
| [Read](../Read/DESIGN.md) | depends on | Admitted sessions on the supplied transaction | Executes reads without opening a second transaction. | Every statement session and cursor closes before checkpoint transitions. |
| [QueryExecution](../QueryExecution/DESIGN.md) | depends on | Bound typed rows and retained ownership | Produces collecting row results with the same meter. | Binding does not confer write or publication authority. |
| [Mutation](../Mutation/DESIGN.md) | depends on | Existing statement mutation subset | Maintains primary/index/relationship effects together. | Its current physical-name resolver is not logical SQL binding. |
| [SQLCatalog](../SQLCatalog/DESIGN.md) | depends on | Exact relation/column mapping and generation snapshot | Admits the explicit S12 mutation subset. | Never fall back to an entity name after logical lookup fails. |
| [Schema](../Schema/DESIGN.md) | coordinates with | Request-local candidate and post-commit publication | Checkpoint restoration covers staged generation state. | Trusted staging is not a public SQL DDL capability. |
| [StorageKit](../../../../storage-kit/Sources/StorageKit/Storage/DESIGN.md) | depends on | Portable checkpoint and ordered overlay contract | Supplies physical restoration below Transaction. | No SQL names, runtime result owners or parser meaning enter storage. |

## Architecture

```text
Database semantic facade -> SQLBlock request owner
    +-> one cumulative admission / meter / temporal environment
    +-> Transaction -> exact admitted physical transaction
    |       +-> Read session -> QueryExecution -> collecting row owner
    |       +-> Mutation -> primary / index / relationship maintenance
    |       +-> opaque checkpoint + logical-state restoration barrier
    |       `-> Schema request-local candidate
    `-> ordered collecting result owner
            -> drain sessions / cursors / transaction / candidates
            -> authoritative close + cancellation check -> Ready
            -> consuming caller-owned result transfer
```

## Contracts and Invariants

### API and result owner

The execution entry consumes Kit `SQLBlock`, canonical parameters and immutable
Framework execution options. The facade's single-statement entry uses this
same path. `SQLStatementResult` is Framework-owned and distinguishes
`rows(QueryResponse)` from typed command outcomes; later DDL adds its own
schema-change outcome. Kit owns command-kind metadata rather than Framework
payloads. `SQLBlockResult` preserves successful outcomes in input-step order,
including checkpoint command outcomes. A successful result before rollback-to
remains in that order even when its data effects are subsequently undone.

The concrete entry uses existing execution SPI:
`SQLBlockExecutor(context: DatabaseContext).execute(_:parameters:options:) async throws -> SQLBlockResult`.
The first argument is Kit `SQLBlock`, parameters are `[QueryParameter] = []`,
and options are `SQLBlockExecutionOptions`. The Database facade delegates to
this same Engine owner. A graph-free portable fixture imports
`@_spi(DatabaseExecution) DatabaseEngine` and invokes that entry directly;
it does not implement a second interpreter or require the GraphIndexes trait.
Source availability does not imply that its complete runtime matrix passed.

`SQLBlockExecutionOptions` requires explicit
`StorageKit.TransactionCheckpointLimits` and reuses the existing request
`ExecutionBudget`, `QueryStructuralLimits`, `SQLSearchPath` and
`TransactionConfiguration` defaults. It introduces no independent mutation
policy value. The options permit a zero row budget for a read-only block.
Only a DML branch derives `DatabaseEntityMutationLimits.maximumChanges` from
`budget.maximumRows`; zero raises a typed row-budget failure before effects
rather than being coerced to one. The internal executor receives empty
preconditions and `maximumPreconditions = 1` solely to meet its existing
positive-limit initializer contract. Mutation's statement-effects executor records
current-statement affected rows cumulatively on the exact meter; adapting its command result
does not record those rows a second time. SELECT output rows are recorded once
at the block result boundary, including surviving results of rolled-back steps.

Concrete collecting and Ready block owners are noncopyable and retain every
row, column and command reservation until the destination transfer. They
expose no early callback, stream or public array. Existing Framework ExecutionQueryRow
is reused. No generic box or caller-accessible readiness factory proves close.

The fixed lower-level entry is
`DatabaseContext.withCheckpointTransaction(configuration:checkpointLimits:workMeter:executionDeadline:_:)`.
Its configuration is `TransactionConfiguration = .default`; checkpoint limits
are required `StorageKit.TransactionCheckpointLimits`; the exact request
`DatabaseWorkMeter` is required; the deadline is
`TransactionExecutionDeadline? = nil`. The callback and result use the existing
`T: Sendable` copyable contract. SQLBlock transfers its noncopyable collecting
aggregate through the existing `DatabaseReadResultBox`, then takes and finalizes
it only after the runner and required schema publication return. The box moves
the value once; it is not authority that a transaction closed. No new public
transfer type or generic-runner change is introduced.

Mutation owns the distinction between per-statement effects and the retained
transaction-wide net-effect journal under its
[statement-effects contract](../Mutation/DESIGN.md#s12-statement-effects-and-explicit-temporal-environment).
SQLBlock consumes only that outcome and passes its captured temporal
environment explicitly to the statement executor; it does not derive either
result from journal history or resample the statement clock downstream.

Each ordinary statement executes within Transaction's package
`withStatementScope<T: Sendable>(_ operation: @Sendable () async throws -> T)`
boundary. It binds the attempt's current candidate schema lease and execution
binding together. A SELECT opens `DatabaseReadSession.withSession(context:workMeter:_:)`
inside that boundary with the same meter; its awaited invalidation closes every
statement read resource before the next checkpoint. Checkpoint steps execute
outside that statement scope, so the actor never waits for its own active
statement during the restoration barrier. The collecting-only
QueryExecution seam transfers rows, visible range and metadata reservation
without constructing a per-statement Ready value. Existing ordinary query
entry points retain their current contracts.

The entire block uses one storage domain and one serializable transaction.
Cross-domain writes fail before execution. Each statement uses the same
operation meter and transaction instant; its statement instant is captured
once on first entry and reused for automatic attempts. CLOCK_TIMESTAMP retains
the injected live clock. Retry discards prior collecting results and names,
creates fresh attempt tokens and never refunds consumed work or deadline.

### Explicit S12 execution subset

SELECT consumes existing SQL binding, typed result and transaction-bound
session contracts. SPARQL semantic statement forms and SQL-embedded graph
features require their concrete admitted executor; missing capability fails
explicitly before execution and never opens an independent transaction.

Each SELECT returns its complete logical statement result, so its execution
uses existing `ReadExecutionContext.withoutExternalPageWindow()`. This removes
only the transport-facing page window; SQL LIMIT/OFFSET and the exact request
meter remain active. A zero row budget therefore accepts an actually empty
result and rejects nonempty output through typed row-budget admission, without
coercing the budget or a pagination size. Ordinary public query pagination is
unchanged.

Column provenance remains S11's admitted physical field contract:
`field.queryType ?? .primitive(field.type, isArray: field.isArray)` in the
existing [canonical source mapping](../QueryExecution/DatabaseContext+CanonicalRows.swift).
A static String field without an SQL descriptor retains primitive-string
metadata, including for zero rows. A SQL text cast supplies its own SQL text
descriptor. SQLBlock preserves those declarations rather than relabeling a
physical field to satisfy a fixture expectation.

The proposed mutation subset is existing INSERT VALUES/DEFAULT VALUES,
UPDATE and DELETE, with their existing supported expression and conflict
behavior. RETURNING, INSERT SELECT, UPDATE FROM, DELETE USING, complete SQL
constraint/default/identity semantics and DDL remain their S13-S15 gates.

The S13 policy-aware mutation entry resolves the logical target through the
admitted SQL catalog/search path, applies column policies, and maps to physical
fields under the attempt schema lease. SQLCatalog owns compiled-model protection;
this orchestrator does not duplicate it with S12's former exact-name mapping gate.
RETURNING, INSERT SELECT, UPDATE FROM and DELETE USING retain S14 ownership.

### Named checkpoint semantics

Each live entry stores exact name identity and Transaction's opaque checkpoint
token. Creation pushes an entry; lookup selects the newest matching live name.
The proposed Transaction actor operations are `createCheckpoint()`,
`rollback(to:)` and `release(_:)`, with `DatabaseTransactionCheckpoint` as the
opaque attempt-bound token. Their implementation and state contract belong to
Transaction, not this name-lookup owner.
Rollback restores its target and invalidates descendants, retaining the target
for reuse. Release invalidates its target and descendants without reverting
writes. Duplicate older names become visible again when their shadow is
removed. Unknown/released/foreign attempt tokens are typed failures.

Before checkpoint/create/restore/release, all producing statement sessions,
cursors and derived cleanup must complete. Transaction owns the physical and
logical restoration barrier; SQLBlock changes its name stack only after the
transition succeeds. Checkpoint or cleanup failure is terminal. Results already
collected are distinct from reversible transaction state and retain their claims.

## Runtime Flows

```text
admit input -> acquire operation/schema authority -> enter one attempt
    -> statement -> drain statement read/mutation resources -> retain outcome
    -> checkpoint step -> await Transaction transition -> update name stack
    -> repeat using exact same meter and transaction
    -> finish transaction -> required schema publication/lease cleanup
    -> post-close cancellation check -> Ready -> public result

failure/cancellation -> discard unpublished outcomes -> rollback/close all owners
    -> preserve primary + cleanup failure -> throw, without synthetic success
```

## State, Ownership, and Lifecycle

| State | Owner and lifetime | Checkpoint/terminal rule |
|---|---|---|
| Immutable steps, parameters, options | One admitted request | No runtime handles in Kit values. |
| Ordered names and tokens | Sequential request attempt | Reset on retry; names never enter StorageKit. |
| Data and logical mutation state | Transaction actor | Physical/logical restoration completes together. |
| Statement read session and cursors | Read/QueryExecution statement scope | Authoritatively close before the next checkpoint transition. |
| Collected rows and commands | Concrete block result owner | Rollback-to preserves successful results and their reservations. |
| Candidate schema/catalog generation | Schema request-local owner | Restore/drop abandoned candidate; publish only after terminal commit. |
| Work, elapsed time, cancellation | Exact request meter/task | Never rewind on checkpoint rollback or retry. |
| Statement instants | Request-owned `SQLBlockStatementTimeline` | One shared `Mutex<[Timestamp?]>` retains first-entry instants across attempts; live clock calls occur outside its lock. |
| Public result | Caller after consuming Ready | Contains no transaction/session/lease/candidate owner. |

Catalog-only Schema staging uses a trusted execution SPI for S12 integration evidence.
Its proposed `DatabaseSchemaStagingState` is retained by Transaction and its
checkpoint records; terminal `takePreparedSchemaPublication()` transfers the
prepared publication to the transaction-completion owner, and only known
successful commit permits nonthrowing publication. It accepts an already
validated request-local catalog candidate with the existing physical schema,
runtime, maintainers and layouts fixed; it is not an extra
public block step, parser form or replacement for S15 physical schema staging
and DDL. The exact staging
API is coordinated with Schema and is not frozen by this document.

## Failure, Concurrency, and Constraints

The immutable public block stops at its first statement or checkpoint error,
rolls back the complete request, awaits mandatory cleanup and throws the typed
failure. Later steps, including rollback-to, are never automatically executed.
No earlier successful result becomes public from a failed request. Existing
Transaction's normal close-after-error behavior is unchanged; only an explicit
lower-level recovery-enabled callback may retain a rollback-only failed scope.

Commit-unknown is preserved as uncertain and never retried as known rollback
or converted into command success. Failure plus cleanup failure retains both
causes. Cancellation before completion cancels the operation; cancellation
after authoritative close and before Ready prevents result publication under
SPEC's aggregate-result contract. No result is exposed while mandatory close
or candidate publication remains pending.

One cumulative structural ledger admits every step, name, parameter and
statement before allocation/execution. Runtime growth uses existing checked
row/byte/work/deadline policies plus admitted checkpoint limits. Rollback
releases only abandoned owners, not consumed work or surviving result claims.
Operational values remain owned by configurable runtime admission policy.

Native, ordinary WASM and Embedded use the same immutable input and request
state owners, Transaction actor, meter and Sendable contract. Any shared
mutable result/lease state uses the same Mutex/actor entry points on all
targets; Embedded does not remove synchronization. No external callback,
event emission or await occurs under Mutex.withLock.

| Logical state | Native / ordinary WASM / Embedded storage | Read and mutation entry | Release |
|---|---|---|---|
| Retry-stable statement instants | Same `Mutex<[Timestamp?]>` | `instant(at:clock:)`, with live clock outside lock | Request owner and its measured reservation end together. |
| Checkpoint names | Same sequential attempt-local Array | Executor's ordered loop and exact-byte lookup | Attempt end; retry creates a new stack. |
| Collecting outcomes | Same linear retained buffer | Admitted append, then private post-runner Ready transition | Failure discards it; success consumes one public-output transfer. |

`SQLBlockExecutionTests` has ten public semantic cases and selects real
SQLite when the owning test target has `SQLITE`; the same cases without that
trait use InMemory only as reference evidence. `SQLBlockLifecycleTests` has
five controlled real-InMemory cases for commit, cancellation, retry, explicit
DML statement-time propagation and
commit-unknown. Their source definitions are not passing runtime evidence.

## Verification and Change Impact

`SQLRequestAdmissionTests.sharedReadsOverlap` proves shared admission by holding
one read's authoritative commit and completing the second read before releasing
that commit. Both reads must succeed. It uses the fixture's ordinary 30-second
request deadline and the suite's one-minute hang guard; a serialized admission
cannot pass because the held commit is released only after the second read
completes. The former incidental 300-millisecond success threshold mixed host
scheduling latency into this ordering proof and failed under concurrent CI load.
`exclusiveOwnerBlocksReaders` separately retains its explicit 100-millisecond
typed deadline rejection. Production budgets and reviewed performance bounds
remain unchanged; latency acceptance belongs to the performance package.


| ID | Falsifiable behavior and owner |
|---|---|
| S12-B01 | Public semantic block writes then reads its own changes in one transaction; independent readback sees only terminal committed state. |
| S12-B02 | Nested/repeated names, target survival, descendant invalidation and release/unknown failures match physical and logical state. |
| S12-B03 | Rollback restores primary data, indexes, relationships, Directory metadata, read caches and trusted staged catalog/generation together. |
| S12-B04 | Earlier successful rows/commands remain ordered after rollback-to, preserving zero-row columns, duplicate labels and types. |
| S12-B05 | Suspended statement close, commit, cleanup and candidate publication prevent public completion; terminal results retain no transaction-bound owner. |
| S12-B06 | Exact/one-past cumulative structural, checkpoint, work, output and retained limits fail explicitly; rollback/retry cannot refund work/deadline. |
| S12-B07 | Cancellation, checkpoint failure, retry, commit-unknown and combined cleanup failure discard unpublished results and preserve typed outcomes. |
| S12-B08 | Logical catalog aliases/remapped/schema-driven mutations fail before effects; unknown logical names cannot reach matching physical entities. |
| S12-B09 | Native public-block evidence and formal ordinary-WASM/Embedded Engine checkpoint/control plus Kit block-declaration evidence retain their exact compiled graph and executed cases; lower-contract success is not public-facade or Graph-enabled proof. |

S12-B10 requires a failed statement followed by otherwise valid rollback-to
and mutation steps to prove those later steps never run, no result escapes
and independent readback observes full rollback. Component tests belong to the corresponding
DatabaseEngine test target under the SQLBlock name; public Database tests own
entry adaptation. Storage conformance owns physical backend semantics, while
Framework real-backend tests prove their composition with logical restoration.
Mock-only or parser-only evidence cannot close S12-B01/B03/B05/B07.

The collecting seam and SQLBlock orchestration reside in DatabaseEngine,
whose production target depends on DatabaseMath, DatabaseTypes, DatabaseKit,
DatabaseWire and StorageKit, without GraphIndex. CanonicalRows imports only
Kit, Types and StorageKit. A graph-free Engine fixture can therefore exercise
the same ordinary SQL orchestrator if that concrete entry is included in its
executed graph; its actual cases must be recorded rather than inferred from
lower checkpoint tests. Database's GraphIndex/OntologyIndex dependencies are
conditional on GraphIndexes, and graph-free facade composition is possible.

S12 native public tests prove the actual facade and collecting-seam path.
Formal Engine/Kit portable tests prove only their selected runtime/declaration
paths. The existing user-approved
[Graph-enabled Database standard-WASM/Embedded obligation](../../Database/DESIGN.md#verification-and-change-impact)
remains [SQL-S18.02.03](../../../../PROGRESS.md), including graph-specific
rewriting and inlining; no S12 Engine-only result is generalized to that graph.
This records the existing deferral and adds no new portable deferral.

Changes to block input/command meaning recheck Kit. Changes to checkpoint
barriers recheck Transaction, Read, StorageKit and schema staging. Changes to
retained output recheck QueryExecution and Database adaptation. New grammar or
remote operations are separate S16/S17 work, not implicit S12 completion.

## S13 Sequence Replay and Constraint Controls

The S13 semantic steps extend the S12 subset with SET CONSTRAINTS and sequence
CREATE/ALTER/DROP. SQLSequences owns durable allocator state and request history.
SQLBlock captures its request before data retries, binds it to selected/default/
identity evaluation and obtains the container's FIFO permit through mandatory
recovery close. SELECT containing NEXTVAL or SETVAL requires write access; the
existing bounded query walker visits every nested expression before admission.
Sequence administration requires administer access. Authority for allocator
transactions includes the selected Partition lease and MultiBase write grant.

A private allocation demand aborts the data attempt. Only the exact unwrapped
demand returned after successful rollback/close starts an independent allocator
transaction on the same injected engine. Its atomic value/recovery marker is
cached, and the block is replayed with the same meter, deadline and temporal
timeline. This is an outer control transition, separate from StorageError retry
and its maximum-attempt policy. Cleanup failures and commit-unknown stay terminal.
No result escapes until data commit/close and recovery-marker cleanup both close.

CREATE and ALTER RESTART stage an incarnation head in the data transaction.
Independent consumption addresses that incarnation; rollback leaves unreachable
allocation gaps and preserves the previously committed head. Thus RESTART is
transactional while NEXTVAL/SETVAL consumption in an active incarnation remains
nonrollback. Request history survives savepoint rollback and is reconstructed
from cached effects on whole-attempt replay. Explicit rewind and CYCLE may
intentionally duplicate values; automatic attempts and restart never do so.

| State | Native / WASM / Embedded | Owner and close |
|---|---|---|
| Request permit and waiters | Same actor | Container; release after terminal recovery close |
| Effect cache, CURRVAL/LASTVAL | Same actor | Request; never checkpoint-restored |
| Recovery/allocation cells | Same Partition-leased runner | Commit/close before replay |

SQLSequenceExecutionTests owns semantic sequence and transaction rollback cases;
SQLBlock lifecycle tests continue to own aggregate readiness and first-error
failure. No PostgreSQL concurrency or uncertain crash-outcome parity is claimed.

The outer request captures one transaction instant before replay and retains the
selected data-root/schema scope across allocator calls. Its callback returns the
collecting box. Final cancellation admission and Ready construction occur only
after that complete scope has returned and released its request leases. A
package-only entry accepts the facade's exact existing meter and absolute
deadline, so parsing and sequence execution do not acquire independent budgets.

The internal single-text-statement entry retains an optional immutable occurrence
arena/root through binding and selected evaluation. Source metadata is valid only
for exactly one statement and must be supplied as a complete pair. The public
semantic block entry has no synthetic source map. Every SQL statement binds an
admitted session scope for the complete mutation, including constraint and FK
DEFAULT evaluation; assignment completion does not end that session scope.

## S14 mutation-bearing statement composition

This section supersedes the S12 mutation subset for S14 implementation. It is a
design contract, not behavioral evidence. Kit owns `SQLWithStatement` and its
ordered `SQLNamedStatement` declarations; existing SELECT-only `NamedSubquery`
remains the nested demand-driven query owner. Mutation owns `SQLMutationOutcome`
with direct statement count/effects and optional retained RETURNING rows.

One mutation-bearing top-level statement opens one existing read session inside
its already admitted transaction statement scope. QueryExecution's
[statement read scope](../QueryExecution/DESIGN.md#s14-statement-read-and-mutation-projection-scope)
prepares the complete source graph and captures every physical read before the
first effect. Preparation includes main DML, all writable definitions, all
reachable ordinary SELECT definitions, nested sources and expression subqueries;
it binds metadata without evaluating lazy SELECT target expressions. Mutation
supplies target layouts and demanded physical field coverage. A later missing
capture is a typed preparation failure, never a postwrite storage read.

For a DML or TRUNCATE main statement, that session uses Read's internal
`withSQLMutationSession` entry bound to the exact enclosing writable transaction. Mutation-owned reads consume that existing
write admission while retaining entity and field read checks; they do not demand
an unrelated Base read grant. A SELECT main statement, including WITH writers
whose main body is SELECT, still uses ordinary read-session admission. Each
independent SELECT step in a mixed block also requires read admission; failure
rolls back any preceding effects with the rest of the block. Session lifetime and checkpoint cleanup are shared by
both entries and do not change aggregate publication.

```text
one SQLBlock statement boundary
    -> bind declarations and demanded source coverage
    -> authorize/capture all base sources
    -> complete writable definitions once in dependency order
         -> publish only private retained RETURNING relations
    -> execute main SELECT or DML against old base + returned relations
    -> await statement session close -> retain direct main outcome
    -> later block steps -> transaction/schema/sequence close -> public result
```

An unreferenced writable definition still completes; repeated references and
LIMIT 0 do not change its effect count. A writable definition without RETURNING
may execute but scanning it fails. `WITH RECURSIVE` may combine a nonrecursive
writable definition with a recursive SELECT. A writable self-reference or
recursive dependency involving a writable definition fails before effects.
The selected execution order follows declaration dependencies. Repeated writes
to the same physical row identity by sibling/main mutation are rejected before the
repeated effect is applied, with a typed product restriction rather than
inventing PostgreSQL's undefined same-row ordering. Row-dependent target identity
may become known only during execution; this failure rolls back the complete
block and publishes no outcome. Alias, schema-name spelling and CTE name cannot
hide physical target identity. Disjoint rows of the same table remain valid.
Each prepared writable definition and main mutation receives one scope-local
owner ordinal. Mutation claims the effective action's validated old identity and
its new identity when relocation changes it, after its own cardinality checks
and before applying that action batch. A physical key comprises entity,
Directory partition path and encoded identifier. Repeated claims by the same
owner preserve that DML statement's existing conflict/cardinality contract;
claims by distinct owners fail and roll back the complete block.

Main statement counts do not include CTE effects. `SQLBlockCollectedRows` preserves
the canonical response's `affectedRows`, ordered columns, visible range, metadata
and all claims. Final `.rows(QueryResponse)` includes that count after authoritative
close; the existing public result algebra needs no new variant. Zero returned
rows still retain bound columns and count zero. No intermediate projection,
callback or count becomes public during an active transaction.

TRUNCATE resolves the complete admitted logical relation list and delegates
dependency closure and clearing to Transaction's `truncateSQLRelations`. Its
outcome carries direct count and the effective relation IDs. RESTART IDENTITY
passes those IDs to SQLSequences' owned-identity restart within the same data
transaction; CONTINUE IDENTITY does not restart. Static compiled model witnesses
and schema/catalog definitions remain intact. Failure at any later step rolls
back all data/index/reference/Directory effects and transactional sequence heads.

| Evidence | Required observable result |
|---|---|
| Unused, LIMIT 0 and multiply referenced writable CTE | One full mutation each; no demand-dependent effects |
| Base table versus RETURNING in sibling/main query | Old base value and new/deleted output image respectively |
| RETURNING consumed by INSERT SELECT | Atomic movement through retained positional rows |
| Later statement/action/projection failure | Entire block rolled back and no earlier rows/counts published |
| Empty/duplicate RETURNING metadata | Ordered labels/types and direct affected count preserved |
| Restricted field and missing capture | Typed failure before effects; no read-authority widening |
| TRUNCATE closure and identity restart rollback | Effective closure cleared atomically; owned sequence reset rolls back |

The existing same-meter, deadline, cancellation, retry, checkpoint journal,
commit-unknown and Ready rules remain authoritative. Per-statement registry
owners are discarded before a checkpoint transition, while completed output
claims remain held by the block until its final transition. Native, ordinary
WASM and Embedded share the same immutable inputs and synchronized runtime state.


## S15 DDL Composition

[SQLDDL](../SQLDDL/DESIGN.md) owns declaration binding and candidate construction.
SQLBlock admits DDL with administer/read/write access in the existing single
transaction and temporal statement scope. A request-owned immutable identity
is reused through sequence-allocation replay. DDL command results remain in the
same collecting buffer as SELECT/DML and cannot become Ready before commit and
authoritative close.

```text
DDL statement -> stable request IDs -> schema candidate + row/index rewrites
  -> following statements see the staged catalog
  -> rollback to checkpoint restores prior catalog/data/index state
  -> temporary namespace removal -> commit/close -> ordered public results
```

Each statement derives an effective search path from the current transaction
catalog. A block-private temporary namespace precedes the requested path only
while it exists. Qualified permanent names retain their exact meaning. Successful
collection removes every temporary relation and its owned dependencies before
commit. Failure relies on the outer authoritative rollback, so no private
namespace or physical generation becomes durable. No process or cross-request
session state is introduced. TRUNCATE invokes the same read-only view/MV rejection
contract as ordinary mutations before physical target resolution.


## S16 Request Controls

The S16 operation contract is owned by [Kit SQL](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s16-block-operations-and-diagnostics).
SQLBlock validates terminal placement before entering storage. BEGIN is optional
and first-only; a present BEGIN requires a last COMMIT or ROLLBACK. A last
ROLLBACK transfers collecting results through a private typed demand. Only the
unwrapped demand after TransactionRunner completed rollback and close can become
success; combined cleanup failures and cancellation remain failures. No schema
publication is prepared for that path.

Attempt-local settings contain search path, temporal configuration and statement
timeout. Named checkpoints snapshot settings; rollback-to restores the selected
snapshot, release keeps current settings. Prepared definitions belong to the
attempt rather than checkpoint state. Both end on retry and request close.
A statement timeout races the same statement task with the injected monotonic
clock, cancels and joins the loser, retains first-entry step deadlines across retries, and remains subordinate to the original
request deadline and exact meter. Zero disables only the statement overlay.

```text
request admission -> terminal-order validation -> one transaction attempt
    -> settings / prepared registry -> statement scope -> ordinary executor
    -> EXPLAIN / COPY -> same ordinary executor closure
    -> last COMMIT -> authoritative commit + close
    -> last ROLLBACK -> private demand -> authoritative rollback + close
    -> post-close cancellation/meter check -> Ready -> caller-owned outcomes
```

The orchestration seam is internal `SQLBlockStatementOutcome`; it retains rows,
command metadata or COPY bytes with their reservations. EXPLAIN and COPY call
an injected ordinary-statement closure rather than public executeSQL, so no
second admission, independent transaction or early Ready value is created.
Verification owners are S16SQLBlock and S16Preparation behavioral tests for
terminal rejection, real rollback/close, savepoint settings restoration,
request-local lifetime, preparation without effects and deadline cancellation.


### S16 prepared binding ownership

`SQLPreparedRequest` is an attempt-local synchronized owner retained by the sequential
block coordinator. Its registry is never checkpointed and cannot escape the
attempt. `SQLPreparedAnalysis` binds the complete definition with NULL carriers
and one shared `CanonicalParameterBindingState`, collecting declared and
contextual types without evaluating a selected expression, default or sequence.
Mutation's analysis seam binds the same assignment/RETURNING contracts without
capturing rows. EXECUTE validates exact name, arity, argument types, catalog
generation, catalog digest and byte-exact search path before returning the ordinary
statement and its independent parameter vector. Selected canonical assignment
conversion consumes already evaluated argument carriers; its invocation retains
conversion leases through ordinary execution. DEALLOCATE removes retained definitions and
claims; a fresh attempt creates an empty registry.


| S16 mutable state | Native / WASM / Embedded owner | Access and release |
|---|---|---|
| Settings and checkpoint snapshots | Same attempt-local values; retained text/path claims | Sequential collect loop; rollback restores snapshot, attempt end releases claims. |
| Prepared definitions | Same `Mutex<[Definition]>` registry | Atomic exact-name lookup/install/remove; analysis and awaits run outside lock; registry ends with attempt. |
| Statement deadline winner | Same strong `Selection` reference owner containing one `Mutex<Bool>` on Native / WASM / Embedded | Both structured tasks capture the owner and atomically select through `claim()`; one bounded owner per race is released only after both tasks join; await and I/O remain outside the lock. |
| Retry-stable statement deadlines | Same `Mutex<[StorageInstant?]>` | Captured outside lock, installed once; request end releases metered array. |
| Physical cancellation | Existing runner `Mutex<Task<...>?>` | Task-local request delegates exactly-once cancel; no raw transaction authority crosses the boundary. |

The optional package `SQLBlockFailurePosition` owns only a Mutex-protected step
index for text-adapter fallback positions. Each attempt clears it; a terminal
step failure records its index only after statement cleanup has unwound. Closing
or publication failures outside a step retain no invented step position. Native
semantic failures remain unchanged and retain their original typed identity.

| Text failure fallback | Same `Mutex<Int?>` on Native / WASM / Embedded | Attempt reset and terminal step record; text adapter reads after failure; request end releases owner. |


| Prepared state | Native / WASM / Embedded storage and isolation | Read / mutation entry | Owner release |
|---|---|---|---|
| Definitions and binding identities | Same `Mutex<[Definition]>` | `execution` / `prepare` insertion and `deallocate` use `withLock` | Attempt scope ends; retry constructs a fresh owner |
| Definition parameter inference | Same `CanonicalParameterBindingState` Mutex | Canonical binding / finalization | Analysis ends after the immutable type vector is retained |
| Converted invocation parameters | Immutable invocation with payload leases | Selected scalar assignment / ordinary executor | Statement completion releases invocation claims |


### S16 review regressions B1/B3/B4

Access admission resolves each EXECUTE against its preceding request prefix,
using exact prepared-name identity and treating matching DEALLOCATE or ALL as
a lookup barrier. PREPARE alone does not execute its body or grant write access.
The runtime registry continues to own missing and duplicate name failures.

Checkpoint controls apply the retry-stable statement deadline directly around
Transaction's checkpoint transition, outside `withStatementScope`; nesting that
scope would prevent the checkpoint restoration barrier from draining.

S16SQLBlockReviewRegressionTests owns live-definition reuse with named/ALL
DEALLOCATE, physical checkpoint creation/rollback/release held beyond the
statement deadline, and caller cancellation while timeout-owned cancellation
is suspended and then fails. It uses a forwarding InMemory transaction decorator
for actual checkpoint and cancellation lifecycles rather than a replacement
SQL interpreter.


| Review repair state | Native / WASM / Embedded owner | Entry and terminal boundary |
|---|---|---|
| Live prepared access | Immutable request prefix, no retained mutable registry | Exact-byte reverse lookup stops at matching/ALL deallocation; runtime registry owns declaration errors. |
| Checkpoint statement deadline | Existing timeline `Mutex<[StorageInstant?]>` and deadline winner `Selection` owning `Mutex<Bool>` | Direct checkpoint control wrapper; no statement scope; producer and cancellation join before return. |


## S17 Complete Response Projection

SQLBlock owns a synchronous precommit encoding projection and its post-close
ready frame owner. Server and application adapters supply only the SQL-specific
encoder. They perform no publication I/O while projecting. Framework does not
import DatabaseWire and the existing public SQLBlockResult entry retains its
post-close native materialization.

### Frozen execution SPI

All declarations below are `@_spi(DatabaseExecution)` public. Input parameters
retain the existing structural, execution, checkpoint and retry options.

```swift
public protocol SQLBlockResponseEncoder: Sendable {
    func measure(_ output: borrowing SQLBlockOutputView) throws -> UInt64
    func encode(_ output: borrowing SQLBlockOutputView,
        into destination: inout MutableSpan<UInt8>) throws
}

extension SQLBlockExecutor {
    public func executeEncoded(_ block: SQLBlock,
        parameters: [QueryParameter] = [], options: SQLBlockExecutionOptions,
        encoder: any SQLBlockResponseEncoder) async throws -> EncodedQueryResponse
}
```

`SQLBlockOutputView` is noncopyable, has no public initializer and owns the
existing retained statement buffer. Its only public metadata is statementCount.
Its synchronous `withStatement(at:command:rows:copy:)` selects one of three Void
visitors: command receives SQLCommandKind and UInt64?; rows receives a borrowing
SQLBlockRowsView; copy receives a borrowing SQLBlockCopyView. No statement array,
public result, COPY bytes property, row array, iterator or span escapes.

| Borrowed view | Metadata | Synchronous Void visitors |
|---|---|---|
| SQLBlockRowsView | columnCount:Int, rowCount:Int, affectedRows:Int? | withColumn(at:_:) borrows QueryColumn; withRow(at:_:) borrows SQLBlockRowView |
| SQLBlockRowView | valueCount:Int | withValue(at:_:) borrows QueryValue; withAnnotations(_:) borrows [String:FieldValue]; withVersion(_:) borrows PersistableVersionToken? |
| SQLBlockCopyView | affectedRows:UInt64 | withBytes(_:) borrows ByteString |

Every view is noncopyable with internal construction only. Visitors are
nonescaping, synchronous and throwing. Their return type is Void; they do not
produce an arbitrary escaping destination. Native work-limit failures and
encoder failures propagate unchanged. The Wire-owned SQL writer SPI accepts
untyped-throwing traversal bodies while retaining its own typed codec failures;
no sentinel conversion or default fallback is introduced.

Before the rows visitor, Framework reserves the current outcome's temporary
column-descriptor array using the existing collection footprint helper. Before
each row visitor it reserves the two canonical-annotation temporary arrays used
by the existing Server conversion and the version digest's exact byte count.
Empty annotations and absent versions allocate no scratch claim. The temporary
claim remains private and lives through the visitor, covering both measurement
and encoding passes separately. Original row/schema/payload claims remain held
by the collecting owner. There is no all-results or all-rows materialization.

After measurement and checked UInt64-to-Int conversion, Framework reserves the
exact complete-frame byte count on the same request meter, then allocates one
final ByteString and invokes encode into its bounded MutableSpan. Wire measures
and validates the complete envelope and result body with the same SQL writer,
then checks final cursor completion. The raw allocation is owned/deallocated by
ByteString; Framework initializes its complete UInt8 region before the safe
span facade. The borrow is synchronous, pointer-free at the public Framework
boundary and cannot outlive the allocation. An encode throw destroys unpublished
bytes and releases the frame claim before the transaction rolls back.

The private collecting encoded payload crosses the existing result box together
with its claim. Automatic infrastructure attempts and sequence-demand replay
retain existing semantics: a failed attempt discards its provisional frame,
reconstructs output on the next attempt, and refunds neither work nor deadline.
A successful ROLLBACK carries its provisional frame through the same private
rollback demand. No frame becomes ready before commit/rollback, schema
publication, sequence close, operation-lease release and final cancellation
admission have completed.

`EncodedQueryResponse` is a concrete final Sendable reference owner with no public
initializer, raw payload getter or callback. It contains immutable byteCount and
one private `Mutex<Payload?>` holding frame bytes plus their reservation. Its
`promoteToOutput() throws -> ByteString` takes the payload exactly once and ends
the private claim only as final caller ownership is transferred. A repeated
promotion fails explicitly. Server may retain this owner in its existing
prepared-result body until endpoint publication; it never receives a claim.

```text
collect under one admitted transaction
    -> borrow SQLBlockOutputView -> measure complete frame
    -> exact same-meter frame reservation -> encode one ByteString
    -> private frame + claim in collecting result
    -> commit / rollback + schema / sequence / lease close
    -> final cancellation admission -> EncodedQueryResponse
    -> endpoint terminal promotion -> caller-owned ByteString

measurement / codec / budget failure -> discard frame -> rollback + close
```

| State | Native / WASM / Embedded owner and isolation | Lifetime / release |
|---|---|---|
| Provisional view and row borrows | Same noncopyable value over existing retained owners | Synchronous projection only; no raw bulk result leaves the scope. |
| Encoding scratch | Same private exact reservations | Current rows/row visitor; released after callback. |
| Collecting encoded frame | Same private ByteString + reservation value | Retained through authoritative transaction and request close; discarded on any failure. |
| Ready encoded frame | Same Mutex<Payload?> concrete reference owner | Server/host retains owner; one short lock takes payload at final promotion; no I/O, await or callback under lock. |

Focused SQLBlock projection tests own actual mutation rollback on measurement
and encoding rejection, frame-claim lifetime, and suspended authoritative close
preventing ready-frame publication. Server and application tests own real Wire
full-frame exact/one-past validation through these same visitors. Existing native
block tests continue to own their public result and ordering contract.

| Behavioral evidence owner | Counterexample rejected |
|---|---|
| `S17SQLBlockProjectionTests.projectionRejectionRollsBack` / `frameBudgetRefusal` | Earlier mutation commits after the complete response is refused. |
| `commitGatesFrameAndClaim` / `cancellationDiscardsFrame` / `commitFailureDiscardsFrame` | Pre-close publication or retained frame/row claims after failure. |
| `sequenceCloseFailureDiscardsFrame` | A successful producing read commit publishes despite later mandatory sequence recovery-close failure. |
| `abandonedReadyReleasesClaim` / `commitGatesFrameAndClaim` | Unpromoted owner leaks a reservation, final promotion retains it, or promotion succeeds twice. |
| `rollbackKeepsEncodedObservations` / `scopedOutputVisitors` | Ordered rollback observations or row/COPY payloads are lost before encoding. |
| `S17EncodedSQLFacadeTests` in DatabaseTests | A facade bypasses projection, loses source diagnostics, or converts typed encoder refusal to success. |

Encoded-entry failures preserve the existing SQLRelationCleanupError and
SQLSequenceRequestCleanupError immutable cause pairs through execution SPI;
[SQLDiagnostics](../SQLDiagnostics/DESIGN.md#s17-cleanup-relay-visibility) owns
the classification/relay assumption. Neither is converted into direct
cancellation or a second failure representation.

### Statement preparation boundary

SQLBlock registers SELECT CTE metadata synchronously in dependency order and
uses QueryExecution's [statement capture driver](../QueryExecution/DESIGN.md#s14-statement-read-and-mutation-projection-scope)
before sealing/running the statement. Mutation retains its earlier drain before
candidate scans. This changes scheduling, not CTE demand, snapshot visibility,
transaction boundaries or authoritative close. A large list of unused SELECT
CTEs before DML must complete without accumulating asynchronous metadata frames.

A mutation change limit derived from `ExecutionBudget.maximumRows` is a SQL
resource rejection, including a nonzero limit exceeded by one row. Its SQLSTATE
and retained cause belong to [SQLDiagnostics](../SQLDiagnostics/DESIGN.md).

## Common Entry and Result Projection

[Database Query Execution](../../Database/QueryExecution/DESIGN.md) adapts the same
SQLBlock executor for SQL text, semantic statements and immutable blocks. Runtime
rows are ExecutionQueryRow; final Kit rows are not execution storage. The output
view exposes metadata through a bounded synchronous borrow, charging conversion
scratch to the same meter. Shared SQLCheckpointPolicy derives portable checkpoint
ceilings from the admitted budget with exact integer conversion. Optional
read-only-query admission validates the lowered single SELECT and rejects sequence
calls and write effects before transaction entry. The original explicit-block
rejection belongs to the linked Database Query Execution entry contract, before
lowering loses the input kind; default complete execution preserves the profile.
No public result or connection can bypass Collecting -> Ready -> promotion.


### RX25 SQL request admission

SQLBlock owns container admission through SQLRequestAdmission; sequence allocation
no longer owns a global all-SQL FIFO. Pure statement SELECT blocks without a
known sequence function request shared permits. Other steps, unknown operational
forms and known sequence use request exclusive permits. This classification
attenuates scheduling only; the original authorization check and transaction
access remain mandatory. A shared request uses read-authorized storage access.

```text
shared SELECT cohort -> original read transaction -> close -> return permit
exclusive effect    -> original transaction/replay/cleanup -> return permit
shared hidden effect -> private demand before any allocation
    -> rollback + authoritative close -> return shared permit
    -> fair exclusive wait -> original authorization with required write access
    -> replay same input/meter/deadline/timeline -> close -> return permit
```

The container actor retains exact permit identities and admitted bookkeeping.
Readers may share a cohort while no earlier exclusive waiter exists. A queued
writer prevents later readers from overtaking it; contiguous queued readers may
start together after an exclusive owner closes. The connection's configured
execution limit still bounds its own admitted work; this coordinator adds no
second caller-selected capacity or unbounded uncharged queue.

Waiting uses the original absolute deadline and cancellation. Cancellation or
expiry removes that waiter and releases only its claim; delivery races return
the actually granted permit before failure. A permit is returned exactly once,
after transaction/read/schema/sequence cleanup; cleanup failure remains failure.
The same actor/state/Sendable declarations apply to Native, standard WASM and
Embedded. Backend transaction capacity is a separate StorageKit contract;
shared Framework admission does not promise parallel single-connection SQLite.

Hidden sequence effects in views/nested definitions trigger the private promotion
demand before reading/modifying allocator history or opening its independent
transaction. Only an unwrapped demand after successful rollback/close may
promote. A combined cleanup failure cannot retry. Promotion retains original
principal, SQL input, temporal timeline, meter consumption and deadline, creates
no result and never replays an already consumed sequence effect. Reacquisition
never upgrades storage authority: the original write permission is checked anew.
A request restricted to read-only pagination rejects the hidden effect instead
of promoting; its rollback/close and permit return complete before the typed
unsupported-paged-query error escapes, without consuming an allocator value.

Native controlled-commit tests must prove overlapping reads and exclusive
blocking without sleeps, queue fairness and cancellation/timeout permit races;
view/CTE sequence and existing allocation/rollback/restart tests retain their
owners. Exact release portable runtime also executes concurrent reads and a
sequence-backed view before final completion. Final before/after benchmarks keep
the original backend configuration and distinguish admission from backend limits.

| RX25 state | Native / standard WASM / Embedded storage and isolation | Read / mutation entries | End of lifetime |
|---|---|---|---|
| Active permits, fair waiters and identities | Same SQLRequestAdmission actor | enter / grant / drain / expire / leave | Exact permit return releases its claim; deadline task is joined |
| Exclusive effect permission and replay history | Same SQLSequenceRequest actor | effect / promoteAdmission / beginAttempt | Request close precedes permit return |
| Scalar control and ORDER descriptor reuse | Invocation-exclusive admitted arrays / decoration-local value | Selected instruction loop / columns(for:meter:) | Scope success or failure releases retained claims |

These source declarations have no target-conditioned storage, conformance or
mutation path. Existing timeline, failure position, prepared registry and CTE
state keep their shared Mutex/actor contracts listed above. Target compilation
and actual concurrent/cancellation/close fixtures verify the composed paths.
