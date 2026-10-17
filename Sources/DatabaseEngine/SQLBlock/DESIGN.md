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
expose no early callback, stream or public array. Existing Framework QueryRow
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

Before invoking the existing physical mutation executor, resolve the logical
target through the admitted S11 SQL catalog and search path. S12 admits only
static-model mappings whose relation/entity and every visible column/physical
field have exact UTF-8 identity and compatible declared types. The explicit
namespace may be removed only after that successful logical resolution.
Catalog aliases, renamed columns, schema-driven mutation backing and mappings
requiring expression rewriting fail with typed unsupported capability before
effects. An unknown logical name is a binding failure; it cannot fall back to
a matching physical entity. This subset is not complete S14 SQL mutation
support. Per-step shape/capability checks precede its first effect, while the
request transaction guarantees rollback if later admission or evaluation fails.

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
