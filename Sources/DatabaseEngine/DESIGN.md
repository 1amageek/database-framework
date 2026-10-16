# DatabaseEngine

## Purpose and Scope

Module authority for platform-neutral in-process execution. DF-06F0 and
DF-06R compose retained polymorphic and regular-model paths from [Read](Read/DESIGN.md),
[Core](Core/DESIGN.md), [Serialization](Serialization/DESIGN.md), and
[QueryExecution](QueryExecution/DESIGN.md), over keyspaces bound by
[Directory](Directory/DESIGN.md), [Registry](Registry/DESIGN.md),
[Mutation](Mutation/DESIGN.md), with [Fusion](Fusion/DESIGN.md) coordination
and optional [Base](Base/DESIGN.md).

- Parent: [database-framework](../../DESIGN.md).
- Children: the component designs linked above.

## Responsibilities and Boundaries

The module owns schema-bound sessions, persistence decoding, query execution,
and request accounting. It exposes purpose-neutral contracts to index modules;
scoring, ranking, bitmap, full-text, and vector policy remain outside it. It
owns no key layout: every keyspace it reads or writes is resolved from a
StorageKit Directory node through [Directory](Directory/DESIGN.md).

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Package](../../DESIGN.md) | parent | Runtime module composition | Fixes dependency direction. | Execution semantics remain in this module. |
| [DatabaseKit SQL](../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md) | depends on | SQL profile and semantic model | Fixes accepted SQL capability and value meaning. | Planned compatibility is not current implementation evidence. |
| [StorageKit](../../../storage-kit/Sources/StorageKit/DESIGN.md) | depends on | Storage transaction and physical checkpoint contract | Supplies backend-neutral atomicity and journal behavior. | Framework restores logical state without implementing backend key semantics. |
| [Database](../Database/DESIGN.md) | used by | Operation admission and ready output | Exposes statement/block entry points. | It cannot acquire independent transaction authority. |
| [Read](Read/DESIGN.md) | child | Session admission | Composes this direct component contract. | Context methods cannot bypass it. |
| [Core](Core/DESIGN.md) | child | Polymorphic orchestration | Composes this direct component contract. | Does not decide query authority. |
| [Serialization](Serialization/DESIGN.md) | child | Retained bounded item read | Composes this direct component contract. | Envelope and chunks are bounded. |
| [QueryExecution](QueryExecution/DESIGN.md) | child | Retained aggregate/destinations | Composes this direct component contract. | Copyable raw values do not escape. |
| [Fusion](Fusion/DESIGN.md) | child | Authorized prepared inputs and selected relational plans | Coordinates bound operators within one admitted session. | Shared parameter identity and exact meter precede source scans. |
| [Mutation](Mutation/DESIGN.md) | child | Prepared mutation and typed adaptation failures | Preserves entity-change authority and query-value causes. | Revalidate direct server error mapping. |
| [Base](Base/DESIGN.md) | child | MultiBase Composition execution | Coordinates retained member rows and provenance. | Enabled only with MultiBase. |
| [Registry](Registry/DESIGN.md) | child | Admitted schema and declared model adaptation | Supplies ordered typed source metadata and direct slot construction. | Descriptor meaning is never inferred from payload shape. |
| [Directory](Directory/DESIGN.md) | child | Declaration-to-node binding | Composes this direct component contract. | A resolved node is valid only in its resolving transaction. |

## Architecture

```text
sealed authority + snapshot + exact request meter
                      -> Read
                          -> Core -> Serialization
                              -> QueryExecution retained owner
                                  -> canonical/index/public destination

Directory binds every declaration to a StorageKit node, so Core and
QueryExecution receive a resolved Subspace rather than a computed prefix.
```

## Contracts and Invariants

### Assumptions

- Storage point reads reject the supplied bound before exposing bytes.
- Runtime registrations canonicalize for the session schema generation.

### Guarantees

- Scan obtains its meter only from the session-owned scope; ordered point
  fetch rejects foreign retained-key meters before storage.
- A nested Read-owned non-forgeable admission is created only inside the
  session closure after sealed list/field coverage validation and is required
  by the Core context API.
- Capacity and every present/missing slot are admitted before allocation or
  decode, and all retained values use the session meter.
- Regular model fetch accepts retained primary keys through an async scoped
  borrow. The source owner and claim remain live through each point read, and
  no canonical path requires a raw `Collection<Tuple>` view.
- Complete regular-model output requires whole-entity field authority. Fusion
  candidate materialization remains projection-bound through a distinct
  session entry point that derives fields from its sealed query authorization.
- The aggregate exposes purpose-specific destinations and consuming public
  promotion, not general copyable model/identifier/array access.
- Point-fetch slots preserve input order and absence; scans preserve cursor
  order. Failure/cancellation release partial owners and complete cleanup.
- A retained RDF graph result leaves linear ownership only by being
  consumed into a shared graph. The shared graph is copyable and holds the
  request reservation until its last owner is released, so canonical
  fingerprinting and multi-page emission may outlive the storage snapshot
  that produced the result. It exposes its element count, a Void-returning
  scoped element borrow, and page materialization that copies only the range
  the caller requests; the shared array backing it stays module-internal, so
  no caller takes ownership of the retained storage, and the requested range
  decides how much of the result reaches an ordinary Array.
- The post-closure cancellation check belongs to a Collecting-to-Ready
  transition over Framework-owned resources, not to the generic transaction
  runner. A public copyable read API returns an ordinary value, so its
  cancellation behavior is the one its callback produced: a caller cancelled
  while its own read-only commit is in flight still receives that value. The
  runner therefore makes no check after its attempt closes, and states no
  result kind, because its tail serves every copyable execution alike.
- The composition read snapshot owns such a transition, because it holds its
  domain transactions directly instead of delegating them to the runner. It
  drains the member vault, brings every domain transaction to its terminal
  state, releases the member Base leases, and only then checks cancellation
  before the result becomes the caller's. Each transaction is read-only by
  construction, so no durable outcome is reported as cancelled.
- A composition member Base lease outlives every domain transaction it
  admitted. `DatabaseBaseLeaseToken` finishes exactly once, so the snapshot
  releases the composition lease explicitly at the package boundary after the
  commit or cancel loop, and a deferred release covers every throwing exit;
  neither release is reachable while a domain transaction is still open. A
  lease that ended at an ARC release point instead would let the drain a Base
  lifecycle transition waits on complete while a domain commit was still in
  flight, advancing retirement, deletion, or placement movement past a
  running transaction. Every holder is inside the module, so the release is
  `internal`: no caller outside it ends an admission lease.
- Issuing an admission lease is module-internal for the same reason. The
  lease initializer, the generation it retains, and the counted token are all
  `internal`, so only the generation store and the Base lifecycle paths that
  own the admission count create one. Package visibility would let a sibling
  target read the generation off a counted lease, wrap it in a no-op token,
  release the counted lease, and go on operating against a Base whose drain
  had already completed.
- The check runs where the closed transaction is beyond cleanup. A closed
  transaction can no longer be cancelled, so a check that could reach its
  owner's cleanup would answer a cancelled read with a cleanup failure
  instead of the cancellation. The composition snapshot therefore checks only
  where every domain transaction is already counted as committed.
- The canonical retained query family owns the same transition on the path
  where it opens its own producing transaction. That storage access closes
  the transaction and releases its PartitionLease before returning, and the
  read session drains inside it, so no transaction-bound resource survives
  the boundary, and the read is read-only by construction, so no durable
  outcome is reported as cancelled. When a caller-owned transaction is
  already bound, the same access runs on it and closes nothing, so the
  enclosing owner still holds the transition and the retained query makes no
  check; a caller cancelled during that nested work learns it from the work
  budget, which reports cancellation while the work is still charging rather
  than at a boundary. Ownership follows whether a transaction was bound on
  entry, not the shape of the result, so both the retained and the promoted
  public response reach their caller through one boundary.
- Collecting and Ready are distinct types, so the transition is rejectable
  rather than merely described. A canonical query result produced inside an
  active transaction is a collecting response: it exposes borrowed row views
  and its metadata, and it has no promotion at all. Only the ready response
  promotes to a public response or to caller-owned retained rows, and its
  construction requires a `PostClosureResultAdmission` whose initializer is
  file-private to the canonical row implementation. `finalizePostClosureResult`
  is the sole producer of that admission: it applies the ownership-scoped
  cancellation check and then performs the consuming move. An active
  transaction callback therefore cannot construct a ready result, and no
  promotion can bypass the check, because neither the admission nor the
  consuming finalizer is reachable from outside that one transition.
- `withPostClosureReadSnapshot` is the scope that owns the transition end to
  end. Its body returns the collecting result inside the read result box, so
  the noncopyable value crosses the closure boundary without being promotable
  inside it; the scope then takes the value out of the box after every
  session, transaction, and lease inside the body has closed, and finalizes.
  Callers that run on a caller-owned transaction finalize with ownership
  false: the enclosing owner still holds the check, and the transition only
  records that this scope did not perform it.
- A retained canonical row page leaves linear ownership the same way. It is
  consumed into shared row ownership so a durable query snapshot can read the
  complete result count and emit successive bounded pages after the read
  snapshot that produced the rows has closed. Complete staging disables the
  client-facing page window instead of paging, so the staged result is never
  a silently truncated prefix; the request row budget still applies and
  reports its own typed limit failure. The shared rows expose the element
  count and the same page materialization only, because no caller borrows an
  individual row.

## Runtime Flows

```text
Read closure/admission -> QueryExecution retained primary-key borrow
  -> bounded retained item read
  -> runtime-bound decode/row authorization -> pre-admitted retained slot
  -> admitted destination or consuming public output -> owner release
```

## State, Ownership, and Lifecycle

| State | Owner | Lifetime |
|---|---|---|
| Authority/transaction/meter | Read session | Session operation |
| Stored bytes | Serialization byte owner | Decode/returned payload |
| Identifier/model/slot | QueryExecution aggregate | Destination/promotion |
| Canonical/index row | Destination owner | Query pipeline |
| Resolved Directory/Subspace | Directory, per transaction | Resolving transaction |
| Shared RDF graph result | Shared graph owners, refcounted | Until the last shared owner is released |

## Failure, Concurrency, and Constraints

Authorization, meter mismatch, malformed/unknown type, bounded-read, decode,
budget, cancellation, and cleanup remain distinct failures. One transaction is
used serially. Intermediate polymorphic reads expose only the retained owner;
raw entity arrays exist only at the consuming public-output boundary.

## Verification and Change Impact

| Contract | Evidence |
|---|---|
| Scan meter provenance | Signature/source audit proves no external meter input and session-meter derivation. |
| Point authority/meter | Regular and polymorphic denial and foreign-key-meter tests observe zero point reads. |
| Admission, order, absence | Budget-before-read and present/missing sequence tests. |
| Failure/cancellation/release | Later-failure and suspended-read tests end at zero retained resources. |
| Public read cancellation | A public copyable read suspended at its own commit and cancelled there still returns its callback value, as does an identically suspended and cancelled write. |
| Retained query cancellation | A canonical query that opened its own transaction and was cancelled at that transaction's commit reports the cancellation. The nested branch has no test: a caller-owned transaction leaves no commit to suspend, and the work budget throws first, so the absence of the check is not separately observable there. |
| Collecting-to-Ready boundary | The collecting response declares no promotion and the ready response has no reachable initializer, so promoting outside `finalizePostClosureResult` fails to compile. |
| Composition lease lifetime | A Base lifecycle drain does not complete while a composition domain commit is suspended, and completes once that commit returns while the test still holds a strong reference to the member lease, so a lease released at an ARC release point cannot satisfy it. |
| No raw escape | Source audit rejects general borrows, raw returns, and unmarked bridges. |
| Bounded item path | Envelope and every chunk are observed as bounded reads. |
| Directory binding | [Directory](Directory/DESIGN.md) owns the canonical component, tag derivation, and layout-rejection evidence. |
| Graph shared ownership | Retained-buffer tests observe the reservation surviving the consumed linear owner, bounded page copies, and zero retained resources after the last shared owner is released. |
| Row shared ownership | The same retained-buffer evidence covers staged rows, and complete staging is observed to carry no continuation so a durable snapshot never publishes a truncated result. |

Owners: [polymorphic retained tests](../../Tests/DatabaseEngineTests/PolymorphicRetainedResourceContractTests.swift),
[item storage tests](../../Tests/DatabaseEngineTests/ItemStorageResourceTests.swift),
and [authorization tests](../../Tests/DatabaseEngineTests/ReadAuthorizationCapabilityTests.swift).

## Schema-driven Authorization

`AuthorizationPolicyHandler(schemaDriven:permits:)` binds an immutable schema
entity to a caller-supplied operation/context decision. It reuses
the schema runtime’s canonical `EntityRuntimeDefinition.identity(for:entity:)` for canonical identifier and dynamic
Partition validation. Entity mismatch and invalid identities throw; no missing
policy becomes permission. The existing request delegate still evaluates field
security after entity authorization. All captured state is immutable and
Sendable, with no platform-dependent isolation changes.

```text
Schema entity + operation/context decision
  -> AuthorizationPolicyHandler -> canonical identity validation
  -> RequestSecurityPolicyDelegate -> field policy -> existing transaction
```

The server owns configuration and role selection. Framework tests must exercise
allow, deny, entity mismatch, malformed identity, and dynamic Partition identity;
server integration must exercise actual persisted CRUD and rejected credentials.

Verification: `Tests/DatabaseEngineTests/SchemaAuthorizationTests.swift` exercises
operation/context decisions, default-deny, invalid entity/identifier/Partition,
and canonical resource identity. The complete `DatabaseEngineTests` target also
covers existing request authorization and field restrictions. Native verification
uses the identity-checked 2026-09-04 toolchain; this change adds immutable captures
only and introduces no target-specific storage or synchronization branch.

### Planned SQL operation kernel

The accepted SQL program is specified by
[DatabaseKit SQL](../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md).
The contracts below are S00 design obligations for later implementation, not
current capability claims. QueryExecution owns relational execution; this
module owns the aggregate operation, catalog, transaction, and generation
lifecycle that composes it with mutation, schema, and StorageKit contracts.

#### Current implementation facts

| Path | Confirmed behavior | Remaining planned change |
|---|---|---|
| [Canonical rows](QueryExecution/DatabaseContext+CanonicalRows.swift) | Source rows use field-name maps; projection validation rejects duplicate output names. | Typed positional relation result bound before evaluation. |
| [Statement mutation](Mutation/DatabaseEntityStatementMutationExecutor.swift) | INSERT/UPDATE/DELETE run on supplied transaction; RETURNING and INSERT SELECT are rejected. | Complete DML using one admitted transaction and retained RETURNING result. |
| [Schema publication](Schema/DBContainer+SchemaPublication.swift) | Schema registry and generation metadata are persisted transactionally. | Logical SQL catalog and transaction-scoped DDL candidate publication. |
| [Mutation journal](Transaction/TransactionMutationJournal.swift) | First-seen identity order, net mutation effects, and footprint reservations are retained. | Restore logical journal state when StorageKit checkpoint rolls back. |

These observations come from source and existing test definitions. S00 has
not supplied new runtime SQL compatibility evidence.

#### Execution composition and transaction authority

```text
statement/block operation owner
    -> schema lease + authority + one exact work meter
    -> logical SQL binding -> QueryExecution
    -> admitted transaction
        +-> primary mutation + indexes + relationships + constraints
        +-> Directory/Partition metadata
        +-> logical SQL catalog + schema generation
        +-> StorageKit ordered checkpoint journal
    -> drain cursors, sessions, vaults and physical transactions
    -> release Partition/schema/sequence leases and required recovery work
    -> authoritative closure + applicable cancellation check
    -> purpose-specific Ready result
```

Same-domain operations share one data transaction. Feature executors consume
bound capabilities and cannot commit, cancel, choose a different Partition,
or construct Ready. MultiBase is a non-default feature and does not turn
SQL blocks into cross-domain atomic transactions. Cross-domain writes are
rejected rather than committed partially. Primary/index/relationship/
constraint/catalog/Directory changes are one invariant; SQL must not bypass
existing precondition, idempotency, authorization, or maintenance contracts.

#### SQL catalog and schema generation

- A persisted logical SQL catalog owns stable relation and column IDs,
  logical schema names, constraints, views, sequences, and SQL typmods.
  Public declarations belong to DatabaseKit; their persisted interpretation
  and runtime assembly belong to this module.
- SQL namespace, SQL primary key, internal row identity, and physical
  Directory/Partition identity are distinct. Rename changes logical catalog
  names without moving a Directory. A table without a SQL primary key still
  has internal row identity; arbitrary and composite SQL keys are supported.
- Dynamic-table DDL builds and validates a candidate runtime before staging
  data, catalog, and generation in one transaction. The candidate becomes
  active only after authoritative commit. Failed or cancelled operations
  do not publish it; commit-unknown is not interpreted as rollback.
- Static Swift model metadata remains its application's contract. A SQL
  change incompatible with its fields, identity, Directory, index, reference,
  or security declarations is rejected before mutation. SQL names never
  reinterpret Directory, Partition, Base, Tenant, or policy authority.
- Existing asynchronous schema-application jobs remain a different
  operation. Job acceptance is not SQL DDL completion. DDL that cannot fit
  configured transaction resources fails explicitly instead of switching to
  a job. Stale prepared plans and schema leases fail or are rebound before
  accessing storage; mixed-generation execution is prohibited.

#### SQL block, checkpoints, and sequence lifecycle

| State | Owner | Restoration or terminal requirement |
|---|---|---|
| Physical writes and read overlay | StorageKit transaction checkpoint journal | Ordered set/clear/range-clear/atomic and native Directory mutations roll back together. |
| Mutation effects, decoded caches, deferred constraints, candidate schema | Framework block state | Restore state matching the physical checkpoint; release abandoned retained claims. |
| Consumed work, elapsed deadline, cancellation | Exact operation meter | Never rewind on ROLLBACK TO or retry. |
| Retained bytes/rows | Concrete owners and reservations | Release only claims whose owners are destroyed; surviving results retain claims. |
| Block SET/PREPARE/temp state | Framework request owner | Exists only in this request; ends before readiness. |
| Sequence allocation cursor and recovery obligation | Framework sequence request lease | Nonrollback consumption, persisted recovery, authoritative close before readiness. |

SAVEPOINT restores both physical and Framework logical state. Releasing a
checkpoint does not release objects still referenced by a later result.
Rollback cannot reset resource consumption to evade the work budget. The
physical journal is a StorageKit responsibility, including FoundationDB
Directory-layer calls; Framework does not duplicate backend key semantics.

Sequence accesses use a request lease and nonrollback cursor. A value
consumed by NEXTVAL remains consumed after savepoint or transaction rollback.
The sequence owner persists allocation/recovery evidence, keeps stable
allocations across automatic attempts of the same request, and completes
required recovery writes and their transaction closure before producing
Ready. Allocation gaps are legal; duplicate allocation after rollback, retry,
or restart is not. CURRVAL/LASTVAL are request-scoped. Request serialization
and intermediate visibility differ from PostgreSQL; this difference is part
of the profile and cannot be presented as PostgreSQL concurrency parity.

The aggregate block retains ordered collecting statement results and emits
no early public output. Failed transaction state admits only the profile's
rollback recovery operations. Operation failure and cleanup failure are
preserved together; cancellation and commit-unknown never become empty
success or fabricated command counts.

#### Verification and change impact

| Invariant | Required behavior evidence in later sprints |
|---|---|
| One admitted transaction | DML/DDL primary, index, relationship, constraint and Directory readback after commit; unchanged state after failure. |
| Checkpoint completeness | Roll back data, native Directory creation, index, catalog, and deferred checks together on each production backend. |
| Static-model protection | Reject incompatible DDL before observable state changes. |
| Generation atomicity | Restart restores committed generation; stale lease and publication failure cannot expose a partial runtime. |
| Sequence nonrollback lifecycle | Rollback, retry, cancellation, restart and commit-unknown preserve allocation/recovery guarantees. |
| Readiness | Suspended commit/cleanup/schema publication/sequence recovery exposes no results; terminal owners retain no transaction resource. |
| Work accounting | Repeated rollback cannot refund consumed work; abandoned retained owners release exactly their own claims. |

A change to these contracts revalidates QueryExecution, Read, Directory,
StorageKit adapters, Database entry points, DatabaseKit semantic and wire
contracts, and server/host publication paths. New runtime subcomponents must
receive their own directory-local design before source implementation; this
module does not invent a second lower-level transaction contract.

### S04 composition

[QueryExecution](QueryExecution/DESIGN.md#s04-frozen-selected-value-execution)
owns selected numeric QueryValue kernels and metered exact aggregate state;
[Base](Base/DESIGN.md#s04-selected-sql-aggregate-composition) consumes that same
contract under MultiBase. Operation authority, transaction closure, retained
Ready publication and raw RDF/primitive behavior retain their owners. Changes
to dynamic workspace require both callers' lifetime evidence before this
module accepts the lower contract.

### S05 raw pattern and public failures

The raw expression evaluator used by Mutation consumes the same metered
[SQLPatternMatching](QueryExecution/SQLPatternMatching/DESIGN.md) program as
selected SQL execution. Its production caller supplies the operation meter;
pattern evaluation without that meter rejects explicitly. SQL pattern operands
are strict on NULL and require text values. Raw evaluation does not substitute
SPARQL XPath semantics or an unmetered matcher.

```text
Mutation -> raw expression evaluator -> SQL pattern program -> operation meter
```

The public expression failure contract distinguishes invalid pattern syntax,
unsupported pattern constructs, and invalid function arguments. Pattern
offsets are nonnegative pattern offsets, separate from SQL occurrence/source
locations. Work limits and cancellation retain their original typed failures.
Server adapts these three failures through its own operation error mapper.
Focused raw-expression, bounded-pattern, and Server mapper tests verify this
boundary; selected SQL integration remains QueryExecution-owned.
