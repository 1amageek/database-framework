# DatabaseEngine

## Purpose and Scope

Module authority for platform-neutral in-process execution. DF-06F0 and
DF-06R compose retained polymorphic and regular-model paths from [Read](Read/DESIGN.md),
[Core](Core/DESIGN.md), [Serialization](Serialization/DESIGN.md), and
[QueryExecution](QueryExecution/DESIGN.md), over keyspaces bound by
[Directory](Directory/DESIGN.md).

- Parent: [database-framework](../../DESIGN.md).
- Children: the five linked component designs above.

## Responsibilities and Boundaries

The module owns schema-bound sessions, persistence decoding, query execution,
and request accounting. It exposes purpose-neutral contracts to index modules;
scoring, ranking, bitmap, full-text, and vector policy remain outside it. It
owns no key layout: every keyspace it reads or writes is resolved from a
StorageKit Directory node through [Directory](Directory/DESIGN.md).

## Related Designs

| Design | Relationship | Contract Used | Cautions |
|---|---|---|---|
| [Read](Read/DESIGN.md) | child | Session admission | Context methods cannot bypass it. |
| [Core](Core/DESIGN.md) | child | Polymorphic orchestration | Does not decide query authority. |
| [Serialization](Serialization/DESIGN.md) | child | Retained bounded item read | Envelope and chunks are bounded. |
| [QueryExecution](QueryExecution/DESIGN.md) | child | Retained aggregate/destinations | Copyable raw values do not escape. |
| [Directory](Directory/DESIGN.md) | child | Declaration-to-node binding | A resolved node is valid only in its resolving transaction. |

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
| Composition lease lifetime | A Base lifecycle drain does not complete while a composition domain commit is suspended, and completes once that commit returns. |
| No raw escape | Source audit rejects general borrows, raw returns, and unmarked bridges. |
| Bounded item path | Envelope and every chunk are observed as bounded reads. |
| Directory binding | [Directory](Directory/DESIGN.md) owns the canonical component, tag derivation, and layout-rejection evidence. |
| Graph shared ownership | Retained-buffer tests observe the reservation surviving the consumed linear owner, bounded page copies, and zero retained resources after the last shared owner is released. |
| Row shared ownership | The same retained-buffer evidence covers staged rows, and complete staging is observed to carry no continuation so a durable snapshot never publishes a truncated result. |

Owners: [polymorphic retained tests](../../Tests/DatabaseEngineTests/PolymorphicRetainedResourceContractTests.swift),
[item storage tests](../../Tests/DatabaseEngineTests/ItemStorageResourceTests.swift),
and [authorization tests](../../Tests/DatabaseEngineTests/ReadAuthorizationCapabilityTests.swift).
