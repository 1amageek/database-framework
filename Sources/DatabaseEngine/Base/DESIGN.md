# Base Composition Execution

## Purpose and Scope

This component owns MultiBase-only Base lifecycle and read-only Composition
execution. Its parent is [DatabaseEngine](../DESIGN.md); it has no children.
The S01 change binds and carries positional query schemas across existing
member sessions, global merge, aggregate, DISTINCT and final materialization.

## Responsibilities and Boundaries

Composition owns member lease and federated snapshot coordination, global
row ordering, provenance and collecting-to-ready completion. QueryExecution
owns expression typing; Read owns session admission. GraphIndex owns RDF
identity qualification. Composition creates no second authority or transaction
to discover a member schema.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | MultiBase execution and lifetime | Provides schema and Base leases. | Default builds do not enable MultiBase. |
| [Read](../Read/DESIGN.md) | depends on | Admitted session and retained page | Binds and reads within the captured member transaction. | A page must retain the bound columns on zero rows. |
| [QueryExecution](../QueryExecution/DESIGN.md) | depends on | Bound output columns and checked value normalization | Supplies static output signatures. | Schema cannot depend on a member's first row. |
| [GraphIndex](../../GraphIndex/DESIGN.md) | used by | Feature-owned member execution and identity qualification | Uses the shared merger for SPARQL. | RDF columns retain primitive term semantics. |

## Architecture

```text
Composition lease + operation schema lease
    -> captured domain transactions + authorized member sessions
        -> bind every member output and require matching columns
            -> positional member pages
                -> merge / aggregate / distinct + provenance
                    -> bounded retained ready events
                        -> close every snapshot transaction and lease
                            -> began(columns) / row events
                                -> host persistence or CompositionQueryResponse
```

## Contracts and Invariants

- Every member output is bound before the first row fetch or began event.
  Column order, type, nullability and labels must agree across members.
- Every continuation page must match that schema, including empty pages.
- Relational request parameters retain their references, payloads and explicit
  declarations until QueryExecution binds the complete query in an existing
  admitted member session. Its resolved immutable declarations feed every
  member schema/page, aggregate operand and cross-Base JOIN binding; splitting
  the query cannot restart contextual parameter inference. Feature-owned
  member executors retain their existing parameter contract.
- Output labels may duplicate. Slots, aggregate operands, identity and row
  alignment remain positional; dictionary projection is an explicit adapter.
- CompositionQueryMetadata carries columns. CompositionQueryResponse retains
  those columns with zero or more provenance-bearing rows, and its public
  initializer validates every row against the complete schema.
- Member schemas are bound within the existing admitted read sessions. Domain
  transactions, authorization, cancellation and lease release remain unchanged.
- Prepared rows, identity storage, aggregate destinations and the complete
  pending event buffer are admitted with the exact request meter before retention.
- External event callbacks run only after the snapshot vault has drained, every
  Domain transaction is terminal and member leases have finished (SPEC F-26).
  A producer close failure or cancellation publishes no metadata or row events.

## Runtime Flows

Source projection and local aggregate-operand projection each bind before
their respective fetches. Global aggregate results normalize to the bound
output type before positional row construction. SQL expressions use SQL type
binding; SPARQL output uses GraphIndex's term and aggregate type contract.
Parameter preflight uses the snapshot's captured member and exact request
meter before any row fetch. It opens no additional authority or transaction.
The Server adapter passes raw request parameters into planner options.

## State, Ownership, and Lifecycle

The snapshot owns member leases and domain transactions. Metadata owns one
immutable column array shared by cursor expectations and emitted output.
Cursors retain member pages until consumed; prepared row owners and DISTINCT
state retain their claims until merge completion or failure cleanup. The planner retains a bounded complete result before leaving the snapshot.
Only after authoritative close does it call an external consumer. Consumer
backpressure stops publication; it cannot interleave host control writes with
producer Domain transactions. Buffer capacity and metadata/provenance storage
have request claims; row payloads retain their existing producer claims once.
Consumer page size bounds delivery batches, not the complete Ready result.
Every pending output keeps its prepared-row and producer-page claims until
publication finishes; an insufficient request budget fails before any callback.
An aggregate member page can retain its source and projected lookahead owners
while one prepared row is admitted. The row overlap is
`2 * (pageSize + 1) + 1`; its minimum is five rows. Page sizing preserves
that overlap rather than releasing known source claims early.
DISTINCT result aliases retain row and workspace provenance claims after
workspace removal until the last returned result owner is released. An opaque
workspace payload lease follows every derived DISTINCT origin into Ready
storage; Ready does not charge that same contributor backing array again.
Cross-Base JOIN owns its newly selected two-member contributor array, admits
its capacity before construction and propagates a distinct opaque payload
lease with each row event. It does not borrow the entire Composition member
array when the JOIN selects only a subset.
Final caller-owned response construction is the explicit materialization boundary.
Every production CompositionQueryRow carries an opaque producer payload lease.
Prepared member rows retain the member page's producer leases; preparation
admits destination structure and newly qualified RDF blank-node text only.
A retained event consumer admits its row and lease-handle storage before
retaining the event, and keeps that lease until final output transfer. Manually
constructed event rows have no producer lease and require full payload admission.

## Failure, Concurrency, and Constraints

Schema mismatch, row arity/type/nullability failure and unsupported operations
remain typed errors. The existing actor/mutex isolation and bounded merge
limits apply unchanged on all supported targets. No lock covers I/O or await.

## Verification and Change Impact

[CompositionQueryExecutorTests](../../../Tests/DatabaseEngineTests/CompositionQueryExecutorTests.swift)
owns aggregate, ordering, DISTINCT, zero-row schema, duplicate labels and
cross-Base behavior. Its publication-lifetime regression requires both Base
lease counts to be zero in metadata and row callbacks, positive retained claims
during delivery, and complete cleanup after consumer cancellation. Budget
zero/one-byte-short cases reject the first Ready buffer admission at result
materialization; invalid aggregate operands fail static type binding before
member fetching. Retained aggregate probe aliases keep their claims until all
probe and local copies are explicitly released.
Parameter regressions cover positional/named identity across paged members,
typed NULL projection, aggregate operands, cross-Base JOIN expressions and
whole-query ambiguity rejection before any event is published.
Server durable-paging fixtures must cover both rejection below complete-result
retention and successful bounded publication with the Server's page destinations
inside the same request budget. SQL aggregate assertions consume their bound
column types and values, including numeric SUM output.
[CompositionSPARQLAuthorizationTests](../../../Tests/GraphIndexTests/CompositionSPARQLAuthorizationTests.swift)
owns RDF primitive values, authorization and provenance. Changes to output
columns require member executors, retained page consumers, model decoding and
external operation adapters to use the same schema. Runtime backend evidence
must exercise MultiBase; a default-trait compile is insufficient.

### S04 selected SQL aggregate composition

[S04 QueryExecution](../QueryExecution/DESIGN.md#s04-frozen-selected-value-execution)
owns numeric overloads, displayScale and widened accumulator behavior. Base
consumes its QueryValue input/result and selected input type directly in
CompositionQueryPlanner. SUM/AVG must not call comparablePrimitive before
accumulation or rebuild displayScale after finalization. The same operation
meter pre-admits dynamic limb/math capacity; the existing fixed state claim
covers only fixed state. Clear/failure/cancel/final-output paths release the
accumulator exactly at its actual lifetime. S04-C09/C10/C12 require MultiBase
widened AVG/cancellation SUM, scale retention, budget failure and release.
