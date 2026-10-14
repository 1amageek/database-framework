# QueryExecution

## Purpose and Scope

The QueryExecution component owns the DF-06F0 retained polymorphic aggregate,
the DF-06R retained regular-model fetch, and the only operations that may
transform retained values into canonical rows, index rows, or an explicit
final public output.

- Parent: [DatabaseEngine](../DESIGN.md).
- Children: none in the current retained-fetch design.
- Planned S01–S10 SQL execution obligations are recorded below; implementation
  completeness remains tracked by the workspace progress record.

## Responsibilities and Boundaries

QueryExecution couples retained primary-key sources, aggregate array storage, present/missing slot claims,
retained identifiers, decoded model reservations, runtime-derived type
metadata, and destination footprint admission. It rejects cross-meter
composition before constructing a destination value.

It consumes a Read-owned admission and never creates or widens authority. It
does not authorize storage access, resolve runtime registrations, decode
stored envelopes, or own specialized scoring/ranking/search policy. It does
not expose a general model or identifier borrow because those values are
copyable and could escape without their reservation.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Aggregate role in the retained flow | Defines cross-component assumptions and outputs. | Any new output must be an explicit destination boundary. |
| [DatabaseKit SQL](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md) | depends on | SQL profile and typed semantic model | Supplies statements, values and ordered column contracts. | Type/equality changes invalidate every relational consumer. |
| [QueryAST](../../QueryAST/DESIGN.md) | depends on | Text-to-semantic syntax contract | Canonical execution receives semantic input from the entry layer. | This is a contract relationship, not permission for an engine-to-parser import. |
| [Core](../Core/DESIGN.md) | used by | Pre-admitted builder append | Produces present and missing entries. | Runtime metadata is validated before append. |
| [Read](../Read/DESIGN.md) | coordinates with | Exact session work meter | Supplies the meter to builder and destination. | Budget equality is not identity. |

## Architecture

```text
Builder
  -> pre-admitted slot
      -> missing
      -> retained identifier + retained decoded model + type metadata
          -> noncopyable aggregate
              +-> append canonical source row
              +-> append index row with admitted annotations
              +-> borrow a retained vector through a noncopyable field view
              +-> consuming public-output promotion

retained primary-key source + Read admission
  -> async scoped key borrow
      -> bounded point fetch
          -> pre-admitted retained regular-model slot

Fusion sealed projection + retained primary-key source
  -> projection-bound Read admission
      -> the same bounded retained point-fetch implementation
```

## Contracts and Invariants

- The aggregate is noncopyable and owns one retained array whose entries keep
  every identifier/model reservation alive.
- Builder append requires identifier, decoded model, and aggregate storage to
  share the same `DatabaseWorkMeter` object.
- Stored type code, runtime entity name, model entity, and retained identifier
  are validated as one entry invariant before append.
- A missing input is represented by a committed missing slot, preserving count
  and order.
- Regular retained fetch rejects a foreign source meter before authorization,
  storage, or destination allocation. Each key is borrowed asynchronously so
  its owner and reservation remain live until the point fetch completes.
- Regular retained fetch preserves source order, duplicates, missing `nil`
  slots, the sealed snapshot mode, and the session transaction.
- The regular complete-model entry point requires all entity fields. Fusion
  uses a separate projection-bound entry point whose field set is derived only
  from its sealed authorization evidence; QueryExecution consumes either as
  the same immutable Read-owned admission and never chooses the field set.
- Canonical retained primary-key owners do not conform to `Collection` and do
  not expose a raw `Tuple` subscript or return value.
- Destination builders must share the aggregate meter. Complete destination
  footprint and array growth are admitted before the row or annotation map is
  materialized.
- Package API does not expose general `withModel`, `withIdentifier`, entry,
  backing array, or reservation access. A borrowing closure around a copyable
  value is not an ownership boundary because the closure can copy it. A
  noncopyable field view may expose only synchronous element borrows; it cannot
  expose the model, copyable vector, or owner.
- Canonical and index conversion are purpose-specific operations whose
  destinations retain their own admitted representations.
- Public output promotion consumes the aggregate and is allowed only at a
  top-level API boundary. Intermediate feature paths cannot invoke it.

## Runtime Flows

```text
prepare slot claim
    -> producer constructs retained entry
        -> append commits claim to aggregate owner
            -> destination computes exact footprint
                -> destination pre-admits
                    -> materialize once into destination owner
```

If producer construction throws, the linear slot admission rolls back. If a
later entry fails, destruction of the unfinished builder releases all earlier
entries and the aggregate array claim.

## State, Ownership, and Lifecycle

| Value | Owner | Released or transferred when |
|---|---|---|
| Aggregate array/capacity | Noncopyable retained buffer | Aggregate is consumed or destroyed |
| Slot row claim | Array append admission, then retained buffer | Append is abandoned or aggregate ends |
| Identifier tuple/bytes | Retained primary key in entry | Entry ends |
| Regular fetch source key | Retained primary-key collection | Its async point fetch completes |
| Decoded model | Retained model entry | Entry ends or explicit final promotion consumes aggregate |
| Canonical/index row | Destination retained builder/owner | Downstream query owner ends |

## Failure, Concurrency, and Constraints

All mutation is task-local inside the noncopyable builder. Published aggregates
are immutable and `Sendable`. Footprint overflow, work-meter mismatch, invalid
entry metadata, destination budget failure, and materialization failure remain
typed failures; no partial destination row is committed.

## Verification and Change Impact

- [PolymorphicRetainedResourceContractTests](../../../Tests/DatabaseEngineTests/PolymorphicRetainedResourceContractTests.swift)
  proves aggregate order, missing slots, later-failure release, and final
  release.
- Source audit must reject any general copyable model/identifier borrow or raw
  collection-returning intermediate API.
- Canonical-row tests prove direct retained-to-destination construction without
  an intermediate `QueryRow` array.
- Specialized index migration tests own annotation and field-comparison
  semantics; this component owns only noncopyable access, footprint-first
  append, and same-meter rejection.
- [RetainedRegularModelFetchContractTests](../../../Tests/DatabaseEngineTests/RetainedRegularModelFetchContractTests.swift)
  proves regular order, duplicates, missing slots, suspended source lifetime,
  authorization, failure, cancellation, and final release.

### Planned typed SQL relational execution

The exhaustive feature/profile contract is owned by
[DatabaseKit SQL](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md).
This component owns binding, planning, SQL evaluation, relational operators,
and retained relation production above its parent's operation lifecycle.
Syntax is owned by [QueryAST](../../QueryAST/DESIGN.md), and primitive value
invariants remain in DatabaseTypes. These are planned implementation
contracts; current dictionary-backed rows and incremental numeric aggregates
do not yet satisfy them.

```text
DatabaseKit statement + schema lease + parameters + admitted scope
    -> relation/name scope resolution
    -> type / typmod / collation / overload / volatility binding
    -> ordered output schema and physical plan
    -> retained positional relations
        -> join / filter / set operation / CTE
        -> grouping / aggregate / window / recursive frontier
        -> ordered projection / pagination
    -> collecting relation owner
    -> parent closes producing resources
    -> purpose-specific Ready relation
```

#### Binding and positional relation contracts

- Bind output columns before evaluating rows: ordered labels, stable column
  references, type, nullable metadata and typmod remain present for zero
  rows. Duplicate labels are valid output; name lookup can be ambiguous
  without data loss. Internal slots and output ordinals are never inferred
  from dictionary iteration, field labels, or the first row.
- Resolve aliases, qualified names, CTE visibility/shadowing, lateral and
  outer scopes, output alias and ordinal precedence before selecting a
  physical access path. Invalid or ambiguous references fail even on empty
  input and cannot disappear through pushdown.
- Bind unknown literals and parameters, operator/function overloads,
  assignment/explicit casts, common types, collation and SQL equality once
  into a typed plan. JOIN, GROUP, DISTINCT, set operations and SQL indexes
  use one equality/hash contract. Output nullability is metadata, not a
  guarantee that outer joins cannot create NULL.
- CASE, COALESCE and related conditional expressions evaluate only required
  branches, including their subqueries. Binding still checks all branch
  types. SQL three-valued logic, empty quantified relations, cardinality
  errors and NULL ordering follow the profile.
- Physical pushdown is legal only with proof of the same typed comparison,
  NULL order, collation, residual predicates and pagination meaning. A
  backend optimization never replaces canonical semantics silently.

#### Numeric, functions, and relational algorithms

SQL numeric remains based on DatabaseTypes' finite 38-digit intrinsic
representation. Framework selects SQL result types, typmod, precision,
scale, rounding and overflow semantics. Pure rounded primitive operations
are added without weakening the existing exact-operation contract. Numeric
display scale belongs to the DatabaseKit query value, not primitive identity.
Wider bounded workspace and exact limb accumulation allow intermediate sums
to exceed result width when the final AVG is representable. A double
conversion is not an implicit numeric fallback.

Function binding carries input/result type, NULL behavior, volatility and
resource effect. Transaction/statement clock values are supplied by the
operation owner; timezone/regex/collation capability follows the explicit
profile and injected data. Unsupported overloads fail before evaluation.

The accepted inventory includes bag-preserving set operators, DISTINCT ON,
grouping sets, FILTER, ordered aggregates, ordered-set statistics, windows
and named frames, nonrecursive/recursive CTEs with SEARCH/CYCLE, and lateral
table functions. Each must execute from SQL text through this typed path.
Recursive working/frontier tables and window peer/frame state are concrete
owned relations, not uncharged Arrays or repeated implicit materialization.

#### Resource ownership and failures

| Resource | Charge before | Terminal requirement |
|---|---|---|
| Bound plan/type slots | Allocation of nodes and metadata | Operation or reusable-plan owner releases reservation. |
| Join/hash/group/set table | Capacity growth and value retention | Partial table released on failure/cancel. |
| Sort/window/CTE relation | Materialization, sort workspace and retained payloads | Same exact meter through every consuming operator. |
| Recursive frontier/visited state | New row, path, cycle key, and next iteration | Depth/work/rows/bytes are bounded; abandoned frontier drains. |
| Numeric/regex/JSON workspace | Parse, expansion, match, or arithmetic work | Resource limit remains typed; no altered-semantics fallback. |
| Output and copied page | External materialization/copy | Result owner retains full staging claim; page copies are measured. |

Configuration owns operational defaults and the selection of portable
limits. A request can tighten but cannot exceed validated backend maxima.
Consumed work and deadlines are monotonic across rollback and retry;
retained claims follow actual lifetime. Pointer borrows are synchronous and
bounded, never escaping their retained owner. Actor/mutex isolation and
Sendable contracts are identical across native, WASM and Embedded targets.

#### Verification and change impact

The existing [SQLRowExecutionTests](../../../Tests/DatabaseTests/SQLRowExecutionTests.swift)
and retained-owner tests prove only their exercised current paths. New
profile cases compare real PostgreSQL with Framework execution, including
column metadata for empty relations, duplicate labels, alias/ordinal binding,
NULL and bag semantics, numeric type/rounding, lazy branches, window peers,
recursive cycles, resource boundary failure and retained-owner release.
Same SQL text must traverse parser, binding and production executor;
independent parser and IR tests cannot jointly replace that proof.

Type/collation/equality changes invalidate all comparison consumers and index
pushdown assumptions. Retained representation changes invalidate canonical
destinations and public entry/wire adaptation. Transaction or readiness
changes belong to the parent operation owner and require its verification.
