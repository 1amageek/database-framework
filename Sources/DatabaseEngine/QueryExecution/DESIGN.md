# QueryExecution

## Purpose and Scope

The QueryExecution component owns the DF-06F0 retained polymorphic aggregate,
the DF-06R retained regular-model fetch, and the only operations that may
transform retained values into canonical rows, index rows, or an explicit
final public output.

- Parent: [DatabaseEngine](../DESIGN.md).
- Children: none in the DF-06F0 scope.

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
