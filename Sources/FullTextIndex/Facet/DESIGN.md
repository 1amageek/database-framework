# Facet

## Purpose and Scope

This component owns full-text facet aggregation and its canonical result
metadata. Its parent is [`FullTextIndex`](../DESIGN.md). It covers read-side
counting only; index writes, facet schema declarations, and authorization
remain with their owning layers.

## Responsibilities and Boundaries

Facet consumes completed `IndexReadResult` rows, extracts the requested field
values, counts them under the session work meter, applies deterministic count
and value ordering, and attaches bounded metadata. It does not borrow raw
persisted models or polymorphic entity payloads, and it does not create a
transaction or re-evaluate authorization.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [`FullTextIndex`](../DESIGN.md) | parent | full-text result and read contract | Supplies search results and feature boundaries. | Keep facet aggregation after row completion. |
| [`DatabaseEngine Read`](../../DatabaseEngine/Read/DESIGN.md) | depends on | `IndexReadResult` and metadata ownership | Supplies canonical row views and retained metadata. | Metadata must use the same work meter as rows. |

## Architecture

```text
retained search results
        -> completed canonical rows
        -> field-value counter + meter
        -> deterministic bounded buckets
        -> retained IndexReadResult metadata
```

## Contracts and Invariants

- Authorization and index resolution precede all storage reads.
- Facet values come from completed canonical row fields, never a raw model
  borrow or a separate entity re-fetch.
- Counts are deterministic; ties are ordered by the facet value.
- Metadata includes only the total count and requested bounded facet buckets.
- Counter and metadata reservations remain live until output ownership is
  established and release exactly once on every failure path.

## Runtime Flows

The parent executor materializes the matching rows, passes their field values
to the counter, sorts and bounds each bucket, then attaches the resulting
metadata to the row result. The output retains rows and metadata under one
session meter.

## State, Ownership, and Lifecycle

The counter owns its dictionary reservation while counting. The metadata
owner transfers its reservation to `IndexReadResult`; temporary counters and
scratch arrays are released before return. Borrowed row views are scoped to
the synchronous row callback.

## Failure, Concurrency, and Constraints

Invalid facet fields, malformed counts, work-limit exhaustion, cancellation,
and row access failures propagate. No facet error is converted to an empty
result. Counting never suspends while holding a lock.

## Verification and Change Impact

Facet tests verify counts, deterministic ties, row-limit versus total-count
semantics, invalid/corrupt input, denial-before-read, foreign-meter rejection,
and final reservation release. Changes to canonical rows or metadata
ownership require rechecking this component and its parent.
