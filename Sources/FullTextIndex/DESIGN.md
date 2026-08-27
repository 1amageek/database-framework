# FullTextIndex

## Purpose and Scope

`FullTextIndex` is the feature module for full-text index maintenance and
read-side query execution in `database-framework`. Its read contract covers
plain, scored, faceted, polymorphic, and autocomplete results. The parent
design is [`database-framework`](../../DESIGN.md); the child designs are
[`Autocomplete`](Autocomplete/DESIGN.md) and [`Facet`](Facet/DESIGN.md).

This design covers only read execution and its intermediate ownership. Index
layout, writes, token analysis, BM25 formula definition, Fusion orchestration,
and application schemas remain outside this module.

## Responsibilities and Boundaries

FullTextIndex owns:

- decoding and combining full-text posting and document metadata;
- preserving full-text match ordering and score annotations;
- translating an admitted read into canonical index rows;
- facet counting from completed retained rows;
- autocomplete cursor traversal and suggestion ordering.

It does not own authorization, transaction creation, schema leases, or work
meter identity. `DatabaseEngine` supplies one authorized
`DatabaseReadSession`, its transaction, and its `DatabaseWorkMeter`. Storage
access is performed through bounded point reads or the session-owned cursor.
The module never replaces a sealed authorization result with ambient state.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [`database-framework`](../../DESIGN.md) | parent | session, authorization, row, and work-meter contracts | Provides the admitted read boundary and canonical output type. | Re-check callers when session or row ownership changes. |
| [`DatabaseEngine Read`](../DatabaseEngine/Read/DESIGN.md) | depends on | `DatabaseReadSession`, `IndexReadResult`, bounded read capability | Owns authorization, snapshot, transaction, and result admission. | FullText must not create a nested transaction or perform policy decisions. |
| [`Autocomplete`](Autocomplete/DESIGN.md) | child | retained suggestion owner and cursor cleanup | Defines autocomplete-specific output ownership. | Limit is applied before public promotion. |
| [`Facet`](Facet/DESIGN.md) | child | row-field aggregation and retained metadata | Defines facet-specific aggregation ownership. | Facets must not inspect raw model objects. |

## Architecture

```text
DatabaseReadSession + DatabaseWorkMeter
              |
              v
      FullText read executor
        | bounded posting/metadata reads
        | retained match/key owners
        v
      session-bound retained fetch
        |
        +--> canonical IndexReadResult rows
        +--> facet metadata from completed rows
        +--> autocomplete retained suggestions
```

The dependency direction is from the feature executor to the `DatabaseEngine`
contracts. The feature module does not reach into backend implementations or
construct an independent resource owner.

## Contracts and Invariants

- Authorization is complete before the first storage read.
- Every posting and document-metadata point read uses the request's bounded
  point-read path and the same session work meter.
- Intermediate match keys, scores, and suggestions remain owned and charged
  until their consumer has completed; no raw collection is returned as a
  resource owner.
- Plain results preserve the posting order and set of matched identifiers.
- Scored results preserve BM25 ordering and deterministic identifier ties, with
  score represented as a row annotation.
- Facet counts are computed from completed retained result-row fields and are
  attached as the smallest metadata required by `IndexReadResult`.
- Missing and corrupt index values fail with typed errors; they are never
  converted to empty success.
- A foreign work meter is rejected. Success, failure, cancellation, and limit
  paths release every reservation exactly once.
- Cursors are finished on every exit path, including cancellation and decode
  failure. Public arrays are promoted only after the requested limit is known.

## Runtime Flows

For a canonical search, the executor decodes parameters, validates the sealed
session admission, resolves the readable index, reads bounded postings and
metadata, fetches retained models or polymorphic entries through the session,
and appends canonical rows. Faceted execution first completes the retained
rows, reads their fields for counting, then attaches retained metadata. Score
calculation is completed before row append so row order is stable.

Autocomplete opens one session-owned range cursor, admits each suggestion into
the session meter, sorts the retained owner, retains only the requested
prefix, promotes that prefix at the public API boundary, and finishes the
cursor before returning.

## State, Ownership, and Lifecycle

The session owns transaction and meter identity. FullText-owned intermediate
buffers own their reservations and are released on all paths. A retained
polymorphic or persisted-model fetch remains alive through row materialization;
rows then own their own retained backing. Autocomplete owns its retained
suggestion buffer until promotion. No pointer or borrowed model view escapes
its synchronous borrow.

## Failure, Concurrency, and Constraints

Read helpers are async because storage and cursors suspend. They use the
session's read capability and do not hold a mutex across `await`. Every bounded
read checks cancellation and meter admission before touching storage. Decode,
missing-value, authorization, work-limit, and cursor failures propagate as
failures. A failed or cancelled operation still performs cursor finish and
reservation cleanup.

## Verification and Change Impact

The owning tests are in `Tests/FullTextIndexTests`. Focused behavioral tests
must cover plain, score/tie ordering, facets, polymorphic output, autocomplete
limit/prefix ordering, missing/corrupt values, denial-before-read, foreign
meters, cancellation, and final reservation release. A change to session,
retained-fetch, or canonical-row contracts requires rechecking this module and
the parent `DatabaseEngine Read` design. Package-level verification is run
only after the feature boundary has converged.
