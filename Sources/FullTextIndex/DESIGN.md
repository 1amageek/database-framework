# FullTextIndex

## Purpose and Scope

`FullTextIndex` is the feature module for full-text index maintenance and
read-side query execution in `database-framework`. Its read contract covers
plain, scored, faceted, polymorphic, and autocomplete results. The parent
design is [`database-framework`](../../DESIGN.md); the child designs are
[`Autocomplete`](Autocomplete/DESIGN.md) and [`Facet`](Facet/DESIGN.md).

This design covers read execution, posting-candidate representation, and
intermediate ownership. A posting candidate is the single FullText-owned
combination of one decoded `Tuple` and its exact, self-contained packed
identifier suffix. Index layout, writes, token analysis, BM25 formula
definition, Fusion orchestration, and application schemas remain outside this
module.

## Responsibilities and Boundaries

FullTextIndex owns:

- decoding and combining full-text posting and document metadata;
- validating and admitting scanned posting identifiers into one candidate;
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
              |
              v
       bounded posting cursor
              |
              v
  scan/admission boundary (once)
    borrow suffix view
    admit slot and exact suffix owner copy
    detach suffix; admit decode allocations
    validate/decode Tuple from suffix once
              |
              v
  FullTextCandidateBatch (one collection reservation)
      Candidate { Tuple, packedSuffix: ByteString }
              |
              v
    ordered posting algebra
    compare packedSuffix and transfer batch ownership
              |
              +--> retained append reuses Tuple
              +--> canonical IndexReadResult rows
              +--> facet metadata from completed rows
```

The dependency direction is from the feature executor to the `DatabaseEngine`
contracts. The candidate is an internal FullText value; it does not change the
public query or result API, reach into a backend implementation, or create an
independent resource owner.

## Contracts and Invariants

- Authorization is complete before the first storage read.
- Every posting and document-metadata point read uses the request's bounded
  point-read path and the same session work meter.
- Each scanned suffix is first borrowed as a view, then its destination slot
  and exact self-contained `ByteString` owner copy are admitted to the batch
  reservation before detachment. Tuple decode allocations are admitted before
  creation through the `Tuple(packed:admitting:)` callback; validation and
  decoding happen exactly once before algebraic filtering.
- A candidate contains exactly one decoded `Tuple` and its exact packed suffix.
  It has no separate reservation and never stores the cursor's borrowed key.
- `FullTextCandidateBatch` owns one collection reservation. Complete candidate
  storage is charged to that reservation before preservation; an admission
  failure never appends a candidate.
- Ordered posting algebra compares candidates by their already-owned packed
  suffix and transfers or releases the batch reservation. It does not decode a
  suffix, materialize an existential element array, call `Tuple.pack()`, or
  reconstruct a second identifier representation. Thus malformed data fails
  even if intersection or union would otherwise exclude it.
- Final retained-key append reuses the candidate's already-decoded `Tuple`; it
  does not decode the suffix or repack the tuple.
- Intermediate match keys, scores, and suggestions remain owned and charged
  until their consumer has completed; no raw collection is returned as a
  resource owner.
- Plain results preserve posting order and matched identifiers. Scored results
  preserve BM25 ordering and deterministic identifier ties. Facets are counted
  from completed retained result-row fields.
- Missing and corrupt index values fail with typed errors; they are never
  converted to empty success.
- A foreign work meter is rejected. Success, failure, cancellation, and limit
  paths finish cursors and release every batch/intermediate reservation exactly
  once.

## Runtime Flows

For a canonical search, the executor decodes parameters, validates the sealed
session admission, resolves the readable index, and opens the bounded posting
cursor. The scan/admission boundary admits destination bookkeeping and the
exact suffix owner copy, detaches it, admits each decode allocation before it
is created, and decodes the `Tuple` once. Only then is the candidate appended
to the batch and passed to ordered algebra. Intersection and union compare
packed suffixes and transfer surviving candidates without tuple conversion;
decode or cursor failure releases the whole batch after cursor cleanup. The
retained-key builder reuses the winning tuples before retained models or
polymorphic entries are fetched through the session and canonical rows are
appended.

Plain, scored, and polymorphic posting reads share this candidate path.
Scoring is complete before row append. Faceted execution completes retained
rows before counting their fields and attaching metadata.

Autocomplete opens one session-owned range cursor, admits each suggestion into
the session meter, sorts the retained owner, promotes only the requested
prefix, and finishes the cursor before returning.

## State, Ownership, and Lifecycle

The session owns transaction and meter identity. FullTextCandidateBatch owns
the single collection reservation and all candidate tuples and detached packed
suffixes until the batch is transferred or consumed by the retained-key
builder. Candidates contain only their tuple and packed suffix. Every batch
transfer has one successor owner; every exclusion, failure, cancellation, and
completed path releases the batch reservation exactly once. No pointer or
borrowed model view escapes its synchronous borrow.

A retained polymorphic or persisted-model fetch remains alive through row
materialization; rows then own their retained backing. Autocomplete owns its
retained suggestion buffer until promotion.

## Failure, Concurrency, and Constraints

Read helpers are async because storage and cursors suspend. They use the
session's read capability and do not hold a mutex across `await`. Every bounded
read checks cancellation and meter admission before touching storage. Decode,
missing-value, authorization, work-limit, and cursor failures propagate as
failures. A failed or cancelled operation finishes the cursor and releases the
batch/intermediate reservations; a decode failure never leaves a partially
admitted collection. Suffix detachment is bounded by the scanned key and has
no pointer escape or unbounded materialization fallback.

This contract does not change public FullText APIs, `StorageKit` APIs,
transaction behavior, persisted index layout, key encoding, tokenization,
scoring, facets, phrase semantics, query semantics, or limit pushdown. It does
not add a second decoded-algebra representation or a compatibility bridge.

## Verification and Change Impact

The owning tests are in `Tests/FullTextIndexTests`. Focused behavioral tests
must prove one decode and one exact suffix owner, malformed suffix failure
before algebraic exclusion, admission before preservation, order and
uniqueness, single-batch reservation transfer and cleanup, unchanged
plain/score/facet/polymorphic/limit behavior, denial-before-read,
cancellation, and cursor finish. A change to session, retained-fetch, or
canonical-row contracts requires rechecking this module and the parent
`DatabaseEngine Read` design.

The Release macOS arm64 FullText benchmark uses one test, two metrics, and 15
samples per metric through the real SQLite public query path. Post-change
medians must be no greater than 12,068.689 microseconds for intersection and
6,698.496 microseconds for union. Benchmark evidence does not replace focused
behavioral proof, and package-level verification runs only after the feature
boundary has converged.
