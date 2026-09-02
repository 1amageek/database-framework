# FullTextIndex

## Purpose and Scope

`FullTextIndex` is the feature module for full-text index maintenance and
read-side query execution in `database-framework`. Its supported read
contract consists of typed regular and polymorphic queries, Fusion inputs,
and autocomplete suggestions. Regular queries cover plain, scored, and
faceted results; polymorphic queries cover plain and scored pages. The parent
design is [`database-framework`](../../DESIGN.md); the child designs are
[`Autocomplete`](Autocomplete/DESIGN.md) and [`Facet`](Facet/DESIGN.md).

This design covers the supported read surface, write-maintainer boundary,
posting-candidate representation, BM25 scoring, and intermediate ownership.
A posting candidate is the single FullText-owned combination of one decoded
`Tuple`, one canonical packed comparison key, and the exact footprint
admitted for that candidate. Schema declarations and index configuration are
owned by `DatabaseKit`; application schemas and Fusion orchestration remain
outside this module.

## Responsibilities and Boundaries

FullTextIndex owns:

- maintaining persisted full-text postings, document metadata, and BM25
  corpus statistics through `FullTextIndexMaintainer`;
- decoding and combining full-text posting and document metadata;
- validating and admitting scanned posting identifiers into one candidate;
- preserving full-text match ordering and score annotations;
- translating an admitted read into canonical index rows;
- facet counting from completed retained rows;
- autocomplete cursor traversal and suggestion ordering.

`FullTextTermNormalizer` is an internal implementation contract. It derives
its tokenizer, n-gram, and minimum-term-length configuration solely from the
schema-declared full-text index definition and is used symmetrically by the
write and canonical read paths. It is not a general text-analysis API.

`BM25Parameters`, `BM25Statistics`, and `BM25Scorer` are active FullText
contracts. The maintainer persists corpus statistics; the canonical read
executors consume those statistics inside the authorized session.

FullTextIndex does not own authorization, transaction creation, schema
leases, or work-meter identity. `DatabaseEngine` supplies one authorized
`DatabaseReadSession`, its transaction, and its `DatabaseWorkMeter`. Storage
access is performed through bounded point reads or the session-owned cursor.
The module never replaces a sealed authorization result with ambient state.

The following are outside the supported product and have no execution
contract in this module: general analyzer/filter pipelines, fuzzy or
phonetic matching, wildcard and prefix query planning, highlighting, and
Boolean query DAG/NOT/range/boost semantics. No source, product, or public
entry point advertises these capabilities.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [`database-framework`](../../DESIGN.md) | parent | session, authorization, row, and work-meter contracts | Provides the admitted read boundary and canonical output type. | Re-check callers when session or row ownership changes. |
| [`DatabaseEngine Read`](../DatabaseEngine/Read/DESIGN.md) | depends on | `DatabaseReadSession`, `IndexReadResult`, bounded read capability | Owns authorization, snapshot, transaction, and result admission. | FullText must not create a nested transaction or perform policy decisions. |
| [`StorageKit Tuple`](https://github.com/1amageek/storage-kit/blob/9df21225765dabf18b91044803d759684d817c5e/Sources/StorageKit/Tuple/DESIGN.md) | depends on | canonical Tuple decoding and admission-aware packing | Measures the exact canonical byte count, admits it before allocation, and encodes once. | FullText must not estimate the canonical allocation from source bytes or encode the key independently. |
| [`Autocomplete`](Autocomplete/DESIGN.md) | child | retained suggestion owner and cursor cleanup | Defines autocomplete-specific output ownership. | Limit is applied before public promotion. |
| [`Facet`](Facet/DESIGN.md) | child | row-field aggregation and retained metadata | Defines facet-specific aggregation ownership. | Facets must not inspect raw model objects. |

## Supported Surfaces

| Surface | Entry point | Owner | Contract |
|---|---|---|---|
| Regular full-text read | `DatabaseContext.search(...).fullText(...).execute()` | `FullTextQueryBuilder` and `FullTextReadExecutors` | Typed session read for `all`, `any`, and position-aware `phrase`; plain, scored, or faceted output. |
| Polymorphic full-text read | `PolymorphicQuery.fullText(...).executePage()` | `PolymorphicFullTextQueryBuilder` and `FullTextReadExecutors` | One schema-bound session read across the declared group; optional BM25 annotations. |
| Fusion full-text input | `Search<T>.fusionInput` | `FullTextFusionIndexReadExecutor` | Fusion admission and execution use the caller's authorized read capability. |
| Autocomplete | `DatabaseContext.autocomplete(...).execute()` | `AutocompleteQuery` and `AutocompleteIndexReader` | Session-owned cursor, bounded suggestions, and deterministic ordering. |
| Index maintenance | `FullTextIndexMaintainer.updateIndex` / `scanItem` | `FullTextIndexMaintainer` | Writable transaction updates postings, document metadata, and BM25 corpus statistics. |
| Relevance calculation | `BM25Parameters` / `BM25Scorer` | FullTextIndex | Pure parameter and scoring contracts; no transaction or backend ownership. |

The public read surface always enters through a typed query or Fusion input.
The query runtime creates and authorizes the session; the FullText executor
consumes that session and cannot create a second transaction or perform a new
policy decision. `FullTextIndexMaintainer` has no supported raw-transaction
read facade. Its persisted statistics are an index-maintenance concern and
are read only by the canonical executor through the authorized session.

## Architecture

```text
Typed query / Fusion input                         Writable transaction
          |                                                |
          v                                                v
 DatabaseEngine authorization + session       FullTextIndexMaintainer
          |                                                |
          v                                                +--> postings
 FullText read executor                                   +--> document metadata
          |                                                +--> BM25 statistics
          v
 session-owned bounded cursor / point reads
          |
          v
 scan/admission boundary (once)
   decode Tuple + canonicalize key + exact footprint
          |
          v
 FullTextCandidateBatch (one collection reservation)
   Candidate { Tuple, canonicalKey, retainedFootprint }
          |
          v
 ordered posting algebra
          |
          +--> retained keys -> canonical IndexReadResult rows
          +--> completed rows -> facet metadata
```

The dependency direction is from the feature executor to the `DatabaseEngine`
contracts. The candidate is an internal FullText value; it does not change the
public query or result API, reach into a backend implementation, or create an
independent resource owner. The maintainer's writable transaction is a
separate mutation boundary; it is never used as a read-side query facade.

## Contracts and Invariants

- Authorization is complete before the first storage read.
- Every posting and document-metadata point read uses the request's bounded
  point-read path and the same session work meter.
- Each scanned suffix is first borrowed as a view, then its destination slot
  and exact self-contained source owner are admitted to the batch reservation
  before detachment. Tuple decode allocations are admitted before creation
  through the `Tuple(packed:admitting:)` callback; validation and decoding
  happen exactly once before algebraic filtering.
- A regular posting candidate decodes a suffix and canonicalizes it exactly
  once at the scan boundary. `Tuple.pack(admitting:)` measures the exact
  comparison-key payload once, passes that count to FullText admission before
  allocation, then allocates and encodes the key once. Alternate encodings
  accepted by the regular Tuple decoder compare by their canonical key, which
  preserves logical equality, ordering, and duplicate removal. Structurally
  malformed suffixes still fail as a typed error before algebraic filtering.
  Fusion has a stricter physical posting contract: its cursor rejects a
  decoded identifier whose canonical bytes differ from the stored suffix.
- A candidate contains exactly one decoded `Tuple`, its canonical packed
  comparison key, and the exact `DatabaseIntermediateFootprint` admitted for
  that candidate. The footprint is the actual metered row and byte claim built
  from the admitted candidate storage, source owner, canonical key, and every
  decoder callback increment; it is not reconstructed from tuple element count
  or packed-key length. The candidate has no separate reservation and never
  stores the cursor's borrowed key.
- `FullTextCandidateBatch` owns one collection reservation. Complete candidate
  storage is charged to that reservation before preservation; an admission
  failure never appends a candidate.
- Ordered posting algebra compares candidates by their canonical keys. When a
  surviving candidate is admitted into a successor batch, its recorded
  footprint is reused verbatim before append; merge code never estimates
  retained bytes from `identifier.count` or `canonicalKey.count`. Input batch
  reservations are released only after successor ownership is established.
  The algebra does not decode a suffix, materialize an existential element
  array, call `Tuple.pack()`, or reconstruct a second identifier
  representation. Thus malformed data fails even if intersection or union
  would otherwise exclude it.
- Final retained-key append reuses the candidate's already-decoded `Tuple`; it
  uses the canonical key's known byte count for admission and does not decode
  the suffix or repack the tuple.
- Intermediate match keys, scores, and suggestions remain owned and charged
  until their consumer has completed; no raw collection is returned as a
  resource owner.
- Plain results preserve posting order and matched identifiers. Scored results
  preserve BM25 ordering and deterministic identifier ties. Facets are counted
  from completed retained result-row fields.
- For regular and polymorphic plain AND/OR reads, a requested result limit is
  a post-validation ordered-output bound. Every source posting cursor still
  consumes its complete range and performs the existing decode,
  canonicalization, admission, tail-corruption, cancellation, and cleanup
  checks. Only the final AND result, each monotonic OR union result, and the
  final retained-key promotion keep the requested prefix. A missing limit
  keeps the complete result; the storage cursor limit remains the meter's
  sentinel and is never replaced by the requested result limit. Scored,
  faceted, phrase, and Fusion paths retain their complete intermediate result
  behavior.
- Missing and corrupt index values fail with typed errors; they are never
  converted to empty success.
- A foreign work meter is rejected. Success, failure, cancellation, and limit
  paths finish cursors and release every batch/intermediate reservation exactly
  once.

## Runtime Flows

For a canonical search, the executor decodes parameters, validates the sealed
session admission, resolves the readable index, and opens the bounded posting
cursor. The scan/admission boundary admits destination bookkeeping and the
exact source owner copy, detaches it, admits each decode allocation before it
is created, and decodes the `Tuple` once. It then calls the Tuple-owned
admission-aware pack contract, which measures the canonical key once, admits
that exact count before allocation, and encodes once. The resulting candidate
records the exact claim made while constructing it. Only then is the candidate
appended to the batch and passed to ordered algebra. Intersection and union
compare canonical keys and reuse the surviving candidates' recorded footprints
without tuple conversion or footprint estimation; decode or cursor failure
releases the whole batch after cursor cleanup. For a plain regular or
polymorphic read, the requested limit is applied only after those source
validations: the final AND reduction and each monotonic OR union retain their
ordered prefix, while all source cursors have already reached their terminal
boundary. The retained-key builder then promotes only that prefix, reuses the
winning tuples and canonical byte counts, and fetches retained models or
polymorphic entries through the session before canonical rows are appended.

Plain, scored, and polymorphic posting reads share this candidate path.
Scoring is complete before row append. Faceted execution completes retained
rows before counting their fields and attaching metadata.

Autocomplete opens one session-owned range cursor, admits each suggestion into
the session meter, sorts the retained owner, promotes only the requested
prefix, and finishes the cursor before returning.

## State, Ownership, and Lifecycle

The session owns transaction and meter identity. `FullTextCandidateBatch` owns
the single collection reservation and all candidate tuples and canonical keys
until the batch is transferred or consumed by the retained-key builder. A
candidate's tuple retains any detached source owner needed by byte-backed tuple
elements; its footprint sidecar is accounting metadata, not another resource
owner. Every batch transfer has one successor owner; every exclusion, failure,
cancellation, and completed path releases the batch reservation exactly once.
No pointer or borrowed model view escapes its synchronous borrow.

A retained polymorphic or persisted-model fetch remains alive through row
materialization; rows then own their retained backing. Autocomplete owns its
retained suggestion buffer until promotion.

## Failure, Concurrency, and Constraints

Read helpers are async because storage and cursors suspend. They use the
session's read capability and do not hold a mutex across `await`. Every bounded
read and posting-scan iteration checks cancellation and meter admission before
retaining work. Decode, missing-value, authorization, work-limit, and cursor
failures propagate as failures. A failed or cancelled operation finishes the
cursor and releases the batch/intermediate reservations; a decode,
canonical-key admission, or candidate-append failure never leaves a partially
admitted collection. Suffix detachment is bounded by the scanned key and has
no pointer escape or unbounded materialization fallback.

The dependency contract adds only StorageKit's admission-aware Tuple packing
entry point; it does not change existing Tuple packing behavior or canonical
encoding. The candidate and session work do not change the supported FullText
query API, transaction behavior, persisted index layout, key encoding,
schema-declared normalization, scoring, facets, phrase semantics, query
semantics, or limit pushdown. Retiring the obsolete read facades and
unconnected utility surfaces is an API-surface correction, not a compatibility
bridge. The design does not add a second decoded-algebra representation.

## Verification and Change Impact

The owning tests are in `Tests/FullTextIndexTests`. The retained behavior map
is:

| Behavior | Canonical evidence owner |
|---|---|
| Plain term, `all`, `any`, phrase, and result order | `FullTextReadContractTests`, `FullTextReadResourceLifecycleTests` |
| BM25 ranking and custom parameters | `BM25ScoringTests` and the canonical scored-read tests |
| Facets, total count, and result limit | `FullTextReadResourceLifecycleTests` and the canonical faceted-read tests |
| Schema-declared normalization symmetry | `FullTextTermNormalizerTests` plus writer/read behavior tests |
| Malformed data, denial-before-read, and typed failure | `FullTextReadContractTests`, `FullTextStorageDecoderTests`, `SearchFusionInputTests` |
| Cancellation, cursor finish, and resource release | `FullTextReadContractTests` and `FullTextReadResourceLifecycleTests` |
| Posting decode, canonical identity, order, uniqueness, and exact footprint | `FullTextPostingListAlgebraTests` and the posting executor path |

The source-cleanup boundary may remove an old surface only after every
observable behavior in this map has a canonical proof. The proof must cover
one decode and one canonical pack per scanned candidate, malformed suffix
failure before algebraic exclusion, exact nested or long-payload footprint
reuse, admission before preservation, single-batch transfer and cleanup,
plain-limit prefix semantics, score/facet semantics, denial-before-read,
cancellation after partial scan, and zero retained rows or bytes on success,
failure, and cancellation. A change to session, retained-fetch, or
canonical-row contracts requires rechecking this module and the parent
`DatabaseEngine Read` design.

The release benchmark contract is owned by this design and defines one test,
two metrics, and 15 samples per metric through the real SQLite public query
path. The executable limit source is `Benchmarks/maximum-medians.tsv`; it
defines the exact maxima for intersection `27,640.938` microseconds and union
`13,793.801` microseconds. `scripts/validate-benchmark-medians` enforces both
measurements without reinterpretation: a median above either maximum fails,
and no reference-plus-multiplier path exists. The benchmark entry point is
`Benchmarks/FullTextQuery/Tests/FullTextQueryPerformanceBenchmarks/`.
Benchmark evidence does not replace focused behavioral proof, and
package-level verification runs only after the feature boundary has
converged.

A maximum is a property of the machine that executes the gate, so both numbers
are calibrated on that machine rather than on a developer workstation. The gate
runs in the `Benchmark verification` job on the shared `xcode-27` arm64 runner,
and the calibration set is the eight recorded runs of that job between
2026-08-28 and 2026-09-01, whose intersection medians span 16,548.625 to
22,112.750 and whose union medians span 7,994.958 to 11,035.041 microseconds.
Each maximum is the largest median in that set multiplied by 1.25. The
multiplier is the reviewed operating value: it places both maxima above the
mean plus three standard deviations of the same set, so ordinary contention on
a shared runner cannot fail the gate, while a regression beyond a quarter of
the observed worst case still does. Recalibration is mechanical -- recollect
the medians the gate reports on the current runner and reapply the rule -- and
it updates this design and `Benchmarks/maximum-medians.tsv` in one change.

## Design Review Record

The following review fixes the active-versus-retiring classification against
the package manifest, source callers, tests, benchmark entry points, and
history:

| Evidence | Active contract | Retiring or removed contract |
|---|---|---|
| `Package.swift` | `FullTextIndex`, `FullTextIndexes`, and `DatabaseRuntime` registration | The removed `FullTextIndexFoundation` target/product and root product-table entry |
| Production callers | `FullTextQuery`, `FullTextReadExecutors`, `Fusion/Search`, `FullTextFusionIndexReadExecutor`, autocomplete, and `FullTextIndexMaintainer` write methods | `executeDirect` variants, maintainer raw-read/statistics methods, and duplicate materialization helpers |
| Workspace callers | Typed query/Fusion paths and the canonical SQLite benchmark | No external caller of `FullTextSearchQuery`, fuzzy matchers, highlighters, general analyzers, or `ASCIIFoldingFilter` was found |
| Tests | Canonical read, scoring, decoder, normalizer, lifecycle, and writer/layout tests | Former direct maintainer search tests were migrated to canonical behavior or test-owned persisted-state inspection |
| Benchmarks | SQLite public-query benchmark for posting algebra; retained maintainer write metrics | FoundationDB raw-transaction search, phrase, scoring, and scalability cases |
| History | `e0eb78b9` canonical query execution and later bounded/session work | `961317ad` speculative advanced utilities, `19814213` reverted oversized execution paths, and `a387258c` retained unused typed-runtime surfaces |

This classification is deliberate: removed declarations are not compatibility
aliases or placeholder TODOs. No public API, owner, lifetime, failure, or
performance decision is left to a consumer-specific interpretation. An
actually required absent API must instead be added as a separately designed
work item.
