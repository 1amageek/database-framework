# Full-Text Measurements

## Purpose and Scope

Independent Native full-text measurement package. Parent:
[Framework](../../DESIGN.md). Children: the existing performance test target.

## Responsibilities and Boundaries

Own SQLite fixtures, output checks and measurements through the public Framework
API. StorageKit owns SQLite binding and backend semantics. Require macOS 27 for
this measurement host because the CI-provided SQLite dynamic library requires
that version. This does not change Framework's deployment floor.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Framework](../../DESIGN.md) | parent, depends on | Public query execution | Adjacent revision under measurement | Preserve production deployment support |
| [FullTextIndex](../../Sources/FullTextIndex/DESIGN.md) | depends on | Posting execution and reviewed median bounds | Owns query/index behavior | Do not relax bounds for host changes |

## Architecture

```text
pinned benchmark harness -> macOS27 measurement target -> Framework -> SQLite
    -> exact output / budget checks -> reviewed median validator
```

## Contracts and Invariants

Keep the committed URL dependency pins, original fixture program and median bounds.
The declared target must support its linked SQLite library. Compiler, linker,
analyzer and runtime warnings remain failures. Host compatibility is not achieved
by suppressing warnings or replacing the SQLite implementation.

## State, Ownership, and Lifecycle

The test owns an isolated SQLite container and directory, drains execution and
awaits shutdown before removing state. The harness owns its archived source and
build products; result bundles and logs survive cleanup.

## Verification and Change Impact

Run `scripts/xcode-benchmark-harness fulltext`: require its exact test and metric
counts, zero warnings, unchanged lock and all existing median bounds. The CI
macOS26-to-SQLite27 link warning is the original failing host contract. A local
linker fixture distinguishes the incompatible and matching deployment floors;
final acceptance still requires the actual full-text build and execution.

The cursor scheduling contract is owned by
[StorageKit Storage](../../../storage-kit/Sources/StorageKit/Storage/DESIGN.md).
The pinned graph includes its bounded handoff quantum. FullText retains complete
posting validation, budget admission, canonical identifiers and tail-error
handling; no source scan is shortened for an output LIMIT. Local matched runtime
observations establish the scheduling improvement; the archived URL-graph CI
run remains authoritative for this package's existing median maxima.
