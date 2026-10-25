# Access Measurements

## Purpose and Scope

Independent Swift package for Native common-access and SQL execution measurements.
Parent: [Framework](../../DESIGN.md). Children:
[execution fixture](Sources/DatabaseAccessPerformanceContract/DESIGN.md) and
[observation boundary](Sources/AccessPerformanceProbe/DESIGN.md).

## Responsibilities and Boundaries

Own the measurement executable, C observation target and fixed SQLite/ScalarIndexes
composition. The measurement host requires macOS 27, matching the CI-provided
SQLite library deployment requirement. Framework retains its separate deployment
floor. Consume the adjacent Framework under test and URL-pinned Client,
Kit, Types and Storage contracts. Production and correctness targets never depend
on this package. Functional standard/Embedded fixtures remain with Framework.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Framework](../../DESIGN.md) | parent, depends on | Public execution products | Adjacent revision under measurement | No production manifest target |
| [Fixture](Sources/DatabaseAccessPerformanceContract/DESIGN.md) | child | Output and matrix validation | Owns measurement interpretation | Preserve workload and budget inputs |
| [Observation](Sources/AccessPerformanceProbe/DESIGN.md) | child | Numeric clock/allocator/RSS snapshot | Owns process-local observation | No portable memory claim |

## Architecture

```text
access-performance-harness -> independent package + committed lock
    -> execution fixture -> public Client / Framework -> SQLite
    -> observation target -> process-local clock / allocator / RSS
```

## Contracts and Invariants

Source relocation preserves fixture code, output checks, authority, budget,
rollback and lifecycle. The package enables only SQLite/ScalarIndexes in Framework;
its observer is never linked into production products. The harness verifies the
fixed release compiler, builds with this package's committed lock and retains
source/lock/compiler provenance. CI matches the Native harness by omitting release
debug information and rejects internal compiler/tool warnings even when the
build process exits zero. Existing performance bounds are unchanged.

## State, Ownership, and Lifecycle

The harness owns its scratch directory and process-local observer. Each execution
owns its isolated container and drains the original scopes before shutdown.

## Failure, Concurrency, and Constraints

Missing pins, wrong compiler, invalid observer accounting, unexpected output or
incomplete timing/observation matrix fail verification. Sequential cold builds
bound local disk use; raw result evidence survives cache cleanup.

## Verification and Change Impact

Run the observer self-check and quick time/observation matrix through
`scripts/access-performance-harness`. CI builds this independent product with its
locked graph and runs its quick time matrix as a correctness smoke check. `scripts/verify-benchmark-separation` guards the root boundary.
Moving fixture inputs invalidates provenance paths, not historical measurements
whose program bytes and settings remain unchanged. Changes to fixture semantics
require their behavioral and measurement gates again.

The 2026-10-25 relocation check used the fixed Swift 6.4.0 release and committed
URL pins: the observer self-check completed 40000 calls on four threads and the
quick matrix completed 12 time and 12 observation records with exact output
validation. Swift/C fixture bytes match the prior paths. This validates package
relocation; it does not replace historical performance acceptance or the final
CI benchmark gates. Raw evidence is in
[the local verification record](../../../.codex-tools/ci-repair-20261025/local-verification.json).

The follow-up host check used the pinned August 14 snapshot, macOS27 target and
debug-free release setting: the actual executable built in 562 seconds with zero
internal tool warnings and completed all 12 quick records. Its exact input and
compiler record is [retained here](../../../.codex-tools/ci-repair-20261025/host-access-verification.json).
The CI guard rejects the first run's 293 input-verification warnings even though
that build exited zero. Actual updated FullText medians remain a final CI gate.
