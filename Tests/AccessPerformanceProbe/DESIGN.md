# Access Performance Observation

## Purpose and Scope

Native test-only C boundary used by the
[performance executable](../DatabaseAccessPerformance/DESIGN.md).
Parent: [Framework package](../../DESIGN.md). Children: none.
It supplies a monotonic clock and Native allocator/process observations.

## Responsibilities and Boundaries

The ordinary target reads macOS heap statistics and getrusage. A separately
compiled process-local observation library intercepts allocation API calls.
It never changes pointer ownership, sizes, alignment or deallocation, stores
allocation pointers, or calls database APIs. Production targets do not link it.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Framework](../../DESIGN.md) | parent | Fixture build graph | Owns internal test target | Not a library product |
| [Performance](../DatabaseAccessPerformance/DESIGN.md) | used by | Numeric observation snapshot | Owns sample interpretation | High-water values are process-cumulative |

## Architecture

```text
Native benchmark -> scoped C observation -> clock / allocator statistics / RSS
Native allocation APIs -> process-local observer -> same original allocation
                                      -> atomic numeric counters only
```

## Contracts and Invariants

Counters use C atomics on all accesses. Allocation APIs preserve their exact
original arguments, results and ownership; the observer records only successful
calls and requested sizes. Multiplication is checked before reporting calloc
bytes. No pointer escapes a normal allocation's caller ownership. C output
parameters borrow initialized local scalar storage and never escape.

The Native observer is loaded before the measurement process starts. Its C declarations and callers use the same explicit function signatures. Dynamic
symbol lookup reports absence; allocation mode cannot
silently fall back to zero counts. Counts represent allocator API entries,
including realloc, not Swift runtime suballocations within retained task slabs.
Heap statistics distinguish live bytes/blocks, allocator reservations and
process-cumulative high-water values. An allocator high-water value of zero is
reported as unavailable, never as a zero-memory peak. RSS is reported in Darwin bytes.

## State, Ownership, and Lifecycle

The process owns the observation library and atomic counters until exit.
Snapshots own numeric values only. The callback performs no allocation or I/O.
No global machine configuration, tracing service or administrator access is used.

## Failure, Concurrency, and Constraints

Unsupported platform, missing observer, clock/getrusage failure or self-check
failure rejects measurement. Native observations make no WASM/Embedded memory
claim. The harness calibrates observer cost rather than treating it as free.

## Verification and Change Impact

Known malloc/calloc/realloc/aligned allocation calls validate observation;
concurrent calls validate atomic accounting. Compare calibrated enabled and
disabled loops and retain the exact compiler/library hash. The owning executable
validates snapshots and complete benchmark output before acceptance.
