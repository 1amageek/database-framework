# DecimalFormatting

## Purpose and Scope

This internal C module owns a fixed-function boundary for finite floating-point
decimal formatting. Parent: [package](../../DESIGN.md). Children: none.
It provides no package product or SQL policy.

## Responsibilities and Boundaries

The module adapts the platform's significant-digit formatting to a caller-owned
bounded byte buffer. It owns C argument validation, locale lifetime and status
reporting. DatabaseMath owns the Swift borrow and typed adapter error; SQL owns
precision selection, casts and numeric result admission.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Package](../../DESIGN.md) | parent | Internal target dependency direction | Keeps this adapter internal to framework composition. | No new external product. |
| [DatabaseMath](../DatabaseMath/DESIGN.md) | used by | Bounded synchronous formatting | Supplies validated bytes and explicit status/count. | The caller retains buffer ownership for the complete call. |

## Architecture

```text
DatabaseMath temporary buffer
    -> fixed C declaration -> platform formatting runtime
    <- status + initialized byte count
```

## Contracts and Invariants

- The public header fixes parameter widths and return/status representation.
  Implementation and imported declaration must agree on the actual targets.
- Validate finite input, admitted precision, non-null required pointers and
  capacity before writing; validate the formatter's count before reporting
  success. Capacity includes the terminator; the reported payload excludes it.
- Borrowed buffers and locale handles never escape the synchronous call.
- Formatting uses the C decimal separator without changing process-global
  locale. Darwin uses an explicit locale; POSIX thread-local adaptation restores
  the prior locale on every exit. WASI's selected libc behavior must be verified.
- Failure reports status without publishing partial output as success.

## Runtime Flows

```text
validate -> acquire local formatting context -> format bounded buffer
    -> restore/release context -> validate result -> return status/count
```

## State, Ownership, and Lifecycle

The caller owns output memory. The call owns any locale resource and releases
it exactly once after restoring thread context. It stores no global mutable
state and retains no output pointer.

## Failure, Concurrency, and Constraints

Malformed input, locale initialization, formatting failure and insufficient
capacity are distinguishable failures. Thread locale is restored before
returning; no await or callback occurs inside the C formatting boundary.
Required capacity and exponent bounds derive from the admitted binary format.

## Verification and Change Impact

S04 adapter tests must call the actual linked C implementation with finite
values, precision and buffer boundaries, and reject malformed/nonfinite input.
Native, ordinary WASM and Embedded WASM evidence stays target-specific. Locale
restoration and C decimal output require behavioral proof on supported native
platforms. Changes invalidate DatabaseMath's adapter proof and SQL cast tests.
