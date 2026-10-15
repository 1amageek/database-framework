# DatabaseMath

## Purpose and Scope

This module owns platform-neutral scalar math and finite floating-point decimal
formatting used by framework execution. Parent: [package](../../DESIGN.md).
Children: none. SQL overloads, domains and result policy remain in
[QueryExecution](../DatabaseEngine/QueryExecution/DESIGN.md).

## Responsibilities and Boundaries

The existing static DatabaseMath API adapts the selected platform C math runtime.
It adds commonLogarithm, cubeRoot, tangent, arcCosine and arcTangent without
changing existing math signatures. SQL callers validate selected widths,
domains, finite results and underflow. The formatting adapter owns bounded
borrowing and typed adapter failures; SQL selects significant digits.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Package](../../DESIGN.md) | parent | Target composition | Indexes this module and its consumers. | Keep platform adaptation separate from SQL policy. |
| [DecimalFormatting](../DecimalFormatting/DESIGN.md) | depends on | Fixed-function finite decimal formatting | Supplies the C runtime boundary without Swift variadic arguments. | Preserve capacity, locale restoration and result-presence checks. |
| [QueryExecution](../DatabaseEngine/QueryExecution/DESIGN.md) | used by | Selected scalar numeric kernels | Applies SQL signatures and failures around these operations. | A libm result alone is not SQL success. |

## Architecture

```text
DatabaseEngine and numeric feature consumers
    -> DatabaseMath
        -> selected platform C math runtime
        -> DecimalFormatting -> bounded C formatting runtime
```

## Contracts and Invariants

- Existing platform selection covers Darwin, Musl, Glibc and WASILibc.
- New scalar wrappers preserve the corresponding C operation's value; SQL
  interpretation remains in the caller.
- `withDecimalFormatting(of:significantDigits:_:)` lends only the initialized
  UTF-8 result span to its synchronous callback and returns the callback result.
  Neither the C boundary nor the callback may retain the pointer.
- `DatabaseMathFormattingError` is the typed adapter failure owner: invalidPrecision,
  nonFiniteValue, insufficientCapacity, localeUnavailable, formattingFailed,
  localeRestorationFailed and invalidResult. Callback errors propagate unchanged.
- The adapter validates finite input, supported precision, status and returned
  byte count before lending output; failure is typed and never empty success.
- Temporary capacity is derived from supported floating-point significant
  digits, sign, decimal/scientific notation, exponent width and terminator.
  The implementation records that derivation at the allocation.

## Runtime Flows

```text
validate input -> allocate bounded temporary buffer -> fixed C call
    -> validate status/count -> synchronous initialized-byte borrow -> release
```

## State, Ownership, and Lifecycle

Math operations retain no state. The formatting call owns temporary storage
until its callback exits, including throws. It owns no global locale or mutable
registration. Locale adaptation is owned by DecimalFormatting.

## Failure, Concurrency, and Constraints

Invalid input, unavailable locale/runtime formatting and insufficient capacity
remain explicit adapter failures. No pointer escapes and no suspension occurs
inside a borrow. Platform differences do not weaken shared-state isolation.

## Verification and Change Impact

Focused S04 numeric tests own value, width, domain and 6/15-significant-digit
cast assertions. Native and separately identified ordinary/Embedded WASM
compile, link and runtime evidence must exercise the real adapter, including
failure and buffer boundaries. Revalidate QueryExecution and package target
composition when this module or its C dependency changes. The Embedded
`withVaList` compile rejection is evidence for the fixed-function boundary;
it does not establish this adapter's runtime success.
