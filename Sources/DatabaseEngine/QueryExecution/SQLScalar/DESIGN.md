# SQLScalar

## Purpose and Scope

Owns selected expression control and scalar kernel composition inside Engine.
Parent: [QueryExecution](../DESIGN.md#rx25-selected-evaluation-machine). Children: none.

## Responsibilities and Boundaries

Advances admitted instructions and values, selects lazy branches, reads bound
positional/group/window values and invokes synchronous scalar kernels. Contextual
subquery, aggregate and sequence work consumes the existing context/session,
original authority and request meter; this component opens no transaction.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [QueryExecution](../DESIGN.md#rx25-selected-evaluation-machine) | parent | Bound plans, positional frames, leases and scalar semantics | Owns selected binding and integration | Preserve exact occurrences and lazy order |
| [SQLRelations](../SQLRelations/DESIGN.md) | coordinates with | Admitted pulls and recursive cleanup | Supplies contextual execution | No meter, snapshot or authority replacement |

## Architecture

```text
bound expression -> admitted instruction/value stack -> synchronous kernel
    -> contextual operation -> same-task await -> original stack
    -> normalized value + retained leases
```

## Contracts and Invariants

Instructions replace scalar recursive descent. Every evaluated node consumes
work and checks the original cancel/deadline. Admitted stack capacity and
payload leases survive contextual suspension and end on success/failure.
Only selected CASE/COALESCE/boolean branches run. CASE operand evaluation is
once, coercion is per WHEN and before that WHEN's other operand. GREATEST and
LEAST evaluate arguments in order, retaining only the current selected value
and the next argument; discarded candidates release their leases before the
following argument is evaluated. The original
selected numeric, collection, temporal, pattern and comparison kernels remain
the sole semantic implementations. Diagnostics retain child occurrences.

## State, Ownership, and Lifecycle

Stacks are invocation-local exclusive values, not shared mutable state. Group,
window and lexical frames are immutable references owned by the existing read
session. All types and isolation remain identical on Native/WASM/Embedded.

## Failure, Concurrency, and Constraints

Stack growth, work and value construction are admitted on the original meter.
The async entry yields once per selected evaluation: an entirely synchronous
instruction run must not return into an unbounded immediate caller continuation
chain. Release standard WASM reproduced that chain in projectRows after the
representations fixture. This adapter boundary is shared across all targets;
ordinary scalar nodes do not schedule. Contextual descent additionally yields
around its existing async work. No new Task, detached work, retry, VM stack change or
cleanup bypass is introduced. Failed evaluation discards all unfinished values.

## Verification and Change Impact

Selected-signature, lazy/error/occurrence, group/window and rich-value Native
suites plus exact ordinary/Embedded runtime fixtures own the behavior. Changes
recheck parent QueryExecution, SQLRelations and Mutation's scalar entry seam.
