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
bound expression -> async adapter (one suspension)
    -> positional leaf -> existing frame read
    -> admitted instruction/value stack -> synchronous kernel
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

| Logical state | Native / standard WASM / Embedded | Access and release |
|---|---|---|
| Control instructions and value stack | Identical invocation-local admitted Arrays; no conditional storage or Sendable conformance | The same evaluator exclusively pushes/pops; stack slots and leases end on return or throw |
| Work and retained bytes | Existing `DatabaseWorkMeter` and reservation `Mutex<State>` on all targets | Existing consume/checkpoint/reserve/release entry points; no callback or await under the mutex |
| CASE operand and extreme candidate | Values in the same invocation-local stack on all targets | Stable operand position or current-candidate tag; retained across contextual await, removed before subsequent branch/argument |

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

### Positional leaf admission

A root positional column without grouped or window interpretation is already a
bound frame read. The async adapter retains its single suspension and the same
work/checkpoint/error-attribution contract, then resolves that leaf without
allocating instruction/value arrays. Other expressions use the admitted machine.
This fast path calls the same selected-reference normalization and bounds checks;
it introduces no second column semantics or authority lookup. A fixture with no
outer frame admits no scratch bytes and consumes exactly one node's work, while invalid
addresses and cancellation retain the existing failures. Native allocator
measurements verify the repeated allocation reduction.

When no outer row exists, the supplied row is already the complete effective
frame. The evaluator reserves no hypothetical overlay and performs no metadata
walk for that identity operation. A real overlay keeps its prior prospective
admission and lexical shadowing. This changes charges only for work/storage no
longer performed; every evaluated node retains its work and cancellation checks.

### TAIL25 control and value ownership

Control instructions carry a root tag or a retained immutable child-array owner
plus ordinal, continuation indices and small tags. They do not copy the complete
`CanonicalBoundExpression` into each command slot. The original root and the
shared bound children own the metadata; each child reference retains its actual
array owner across suspension, without raw pointers or a flattened metadata copy. They never embed evaluated values or lease vectors: otherwise the
largest CASE continuation determines every instruction slot's storage, including
ordinary arithmetic. The admitted value stack owns CASE's original operand and
selected comparison operand, and the current GREATEST/LEAST candidate.

Simple CASE keeps the original operand at a stable stack position, coerces it
independently before each WHEN's other operand, and removes all control-owned
values before the selected result/fallback is evaluated. Searched CASE retains
no operand. Extrema retain only the selected candidate and the next argument;
discarded candidates end before evaluation of the following argument. Nested
operators cannot consume a parent's pending control values. All stack capacity
is admitted before growth on the unchanged meter and discarded on failure.

Synchronous kernels read a zero-based, owner-retaining view of the existing value
stack. The view's complete lifetime ends inside the synchronous kernel before
the caller removes operands or appends the result, so COW cannot duplicate the
stack at mutation. At the sequence boundary the machine materializes the primitive argument array
required by its async API, admitting that actual container and retaining the
original value stack's leases until the await returns. Domain kernels retain
their existing explicit input ownership boundaries. Numeric kernels consume a lazy zero-based QueryValue view rather than mapping
these operands into another Array. Their existing arithmetic, casts, functions,
validation and original meter remain the only semantics.

Instruction admission and pushes mutate one explicit `AdmittedArray` owner.
A projected property-wrapper writeback must not leave a retained Array alias
across the admitted batch: allocation stacks reproduced a full COW buffer copy
on each push despite enough capacity. The loop reads/removes the stored buffer
directly, and shares no snapshot of that mutable owner with its scheduling code.

The initial root instruction lives in the driver's current slot. A known eager
child-command batch admits its complete pending instruction capacity once before
any child runs. This is the same pending-command peak as individual appends;
value-stack capacity still grows only as values are produced, so a child's
transient payload is not overlapped with speculative sibling value capacity.
No speculative operand array or extra reservation is constructed for ordinary
synchronous kernels.

The arithmetic storage regression admits the existing scalar-kernel/operand
space plus compact control space; it must reject a representation that charges
rare branch payload storage to ordinary operators. Native allocation observations
must confirm reduced cumulative bytes against both47df0b2a and a7bcf834. Exact
standard/Embedded runtime covers nested CASE, lazy failures, extrema ownership,
typed SQL/IR results, cancellation/budgets, rollback and authoritative close.
