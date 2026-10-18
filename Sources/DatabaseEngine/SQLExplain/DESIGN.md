# SQL Explain

## Purpose and Scope

This DatabaseEngine component owns rendering the selected execution plan and
measuring optional execution. Parent: [DatabaseEngine](../DESIGN.md). It has no
child components.

## Responsibilities and Boundaries

QueryExecution owns binding and physical/relational selection. SQLPreparation
owns non-executing ordinary-statement admission. SQLExplain consumes their
selected plan; it does not infer a plan from SQL syntax. SQLBlock owns the
transaction, statement scope, terminal disposition, and actual execution.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [QueryExecution](../QueryExecution/DESIGN.md) | depends on | Bound selected plan | Source/operator decisions | Planning must never pull rows or evaluate expressions. |
| [SQLBlock](../SQLBlock/DESIGN.md) | used by | Same-scope ordinary execution | Transaction owner invokes ANALYZE once | Failures propagate and invalidate output. |

## Architecture

```text
SQLBlock -> SQLExplain -> non-executing ordinary admission -> selected plan
                     -> ANALYZE -> SQLBlock ordinary executor (same transaction)
                     -> metered TEXT or JSON -> retained query rows
```

## Contracts and Invariants

- Non-ANALYZE performs binding/admission only; no producer pulls, sequence
  evaluation, or mutations occur.
- Both output formats consume the same immutable selected tree.
- Each source's index inspection consumes that source plan's captured
  `binding.sqlResolution`. Nested view plans preserve stored relation and
  column identities after logical renames; root request options do not replace
  a nested plan's resolution. The request meter and all remaining execution
  policy stay attached to the original admitted context.
- VERBOSE adds bound output and source details; absent estimates remain absent.
- ANALYZE invokes the injected ordinary executor exactly once after admission.
  Its measured rows, work units, and monotonic elapsed time describe the whole
  statement, not invented per-node counters or PostgreSQL cost/buffer data.
- Retained rendering and result rows consume the same request resource budget.

## Runtime Flows

Plan admission precedes execution. ANALYZE snapshots monotonic clock and meter,
awaits the same transaction's ordinary execution, snapshots again, then renders.
Cancellation, resource limits, and execution errors escape unchanged. SQLBlock
owns rollback and commit; EXPLAIN never commits independently.

```text
root selected plan -> nested view selected plan -> source binding.sqlResolution
                                                   -> ordinary index selector
ordinary view execution ---------------------------^ same captured identities
```

## State, Ownership, and Lifecycle

The executor holds immutable request references. The plan retains binding
reservations. A rendering reservation accounts for text capacity and remains
alive until each TEXT line owns its canonical scalar. JSON performs one admitted
UTF-8 copy into the existing QueryJSONValue validator, retaining those bytes
with one `.sql(.json)` row. TEXT returns one `.string` row per rendered line.

## Failure, Concurrency, and Constraints

No global state is introduced. Rendering checkpoints and consumes request work
for every node and byte. JSON escaping is bounded and preserves all source text.
Only actual selected plan details are reported.

## Verification and Change Impact

Focused tests cover format agreement, JSON escaping, VERBOSE output,
ANALYZE measurements, cancellation and resource failure. SQLBlock integration
owns no-effect planning, selected source branches, once-only execution, and
commit/rollback evidence. Changes to preparation or physical selection must
revalidate the shared plan seam. `S16SQLExplainExecutionTests.viewCapturedResolutionAfterRename`
compares an ordinary view read with non-ANALYZE EXPLAIN after its base table is
renamed, then adds a catalog index and compares ordinary read with ANALYZE's
selected index and actual row count. The pre-index branch rejects the concrete
missing-relation counterexample even when the catalog has no candidate index.
