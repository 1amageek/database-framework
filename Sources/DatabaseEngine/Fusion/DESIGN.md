# Fusion

## Purpose and Scope

Fusion owns staged candidate intersection, union, scoring, and relational
input execution inside one admitted read session. Parent:
[DatabaseEngine](../DESIGN.md). Children: none.

## Responsibilities and Boundaries

Fusion retains prepared input order and routes each relational input to its
exact bound query. QueryExecution owns type selection and parameter inference;
Read owns authority, transaction lifetime, schema generation, and meter identity.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | In-process composition | Composes Fusion execution. | No independent authority. |
| [QueryExecution](../QueryExecution/DESIGN.md) | coordinates with | Ordered batch binding and bound relational evaluation | Selects operators before execution. | All inputs share one parameter inference state. |
| [Read](../Read/DESIGN.md) | depends on | Sealed session, exact meter, index lease | Owns physical reads and cleanup. | Bind failure must precede source reads. |

## Architecture

```text
prepared stages + frozen invocation parameters + admitted session
    -> relational queries in stage/input order
        -> one QueryExecution binding batch
            -> retained ordinal-to-query/operator bindings
                -> staged candidate execution through the same session
```

## Contracts and Invariants

- S05 preflight visits every concatenation and dynamic pattern child under
  [QueryExecution's traversal contract](../QueryExecution/DESIGN.md#s05-selected-string-pattern-binary-and-cast-execution),
  preserving physical source admission even when a child contains a subquery.

- Bind every relational filter/order input before the first source executor read.
- Feature preflight validates index and connection contracts after field
  authorization. `FusionExecution.make` sends all relational inputs through
  `DatabaseReadSession.boundFusionRelationalQueries` in one batch with the
  invocation's actual parameters and finalizes their shared inference state
  before `FusionExecutor.execute`. No parameterless per-input type binding
  precedes that batch. Canonical table binding supplies declared field
  types and optionality from the admitted schema before any source executor read.
- Stage/input ordinals identify exact bound operators without expression equality
  lookup or per-row rebinding. Parameter references share the batch's state.
- Query/operator storage remains admitted under the session's exact work meter.
- Session authority, field projection, schema, transaction, and index leases
  remain the existing sealed execution contract.
- Preflight table visits preserve lexical nonrecursive CTE scope: definition
  bodies see earlier siblings and inherited definitions, while the owning
  query body sees all local definitions. A not-yet-visible definition does not
  shadow an inherited CTE or physical table; unresolved names remain subject to
  canonical binding failure. Only physical tables contribute storage list
  requirements. Scope admission uses its stored contents, reservation reference,
  and the existing retained-layout shared-owner allowance.

## State, Ownership, and Lifecycle

The execution value retains immutable input bindings, parameter values, prepared
graph, and session through all stages. Binding failure or execution cancellation
releases partial reservations through their existing owners.

## Failure, Concurrency, and Constraints

Binding failures propagate as typed failures before physical input reads. Invalid
ordinal/stage/input pairing is an execution contract violation. Read continues
to enforce cancellation, request budgets, and authoritative lease shutdown.

## Verification and Change Impact

[FusionExecutionContractTests](../../../Tests/DatabaseEngineTests/Fusion/FusionExecutionContractTests.swift)
owns parameterized filter/order execution, pre-read binding rejection, sealed
authorization, exact meter ownership, and lease regressions. Changes to parameter
selection require QueryExecution verification; changes to authority or lifetime
require Read verification.
