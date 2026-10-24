# Model Fetch

## Purpose and Scope

Owns model fetch descriptions, typed field evaluation and scalar access
selection within DatabaseEngine. Parent: [Engine](../DESIGN.md). Children: none.
N00 separates pure predicate declarations from their existing runtime meaning.

## Responsibilities and Boundaries

Query<T> and QueryExecutor<T> retain existing fetch options, partition bindings,
context references and execution entry points. FieldComparison.evaluate reads
the generated model adapter and owns model comparison behavior. Scalar access
selection consumes the same operator cases; N00 changes their declaration owner.
Schema authority, transaction/resource admission and storage lifecycle remain
with Engine Core and its execution contracts. No Client dependency is introduced.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Engine](../DESIGN.md) | parent | Execution composition | Owns the module boundary | Keep authority and budgets in existing execution |
| [Kit Fluent](../../../../database-kit/Sources/DatabaseKit/Query/Fluent/DESIGN.md) | depends on | Predicate, FieldComparison, FieldComparisonOperator | Owns pure field/value declarations | Canonical SQL lowering has distinct NULL semantics |
| [Core](../Core/DESIGN.md) | depends on | Container/context lifetime | Supplies admitted model access | No raw-engine bypass |

## Architecture

```text
Kit Predicate / FieldComparison -> Query<T> -> existing context fetch
                                    |
                                    +-> scalar access selection
model generated field adapter -> FieldComparison.evaluate -> model predicate result
```

## Contracts and Invariants

N00 preserves the model evaluator body exactly. Missing fields and encoding
failures remain typed errors. Existing null, ordering, membership and string
comparison behavior is retained. Kit Selection lowers canonical SQL instead;
its NOT/NULL behavior must not be substituted for model fetch evaluation.
The model operator is FieldComparisonOperator, distinct from SQL comparisons.

## State, Ownership, and Lifecycle

Fetch descriptions are value-local/COW configuration. Executors retain their
existing context; resource and execution state remain owned by Core. Moving
predicate declarations introduces no shared mutable state or target branch.

## Verification and Change Impact

[ValueAccessLayerTests](../../../Tests/DatabaseEngineTests/ValueAccessLayerTests.swift)
verifies generated field access, model comparison failures and sorting.
[LiteralConversionTests](../../../Tests/DatabaseEngineTests/LiteralConversionTests.swift)
verifies the existing model/IR bridge and SQL residual predicates.
[QueryConnectionTests](../../../Tests/SQLiteTests/QueryConnectionTests.swift)
compares new typed construction with SQL through real SQLite, including NULL
negation, while retaining authority, budgets, rollback, restart and close tests.
Changes to declaration representation require Kit and these consumers to be
rechecked; execution policy changes remain owned by their Engine contracts.
