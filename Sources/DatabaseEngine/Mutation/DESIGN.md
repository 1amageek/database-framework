# Entity Mutation

## Purpose and Scope

Mutation owns prepared entity changes, preconditions and their execution within
[DatabaseEngine](../DESIGN.md). Parent: DatabaseEngine. Children: none.

## Responsibilities and Boundaries

Preserve logical entity identity, declared primitive persistence fields,
preconditions, typed adaptation failures and request accounting. QueryExecution
owns canonical query values and the bounded value meter. Storage commit and
container lifetime remain with the transaction/container owners.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Transaction execution authority | Composes prepared changes. | Success requires authoritative close. |
| [QueryExecution](../QueryExecution/DESIGN.md) | depends on | Bounded value footprints | Measures mutation payloads directly. | Synthetic query rows confer no mutation authority. |
| [Kit Model](../../../../database-kit/Sources/DatabaseKit/Model/DESIGN.md) | depends on | PersistableEncodingError | Preserves typed query-value causes. | Rich meaning requires a declared descriptor. |

## Architecture

```text
Declared model -> primitive persisted fields -> prepared mutation
    -> identity/precondition checks -> caller-owned transaction -> close
```

## Contracts and Invariants

Mutation footprint measurement consumes primitive named fields directly and
does not construct QueryRow or infer a query schema. A model encoding failure
with QueryValueError is preserved as
DatabaseEntityMutationError.invalidQueryValue(entity:cause:). Existing scalar,
field identity and compiled-schema errors retain their prior mappings.

## State, Ownership, and Lifecycle

Prepared changes retain request claims through execution and transaction
completion. Measurement grants no transaction or publication authority.

## Failure, Concurrency, and Constraints

Malformed rich fields, unknown retained capacity, cancellation and budget
failure propagate explicitly. S01 adds no mutable shared state or isolation
branch in this component.

## Verification and Change Impact

DatabaseEngineTests mutation/model-projection suites verify identity,
preconditions, failure mapping and request claims. S01 rich-model persistence
and negative adaptation cases exercise the changed caller boundary. Changes
revalidate Kit Model and direct server error mapping; source compilation alone
is not behavioral evidence.
