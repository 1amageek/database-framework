# Entity Mutation

## Purpose and Scope

Mutation owns prepared entity changes, preconditions and their execution within
[DatabaseEngine](../DESIGN.md). Parent: DatabaseEngine. Children: none.

## Responsibilities and Boundaries

Preserve logical entity identity, declared primitive persistence fields,
preconditions, typed adaptation failures and request accounting. QueryExecution
owns canonical query values and the bounded value meter. Storage commit and
container lifetime remain with the transaction/container owners.

Statement assignment planning consumes the compiled `FieldSchema.queryType`,
or the field's primitive declaration when absent. QueryExecution owns binding,
cast admission and scalar conversion; Mutation supplies the target declaration
and `.assignment` context, then adapts the converted value at the existing
persisted-model boundary. Mutation does not implement text, numeric, byte or
UUID conversion algorithms.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Transaction execution authority and prepared mutation scan handoff | Composes prepared changes and admits scan destinations. | Keep temporary payload owners through destination admission; success requires authoritative close. |
| [QueryExecution](../QueryExecution/DESIGN.md) | depends on | `CanonicalQueryTypeBinder.bindAssignment`, `DatabaseContext.evaluateCanonicalScalar`, bounded value footprints | Binds compiled target assignments and converts selected scalar values. | Pass `.assignment`; retain generated payload claims until the prepared change admits its destination. Synthetic query rows confer no mutation authority. |
| [Kit Model](../../../../database-kit/Sources/DatabaseKit/Model/DESIGN.md) | depends on | PersistableEncodingError | Preserves typed query-value causes. | Rich meaning requires a declared descriptor. |

## Architecture

```text
Declared model -> primitive persisted fields -> prepared mutation
    -> identity/precondition checks -> caller-owned transaction -> close

Compiled fields -> bind supplied INSERT cells / UPDATE assignments
    -> bind UPDATE / DELETE / conflict predicates as Boolean
    -> shared context-free selected scalar evaluation
    -> outer assignment CAST (assignment context)
    -> declared persistence encoding + model adaptation
    -> prepared change with temporary payload owner
    -> destination reservation -> release temporary owner
    -> execute all prepared changes in the caller's transaction
```

## Contracts and Invariants

Mutation footprint measurement consumes primitive named fields directly and
does not construct QueryRow or infer a query schema. A model encoding failure
with QueryValueError is preserved as
DatabaseEntityMutationError.invalidQueryValue(entity:cause:). Existing scalar,
field identity and compiled-schema errors retain their prior mappings.

The statement executor resolves and validates assignment targets before an
UPDATE source scan or INSERT existing-row lookup. INSERT supplied columns,
UPDATE assignments and conflict-update assignments use the same compiled target
declaration. Unknown or duplicate targets and invalid conversion signatures do
not depend on a matching source row. Value-dependent length/domain failures
occur when the corresponding assignment is evaluated; all changes are prepared
before any are applied. UPDATE, DELETE and conflict-update predicates bind as
Boolean expressions before scanning or existing-row lookup and use the same
selected scalar evaluator, including codepoint and C/POSIX text semantics.
NULL predicates do not select a row. Omitted-field defaults,
model adaptation, identity, preconditions and conflict policies remain owned by
their existing paths. This work does not add INSERT SELECT, UPDATE FROM or
RETURNING.

Explicit CAST inside an assignment retains its explicit context; only the outer
target-field conversion uses assignment context. CHAR/VARCHAR assignment must
not silently acquire explicit truncation semantics. The scalar owner defines
codepoint length, permitted trailing-space removal, padding and typed failure.
Declared rich values are encoded through `QueryFieldStorageAdapter` so numeric
display scale is preserved; primitive fields keep their declared model adapter.

## Runtime Flows

INSERT binds all supplied cell signatures before reading conflicting rows. Each
row then evaluates, converts and adapts its supplied fields before producing a
change. UPDATE binds its complete assignment list before scanning and evaluates
all right-hand sides against the original row. Conflict-update assignments are
and conflict predicates are also bound before the INSERT loop. The async scan
transform calls `DatabaseContext.evaluateCanonicalScalar(_:on:workMeter:)`
with an admitted positional row aligned to the compiled binding slots. This
context-free entry rejects relation/subquery/aggregate evaluation before I/O.
Neither a binding error nor a later
row-dependent conversion error may publish an earlier prepared change.

## State, Ownership, and Lifecycle

Prepared changes retain request claims through execution and transaction
completion. Measurement grants no transaction or publication authority.

A Mutation-owned prepared change couples `EntityMutationChange` with its
temporary scalar payload owner. The transaction scan consumes that internal
handoff and keeps it alive while measuring and admitting the destination change.
Borrowed selected-column results retain the admitted positional source-row
payload claims through this handoff as well. The payload owner and destination
reservation use the same `DatabaseWorkMeter`.
Success, conversion/adaptation failure, admission failure and cancellation
release temporary claims exactly once; no raw payload escapes an unclaimed
interval. Payload-owner collection capacity is admitted through the existing
retained-array builder. No shared mutable state or new isolation boundary is
introduced.

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

Statement-assignment tests use compiled CHAR/VARCHAR field descriptors and the
real statement executor to check INSERT/UPDATE success, Unicode codepoint
length, padding, trailing-space handling and overlength failure. A multi-row
failure leaves every stored row unchanged; an invalid assignment signature is
rejected before source scanning. Exact/one-past resource admission, failure and
cancel cleanup, and subsequent valid mutation verify the temporary-to-destination
ownership handoff. Existing statement mutation lifecycle/precondition/budget
tests remain regressions for primitive fields and model adaptation.
