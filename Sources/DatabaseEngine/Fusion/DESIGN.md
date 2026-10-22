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

- The metered query graph walker exposes an external-table observer after query-local WITH filtering
  and before statement-private filtering.
  QueryExecution uses it only to inventory syntax for
  [snapshot lexical compatibility](../QueryExecution/DESIGN.md). Observing a
  table name grants no list/field authority; existing physical-source and ordering
  callbacks retain their filtering, scope and failure contracts.

- S05 preflight visits every concatenation and dynamic pattern child under
  [QueryExecution's traversal contract](../QueryExecution/DESIGN.md#s05-selected-string-pattern-binary-and-cast-execution),
  preserving physical source admission even when a child contains a subquery.

- Ordering companions consume the statement collector's lexical row/CTE provenance
  under [Engine's field-security contract](../DESIGN.md#execution-composition-and-transaction-authority).
  They preserve each original requirement window and contain no cached policy decision.
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


### S09 query envelope preservation

**PLANNED; not implementation or verification evidence.** Fusion consumes the
Kit query envelope as an immutable semantic contract. It preserves grouping
elements and named windows in reconstruction and delegates SQL-only aggregate,
grouping and window interpretation to canonical QueryExecution. A path that
cannot execute those forms rejects them explicitly rather than reducing them
to expression grouping or omitting window metadata. Existing Fusion behavior
remains the regression owner for its supported subset.


### S10 recursive scope and function-source preflight

[QueryExecution's frozen execution contract](../QueryExecution/DESIGN.md#s10-frozen-recursive-and-function-execution) and [Query carriage](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s10-frozen-recursive-and-function-source-carriage) refine CTE scope: nonrecursive bodies retain earlier-sibling visibility, while every body in recursive WITH sees all local declarations, including self and forward siblings. Local names shadow inherited/physical entities only in their admitted scope; explicitly qualified physical tables remain physical. Canonical binding alone rejects invalid recursion. Fusion never grants a recursive self-reference physical list authority or drops authorization for genuine physical reads.

Preflight iteratively visits every table-function argument, including its nested queries, under that same lexical scope and existing meter/reservation owner. Function sources themselves require no physical entity lookup. Immutable reconstruction preserves all recursive metadata. Focused Fusion tests distinguish self/forward logical references from same-named entities, retain physical reads, and visit physical subqueries inside function arguments before execution. No mutable-state/isolation/target contract changes.


### S11 catalog-aware preflight and candidates

**Implementation contract; not completion evidence.** Fusion consumes Read's
explicit optional SQL resolution and
[QueryExecution's catalog-bound source contract](../QueryExecution/DESIGN.md#s11-frozen-catalog-bound-select-execution).
`resolveGraph`, additional index authorization and prepared graph entries use
the exact catalog-resolved relation/physical field map later retained by the
canonical source plan. A terminal-name `Schema.entity` lookup before binding
cannot choose SQL authority or executor identity. Qualified sources bypass CTE
shadowing; unqualified sources retain the S10 lexical walk. Typed QueryIR and
SPARQL calls without SQL resolution retain physical lookup semantics.

```text
SQL resolution -> resolved physical list/field requirements -> authorization
  -> prepared graph + same generation/source identity
     -> physical feature candidates -> common SQL source alignment
        -> bound canonical logical operators
```

Catalog resolution errors retain their source occurrence and typed cause before
reads. Feature-specific connected/index declarations remain physical contracts
unless their explicit owner selects a catalog mapping; SQL table aliases do
not reinterpret those declarations. Prepared entry lookup and generation
validation cannot cross two same-named catalog relations. Physical candidate
columns reach canonical evaluation only through the selected alignment; a
missing required field fails instead of yielding NULL. Wildcard candidates
retain the existing sealed authorized subset and do not gain complete-model
authority.

Focused Fusion fixtures cover identical terminal relation names, relation and
column rename, physical deny-before-I/O, parameterized filter/order, candidate
projection and existing recursive/forward CTE scope. Binding metadata and
lookup reservations include retained resolution/map storage before allocation.
No mutable state, target-specific synchronization, transaction or lease owner
changes are introduced.


S11 relational candidate generation and incoming-candidate filter/order retain
the original physical source owner and identity. Initial generation reuses the
existing table fetch, pushdown, selected binding and sealed list requirement;
Registry carries persisted identity as internal row metadata under the retained
model claim, without adding a field to the authorized or SQL-visible layout.
The same selection bridge returns physical candidate rows to the existing
`FusionCandidateDomain` constructor; only that internal response may carry its
required identity column. Incoming candidates keep `FusionCandidateDomain` as
the owner of physical row identity. QueryExecution carries an internal source ordinal
through logical alignment, bound filtering and ordering; that ordinal is not
a SQL column or annotation. The internal stage response retains the selected
original physical candidate rows and their claims, so existing candidate and
score composition consumes its physical identity contract. Final request
projection alone exposes catalog columns. A missing ordinal or expected
physical identity fails explicitly; logical value equality never reconstructs
identity, and unadmitted wildcard fields are not evaluated. This bridge does
not widen field/list authorization or alter the non-SQL source path.
Incoming SQL filter/order reconstructs its candidate domain by selecting keys
from the original domain through `make(selecting:from:)`. Internal response
identity columns serve routing/scoring only: they cannot enter domain column
algebra. The original domain's columns, entries and retained claims remain
identical across union/intersection boundaries.
