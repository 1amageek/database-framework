# SQL Table Functions

## Purpose and Scope

This component is a child of [QueryExecution](../DESIGN.md#s10-frozen-recursive-and-function-execution). It owns the selected FROM-function contract, one-invocation cursor lifecycle, series advancement and ordinality required by [SQL S10-C05–C10](../../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s10-frozen-recursion-and-table-functions). It has no children. Parent source composition owns lateral scope, aliases, JOIN presence and source provenance.

## Responsibilities and Boundaries

The component selects the closed UNNEST/GENERATE_SUBSCRIPTS, JSON/JSONB row, REGEXP_MATCHES/REGEXP_SPLIT_TO_TABLE and GENERATE_SERIES families before reads. Existing scalar built-ins use the scalar binder/evaluator and produce one row even when their result is SQL NULL. Scalar SRF calls remain errors. ROWS FROM, column-definition lists and SELECT-list SRFs are outside the admitted language.

Closed composite descriptors expand into their ordered fields, including NULL fields for a NULL composite. This product deliberately does not require PostgreSQL's anonymous-record column-definition syntax because the complete descriptor already exists before execution.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [QueryExecution](../DESIGN.md) | parent | Selected expressions, relation schema, evaluation and source composition | Parent supplies typed arguments and owns correlation | No transaction/session/authority retained by outputs |
| [SQLRelations](../SQLRelations/DESIGN.md) | depends on | SQLRelationProducer, retained positional rows | One-row demand and awaited close | Emit exactly matching schema |
| [SQLArray](../SQLArray/DESIGN.md) | depends on | SQLArrayRowCursor | Flat row-major UNNEST, longest-input NULL padding, actual subscripts | Cursor is noncopyable and invocation isolated |
| [SQLJSON](../SQLJSON/DESIGN.md) | depends on | SQLJSONRowCursor | Lexical JSON duplicate/order and canonical JSONB rows | Wrong roots retain typed failures |
| [SQLPatternMatching](../SQLPatternMatching/DESIGN.md) | depends on | Program and match/split iterators | Bounded captures, zero-width progression and typed pattern errors | No eager result collection |
| [SQLTemporal](../SQLTemporal/DESIGN.md) | depends on | Selected arithmetic, semantic identity and admitted zones/clocks | Month/DST-aware series | Preserve infinities and injected provider authority |

## Architecture

```text
FunctionCall + preceding lexical schema
  -> CanonicalQueryTypeBinder.bindTableFunction
  -> immutable CanonicalBoundTableFunction
  -> parent evaluates selected argument children (scalar: selected bound once)
  -> makeSQLTableFunctionProducer
  -> invocation actor -> lower cursor / series state / scalar row
  -> descriptor expansion -> optional bigint ordinality -> retained positional row
  -> parent aliases / JOIN / selected query demand
```

### Internal API seam

`CanonicalBoundTableFunction` stores `bound: CanonicalBoundExpression`, ordered `columns: [QueryColumn]`, a closed family `kind`, and `withOrdinality: Bool`. `bound.children` are the selected arguments for row functions; for scalar functions `bound` is the ordinary selected scalar expression. Binding uses the existing binder's child occurrence, coercion, overload selection, metadata reservation and typed error paths. No independent conversion policy is introduced.

`CanonicalQueryTypeBinder.bindTableFunction(_:withOrdinality:occurrence:)` returns that selection. The parent passes the function source occurrence itself; argument edges use its existing `.argument` children.

`makeSQLTableFunctionProducer(schema:selected:operands:temporalEnvironment:workMeter:limits:) throws -> any SQLRelationProducer` accepts `[DatabaseContext.ProjectedQueryValue]`. Each operand retains its evaluated payload leases. For `.scalar`, the one operand is the evaluated `selected.bound`; other families receive evaluated `selected.bound.children` in order. The parent owns evaluation and thus correlation/error provenance. The component validates meter identity and selected arity before retaining inputs.

## Contracts and Invariants

- Output metadata exists before evaluating arguments or reading source rows, including empty/NULL invocations. Each row matches the selected schema and ordinal positions.
- UNNEST pads shorter arrays with SQL NULL and expands each closed composite element positionally. GENERATE_SUBSCRIPTS preserves actual lower bounds and reverse order.
- Pattern/JSON strict SQL NULL yields zero rows; scalar NULL yields one row. A malformed pattern, flag or JSON root fails typed.
- Series has integer, bigint, finite numeric and timestamp/timestamptz signatures. Numeric defaults step to one; temporal step is required. All-unknown series signatures are ambiguous. Existing binder coercion selects arguments.
- Series endpoints are inclusive and direction sensitive. SQL NULL suppresses the invocation before zero-step validation. Advance beyond the final endpoint must terminate without spurious integer/numeric overflow. Months and DST use S06 arithmetic; explicit timestamptz zone replaces only the per-invocation zone while retaining admitted clock/provider capabilities.
- Timestamp infinities retain S06 arithmetic meaning. A series that repeats infinity is demand driven and stops only through ordinary demand, cancellation or existing work/row/byte/deadline limits.
- WITH ORDINALITY appends bigint NOT NULL, starting at one per invocation. Parent aliases are leading positional replacements; excessive aliases fail before reads.
- Every emitted/advanced step consumes common work. Cursor, descriptor, input and output claims are admitted before retained allocation/publication. No new budget or eager all-results buffer exists.

## Runtime Flows

```text
selected -> open invocation
  -> next -> checkpoint -> advance one lower cursor -> expand one row
  -> append ordinal -> retain row claims -> publish
  -> next ...
  -> exhaustion / failure / close -> destroy cursor and inputs -> terminal
```

The actor's `next` body performs synchronous cursor work without suspension. Actor isolation serializes next/close; no reentrancy gap exists inside a transition. Explicit close, exhaustion and failure clear the same owned state. Returned rows contain values and claims only, so they may outlive producer close without retaining the producer.

## State, Ownership, and Lifecycle

| Logical state | Native | WASM | Embedded WASM | Read/mutation entry | Release |
|---|---|---|---|---|---|
| Lower noncopyable cursor | actor-owned optional cursor | same | same | actor next/close | clear on terminal |
| Series current/end/step and ordinal | actor-owned state | same | same | actor next | clear on terminal |
| Input/descriptor claims | retained immutable leases in actor state | same | same | factory admission then next | state clear; emitted row keeps independent claims |
| Output values and retained claims | immutable CanonicalRelation | same | same | consumer only | final consumer release |

No feature-conditioned storage, Sendable weakening, unsafe state or no-op synchronization is introduced. Lower non-Sendable cursors never cross the invocation actor boundary.

## Failure, Concurrency, and Constraints

The factory and each selected producer pull preserve the parent's bound-expression error boundary. After the existing pattern owner maps its kernel errors, SQLTimeZoneError, DatabaseExpressionEvaluationError and QueryValueError become CanonicalReadError.expressionEvaluation with the selected function occurrence. Existing expressionEvaluation failures retain their original occurrence, using the selected function occurrence only when none was attached. CanonicalSQLBindingFailure, other nested-read failures, DatabaseWorkLimitError, reservation failures, cancellation and cleanup failures retain their original error type. No additional argument or row is evaluated to attach provenance. The Database SQL bridge maps this envelope to SQLBindingSourceFailure and resolves the occurrence through its source map.

All lower typed semantic causes propagate through that envelope, with pattern errors mapped by the existing pattern owner. Producer failure first clears owned cursor/input state, then throws; a repeated pull does not create synthetic success. Close is idempotent. Ordinal overflow fails before publication. The common work meter checks cancellation/deadline at each next and all lower loops retain their metering. Existing resource budgets and target stacks remain unchanged.

## Verification and Change Impact

New Engine component tests own binder selection/typed empty metadata, strict NULL/zero step, inclusive/descending integer and numeric boundaries, temporal months/DST/infinity, multi-array/composite expansion, ordinality, demand, cancellation/deadline, exact/one-past resource limits, close/failure and retained-output lifetime. Public SQL tests owned by QueryExecution prove implicit/explicit lateral scope, aliases and LEFT null extension. Parent integration owns native affected/consolidated and exact WASM/Embedded runtime proof; host-only tests do not prove target-specific behavior. Changes to lower cursor contracts require rechecking their adapter and this parent; signature or output changes require parent scope/binding and public SQL regressions.

### Fixed output layout authority

`SQLTableFunctionOutput` owns the names/arity of the closed families whose output
layout does not depend on an argument type. Binding consumes those names and
validates the selected arity. Field-read provenance consumes the same layout,
including WITH ORDINALITY, before JOIN/alias ordinal remapping. It collects
argument dependencies independently of output layout; an unrelated projected
field never becomes a known public function output's ordering dependency.
UNNEST/composite/scalar results whose width requires type selection remain
explicitly unknown to untyped provenance and keep conservative dependencies.
No alias list is treated as proof of complete width. Policy decisions stay in
DatabaseEngine Security; the descriptor owns no principal, meter or mutable state.
Tests compare selected metadata, aliases on both JOIN sides and actual SQLite
policy allowance/rejection. Native and portable scalar/JSON/function regressions
preserve binding failures and real secret-input dependencies.
