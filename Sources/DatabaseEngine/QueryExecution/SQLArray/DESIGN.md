# SQLArray

## Purpose and Scope

This component owns selected SQL array construction, access, array conversion,
comparison, concatenation, F08 scalar functions and bounded row cursors. Its
parent is [QueryExecution](../DESIGN.md); it has no children. This document
freezes the S07 internal implementation boundary, not implementation success.

The single semantic authority is [Kit SQL S07-C01 through C06 and C12](../../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s07-frozen-array-and-json-contract),
including its F08 signature table. General relation composition remains S10;
ROW/composite typing remains S08. Neither dependency removes the required S07
UNNEST or GENERATE_SUBSCRIPTS kernels.

## Responsibilities and Boundaries

S08 consumes [the parent's selected composite and ARRAY(subquery) contract](../DESIGN.md#s08-frozen-relational-execution)
and [Kit's flat value/builder contract](../../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s08-frozen-composite-values-and-ownership).
The parent supplies bound one-column subquery rows and correlation; SQLArray
owns checked destination shape/element assembly and selected element
conversion/comparison. Existing scalar-array semantics remain unchanged as
elements/dimensions become retained views. Composite elements route to the
parent's selected record semantics, without primitive/object conversion.
S08-E03 owns public construction/bounds/failure evidence; SQLArray tests own
changed local shape, capacity, retained-input and comparison behavior.

| Owner | Responsibility |
|---|---|
| SQLArray | Checked shape arithmetic, row-major traversal, access/slices, lexical array framing, scalar F08 operations, array comparison and bounded row production. |
| Parent QueryExecution | Binding, common types, cast admission/context, selected child declarations, evaluation order, lazy branches, occurrences, aggregate/subquery execution and global SQL identity. |
| Parent scalar owners | Numeric, character, binary, UUID and temporal scalar conversion/output/equality/order; the operation temporal environment. |
| Kit | Immutable array/value representation, intrinsic validation, QueryArrayInput grammar and logical QueryValueLimits. |
| QueryAST / Wire | Syntax, serialization, source occurrences and semantic carriage. |

SQLArray never resolves overloads from runtime cells, invents a nested array
element type, or falls back to raw AST evaluation. Native primitive arrays
remain valid on their existing paths. Entering a selected SQL array signature
requires an explicit bound normalization; it does not reinterpret arbitrary
FieldValue arrays globally or change primitive/RDF identity.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [QueryExecution](../DESIGN.md) | parent | CanonicalBoundExpression, selected scalar owners, SQLCollectionError | Supplies declarations and places retained results. | Must rebuild ARRAY from bound children and propagate input claims. |
| [Kit SQL](../../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s07-frozen-array-and-json-contract) | depends on | Array semantics, F08, QueryArrayInput | Owns meaning and intrinsic representation. | Runtime admission is additional to intrinsic limits. |
| [Kit Query](../../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s07-collection-expression-carriage) | depends on | arrayAccess, ArraySubscript, CollectionOperator | Supplies child layout and occurrence roles. | Separate contiguous access groups remain separate nodes. |
| [Wire](../../../../../database-kit/Sources/DatabaseWire/DESIGN.md#s07-collection-expression-carriage) | depends on | Bounded expression/value carriage | Preserves shape and semantic children. | Decode admission does not replace execution admission. |
| [DatabaseEngine](../../DESIGN.md) | ancestor | ExecutionBudget and DatabaseWorkMeter | Owns work, deadline, cancellation and retained reservations. | Claims must belong to the identical request meter. |
| [SQLTemporal](../SQLTemporal/DESIGN.md) | depends on | Selected temporal conversion and comparison | Supplies temporal element policy. | No host clock or timezone fallback. |
| [SQLPatternMatching](../SQLPatternMatching/DESIGN.md) | coordinates with | Scoped result and bounded iterator precedent | Uses the same parent resource/retention boundary. | Array kernels do not acquire pattern semantics. |

The [source understanding](../../../../../.codex-tools/sql-s07-evidence/array-execution-understanding.json)
records the existing callers and behavioral fixtures. It is investigation
evidence, not proof that these new entries execute.

## Architecture

```text
QueryAST / Wire -> parent binder -> selected bound children and result type
    -> parent evaluates children once with input payload claims
        -> CanonicalSQLArrayEvaluator
            -> shape/access/F08 traversal + request meter
            -> QueryArrayInput -> existing selected scalar caster
            -> existing scalar output/equality/order + temporal environment
                -> admitted QueryValue + optional destination lease
                -> SQLArrayRowCursor -> one admitted row + retained leases
    -> parent promotes output with every referenced input claim
    -> S10 relation adapter (explicit public gate until implemented)
```

All implementation entries are internal, pure Swift and request-scoped. There
is no platform array engine, mutable global cache or mutable shared cursor.

## Contracts and Invariants

### Exact Internal Interface

```swift
enum CanonicalSQLArrayEvaluator {
    static func evaluate(_ bound: CanonicalBoundExpression,
        operands: [QueryValue], temporalEnvironment: SQLTemporalEnvironment?,
        workMeter: DatabaseWorkMeter, limits: QueryValueLimits) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?)

    static func cast(_ value: QueryValue, from source: QueryColumnType,
        to target: QueryColumnType, context: CanonicalQueryTypeBinder.CastContext,
        temporalEnvironment: SQLTemporalEnvironment?,
        workMeter: DatabaseWorkMeter, limits: QueryValueLimits) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?)

    static func compare(_ left: QueryValue, _ right: QueryValue,
        leftType: QueryColumnType, rightType: QueryColumnType,
        temporalEnvironment: SQLTemporalEnvironment?,
        workMeter: DatabaseWorkMeter) throws -> QueryComparison

    static func makeRowCursor(_ bound: CanonicalBoundExpression,
        operands: [QueryValue], inputLeases: [DatabaseQueryPayloadLease],
        temporalEnvironment: SQLTemporalEnvironment?,
        workMeter: DatabaseWorkMeter, limits: QueryValueLimits) throws
        -> SQLArrayRowCursor
}

struct SQLArrayRowCursor: ~Copyable {
    func metadata() throws
        -> (columns: [QueryColumn], lease: DatabaseQueryPayloadLease)
    mutating func next() throws
        -> (values: [QueryValue], leases: [DatabaseQueryPayloadLease])?
    mutating func close()
}
```

`evaluate` accepts ARRAY, arrayAccess, the three array collection predicates,
selected array concatenation and scalar F08 calls. `makeRowCursor` accepts only
selected UNNEST and GENERATE_SUBSCRIPTS calls. Other selected families belong to
their respective owners. Dispatch reads the bound expression and declarations;
it does not repeat name resolution, parameter unification or common-type choice.
The caller verifies source/target cast admission even for NULL and empty values.
The facade verifies operand count and declaration compatibility before applying
the operation's selected strictness; F08 is not uniformly strict.

| Expression | Operand layout consumed by the child |
|---|---|
| ARRAY | Immediate selected children in source order. Scalar children share the selected element declaration; array children share that element declaration and must have matching inner shape. Nested bound ARRAY children are evaluated first, not recovered from raw syntax. Empty construction obtains its element type from bound.type. |
| arrayAccess | Base first; then each present bound in dimension order, lower before upper. An index contributes one child. Omitted slice bounds contribute none. The original ArraySubscript metadata distinguishes omission from a NULL-valued child. |
| Collection predicate / concatenate | Left then right, already normalized to the selected compatible element declaration. |
| Ordinary F08 call / cursor | Selected call argument order, including binder-supplied defaults; declarations distinguish an element from an array and unknown NULL from an explicitly typed NULL. |

No SQL type or nullability is inferred from the first row. Scalar results are
normalized to bound.type by the parent. Empty results and cursor metadata use
the selected declarations without evaluating sample cells. A scalar ARRAY
constructor of scalar cells produces lower bound one. A constructor of arrays
requires identical inner lengths and lower bounds, adds an outer lower bound
one and preserves those inner bounds. All NULL/empty subarrays yield canonical
empty; mixing them with a nonempty subarray fails shape validation. This follows
the pinned [ExecEvalArrayExpr](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/execExprInterp.c#L3297-L3365)
contract. Nested child validation occurs before final destination allocation.
Rank and lower bounds remain value
metadata, not repeated DataType.array wrappers.

### Selected Scalar Reuse and Comparison

Array element casts use `CanonicalSQLScalarCast.evaluateValue` or
`evaluatePayload` according to `usesPayload`, with the selected source/target,
cast context and the same temporal environment. The child retains derived
payload reservations before the next conversion or scratch release. JSON/JSONB
cell conversions use the parent's `evaluateCollection` with the same limits;
its producer tuple stays alive until the destination admits the complete
result footprint. Exact same-declaration JSON/JSONB identity borrows the input
and its parent-retained lease. CHAR casts do not use that identity shortcut.
Array
text input uses only Kit QueryArrayInput for framing/shape and per-cell text;
array output uses `CanonicalSQLScalarText.withText` for each non-NULL scalar.
The child owns array quoting, escaping and bound framing, not scalar lexical
conversion, numeric scale, CHAR padding/trimming or temporal formatting.

`cast` covers admitted text-to-array, array-to-text, array-to-array and explicit
primitive-array-to-selected-array normalization. It preserves shape/lower bounds
for elementwise casts, canonical empty shape and typed NULL. Native primitive
array input is traversed under its declared element type and normalized into
the existing SQL carrier; it is not treated as an already validated SQL array.
Runtime conversion failures remain runtime failures; binding still rejects
invalid constant conversions through the existing constant path, including
unselected branches. No hidden eager conversion changes lazy evaluation.

`compare` accepts non-NULL, already selected array values. It implements only
the array ordering layer of C04 and calls parent
`CanonicalSQLValueSemantics.compareValues` / `equalValues` on scalar cells with
their selected element declarations. It never calls the parent's array branch
recursively. The parent owns outer SQL NULL handling, comparison operators,
ANY/ALL, grouping, DISTINCT, set operations, joins and typed key/hash identity.
Those consumers must agree with this array comparison; structural QueryValue
hashing is not a substitute. Containment/overlap and NULL-safe F08 search use
their distinct C04/C05 cell-NULL policies before invoking scalar equality.

### Cursor Metadata and Results

UNNEST metadata has one column per selected array, in argument order: contiguous
numbers from one, name `unnest`, exact selected element type and nullable true
(input NULL cells and multi-array padding are possible). GENERATE_SUBSCRIPTS
has one column numbered one, named `generate_subscripts`, SQL INTEGER and
nonnullable. Metadata is constructed and admitted before iteration, even for
zero rows. `metadata()` returns an independent lease that must accompany its
columns; invoking it after close throws the existing
`DatabaseExpressionEvaluationError.typeMismatch(operation: "closed SQL array cursor metadata")`.

Each successful `next` returns exactly one row with that arity and declarations.
Its leases retain its row/lease-array capacities and all referenced full input
owners. Advancing, exhausting, failing or closing the cursor cannot invalidate
an already returned row. Multi-array UNNEST advances one flat cell index and
pads exhausted inputs with SQL NULL. GENERATE_SUBSCRIPTS uses actual bounds and
checked forward/reverse arithmetic. Neither eagerly materializes all rows.

## Runtime Flows

1. The parent binds every child and selects casts/common types before opening
   source cursors. It evaluates only the selected lazy branch and supplies
   evaluated operands in the frozen layout.
2. The child validates the selected declaration and operation-specific NULL
   rules, checkpoints, and measures/admit shapes, traversal state and output.
3. Each scan, scalar conversion, cell comparison and destination write consumes
   request work before execution. Output buffers grow only after admission.
4. On success the parent retains the new lease and any referenced input leases
   through result placement/promotion. On failure scratch and unpublished
   outputs release their claims and the original typed failure propagates.

`SQLArrayInputWorkspace.withInput(sqlLexicalForm:limits:workMeter:_:)` is the
shared internal synchronous parser workspace, also used by parent binding
constant validation. It retains the parsed input claim through its borrowing
callback; callers must not return an unaccompanied parsed owner. The workspace
first admits rank-at-most-six bounds, shape and recursive-frame transient
layouts. It then atomically reserves the remaining request intermediate byte
quota, capped by QueryValueLimits.maximumRetainedBytes, and supplies that amount
as QueryArrayInput's effective retained limit. No private Cell stride or guessed
cell ceiling is duplicated. A racing admission may fail normally; no retry
silently changes the outcome. The full-quota claim lives only through synchronous
construction and is immediately reconciled to retainedByteCount; transient
parser scratch releases before the callback. Input scan work is admitted before
construction, with cancellation/deadline checkpoints before and after it.

Array lexical conversion then admits each decoded cell
String before `elementText(at:)`, converts through the scalar owner and retains
only required destination owners. No eager `[String]`, duplicate parser or
serialize/parse bridge is introduced. A decoded cell's UTF-8 allowance is bounded
by the input owner; unused allowance is released after actual output measurement.

## State, Ownership, and Lifecycle

The facade borrows operands for the call. An optional returned lease accounts
for newly retained destination storage; nil means no new destination payload,
not permission to release aliased source claims. A result sharing cells, bytes
or a complete JSONB owner retains the producer claim through the parent result
frame. Full external owners, not visible slice lengths, determine retention.

The cursor owns immutable operands, their producer leases, selected metadata,
the meter/environment and admitted iteration scratch. Its factory requires
complete input claims from that same meter, including the full source array
cell/dimension capacities and external cell owners. Fixed inline inputs need
no fabricated payload lease. The factory admits its own operand/lease vectors
before retaining them; it never silently replaces missing or mismatched claims
with unmetered storage. Metadata and returned rows hold independent claims.

```text
factory -> open -> next(row)* -> exhausted -> closed
              \-> close / cancellation / deadline / error -> closed
returned metadata/rows -> independent final-owner release
```

`close` is idempotent and nonthrowing. Exhaustion and every failed `next` close
the cursor before returning/throwing; destruction provides the same release.
After close, `next` returns nil without work. Closing releases only cursor-owned
claims/references; it never explicitly releases shared producer reservations.
Reservations exported by `metadata` or `next` are likewise never explicitly
released by close; the cursor drops its own reference and final-owner release
controls their lifetime. Only exclusively owned scratch can be released eagerly.
All operations are synchronous and task-local, with no unsafe borrowed pointer
or mutable scratch escaping a call. Immutable values and leases may be retained
by independently owned result frames.

## Failure, Concurrency, and Constraints

Use the parent's single `SQLCollectionError`, wrapped by
`DatabaseExpressionEvaluationError.collection`; no child error algebra exists.
The root-frozen cases consumed here are `invalidArrayShape`,
`invalidArrayInput(offset:reason:)`, `invalidArrayArgument(function:)`,
`nullArraySubscript(function:)` and `arraySubscriptOutOfRange`. These distinguish
shape/rank incompatibility, malformed lexical framing, function argument
failure, the specified NULL-start failure, and checked arithmetic failure.
Normal missing/out-of-range reads and empty slices follow C03 and are not errors.
Kit malformedArrayInput maps to invalidArrayInput with its UTF-8 offset; other
QueryValueError, scalar conversion, meter, cancellation and deadline failures
retain their original typed cause. Occurrence wrapping remains with the parent.

Logical limits come from QueryValueLimits and Kit intrinsic rank/shape bounds.
ExecutionBudget and DatabaseWorkMeter alone own runtime work, retained bytes,
rows and time. There is no guessed per-array/cursor/function operational cap.
Checked products, offsets, lengths, capacities and byte totals precede indexing
and allocation. Every collection/vector uses the existing retained layouts and
growth admission, measures actual capacity after allocation and reconciles
before publication; allocator excess must be admitted or the operation fails
and discards the unpublished buffer. Transient input ranges, decoded Strings,
output cells/dimensions, scalar temporaries, cursor metadata and lease vectors
are distinct owners with explicit reservations. Shared producer content is
retained without being charged again as a newly copied destination.

Long scans, including shape/rank validation and skipped NULL cells, consume
work/check cancellation and deadline with the existing meter convention.
`next` checkpoints before producing each row. The cursor does not increment
final response row counts; the parent result-materialization owner does so once.
No Embedded-specific raw mutable state or weaker lifetime contract is allowed.

## Verification and Change Impact

Implementation evidence must exercise these owner entries, not only AST/Wire
roundtrips. Planned owner fixtures are
`Tests/DatabaseEngineTests/CanonicalSQLArrayEvaluatorTests.swift` and
`Tests/DatabaseEngineTests/SQLArrayRowCursorTests.swift`; final selectors/counts
belong to their implementation work item, not this design-only freeze.

| Invariant | Required decisive behavior |
|---|---|
| Selected construction/casts | Mixed known/unknown and all-NULL common types, contextual empty, rectangular/ragged nesting, rank boundary, lower-bound preservation, primitive normalization, scalar typmods and temporal environment. |
| Access / F08 | Grouped index/slice omission versus NULL, clipped/rebased shapes, rank excess, C04 concat identities/incompatible bounds, all F08 NULL/empty/search/output rules. |
| Comparison composition | Numeric/CHAR/temporal cell semantics, NULL ordering, shape tie-breaks, NULL-safe search versus containment NULL mismatch; parent equality/order/hash congruence through real relational callers. |
| Cursor ownership | Exact zero-row metadata, multi-array padding, bounds/reverse, incremental work, no eager output; early close/exhaustion/error/cancel/deadline releases cursor claims while earlier rows remain readable. |
| Resource admission | Exact/one-past work and retained limits, capacity reconciliation, failed growth/conversion cleanup, complete external-byte/JSONB-owner retention and final release; no duplicate producer charging. |
| Public boundary | Real SQL bind/evaluate/serialize paths and occurrence failures; S10 public SRF invocation rejects before reads until relation composition exists. S08 composite gates remain explicit. |

Parser workspace evidence fixes the intrinsic retained limit to the measured
input retention, then checks exact and one-past-short request admission, work
exhaustion and cleanup/reuse after malformed input. The temporary full-quota
peak is not presented as final retained input size.

Reuse the retained behavioral fixtures listed in the source-understanding
artifact for primitive arrays, rich projection, quantified arrays, literal
conversion and retained footprints. Existing capability is preserved; those
fixtures are not replaced by unsupported assertions. Parent Binder/Bound/Rows
tests prove pre-read binding, bound child materialization and empty-relation
metadata. Scalar owner tests remain the authority for scalar policy. Native,
ordinary WASM and Embedded WASM run the same bounded facade/cursor contracts
with the pinned toolchain/SDK; target-specific results are not generalized.

Changing the selected operand layout requires coordinated parent, Kit Query,
QueryAST occurrence and Wire verification. Changing resource/lease ownership
requires parent result promotion and retained-footprint verification. S10 must
compose these existing cursors with relation authority/lifecycle rather than
replacing them with eager arrays; S08 must supply its own ROW/composite metadata.
