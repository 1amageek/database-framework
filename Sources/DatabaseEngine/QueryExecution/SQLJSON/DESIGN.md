# SQLJSON

## Purpose and Scope

SQLJSON owns the selected SQL JSON/JSONB conversion, extraction, operator,
mutation, comparison and bounded row-production algorithms. Its parent is
[QueryExecution](../DESIGN.md); it has no children. This is the S07 implementation
contract, not evidence that the declared runtime paths already exist or pass.

The semantic authority is [DatabaseKit SQL, S07-C07 through
C12](../../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md).
The complete F09/F10 inventory, overloads, strictness and mutation exit rules
remain there. Public ROW/composite binding and TO_JSON composite invocation are
required S08 integration. General SRF/FROM/LATERAL/ordinality composition is
required S10 integration. S07 proves the lower conversion and cursor kernels;
those later gates do not remove functions from the inventory.

## Responsibilities and Boundaries

S08 consumes the parent's [selected composite contract](../DESIGN.md#s08-frozen-relational-execution)
and [Kit's flat composite views](../../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s08-frozen-composite-values-and-ownership).
ROW_TO_JSON/TO_JSON/TO_JSONB reuse the existing ordered convertRow kernel for
actual composite fields, including nested composites. Parent binding supplies
the descriptor and distinguishes whole-value SQL NULL from a present all-NULL
record. SQLJSON owns the resulting lexical/semantic JSON shape, duplicate-key
policy and bounded destination; it does not reconstruct relation presence.
S08-E02 proves public invocation, while local SQLJSON tests prove nested
conversion, strict NULL and retained-owner/resource behavior.

- Consume bound signatures and typed QueryValue operands; implement JSON value
  semantics without re-resolving overloads or interpreting SQL source text.
- Preserve lexical JSON subtree bytes and complete backing-owner retention.
  Implement JSONB operations over its existing flat immutable owner.
- Build each changed JSONB result in one planned flat destination, with
  iterative traversal and independently admitted traversal/child-ID scratch.
- Own bounded synchronous JSON row cursors, typed columns, input retention,
  one-row output admission and terminal cleanup.
- Reuse selected scalar output, CHAR handling, numeric rendering and temporal
  environment routes. Do not introduce another scalar conversion policy.
- Leave binding, child evaluation order, occurrence attribution, SQL-wide
  identity keys, relational equality/grouping/sorting and public composition
  with the parent. Leave carrier validation and construction with Kit.
- Add no QueryValue/DataType case, ROW surrogate, lexical-slice wrapper, private
  Kit buffer access, JSON serializer/parser roundtrip bridge or storage policy.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [QueryExecution](../DESIGN.md) | parent | Bound signatures, selected scalar output, SQL identity and error facade | Routes selected operators/functions/casts and retains result leases. | The parent rejects JSON equality/order before source reads, including empty DISTINCT/GROUP inputs. |
| [Kit SQL](../../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md) | depends on | S07-C07–C12 and planned JSONB construction | Defines function outcomes, limits and intrinsic owner guarantees. | Kit value limits confer no runtime execution authority. |
| [Kit Query](../../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md) | depends on | Existing typed values, collectionOperation and explicit variadic carriage | Preserves overload and operand identity. | finalArrayIsVariadic is explicit metadata, never inferred from an array argument. |
| [DatabaseWire](../../../../../database-kit/Sources/DatabaseWire/DESIGN.md) | coordinates with | Existing value encoding and expression tags 54/55 | Carries operators and explicit variadic calls. | SQL semantic identity does not replace representation Hashable or wire bytes. |
| [DatabaseEngine](../../DESIGN.md) | ancestor | Work meter, intermediate reservations and retained query rows | Owns execution resources and lifetime primitives. | Require the same meter instance, not merely equal budget values. |
| [SQLPatternMatching](../SQLPatternMatching/DESIGN.md) | coordinates with | Bounded noncopyable iterator lifecycle | Supplies the S05 lifecycle precedent for lower SRF kernels. | JSON traversal and semantics remain here. |
| [Temporal Formatting](../SQLTemporal/Formatting/DESIGN.md) | depends on through selected scalar output | Environment-bound temporal text | Supplies existing temporal JSON string conversion. | No ambient clock or time zone is introduced. |

The [source-path investigation](../../../../../.codex-tools/sql-s07-evidence/json-execution-understanding.json)
records pre-S07 implementation facts. The [pinned PostgreSQL contract
observations](../../../../../.codex-tools/sql-s07-evidence/postgresql-contract-observations.json)
and referenced REL_18_6 sources establish reference outcomes, not Framework
implementation success. New interfaces below are implementation obligations.

## Architecture

```text
QueryAST / Kit expression
    -> parent binding: selected signature, child types, explicit VARIADIC
        -> parent evaluates children and preserves owners
            -> CanonicalSQLJSONEvaluator
                + lexical scan -> selected byte range -> validated JSON slice
                + source JSONB views -> mutation plan -> one flat builder
                + semantic JSONB comparison -> QueryComparison
                + lower row conversion / SQLJSONRowCursor
                    -> QueryValue + payload lease / one retained row
                        -> parent Rows / S08 composite / S10 relation adapter

parent SQL identity owner
    -> admitted canonical key + cached digest + metered collision equality
    -> fixed-size digest lookup
        -> explicit throwing/metered collision equality outside standard callbacks
```

The component uses pure Swift and existing ByteString/QueryValue owners. There
is no Foundation JSON implementation, backend parser, platform fallback or
request-external mutable cache.

## Contracts and Invariants

### Selected internal facade

```swift
enum CanonicalSQLJSONEvaluator {
    static func evaluate(
        _ bound: CanonicalBoundExpression,
        operands: [QueryValue],
        temporalEnvironment: SQLTemporalEnvironment?,
        workMeter: DatabaseWorkMeter,
        limits: QueryValueLimits
    ) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?)

    static func cast(
        _ value: QueryValue,
        from source: QueryColumnType,
        to target: QueryColumnType,
        context: CanonicalQueryTypeBinder.CastContext,
        temporalEnvironment: SQLTemporalEnvironment?,
        workMeter: DatabaseWorkMeter,
        limits: QueryValueLimits
    ) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?)

    static func convertRow(
        columns: [QueryColumn],
        values: [QueryValue],
        resultType: QueryColumnType,
        pretty: Bool,
        temporalEnvironment: SQLTemporalEnvironment?,
        workMeter: DatabaseWorkMeter,
        limits: QueryValueLimits
    ) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?)

    static func compare(
        _ left: QueryJSONBValue,
        _ right: QueryJSONBValue,
        workMeter: DatabaseWorkMeter
    ) throws -> QueryComparison

    static func makeRowCursor(
        _ bound: CanonicalBoundExpression,
        operands: [QueryValue],
        inputLeases: [DatabaseQueryPayloadLease],
        temporalEnvironment: SQLTemporalEnvironment?,
        workMeter: DatabaseWorkMeter,
        limits: QueryValueLimits
    ) throws -> SQLJSONRowCursor
}

struct SQLJSONRowCursor: ~Copyable {
    borrowing func metadata() throws -> (columns: [QueryColumn], lease: DatabaseQueryPayloadLease)
    mutating func next() throws -> (values: [QueryValue], leases: [DatabaseQueryPayloadLease])?
    mutating func close()
}
```

These are internal Engine entries, not new package products. evaluate consumes
only the JSON-selected collection operators, existing JSONB subtract/concatenate
branches and the registered F09/F10 scalar functions. cast admits only the
parent-selected JSON/JSONB input/output route. Operand count and value/type
mismatches fail through the parent's existing typeMismatch contract. No child
silently changes a selected signature. The parent supplies bound child metadata
including finalArrayIsVariadic, retaining heterogeneous argument types.

Strictness follows the selected signature, not a universal early NULL check:
TO_JSON is strict; build arguments map SQL NULL to JSON null; a NULL explicit
variadic builder array returns SQL NULL; NULL SRF input yields no rows. Ordinary
array arguments remain one recursively converted value. Explicit variadic cells
expand row-major with their selected element type. Operator overloads are never
selected from runtime value shape alone.

convertRow requires validated ordered, consecutively numbered columns and exact
value arity, with each value admitted by its column type. resultType is JSON or
JSONB. Descriptor names/types are authoritative; the S08 adapter supplies
f1...fN for anonymous fields. This is a lower tuple conversion, not a scalar ROW
value. Pretty applies only to lexical JSON. Mismatched public tuple arity fails
existing typeMismatch rather than truncating zip or inventing values.

### Lexical selection and text output

The lexical scanner uses bounded UTF-8 ranges and explicit traversal frames.
It preserves object order, duplicate members, number spelling and subtree
whitespace. Object lookup finishes the relevant object scan to select its last
matching duplicate; iteration exposes every member in source order. Negative
array indices may require a counted first pass and a locating second pass;
both passes are metered. No eager decoded tree is required.

A selected JSON range uses the existing ByteString slice and
QueryJSONValue(bytes:limits:) constructor. Locate, UTF-8 and syntax-validation
work are admitted and charged. The result retains and charges the complete
source byte owner, even for a one-byte visible slice. No validation bypass or
slice-length-only footprint is permitted. JSONB views likewise retain their
complete flat owner. Conservative alias claims follow the existing Engine
footprint contract rather than unproved identity-based deduplication.

Text extraction decodes only the requested string/scalar, with JSON null mapped
to SQL NULL. Existing lexical acceptance of escaped NUL/surrogates and large
numbers does not promise successful selected text decoding or JSONB numeric
conversion. Intrinsic failures remain typed. Exact UTF-8 key identity is used;
no Swift canonical-equivalence comparison or Unicode normalization is added.
Selected scalar output owns Boolean/numeric/CHAR/temporal rendering. JSON
conversion requests preserved CHAR padding for string values and scalar object
keys; it does not substitute CHAR-to-TEXT coercion. JSON and JSONB output
spacing follows the frozen SQL output contract; converting an
existing lexical JSON value does not normalize it incidentally.

### Flat destination construction

Mutation/deletion/concatenation/strip/conversion first inspect source views and
compute bounded output nodes, element indices, members, UTF-8 payload and depth.
Explicit frames and source/output child-ID tables are separately admitted. A
single task-local QueryJSONBBuilder is initialized with the planned capacities,
appended in postorder, and consumed by finish. Each emitted node is appended
once. Source subtrees are traversed into that destination, never copied into a
new ancestor subtree at each depth. No serialize-then-parse conversion is used.

Object output resolves duplicate keys by the selected last/right-wins rule and
emits unique keys in the intrinsic canonical byte order. Destination planning
and key sorting/search work are charged. Lexical JSON builders instead preserve
argument member order and duplicates, using a bounded final byte destination.
Both routes validate final output before publication.

Mutation paths obey C10's reached-component rules: an earlier missing component
or intermediate scalar can select unchanged output without inspecting an
unreachable suffix. Reached NULL/malformed array indices still fail. Extraction
paths instead implement C08's flattened multidimensional text[] and NULL-result
rules. One path validator must not conflate these distinct contracts. An
unchanged result is a semantic exit, never a resource-error fallback.

### Semantic comparison boundary

compare performs iterative JSONB semantic comparison using canonical keys,
container counts before recursive content and exact numeric equality independent
of displayScale. It preserves the pinned compareJsonbContainers top-level empty
array exception as well as general type ordering. SQL JSON has no corresponding
comparison entry. Containment/existence are selected operator algorithms and do
not substitute structural representation equality.

The parent owns the admitted flat canonical SQL identity key and cached digest,
charges key construction/hash work before use, and performs metered collision
equality outside standard nonthrowing callbacks. It retains original typed
values for metered sorting through compare. This child exposes no hash(into:),
Hashable identity wrapper or replacement QueryValue equality. Kit Hashable and
CanonicalRowFingerprint continue representing their existing contracts.

### Exact lower SRF metadata

Columns are known before the first next, including a NULL input's empty stream.
Numbers begin at one and follow QueryColumn.validate. Names and types are:

| Selected function | Columns in order | Nullable |
|---|---|---|
| json_each / jsonb_each | key: TEXT, value: JSON / JSONB | false, false |
| json_each_text / jsonb_each_text | key: TEXT, value: TEXT | false, true |
| json_array_elements / jsonb_array_elements | value: JSON / JSONB | false |
| json_array_elements_text / jsonb_array_elements_text | value: TEXT | true |
| json_object_keys / jsonb_object_keys | json_object_keys / jsonb_object_keys: TEXT | false |

A non-NULL wrong root fails the parent's typed collection error before emitting
rows. Lexical duplicate/source order and JSONB canonical member order are
preserved. WITH ORDINALITY, aliases and relation joins belong to S10 and do not
change the lower metadata above.

## Runtime Flows

```text
input + same-meter claims
    -> validate selected shape/signature
        -> admit scan/traversal work and scratch
            -> select range / plan destination / initialize cursor
                -> admit output and construct
                    -> reconcile observed retained storage
                        -> attach payload lease before publish

any failure/cancellation/deadline
    -> release unpublished output and scratch
    -> preserve original typed cause and already returned row owners
```

A scalar call's parent keeps input owners and claims live throughout evaluation.
A returned borrowed subtree receives a result claim for its actual retained
owner before return; output does not depend on the parent's temporary child
claims surviving. A freshly built owner similarly receives its lease before
publication. Fixed-size NULL/Boolean/integer results may return no payload lease
where the existing footprint contract requires none.

## State, Ownership, and Lifecycle

The cursor is one noncopyable task-local owner. Its initializer prepares
fallible storage before initializing Self, retains the input values and
same-meter input leases, and owns admitted traversal/metadata state. It does
not materialize all output rows. metadata returns its schema claim together with the ordered columns and throws
existing typeMismatch after close or terminal release. Previously returned
metadata and its lease remain independently valid. Each
successful next returns exactly one equal-arity value array plus every payload
lease referenced by that row, including source claims retained by borrowed
values. Its independent payload claim reserves one intermediate row until the
last returned lease is released, including after cursor close. It does not try to merge arbitrary claims into one payload lease. The
parent retains metadata and row leases while admitting its destination row
storage, and publishes through the existing retained-row contract. Returned
values and their leases remain valid after later next calls, cursor close or
input-root release.

Exhaustion, close and a failed next terminally release cursor input/scratch
claims. close drops its own metadata/input references, is synchronous and
idempotent; after termination next returns nil. A metadata call after that
release throws existing typeMismatch rather than retaining cursor-owned claims.
The failing call rethrows its original cause. Owner destruction releases any
remaining claims exactly once. No I/O, async task, background stream or shutdown
service is introduced. Cancellation/deadline checks occur during scanning and
iteration, not only at construction.

Cursor mutation is confined by noncopyable ownership; immutable source owners
retain their existing Sendable contracts. Any introduced shared mutable owner
must use the same Mutex/actor isolation on Native, ordinary WASM and Embedded;
there is no Embedded raw-state or weakened conformance branch.

## Failure, Concurrency, and Constraints

The parent owns SQLCollectionError and wraps it in
DatabaseExpressionEvaluationError.collection. This component uses these parent
cases without declaring another error algebra:

| Parent cause | Selected failure |
|---|---|
| jsonArrayRootRequired(function: String) | Array-only function given another root. |
| jsonObjectRootRequired(function: String) | Object-only function given another root. |
| jsonContainerRootRequired(operation: String) | Mutation requires a nonscalar root. |
| oddJSONObjectArgumentCount(actual: Int) | Object builder has an odd expanded argument count. |
| nullJSONObjectKey(argument: Int) | Object builder key is SQL NULL. |
| invalidJSONObjectKey(argument: Int) | Object builder key is a disallowed collection/composite/JSON value. |
| multidimensionalJSONMutationPath(rank: Int) | Mutation requires a one-dimensional key/path array. |
| nullJSONMutationPathElement(position: Int) | A reached mutation path element is SQL NULL. |
| invalidJSONMutationPathIndex(position: Int, text: String) | A reached array path element is not an admitted integer. |
| existingJSONInsertKey(key: String) | INSERT reaches an existing object key. |

Argument and path locations use one-based SQL positions, including expanded
argument positions. Lexical UTF-8 offsets remain zero-based. Extraction's
missing/wrong-shape/invalid-index cases remain SQL NULL, not these mutation
errors. Lexical/constructor QueryValueError remains the
original typed cause through that facade. Work/resource/deadline/cancellation
errors pass through unchanged; they never become NULL, empty rows or unchanged
JSONB. Invalid public tuple arity uses existing typeMismatch.

### Prospective capacity and physical reconciliation

Framework uses the planned builder initializer and
constructionByteCount(nodeCapacity:elementIndexCapacity:memberCapacity:).
Before initialization it admits that canonical budget plus separately counted
UTF-8 payload, traversal frames and child-ID buffers. The formula includes all
five intrinsic arrays, including depth and parent tracking. It is not a malloc
or OS physical-allocation guarantee; allocator internal slack is excluded by
DatabaseRetainedArrayLayout's existing budget definition.

Immediately after initialization, before appending, Framework reconciles
constructionRetainedByteCount against the reservation, admitting any observed
capacity difference or releasing the unexposed builder on failure. Planned
logical ceilings cannot grow because Array reports extra capacity. Framework
maintains one unique builder without COW aliases. It reconciles again before
publication; finish consumes the builder and releases construction-only scratch.
Final storageFootprint and UTF-8 retention receive the output claim before the
transient claim ends. Caller scratch is independently released on every exit.

The old growable init(limits:) remains an intrinsic factory API and is not the
runtime construction route. Exact/one-past logical limits, checked arithmetic,
output limits and observed-capacity additional-claim failure must be detectable.
No guessed capacity multiplier, clipping or admission-by-Kit-validation suffices.

## Verification and Change Impact

The following dedicated test owners cover these obligations. Source and tests
are not runtime success evidence until the assigned Native/formal lanes run:

| Test owner | Falsifiable behavior |
|---|---|
| CanonicalSQLJSONEvaluatorTests (conversion) | Strict/non-strict NULL, heterogeneous/explicit VARIADIC builders, duplicates, scalar output and CHAR/temporal reuse, multidimensional array nesting, ROW descriptor/arity and pretty output. |
| CanonicalSQLJSONEvaluatorTests (extraction) | Last duplicate lookup versus duplicate iteration, lexical bytes, negative indices, multidimensional/empty/NULL paths, wrong-shape/missing NULL, requested decode failure and full-owner slices. |
| CanonicalSQLJSONEvaluatorTests (mutation) | Every C10 overload, reached versus unreachable errors, final-only creation, insertion collision, recursive strip and single flat destination at deep nesting. |
| CanonicalSQLJSONEvaluatorTests (comparison) | Numeric dscale-insensitive values, container count priority, top-level empty-array exception, exact key bytes, containment and existence distinctions. Parent tests own semantic key/group/order consistency. |
| SQLJSONRowCursorTests | Exact columns before iteration, NULL zero rows, wrong-root errors, duplicate/canonical order, one-row progress, early close, repeated close, failure/cancel and retained row after cursor/root release. |
| CanonicalSQLJSONEvaluatorTests (resource) | Exact/one-past input/output/traversal/ID/UTF-8 budgets, planned builder reconciliation, deadline/cancel, final-owner release and successful next operation after failure. |
| Parent SQL execution tests | Actual selected binding and supported public scalar operators/functions, occurrence/cause preservation and result lease placement; no mock kernel substitute. |

Kit independently proves the promoted builder's intrinsic planned-capacity,
poisoning, canonicality and owner invariants. Framework tests must prove runtime
admission around that contract. Native and exact release Swift 6.4.0 ordinary
WASM / Embedded matching-SDK executions must exercise the common kernels,
retained outputs and failures. Compilation alone is insufficient. Actual target
traits, source inputs, runtime counts and cleanup evidence remain distinct.
Graph-enabled Database/MultiBase portable and final URL/integrated evidence
remain under the existing S18 gate.

Changes to intrinsic builder/byte ownership require resource and retained-view
rechecks. Changes to scalar output/temporal configuration require conversion
rechecks. Changes to semantic comparison require parent key/group/order checks.
S08 and S10 must prove their public invocation/lifecycle paths using these lower
contracts before claiming the corresponding ROW/SRF inventory complete.
