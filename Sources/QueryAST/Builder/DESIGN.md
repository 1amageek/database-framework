# Builder

## Purpose and Scope

Builder owns fluent SQL/SPARQL semantic construction and the canonical SQL
serializer. Parent: [QueryAST](../DESIGN.md). Children: none.
[SQLSerializer](SQLSerializer.swift) owns executable SQL emission;
[SQLQueryBuilder](SQLQueryBuilder.swift) delegates to that owner.

## Responsibilities and Boundaries

Construct DatabaseKit semantic values and emit supported SQL preserving their
meaning. The builder does not infer schema types, plan access, execute SQL,
or own DatabaseKit semantics. SPARQL emission remains its explicit contract.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [QueryAST](../DESIGN.md) | parent | Syntax adaptation | Owns module composition. | One serialization owner. |
| [Parser](../Parser/DESIGN.md) | coordinates with | Strict emitted SQL acceptance | Validates round trips. | Compare semantic structure, not formatting equality. |
| [DatabaseKit SQL](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md) | depends on | Types, statement/block and profile | Defines emit-able semantic constructs. | Non-SQL product values require explicit unsupported result. |
| [Database](../../Database/DESIGN.md) | used by | Built statements and serialized SQL | Consumes equivalent semantic inputs. | No generation-time execution or authorization decisions. |

## Architecture

```text
fluent input -> DatabaseKit semantic value -> structural validation
    -> canonical SQL serializer -> owned SQL text
                                        -> Parser -> same semantic value
```

## Contracts and Invariants

- Builders construct semantic values; all SQL text emission delegates to one
  serializer in this component. Existing duplicate formatting paths are
  migrated, callers updated, and obsolete implementations removed together.
- Emit folded or quoted identifiers with escaped embedded quotes. Preserve
  precedence/parentheses, SQL type modifiers, lateral markers, CTE hints,
  outer ORDER/pagination, window frames, DML conflict/RETURNING clauses,
  DDL and transaction control exactly as represented.
- Literal emission preserves exact numeric and owned SQL value meaning.
  Parameterized generation keeps parameter identity separate from text.
- Serialization returns typed failure for unsupported semantic constructs.
  It never emits `<source>`, omits a required clause, silently reduces regex
  to LIKE, or drops metadata to produce plausible text.
- SQL text and semantic structures respect validated limits. Emission size
  and recursion are bounded before growth; configuration owns defaults.

### S02 canonical SQL and constant normalization

This component is the sole owner of executable SQL text, including SQL/PGQ
MATCH/GRAPH_TABLE/property-graph DDL and type/identifier escaping formerly in
Kit. SQLQueryBuilder.toSQL delegates to this serializer. A public pure
SQLSerializing protocol declares the required root serialization operations;
the immutable SQLSerializer implementation accepts existing QueryStatement/SelectQuery/Expression/
Literal/DataType roots as required by callers; all roots share one bounded
writer and one typed SQLSerializationError contract. Unsupported language,
source, metadata, type, literal, invalid structure and output-budget failures
are distinguishable. No SQL generation remains in Kit, and no compatibility
alias points back to the removed owner. Value formatting and SPARQL generation
remain with their existing Kit owners.

| Canonical input | Emitted SQL and normalized parse meaning |
|---|---|
| Identifier | Always double-quote exact semantic names and double embedded quotes. This includes field/table/schema names, aliases and graph names; keyword/function/operator spelling follows its grammar rather than identifier interpolation. Reject empty/U+0000 names where the declaration requires an identifier. |
| Integer/finite floating primitive | Preserve the declared SQL width using CAST where bare SQL classification differs. A floating primitive uses an explicit REAL/DOUBLE PRECISION cast so its text is not silently numeric. An unsigned/non-SQL primitive declaration with no SQL type-preserving representation fails explicitly rather than changing the result type. |
| Rich numeric | Emit an exact numeric lexical form preserving displayScale, including scaled zero. Fixed decimal spelling includes a decimal point or explicit NUMERIC cast when necessary to avoid integer classification. Expansion is checked against output bytes and PostgreSQL numeric bounds before allocation. |
| JSON | Emit JSON followed by one escaped SQL string containing the byte-identical validated UTF-8 lexical value; SQL string escaping does not normalize JSON whitespace or duplicate keys. |
| JSONB | Emit JSONB followed by canonical JSON text. Preserve canonical keys, scalar distinctions and exact numeric display scales; use iterative owner traversal and bounded escaping. Parsing normalizes through the existing intrinsic JSONB constructors. |
| Mixed interval | Emit INTERVAL followed by one SQL string of signed integer months with mons, signed integer days with days, and signed Int64 microseconds with microseconds. Each component remains separate; avoid abs(Int64.min) overflow and no calendar/timezone inference. |
| SQL array | Emit CAST of PostgreSQL array input text AS the exact element DataType[]. Include explicit [lower:upper] bounds for every nonempty dimension and nested braces in row-major order; empty shape is '{}', with explicit element type. Quote/escape element text, distinguish unquoted NULL from the text NULL, preserve JSON/JSONB/numeric/interval element metadata and multidimensional bounds. Checked dimension products and the existing six-dimension limit apply before traversal. |
| Opaque typed input | DATE/TIME/TIMESTAMP and general type-prefixed string syntax lower to existing cast(string, canonical type) where conversion requires runtime policy. Serializer emits CAST without host conversion. Canonical intrinsically decidable constant casts can normalize to an owned QueryValue literal while preserving value and declared type. |
| Constructor and parameter | Emit ARRAY[...] and ROW(...) from their ordered expression children; emit explicit $n, retaining product named-parameter identity. Explicit parameter/literal type uses CAST where required. Preserve all nested declared type metadata. |

S02 FunctionCall.name and DataType.custom stored one String. S03 adds separate
optional schema components; each exact component is quoted separately and an
embedded dot remains part of that identifier, never split or emitted raw.
Dedicated expression and aggregate variants own built-in grammar spellings.
Every generic FunctionCall name uses exact identifier quoting, including names
that collide with COALESCE or NULLIF; serialization never changes their case or
lowers them into a different expression variant. Custom type identifiers also
use exact validated quoting. S03 supplies qualified representation/binding;
dynamic catalog lookup remains S11. No SQLIdentifier
algebra or quoted-state field is introduced solely for this serializer.

One internal constant adapter in this Builder component supplies both canonical
rich constant encoding and pure intrinsic decoding to Parser. It normalizes
NUMERIC, JSON, JSONB, the canonical mixed interval and typed array constant
forms to existing Literal.queryValue values with their declaration. It never
uses Foundation DateFormatter, locale, current time, host timezone or database
state. Broader temporal/interval input remains a lexical cast for its runtime
owner. Invalid canonical constant input is a typed parse/value failure, not a
fallback to an untyped string. General expression casts remain casts.

NUMERIC, UUID and BYTEA lexical formation consumes the
[Kit-owned bounded constructors](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#sql-lexical-value-construction).
SQLConstantSyntax retains literal classification, target declarations and
canonical output generation, but does not duplicate those input parsers.
Valid UUID braces/compact/four-digit hyphen forms and BYTEA hex/escape forms
normalize to the existing typed primitive values. Malformed supported typed
constants still throw SQLConstantError at the original parser source token;
they are not deferred into a runtime CAST merely to bypass validation.
The adapter maps Kit lexical/range/resource errors to its existing positioned
constant failure categories. Runtime policy, typmod rounding and work metering
remain with QueryExecution; no Builder-to-Engine dependency is introduced.

The parser-facing internal contract is SQLConstantSyntax.numericLiteral(_:limits:)
returning Literal and SQLConstantSyntax.normalize(_:as:limits:) accepting an
Expression plus DataType and returning Literal?. Both accept
QueryStructuralLimits and throw SQLConstantError. S03 permits only bare untyped
NULL casts to normalize to typed null without conversion. Already-typed NULL
retains its CAST edge and source declaration for the execution owner's legality
check, including temporal precision, under
[existing-syntax output typing](../../DatabaseEngine/QueryExecution/DESIGN.md#existing-syntax-output-typing).
The target descriptor must be valid, and nonliteral/non-NULL conversion remains
with its runtime owner. This preserves builder-to-text agreement without claiming rounding.
Normalization returns nil only for input whose conversion is explicitly owned
by a later runtime phase; malformed supported intrinsic input throws. Parser
adds the source-token span to that typed failure. The same adapter supplies
canonical rich emission to the serializer's bounded writer; no second numeric,
array-text, JSON or interval normalization implementation is introduced.

Semantic round trip permits canonical lowering of equivalent syntax (typed
prefix versus CAST/::, folded unary sign and numeric width, bare versus typed
constant); it requires identical value, explicit type/typmod, dscale, lexical
JSON, normalized JSONB, array bounds/cells and mixed interval components.
Repeated parse -> serialize -> parse must stabilize; raw AST equality is not
used to erase or invent metadata during normalization.

Statement serialization preserves represented precedence, exact aliases and
qualified scopes, projection order, CTE column lists and MATERIALIZED hints,
LATERAL after JOIN, ordinary DISTINCT, sort directions/NULLS, supported DML
and SQL/PGQ clauses. Supported nested/set source forms preserve operand and
outer modifier meaning; a source requiring a later unsupported grammar fails
with typed unsupported source rather than '<source>'. SQL serializer rejects
SPARQL variables/RDF operators, dataset/update forms, graph service sources,
Fusion/physical access hints, nondefault product execution metadata and
MultiBase target annotations that text cannot retain. It never substitutes
regex with LIKE or JSON functions with RDF-star operations.

Use caller-provided QueryStructuralLimits for tree depth/node/collection and
maximumRetainedBytes for output UTF-8. Check escaping/decimal/array expansion
before appending. Shared source/value owners are borrowed; copying occurs only
at the required final String boundary, with checked output allocation.
Multi-statement serialization uses the same cumulative budgets and one writer.
Array constant decoding owns a cumulative payload ledger. Before decoding a
cell it reserves that array element and supplies only the remaining node,
collection, nesting and retained-byte budgets to the common constant adapter.
It measures and admits the child before retaining it in the array. A later
malformed cell cannot mask an earlier resource violation. Array text scanning
also checks decoded cell-byte growth against the remaining retention budget.

Explicit scalar CHAR/VARCHAR declarations preserve the owned string exactly.
Serialization rejects a CHAR declaration that would pad or truncate the string,
and rejects a VARCHAR declaration that would truncate it, as typed unsupported
metadata. Exact compatible strings retain their declared typmod. This is the
same carriage requirement applied to typed array cells; runtime casts from
lexical expressions retain their runtime conversion contract.

The no-FROM singleton relation remains the existing zero-column VALUES row.
Other VALUES data sources require S08's alias and scope grammar, so their
serializer branch returns typed unsupportedSource and is marked incomplete
until source-scope round trips prove that grammar. INSERT VALUES retains its
separate, supported statement contract.

Migrate generation tests from Kit to QueryAST and assert literal fidelity,
semantic round trips, unsupported language/metadata failures and output limits.
Previously S01-required rich toSQL failures become positive S02 round trips;
unsupported variants retain explicit failures. Public Database tests compare
builder and emitted SQL through the real executable subset, including quoted
static names, exact numeric types and zero-row metadata. Future ARRAY/ROW/
temporal runtime capability is tested as explicit failure until its owner
implements it; serializer success is not execution success.

### S03 typed text and qualified serialization

Consume [S03 semantic declarations](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s03-semantic-carriage-and-occurrence-addresses).
Emit sqlUnknownString as a bounded SQL string without a text CAST. Emit existing
Literal.string and explicitly typed text with their text declaration, so parsing
cannot silently turn typed text into unknown. Text round trips preserve SQL
value/type meaning; exact enum identity is guaranteed by Kit/Wire rather than
requiring Literal.string and an explicitly typed queryValue to collapse.
Functions/custom types emit optional schema and terminal name as separate exact
quoted components. Constant normalization preserves typed NULL source-to-target
edges and unresolved custom targets for binding; only bare NULL can acquire a
valid resolved target. No new runtime conversion kernel is introduced.
S03-C01/C04/C06/C10 cover these changes through parser, serializer and public
execution; existing structural/output budgets remain authoritative.

Query-level GROUP BY and ORDER BY emit `Literal.int` as a bare output position,
matching [Parser's syntax carriage](../Parser/DESIGN.md#s03-output-position-syntax-carriage).
Other integer values retain their existing width-preserving emission. In these
query clauses, integral `Literal.queryValue` constants retain an explicit type
CAST even when ordinary emission could omit it, so reparsing cannot turn a
constant into an output position. Explicit CAST edges remain represented.
An invocation-local writer flag suppresses redundant natural-width INTEGER and
BIGINT declarations only below the output-reference root, retaining the CAST
edges already carried by Parser. A nested SELECT or aggregate resets that flag;
no context crosses its syntax boundary. Non-natural-width values keep their
required type CAST, and root typed integer constants always keep a CAST.
Aggregate-local ORDER BY uses ordinary expression emission. S03-C07/C10 syntax
and source-annotation tests cover repeated position/constant round trips; public
Database tests own execution equivalence and invalid-position rejection.

## Runtime Flows

Builder state produces a semantic tree, the shared validator checks it, and
serializer emits text. Parsing back must retain semantic meaning; execution
from the builder and emitted SQL must agree under the same schema/parameters.

## State, Ownership, and Lifecycle

Builder is a value-owned semantic accumulator. Serializer has invocation-local
output state and owns its returned text. It retains no transaction, schema
lease, pointer or globally mutable registration.

## Failure, Concurrency, and Constraints

Structural overflow and unsupported serialization are typed errors. Expanding
SQL support must migrate callers when replacing an infallible emission API;
compatibility aliases and placeholder fallback are not retained in this
initial-development project. Immutable values can be shared; mutable output
buffers remain invocation-local.

## Verification and Change Impact

[QueryASTTests](../../../Tests/QueryASTTests) owns builder and parser round-trip
coverage. Every supported inventory construct has semantic round-trip checks;
quoted names, precedence, exact literals, CTE hints and LATERAL have specific
regressions. Non-SQL constructs must fail explicitly. Database tests compare
builder execution and emitted SQL execution, including error behavior and
empty typed results. Semantic model changes require Parser, DatabaseKit
validator/codecs, serializer callers, and QueryExecution revalidation.

### Historical S01 rich-literal generation boundary (superseded by S02)

The pre-S02 SQL generation path was SQLQueryBuilder.toSQL -> expression.toSQL ->
DatabaseKit Query/SQL/MatchPattern.swift Literal.toSQL. Literal.description
is a separate nonthrowing CustomStringConvertible diagnostic path; it is not
an executable SQL representation. S01 does not require a rich value to pass
through SQL text to be projected: semantic literals and bound parameters use
the direct query execution path.

Before S02 consolidation, existing generation entry points become throwing
and preflight the complete emitted semantic tree. Rich Literal.queryValue
syntax unsupported by the current serializer throws Kit's
QueryLiteralConversionError.unsupportedSQLLiteral before any SQL text is
constructed. The five rich alternatives are rejected recursively, including
inside arrays/expressions. Literal.queryValue(.primitive(value)) explicitly
delegates to the existing lossless FieldValue-to-Literal conversion and
primitive SQL emission; unsupported primitive conversion retains its existing
typed failure. Rich values never use metadata-dropping delegation. Recursive
toSQL callers propagate that typed failure; no
placeholder, description interpolation or conversion to a primitive
surrogate can produce successful SQL. Kit remains independent of Framework;
its existing generation code is the temporary physical owner, while this
component is the target semantic owner. Full ownership migration and grammar
expansion remain S02.

S01-C04–C08 syntax-boundary tests require rich semantic literal/parameter
execution success and SQL text generation failure for unsupported rich
syntax. Existing primitive SQL emission remains unchanged. Diagnostic
Literal.description can describe kind/metadata but is never passed into the
parser or used as a persistence/wire encoding. Required callers migrate to
throws in the same source change; aliases and parallel unchecked generation
APIs are not retained.

### S01 SPARQL generation failure propagation

SPARQLQueryBuilder.toSPARQL propagates QueryLiteralConversionError from the
Kit graph serializer. Primitive RDF generation preserves its existing text;
unsupported wrapped rich values fail explicitly rather than reaching a
nonthrowing builder or diagnostic Literal.description. Direct generator
callers handle the typed failure. PropertyPath-only generation remains pure
and nonthrowing because it contains no query literal.

### S04 conditional and quantified serialization

The canonical SQL serializer consumes [S04 semantic forms](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s04-semantic-additions-and-scope).
Emit simple CASE operand once, dedicated GREATEST/LEAST syntax, truth tests,
null-safe DISTINCT predicates, and scalar comparison ANY/ALL with array-valued
expression or subquery. Preserve generic quoted/schema-qualified function
identity and operator precedence. Structural/parameter walks charge each new
child once; unsupported forms return typed errors instead of placeholders.
S04-C11 requires semantic parse/serialize/parse identity including operand
count and quoted versus special-syntax distinctions.

### S05 exact pattern emission

Pattern serialization emits explicit ESCAPE for LIKE/ILIKE/SIMILAR: absent IR
escape is ESCAPE empty string, while a present expression is emitted exactly.
This preserves programmatic Graph no-escape meaning and explicit SQL default
backslash across round trips. ERE uses ~ or ~* and rejects an escape child.
Concatenation emits ||. The SQL whereLike builder supplies explicit backslash
escape; a quoted generic function remains an ordinary exact function call.
Special SQL grammar uses the canonical ordinary call representation chosen by
Parser, so serialization need not reconstruct syntactic spelling.


### S06 temporal emission

Consume [Parser temporal normalization](../Parser/DESIGN.md#s06-temporal-syntax-and-provenance)
and the Kit-owned temporal declarations. Emit currentValue using its SQL keyword
and optional precision, never by inventing an ordinary quoted function. Generic
FunctionCall emission remains exact: extract and timezone are actual catalog
calls, while quoted current-like names retain their independent generic identity.
Normalized MAKE_INTERVAL arguments serialize in positional semantic order.

Emit unconstrained INTERVAL directly, full-range precision as INTERVAL(p), and
restricted fields with precision only after terminal SECOND. Preserve absent
versus explicit precision and every field range. DATE/TIME/TIMESTAMP input stays
lexical CAST; the serializer does not consult host clocks, locale or timezone.
New time/offset-time values and temporal infinities emit bounded typed lexical
forms with their exact declaration. Infinity direction and timestamp zone
identity survive scalar and array text. Modified interval values retain explicit
target metadata; canonical component formatting never performs their adjustment.
Existing output admission precedes growth and existing typed serialization
errors retain invalid declarations, unsupported language and resource failures.
S06-SYN01...06 require parse/serialize/parse stabilization and exact metadata;
public execution of those forms remains the Framework verification owner.


Wide-offset TIMETZ values whose wall time is below 24:00 emit an actual
`timezone(interval-offset, TIMETZ baseUTC)` expression. Checked arithmetic and
Euclidean day modulo derive baseUTC while retaining the complete Int32 offset.
A wide-offset 24:00 endpoint cannot survive that operation, so serialization
returns typed unsupportedLiteral for that exact combination; it never emits
00:00 instead. Directly representable offsets retain ordinary typed literals.
Wire, IR and persistence remain lossless for every carrier value. A wide-offset
TIMETZ array cell has no equivalent lexical array element and also fails typed
at this text-only boundary; it is not silently transformed into a different
array value or a future S07 constructor.

### S07 collection emission and intrinsic constant capability

Consume [Query's grouped collection expressions](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s07-collection-expression-carriage),
[SQL S07-C01...C12 and QueryArrayInput](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s07-frozen-array-and-json-contract)
and [Parser's grammar](../Parser/DESIGN.md#s07-collection-grammar-and-occurrence-boundaries).
Kit owns shape/value invariants; Framework execution owns selected element
conversion, runtime services and admission. This component owns lossless SQL
text and its pure intrinsic constant capability only.

The existing private SQLArrayInput in SQLConstantSyntax+Collections.swift mixes
array shape scanning and selected element conversion. Extract its shape grammar
to the Kit-owned QueryArrayInput and migrate this adapter to that contract;
remove the private scanner only after replacement behavior is proven. Builder
retains its cumulative cell/payload ledger and selected pure scalar adapter.
There is one shape grammar, no serialize/parse bridge through DatabaseEngine,
and no Engine dependency in QueryAST. Request individual cell text only at the
existing scalar input boundary and retain the full immutable input owner while
borrowing ranges. Kit intrinsic limits do not replace the caller's cumulative
structural and decoded-value accounting.

```text
typed array text -> QueryArrayInput (shape / bounds / cell ranges)
    -> explicit constant-capability decision
        -> pure selected cell normalization -> typed literal
        -> runtime-owned conversion -> original lexical CAST
malformed syntax / supported input / exhausted limit -> typed failure
```

Choose capability deterministically from the target descriptor and the admitted
cell forms before attempting a conversion that can fail. Reuse the scalar
adapter's explicit capability result where conversion is value dependent;
nil means runtime ownership, not a caught conversion failure. Every non-NULL
cell must qualify before the entire array can normalize to one literal. A
valid shape with any context-dependent cell preserves the complete original
lexical expression and exact array target as CAST. Empty/all-NULL arrays can
normalize when their resolved descriptor is intrinsically valid. Malformed
shape, malformed input in a supported intrinsic capability, invalid descriptor
and resource failure throw; catch-and-retry-as-CAST is forbidden. Runtime CAST
retention never repairs malformed shape or invents a value.

Preserve existing numeric/Boolean/text/UUID/BYTEA/JSON/JSONB and canonical mixed
interval constant capabilities, including compatible typmods and their explicit
runtime-owned cases. General temporal interpretation, timezone configuration,
typmod adjustment and custom input remain execution responsibilities. Canonical
temporal/infinity array text emitted by S06 must reparse as an explicit runtime
CAST when intrinsic decoding needs those services. Semantic round trip compares
executed value and declaration in that case, as with the S06 wide TIMETZ
precedent; it never demands a fabricated pure literal or silently drops a CAST.
The existing wide-offset TIMETZ array-cell text limitation remains explicit.
Ordinary admitted DATE/TIMESTAMP/TIMESTAMPTZ, primitive TIME, UUID and BYTEA
array cells use the existing QueryLiteralEncoding ISO/hex, UUID description and
timeText primitives. CivilDateTime composes that date/time output without a
timezone lookup. At this SQL text boundary only, astronomical nonpositive ISO
years become the equivalent positive year plus BC suffix, matching the existing
SQL input contract; the calendar and instant do not change. BYTEA hex expansion
and microsecond alignment are checked before output. No host formatting service
or independent calendar/zone policy is introduced. Required evidence covers
these cells, lower bounds and BC spelling; temporal values reparse through the
runtime CAST branch, while UUID/BYTEA retain intrinsic normalization.

Serialize collectionOperation with the fixed token table in Parser and explicit
binary parentheses, preserving arbitrary mixed operator trees independently of
precedence. Reuse existing concatenate and subtract output for their new
overloads. Emit every arrayAccess as a parenthesized base followed by exactly
that group's suffixes. Parenthesize an arrayAccess base before adding another
group, never concatenate two groups into one. Index and slice syntax preserve
omitted versus present bounds; explicit NULL is an expression. Parenthesize
every present subscript operand, including named parameters, so a leading colon
cannot become a slice delimiter. Expression and output limits are checked
before writing parentheses, delimiters, bounds and expanding cell text.

FunctionCall.finalArrayIsVariadic is explicit final-argument metadata under
Query's S07 contract. Emit VARIADIC immediately before that final argument only
when true, preserving schema/name/DISTINCT and all argument order. Reject true
mode with no argument; false-mode zero-argument calls remain valid. Never
expand an array at serialization time, erase the marker, or invent a different
function name. This distinction preserves SQL NULL versus empty variadic array
and ordinary array argument behavior for registered JSON builders and path
functions. All builder rewrites must copy the flag. Parser owns final-only
grammar and binding owns registered overload/type legality;
[Wire](../../../../database-kit/Sources/DatabaseWire/DESIGN.md#s07-explicit-variadic-function-layout)
owns its lossless production. SPARQL serialization rejects these SQL-only
collection expressions and unsupported explicit-variadic calls with typed
language failures rather than emitting plausible SPARQL.

| Evidence ID | Required behavior |
|---|---|
| S07-SYN01...03 | Parse/serialize/parse preserves operator trees, exact group boundaries, bound presence, parameters and source-edge shape under Parser's grammar. |
| S07-SYN04 | Pure constant capability and runtime CAST selection stabilize; existing literal capabilities still succeed; malformed intrinsic input and exact/one-past budgets fail explicitly. |
| S07-SYN05 | Builder versus emitted public scalar SQL produces equal selected types, values, array bounds, lexical JSON and JSONB meaning; temporal arrays use the same explicit environment. |
| S07-SYN06 | Emit and reparse final VARIADIC metadata without expansion; ordinary/final-array distinction, malformed true/empty arguments, NULL/empty arrays and argument-preserving rewrites. |

QueryASTTests own syntax, constant capability and output-budget evidence;
Database tests own executed semantic round trips and typed failures. Public
ROW/composite conversion remains the required S08 gate and general SRF relation
composition S10, following the single SQL authority. ARRAY(subquery) requires
S08 relational carriage; ARRAY[...] does not satisfy it. Function-call argument
mode follows the explicit carriage above; ordinary FunctionCall syntax alone
cannot prove that distinction.

### S08 relational emission and lossless construction

**PLANNED; not implementation or verification evidence.** Consume
[Query's frozen public declarations](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s08-frozen-relational-query-carriage),
[SQL's composite descriptor/value contract](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s08-frozen-composite-values-and-ownership)
and [Wire carriage](../../../../database-kit/Sources/DatabaseWire/DESIGN.md#s08-relational-query-carriage).
[Parser's S08 contract](../Parser/DESIGN.md#s08-relational-grammar-and-construction-provenance)
owns accepted syntax, source normalization and S08-SYN01...06. This component
constructs those same Kit values and emits executable text without another AST,
query layout or semantic evaluator.

```text
fluent input -> canonical Query fields -> structural validation
    -> bounded SQL writer -> shared Parser grammar -> represented meaning
unsupported exact metadata -----------------------> typed serialization failure
```

#### Canonical query and source output

Projection emission walks the ordered ProjectionItem sequence, emitting each
expression/optional AS alias or bare/qualified wildcard in place. Preserve
duplicate names and exact quoted components. SelectQuantifier is the sole
DISTINCT owner: emit ALL/default, DISTINCT or DISTINCT ON with its ordered
nonempty keys. Emit collate operands with sufficient parentheses and exact
quoted C/POSIX identity; no locale lookup or alternate collation field enters
the writer. Direction and NULL ordering remain SortKey metadata. Existing
output-ordinal versus explicitly cast integer constant emission remains in
force, including query-boundary resets.

DataSource.aliased emits one AS name and optional column list around the source
it owns. Parenthesize joined groups/derived queries where required to preserve
scope and hide constituent names exactly as represented. A bare subquery stays
without an invented alias. TableRef.alias in DataSource.table is an intrinsic
shape failure; direct mutation-target serialization retains its legitimate
alias. Logical/graph-table aliases migrate through the same source wrapper.
JOIN output preserves the tree, LATERAL position and condition legality; comma
input may canonicalize to explicit CROSS JOIN with grouping that retains its
lower precedence and correlation scope.

VALUES emits general expression rows in order, retaining parameters, nested
queries and column alias wrappers. SQL rows must be nonempty. Require equal raw
arity only when no direct cell is fieldExpansion; binding owns final arity
after parent-context expansion. Preserve those raw cells without normalizing
their AST or provenance. A pure wildcard/all VALUES query envelope emits bare
VALUES with its CTEs and query tails. A direct VALUES source emits (VALUES ...); a deliberate
subquery source over the same envelope emits (SELECT * FROM (VALUES ...)) so
round trips retain that query boundary instead of adding a wrapper each time.
Omit FROM only for Query's one-zero-column-row identity. The distinct
programmatic empty zero-column relation has no ordinary SQL VALUES spelling;
return typed unsupportedSource if its complete meaning cannot be represented,
never substitute a singleton, fabricate columns or silently omit FROM.

Emit a direct setOperation envelope as a binary complete-query expression,
preserving each operand's projection, CTEs, quantifier, local ORDER/pagination
and its own ALL/DISTINCT mode. Parentheses retain INTERSECT precedence and
UNION/EXCEPT association even for non-default constructed trees. Emit envelope
CTEs before the whole expression and outer ORDER/pagination after it. A set
used as an ordinary relation remains a complete DataSource.subquery; do not
serialize it as source operands or synthesize aliases. Reject invalid set
envelopes before output. This replaces the current unconditional setSource
and nonsingleton VALUES failures only after S08-SYN01/03/06 pass.

QueryPagination has one emission path. Preserve absent limit, explicit ALL,
fixed versus expression counts, offset and withTies. Use a legal canonical
LIMIT/OFFSET form for ordinary limits and FETCH WITH TIES for the tied mode;
omitted FETCH count was already normalized to fixed(1). Expression operands
retain parameter identity, NULL and CAST edges, with parentheses where grammar
requires them. Bare NULL WITH TIES must not become typed NULL or ALL, and a
typed NULL must not lose its cast. SQL fixed counts above Int64.max fail typed
instead of wrapping; this SQL text boundary does not narrow SPARQL's full
UInt64 fixed-count carriage. Preserve CTE columns and materialization hints
through every query expression. The writer does not select a DISTINCT row,
evaluate pagination, compute peers or decide CTE demand/volatility.

#### Composite expressions and text representability

Emit ROW constructors, field access, fieldExpansion and ARRAY(subquery) through
Parser's shared grammar, preserving the complete nested query and operand
parent context. List expansion and function-argument composite semantics must
agree for direct QueryIR and emitted SQL. A wholeRow reference is relation
identity; ordinary unqualified-column syntax retains column-name priority.
Do not use a spelling that changes that binding or turns one composite into
several list items. Keep fieldExpansion's child provenance honest rather than
manufacturing function arguments or dropping an explicit composite operand.
If a represented node has no scope-preserving SQL spelling, return typed
unsupported serialization instead of inventing aliases or object conversions.
The serializer never performs ROW comparison, composite JSON conversion or
ARRAY accumulation. The accepted PostgreSQL18.6 inner bounds plus outer-one
ARRAY policy belongs to execution; emitted syntax retains the query producing
those arrays without rebasing their values.

Composite literal emission succeeds only when reparsing/execution can retain
the exact DataType.composite descriptor and QueryValue.composite value,
including ordinal/name/type/nullability and nested array descriptors/bounds.
Do not flatten a descriptor into primitive objects, guess named-type lookup,
invent output aliases, or silently treat an empty composite array as unknown.
Use existing SQLSerializationError.unsupportedLiteral/unsupportedType for
unrepresentable named, nested or empty-array metadata; return no partial SQL.
Representable cases require successful semantic round trips. Typed Wire
parameters/results remain lossless for every valid descriptor independently
of SQL text representability. No serialize/parse bridge is introduced into
execution or the Kit value owner.

#### Rebuilds, limits and completion boundary

All builder query rewrites use Query's canonical immutable replacements and
preserve the complete envelope: projection/source/accessPath/filter/groupBy/
having/orderBy/quantifier/pagination/subqueries/reduced/dataset. Optional nil
replacement clears the field. Migrate old distinct/limit/offset and Projection
consumers together; retaining a nil-coalescing copy path or duplicate DISTINCT
state is not an admitted shortcut. Preserve finalArrayIsVariadic and all new
children in parameter and expression rewrites. SPARQL retains separate graph
union/minus and fixed unsigned pagination, and rejects SQL-only forms through
typed language errors.

Use the existing structural and retained-output budgets across the whole
query, including alias lists, VALUES cells, DISTINCT keys, set operands,
pagination, descriptors and expanding literal text. Admit growth before
writing and retain one output owner; nested queries share the ledger. Preserve
the original typed cause for malformed shape, unsupported text and exhaustion.
Public table functions/SRFs and recursive CTE execution remain S10;
schema-qualified table execution remains S11. Successful serialization does
not confer these runtime capabilities.

S08-SYN01...06 require builder-versus-text public execution for supported
relational/composite forms, repeated semantic round trips, exact query rebuild
fields, typed unsupported literal/type cases and exact/one-past output limits.
The Parser case table owns their identifiers and reference-observation links;
do not duplicate the oracle as another contract. S07 ROW/ARRAY gates are
replaced only by those implemented paths and their runtime evidence, while
unrelated S07 intrinsic constant capability and S10 public SRF gates remain.


### S09 grouping, aggregate and window emission

**PLANNED; not implementation or verification evidence.** Consume Kit's
canonical S09 declarations and [Parser's shared grammar](../Parser/DESIGN.md#s09-grouping-aggregate-and-window-grammar).
One SQLTextWriter emits every SQL aggregate through SQLAggregateCall; retained
SPARQL aggregate variants and the GROUP_CONCAT extension keep their language
meaning. Existing SQLQueryBuilder aggregate conveniences migrate to .sql.

GroupingElement emission preserves ordered grouping expressions, empty grouping
sets, ROLLUP/CUBE compound units and nested GROUPING SETS without expansion or
deduplication. Existing fluent expression groupBy wraps expressions explicitly;
a semantic grouping-element overload admits the new forms without a second
query model. Query rebuilds preserve windows as well as all preexisting fields.
An explicitly empty groupBy emits GROUP BY () rather than disappearing. Nested
single grouping-set operations emit bare ROLLUP/CUBE/GROUPING SETS terms;
parenthesized members contain scalar expressions. Semantic set members mixing
nested operations with sibling expressions, empty ROLLUP/CUBE lists/units and
empty outer GROUPING SETS have no direct grammar-preserving spelling and return
typed unsupported metadata without expanding or changing the represented sets.

SQL aggregate output preserves exact optional schema and closed canonical name,
all argument expressions, DISTINCT, internal ORDER BY, WITHIN GROUP and FILTER.
STRING_AGG's delimiter is emitted as its second expression. COUNT's valid
zero-argument form emits star. Malformed intrinsic representation fails typed;
execution-specific type, scope and modifier legality remains Engine-owned.
GROUPING emits each argument. WindowCall emits its analytic or aggregate call
followed by OVER. Direct named references emit OVER name; specifications always
emit parentheses, preserving OVER (name) inheritance. WINDOW definitions emit
after HAVING and before outer ORDER BY, with exact definition names and no
inlining, default-frame synthesis or peer computation.

Frames emit the represented ROWS/RANGE/GROUPS mode using BETWEEN for both
canonical bounds, each
offset expression and EXCLUDE mode. Parentheses around offsets preserve their
expression boundaries. Every aggregate argument/filter/sort, grouping child,
partition/sort/frame child and named definition enters structural, parameter
and retained-output accounting. Nested calls share one bounded text owner and
admit punctuation/name growth before mutation. SQLTextWriter never evaluates
offsets, filters, ordering, aggregates or window inheritance.

SPARQL generation explicitly rejects SQL-only aggregate calls, GROUPING,
non-expression grouping elements, windows and WINDOW metadata through its typed
language failure path. It must not flatten grouping sets, drop modifiers, emit
a plausible ordinary function, or silently omit windows. Ordinary SPARQL
grouping/aggregates preserve their existing behavior and failure propagation.
S09-SYN01...06 are owned by Parser's evidence table; public Database execution
provides semantic text-versus-builder witnesses independently of AST equality.


### S10 recursive and function-source emission

Consume [Query's frozen fields](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s10-frozen-recursive-and-function-source-carriage) and [Parser's grammar](../Parser/DESIGN.md#s10-recursive-and-function-source-grammar). Emit WITH RECURSIVE once for the enclosing clause; emit each CTE's SEARCH then CYCLE clauses after its body/materialization spelling. CYCLE TO/DEFAULT use the existing Literal writer and require both constants or neither. Invalid/unrepresentable constant metadata fails typed. Fluent and immutable query copies preserve withRecursive, search and cycle.

Emit function sources through the existing FunctionCall writer, followed by optional WITH ORDINALITY. Aliased source alone emits relation/leading positional column aliases. No explicit-LATERAL bit is synthesized; explicit lateral subqueries retain their current join spelling. Function arguments and constant metadata share existing structural/output admission. SPARQL rejects SQL-only recursive metadata/function sources explicitly. Parse/serialize/parse stability, source-argument correspondence and malformed metadata tests establish carriage, while public Database tests establish execution.

S10 CYCLE text carriage accepts only constants with an exact intrinsic Literal representation. Canonical positive-year ISO DATE uses the existing pure XSD date scanner plus SQL value validation; DateStyle-dependent and relative date input remains a typed unsupported constant. General DATE/TIME/TIMESTAMP lexical evaluation remains Engine-owned. The serializer emits a typed constant only when the same intrinsic adapter reconstructs its exact value and declared type; unsupported temporal forms, typed NULL and other unrepresentable metadata fail instead of producing text that CYCLE cannot parse.
