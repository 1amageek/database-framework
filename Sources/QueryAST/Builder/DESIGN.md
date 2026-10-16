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
