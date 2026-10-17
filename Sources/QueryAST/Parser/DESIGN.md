# Parser

## Purpose and Scope

Parser owns SQL/SPARQL lexical and grammar adaptation inside QueryAST.
Parent: [QueryAST](../DESIGN.md). Children: none. Planned SQL coverage and
explicit differences are owned by [DatabaseKit SQL](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md).

## Responsibilities and Boundaries

Parser recognizes complete source input, preserves semantic distinctions and
source diagnostics, and applies structural limits. It does not resolve
schema names, infer SQL result types, authorize or execute statements.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [QueryAST](../DESIGN.md) | parent | Text adaptation boundary | Composes parser with builder and consumers. | Acceptance does not imply runtime support. |
| [Builder](../Builder/DESIGN.md) | coordinates with | Canonical emitted SQL | Emitted SQL must parse back without lost meaning. | Unsupported serialization must fail explicitly. |
| [DatabaseKit SQL](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md) | depends on | Statements, types, and structural validation | Semantic model used by every entry. | Add all new structural child edges to validation. |
| [Database](../../Database/DESIGN.md) | used by | Complete statement/block parsing | Public admission consumes parsed statements. | No incomplete-prefix success. |

## Architecture

```text
owned source text -> bounded lexical state -> grammar and source locations
    -> DatabaseKit statement/block -> structural validator -> accepted value
lexical/syntax/limit failure ------------------------------> typed failure
```

## Contracts and Invariants

### Current implementation and evidence boundary

[SQLParser](SQLParser.swift) provides complete `parseSelect` and `parse` entries
and the bounded `parseStatements` syntax batch entry. SELECT and general
statement dispatch share WITH-leading SELECT grammar; existing INSERT, UPDATE,
DELETE and property-graph DDL remain supported. Every entry resets invocation
state, admits bounded UTF-8 source before copying, consumes complete input and
validates its resulting semantic structures.

The S02 lexer recognizes quoted/folded identifiers and the supported lexical
forms. Builder owns the canonical SQL serializer and the shared intrinsic
constant adapter; Parser consumes their semantic contract and does not use
former Kit SQL emitters. S01 set-operation execution evidence used semantic
ASTs. Set-source text grammar and nonsingleton VALUES sources remain S08 work;
S02 parsing or serialization never claims their runtime implementation.

Generic function syntax preserves its exact semantic name and DISTINCT flag.
Quoted conditional-syntax names remain generic; S09 canonical lowercase
aggregate/analytic catalog names use their dedicated call representation even
when quoted. Unquoted COALESCE/NULLIF use dedicated expression cases. A qualified
wildcard projection maps to the existing `Projection.allFrom` case. Type-modifier
range failures retain the offending number token's original UTF-8 start before
advancing. These are syntax and value-carriage obligations, not evidence of
function execution or name binding.

### Planned contracts

- SELECT and general statement/block entries share grammar. WITH-leading
  SELECT and data-changing CTEs are dispatched according to the semantic
  model, not rejected by a narrower top-level keyword switch.
- The accepted inventory includes quoted/folded identifiers, E/Unicode/dollar
  strings, exact exponent numeric literals, typed literals, `$n` plus product
  parameter syntax, nested comments, `::` and full query/DML/DDL/block grammar.
- Preserve quoted identity, identifier source spans, expression precedence,
  lateral/outer scope syntax, type modifiers, frame clauses and control
  statements for the binder. Unknown literals remain inferable values rather
  than premature float conversion.
- Reject missing operands, aliases, NULLS directions, required JOIN clauses,
  malformed literals, mixed unsupported parameter styles and trailing input.
  Semicolons inside strings/comments do not split blocks.
- Structural validation visits every new statement, expression, relation,
  window, CTE and DDL child. Limits apply during lexing and recursion as well
  as to the completed tree, so invalid input cannot first allocate unbounded
  intermediate tokens or nodes.

### S02 strict lexical and grammar contract

The [Kit SQL contract](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s02-frozen-syntax-to-value-boundary)
fixes semantic normalization; [Builder](../Builder/DESIGN.md#s02-canonical-sql-and-constant-normalization)
owns canonical constant decoding/encoding shared by parser and serializer.
This component owns token recognition, clause grammar, input positions and
complete-input rejection. Parser and Builder do not implement competing rich
literal adapters.

| Area | Required S02 behavior |
|---|---|
| Identifiers | Quoted and Unicode-escaped quoted names preserve exact identity, doubled quotes and valid escapes; reject empty names and U+0000. Unquoted identifiers include legal non-ASCII letters, underscores and subsequent digits/$; fold ASCII case as PostgreSQL UTF-8 scanning does. Keywords are contextual where the grammar allows names. No silent 63-byte truncation or exact-name fallback: existing product name limits remain authoritative. |
| Strings | Ordinary strings use standard_conforming_strings=on, doubled quotes and PostgreSQL newline concatenation. String-token continuation admits whitespace and line comments containing a newline; a block comment between quoted fragments ends that continuation and cannot splice the fragments. E strings support PostgreSQL byte/Unicode escapes and validate resulting UTF-8; an otherwise unrecognized escaped character is taken literally, including x when no hexadecimal digit follows. U& strings and identifiers support UESCAPE, surrogate pairs and six-digit escapes. Dollar quoting uses an exact case-sensitive delimiter, recognizes $n parameters separately, and treats its body as uninterpreted string content. Invalid Unicode escapes, invalid resulting UTF-8, U+0000 and unfinished forms fail. |
| Numeric tokens | Decimal point, exponent, leading/trailing decimal point, base-2/8/16 integer prefixes and legal digit underscores are accepted. A direct unary +/- is folded before SQL-width selection. Exponent/digit scanning is checked and linear; never use Double or unbounded decimal expansion. Typed literal normalization preserves dscale. |
| Comments | Line comments and nested block comments are whitespace between SQL tokens; string-token continuation follows the separate Strings contract. Bounded nesting and byte scans apply before growth. Comment/string/dollar bodies do not expose statement delimiters or parameter markers. |
| Named parameter markers | A colon and its following identifier form one contiguous product parameter marker. Whitespace and comments cannot separate them. Preserve exact identifier spelling, including quoted names and keyword names, without consuming a following clause as a missing parameter name. |
| Type and literal grammar | Typed DATE/TIME/TIMESTAMP/INTERVAL/UUID/BYTEA plus JSON/JSONB canonical forms, ARRAY[...] and ROW(...), CAST and postfix :: share canonical DataType parsing. Preserve temporal precision and zone qualifiers, signed numeric scale and all accepted aliases. PostgreSQL type'...' does not admit array types; array text uses CAST/::. |
| Existing query grammar | General parse and parseSelect share the WITH-leading SELECT path; preserve CTE hints, LATERAL ordering, DISTINCT, sort NULLS and supported DML/SQL-PGQ semantics. Require AS aliases, qualified-name components, NULLS FIRST/LAST, nonempty WHERE/ON expressions, valid USING lists and required ordinary JOIN conditions. CROSS/NATURAL cannot acquire ON/USING. |
| Complete input | Existing parse/parseSelect return one complete statement and reject additional statements. Add parseStatements for bounded ordered existing statements; accept trailing separators/empty separators as delimiters without creating empty statements. Quoted/comment semicolons never split input. This API performs syntax only, not transactional SQL blocks. Unsupported BEGIN/DDL/new query features remain typed unsupported until their owning sprint. |

TableRef schema/table and ColumnRef table/column already distinguish
qualification from an exact quoted name. S03 adds separate optional schema
components to FunctionCall and DataType.custom under the contract below;
dynamic lookup remains S11 rather than losing qualification.

Invocation starts with checked UTF-8 source-size admission before retained
source/token copying. The existing maximumRetainedBytes policy bounds source
and aggregate decoded semantic/token content independently; a single token
cannot exceed that cap. Source counting stops at one-past without allocating a
source byte array. Token/node/collection/nesting work is charged before growth;
a statement batch shares one cumulative ledger rather than resetting limits
per statement. Overflow and malformed UTF-8/escape/input are typed failures.
Keep the current invocation-local stateful parser model: sequential reuse must
reset source, offsets, parameters, errors and every ledger; concurrent use is
not a supported contract and no unchecked Sendable conformance is introduced.

Every token retains a zero-based UTF-8 start and half-open source range.
Syntax diagnostics use the offending token's start, or the opening delimiter
for an unfinished literal/comment; EOF is the source UTF-8 count. Multibyte
characters, CRLF, comments and escaped text cannot change that unit. Source
length is bounded, so conversion to public Int positions is checked. Existing
ParseError positions migrate to this convention and unexpected EOF retains its
position; callers preserve the typed underlying failure. Line/column display
is derived at the presentation boundary, not stored as a second authority.

S02 does not add a parsed-document source-map API or claim binding positions.
S03 owns the later diagnostic sidecar and its sequential parse -> binding
failure annotation boundary. Likewise L04 syntax does not claim temporal
interpretation (S06), general ARRAY execution (S07) or row semantics (S08).
New unsupported execution branches are marked and explicitly fail.

S02 tests cover every L01-L08 success, malformed and exact/one-past boundary,
parser reuse, token-start positions, multibyte input, huge single tokens and
nested comments, and cumulative batch limits. Previous tests expecting SQL
UInt64 literal selection or rejecting DECIMAL scale>precision migrate to the
accepted profile; primitive/SPARQL fixtures retain their previous semantics.

### S03 occurrence source sidecar

Parser consumes [Query's S03 declarations and occurrence arena](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s03-semantic-carriage-and-occurrence-addresses).
Bare decoded SQL strings become Literal.sqlUnknownString; typed strings retain
typed meaning. Function/custom-type qualification preserves exact schema and
terminal components; a quoted dotted identifier is one name. Qualified custom
input remains CAST input until binding resolves it. Bare NULL may normalize to
a valid resolved target; typed NULL preserves its CAST edge and incoming type.

An immutable annotated parse result owns statement, Kit arena/root and sparse
occurrence-to-source ranges. Existing parse/parseSelect consume the same grammar
path. Shared semantic field/index edges identify the returned tree; equality
and independently guessed traversal IDs cannot identify occurrences. Identifier
ranges may differ from enclosing expression ranges. Normalization explicitly
maps the resulting occurrence to original syntax. Invocation-local shadow
capture is permitted; discarded nodes cannot leave stale identities. Decisive
nested CTE/subquery/CASE/function/CAST failures must retain correct ranges even
when identical values recur in other scopes.

Ranges are zero-based UTF-8 half-open offsets in the original complete input,
including batch statements; EOF is byte count. Existing structural limits
pre-admit source, token captures, map entries, arena edges/indexes, depth and
logical retained bytes before growth. This is representation accounting,
not an allocator-capacity claim. Shared parent/edge storage avoids per-node
ancestor-path copies. Results remain independent of mutable parser state after
reuse. Source text, quoted flags and ranges never enter QueryIR. S03-C01/C04/
C06/C10–C12 prove semantic distinction, source correspondence, reuse and
exact/one-past bounds. Database owns the local failure bridge; remote diagnostics
remain S16. Database's prepared execution remaps rewritten failure addresses
through explicit origin IDs before consulting the original sparse source map,
including producer-to-many-literals fanout. QueryAST does not copy another map
for that operation. Missing original ranges remain optional; unrelated
occurrences are never matched by value equality.

### S03 output-position syntax carriage

Query-level ORDER BY and GROUP BY preserve a bare signed or parenthesized
integer as the existing `Literal.int` output-position representation. Ordinary
projection and nested arithmetic integers retain SQL INTEGER/BIGINT declarations.
Explicit integer CAST, postfix cast and typed-prefix syntax retain a CAST edge
inside an output-reference expression, so normalization cannot turn a constant
into a position. Nested SELECTs reset that syntax context, and aggregate-local
ORDER BY keeps ordinary expression semantics. Engine resolves positions against
the expanded projection and owns invalid-position and aggregate-GROUP rejection.

```text
query ORDER/GROUP bare integer -> Literal.int -> output-position resolution
query ORDER/GROUP explicit cast -> Expression.cast -> ordinary expression binding
ordinary integer expression -> typed queryValue -> SQL width-preserving binding
```

The parser replaces only the completed position expression's construction node;
its source occurrence covers the original signed/parenthesized expression. No
source-text lookup or value-equality lookup enters execution. The serializer
emits positions as bare integers only in query-level ORDER/GROUP context;
explicit casts remain casts across repeated serialization and parsing.
[SQL binding syntax](../../../Tests/QueryASTTests/SQLBindingSyntaxTests.swift)
and [source annotations](../../../Tests/QueryASTTests/SQLSourceAnnotationTests.swift)
prove representation, round-trip distinction and occurrence-arena shape.
Database's public SQL tests own empty/nonempty execution and rejection evidence.

## Runtime Flows

One entry resets source, position, parameter-style state, lexical failure and
structural ledger. Lexing and parsing propagate their precise error;
structural/lexical failure precedence remains deterministic. The entry
consumes the whole statement or bounded block before returning its value.

## State, Ownership, and Lifecycle

Invocation-local state owns source and indexes. Reuse is sequential only;
no invocation inherits parameter numbering, failure or ledger from another.
Returned semantic values own their data and retain no parser pointer.

## Failure, Concurrency, and Constraints

All malformed, unsupported and budget-exhausted input fails explicitly.
Typed source diagnostics use the zero-based UTF-8 token-start convention
defined above. Parser instances must not concurrently mutate
state. Native/WASM/Embedded share grammar and synchronization assumptions.

## Verification and Change Impact

Existing [SQL strictness](../../../Tests/QueryASTTests/SQLParserStrictnessTests.swift),
[parameters](../../../Tests/QueryASTTests/SQLParameterParserTests.swift),
[mutations](../../../Tests/QueryASTTests/SQLParserMutationTests.swift) and
[structural limits](../../../Tests/QueryASTTests/SQLParserStructuralLimitTests.swift)
are current behavioral test definitions. Later sprints execute all profile
syntax IDs, malformed counterparts, boundary nesting, parser reuse and
canonical-builder round trips. Full text-to-backend execution belongs to
Database and QueryExecution; tests of syntax alone cannot close that gap.

### S04 conditional and quantified grammar

Consume [S04 semantic forms](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s04-semantic-additions-and-scope).
Parse simple CASE operand once before WHEN; never duplicate it into equality
conditions. Parse unquoted/unqualified GREATEST/LEAST into dedicated forms;
quoted or qualified names remain exact generic calls. Admit IS [NOT]
TRUE/FALSE/UNKNOWN, IS [NOT] DISTINCT FROM and scalar comparison ANY/SOME/ALL
with existing array-valued expressions/parameters or a single-column query.
SOME normalizes to ANY. ARRAY constructors remain S07 runtime work, ROW
comparison S08. Source capture and semantic occurrence traversal use one
operand edge and the exact child edges fixed by Query; normalized aliases
preserve provenance. Binding checks type/cardinality, not the parser.
S04-C07/C08/C11 prove round trips, repeated-expression source positions,
strict malformed input and selected runtime behavior through Database.

### S05 string and pattern syntax

SQL pattern operators lower to the single Kit pattern predicate. Omitted
LIKE/ILIKE/SIMILAR escape becomes an explicit backslash expression; an explicit
dynamic ESCAPE expression retains its own source occurrence. An empty constant
ESCAPE canonicalizes to the disabled nil form. ERE operators never add
an escape. NOT forms use the existing negation expression. Concatenation lowers
to the strict Kit operator, separate from CONCAT calls.

Unquoted, unqualified SUBSTRING, POSITION and TRIM special grammar lowers to
ordinary canonical function calls: substring(source,start[,length]) or its
text-pattern overloads; strpos(source,needle); btrim/ltrim/rtrim(source[,chars]).
FOR-only substring inserts start 1. Quoted/schema-qualified calls retain their
exact generic identity. Source capture reorders child edges into semantic
argument order while retaining the original token ranges; synthetic defaults
are marked at the source clause that introduces them. Scalar array and later
row-returning functions retain generic call syntax without claiming execution.

General SELECT/FROM VALUES expression grammar remains S08-owned. Existing
canonical IR VALUES cells are Literal values; S05 CHAR/VARCHAR CAST padding and
truncation stay runtime policy rather than a duplicated constant-folding
algorithm in Parser or a new lower-layer constructor.


### S06 temporal syntax and provenance

Consume the Kit temporal declaration contract in
[Query](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md) and its
[SQL profile](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md).
Parser owns syntax and source capture, not date/time interpretation, clock
sampling, zone resolution, interval adjustment or precision rounding.

| SQL grammar | Canonical semantic result |
|---|---|
| CURRENT_DATE | currentValue(kind: .date, precision: nil) |
| CURRENT_TIME[(p)], CURRENT_TIMESTAMP[(p)] | currentValue with .time or .timestamp and optional constant precision |
| LOCALTIME[(p)], LOCALTIMESTAMP[(p)] | currentValue with .localTime or .localTimestamp and optional constant precision |
| EXTRACT(field FROM source) | ordinary extract(typed SQL text field, source) |
| source AT TIME ZONE zone | ordinary timezone(zone, source) |
| DATE/TIME/TIMESTAMP typed input | existing lexical CAST with exact target descriptor |
| INTERVAL[(p)] string, INTERVAL string fields[(p)] | existing lexical CAST with interval fields/precision metadata |

CURRENT_DATE forbids parentheses/precision; the other current-value forms accept
only a literal integer precision in the intrinsic Kit range. These expressions
have no expression children. Quoted ordinary calls retain FunctionCall identity
and are never converted into currentValue. EXTRACT preserves its actual numeric
catalog identity rather than lowering to double-returning date_part.

AT TIME ZONE is left-associative, above multiplicative arithmetic and below unary
signs/postfix casts, following REL_18_6 gram.y precedence. Its two argument
occurrences are reordered into semantic zone/source order while retaining both
original token ranges. No exponentiation or unrelated operator grammar is added.
The interval grammar follows REL_18_6 SimpleTypename and AexprConst: full-range
precision appears immediately after INTERVAL; restricted-field precision follows
its terminal SECOND. Duplicate precision, illegal field ranges and modifiers
outside intrinsic admission are positioned ParseError failures. Field qualifiers
after a literal extend the type annotation through the qualifier; the literal
keeps its separate original child range. Modified non-NULL interval constants
remain CAST expressions so runtime adjustment is not duplicated by constant
normalization. Bare typed NULL may normalize only after target admission.

MAKE_INTERVAL named syntax is admitted for the exact lowercase builtin name,
unqualified or pg_catalog-qualified, including equivalent quoted lowercase
identifiers under the existing exact resolver. Its seven canonical slots are
years, months, weeks, days, hours, mins, secs. Supplied expressions retain their
original ranges and are reordered into those slots; omitted slots become typed
SQL integer zero, except secs which becomes typed SQL double zero. Synthetic
default ranges identify the function token that introduces them. Duplicate or
unknown names, positional arguments after named arguments and excessive slots
fail before construction. The => and := spellings normalize identically. Ordinary
positional calls remain ordinary calls; binding owns their trailing defaults.
Unrelated named function syntax is explicitly unsupported, without a new generic
named-argument semantic algebra.

S06-SYN01 covers current-value kind/precision identity and quoted-name separation;
S06-SYN02 covers EXTRACT/DATE_PART type-preserving lowering;
S06-SYN03 covers timezone precedence, chaining and reordered source positions;
S06-SYN04 covers all interval qualifiers, precision, malformed syntax and NULL;
S06-SYN05 covers MAKE_INTERVAL named/default argument order and source positions;
S06-SYN06 covers lexical temporal casts and canonical repeated round trips.
These are completion obligations, not runtime evidence.

### S07 collection grammar and occurrence boundaries

Consume [Query's collection carriage](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s07-collection-expression-carriage),
[Wire's tags 53/54](../../../../database-kit/Sources/DatabaseWire/DESIGN.md#s07-collection-expression-carriage)
and [SQL S07-C01...C12](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s07-frozen-array-and-json-contract).
These links own value and execution semantics; this section owns text grammar,
normalization and provenance. No QueryAST dependency on DatabaseEngine is added.

```text
primary -> postfix casts / contiguous subscript groups -> unary / AT TIME ZONE
    -> multiplicative -> additive -> generic operators (left fold)
    -> comparisons / predicates -> NOT -> AND -> OR
```

The diagram retains the detailed S06 ordering of unary signs, casts and AT TIME
ZONE. Subscripts and postfix casts are consumed in source order before outer
unary signs. The generic level extends the current concatenate parser, below
arithmetic and above comparisons. All operators at that level associate left,
including mixed collection operators and concatenation. Existing symbolic ERE
operators use that same generic level; LIKE/ILIKE/SIMILAR retain their predicate
grammar and ESCAPE handling. JSONB `-` retains ordinary additive precedence,
because it reuses subtract; type binding selects its overload.

| Token | Expression carriage |
|---|---|
| @>, <@, && | collectionOperation contains, containedBy, overlaps |
| ->, ->>, #>, #>> | collectionOperation jsonField, jsonFieldText, jsonPath, jsonPathText |
| ?, ?\u007c, ?&, #- | collectionOperation jsonHasKey, jsonHasAnyKey, jsonHasAllKeys, jsonDeletePath |
| \u007c\u007c | existing concatenate |
| - | existing subtract at additive precedence |

Lexing selects the longest admitted token before a shorter prefix: ->> before
-> or -, #>> before #>, and ?\u007c/?& before ?. Preserve existing !~*, comparison,
cast ::, named-call =>/:= and SQL/PGQ arrow <- tokens. The token -> remains a
complete anonymous graph arrow in graph grammar; expression grammar gives it
its collection meaning. Prefix ? remains an anonymous positional parameter;
infix ? is the collection operator. Existing anonymous/$n mixing rejection and
numbering remain unchanged. No operator spelling is inferred from whitespace
or operand types, and missing operands fail at their actual token positions.

One or more directly consecutive [subscript] suffixes construct one arrayAccess
with one through six entries. Each entry is an index expression or a slice
with independently optional lower and upper expressions. A seventh entry fails
structural admission. Parentheses terminate a group: an access on a
parenthesized access constructs a nested base and never merges their entries.
Postfix casts also separate groups. The parser must retain this boundary during
construction even though parentheses do not themselves create an IR node.

Colon lexing stays uniform. At the immediate subscript level a colon is the
slice separator and takes precedence over product named-parameter syntax:
`a[:p]` has omitted lower bound and upper column p. A named operand must be
explicitly grouped, as in `a[(:p)]` or `a[(:lo):(:hi)]`; ordinary prefix named
parameters outside that immediate level remain supported. Grouped expressions
use ordinary expression grammar. Preserve exact existing named identity,
including keyword/quoted names, and UTF-8 offsets for old unambiguous syntax.
Neither omitted bounds nor explicit NULL bounds normalize into the other.

Source capture uses the exact Query operand/argument(2*d)/argument(2*d+1) and
left/right edges. Omitted bounds create no capture. A nested access retains its
base occurrence rather than flattening or matching by semantic equality.
Operator, group and expression ranges use original UTF-8 token boundaries;
normalizing a constant uses the existing explicit replacement capture path.

ARRAY[...] remains an ordered scalar constructor, including nested bracket
constructors; common-type selection and rectangularity belong to binding and
execution. ARRAY(SELECT...) requires S08 relation/array-subquery carriage and
binding and fails explicitly until that gate; a scalar constructor does not
claim that surface. ROW and public composite conversion remain required S08;
general SRF/FROM/LATERAL composition remains required S10 under C06/C11.

#### Explicit variadic argument carriage

The inspected FunctionCall originally represented only schema, name, ordered
arguments and distinct. That form cannot distinguish an array argument from
PostgreSQL explicit VARIADIC array expansion, including a NULL final array.
The [S07 Query contract](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s07-explicit-variadic-function-carriage)
adds finalArrayIsVariadic: Bool, default false. Generic
call parsing admits VARIADIC only immediately before the final argument, which
must be present; no later argument or repeated marker is accepted. Preserve
schema/name/DISTINCT and every argument expression. The marker is metadata,
not a synthetic expression, name rewrite or eager array expansion. It retains
the final child's ordinary argument index and captures that child's actual
range; the call range includes VARIADIC. Existing false-mode calls, including
zero-argument builders, remain unchanged. Structural validation rejects true
mode with no final argument; binding validates admitted catalog identity,
argument shape/type and applicable DISTINCT legality.

The [Wire production](../../../../database-kit/Sources/DatabaseWire/DESIGN.md#s07-explicit-variadic-function-layout)
owns tag55 for true mode and unchanged tags28/43 for false. Query identity and
all argument-preserving rebuilds must preserve the flag. This carriage is
required for the registered S07 JSON builder and
path function forms; it does not open unrestricted catalog resolution. Unknown
or unsupported function binding retains its explicit typed failure.
SPARQL adaptation rejects unsupported SQL-only collection/explicit-variadic
forms through its existing typed language boundary.

Builder's [S07 constant capability boundary](../Builder/DESIGN.md#s07-collection-emission-and-intrinsic-constant-capability)
selects intrinsic normalization or lexical CAST before conversion. Parser uses
the same adapter and preserves runtime-owned CAST edges; malformed supported
intrinsic input remains a positioned failure, never a deferred success.

| Evidence ID | Required behavior |
|---|---|
| S07-SYN01 | Every operator, longest-prefix collision, arithmetic/generic/comparison mixing and left association; existing graph arrows, ERE and anonymous parameters retain their grammar. |
| S07-SYN02 | Index/slice/mixed/omitted/NULL bounds, nested groups, cast-separated groups and six/seven dimensions; repeated serialization preserves distinct groups. |
| S07-SYN03 | Named subscript grouping and slice-colon priority; old parameter identities, parser reuse and multibyte source ranges; every present child has the canonical occurrence edge exactly once. |
| S07-SYN04 | Shared array-input grammar, unchanged intrinsic constants, runtime-owned CAST distinction and typed malformed/limit failures; exact/one-past cumulative admission. |
| S07-SYN06 | Explicit VARIADIC versus ordinary final array, zero ordinary arguments, NULL/empty variadic arrays and fixed-plus-path arguments; final-only grammar, flag-preserving repeated round trips/substitution and exact child source ranges. |

These are planned behavioral obligations in QueryASTTests and public scalar SQL
tests, not evidence of execution from successful parsing. Kit/Wire changes
require revalidation of these consumers; S08/S10 own their explicit public gates.

### S08 relational grammar and construction provenance

**PLANNED; not implementation or verification evidence.** Consume
[Query's frozen declarations](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s08-frozen-relational-query-carriage),
[SQL's composite value contract](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s08-frozen-composite-values-and-ownership)
and [Wire's canonical carriage](../../../../database-kit/Sources/DatabaseWire/DESIGN.md#s08-relational-query-carriage).
These are the sole semantic carriers; Parser owns grammar, normalization and
source provenance. This section replaces the S02/S07 VALUES, set-source and
ARRAY(subquery) syntax gates only after the cases below pass. ROW binding and
JSON conversion additionally require QueryExecution's behavioral evidence.

```text
complete query grammar -> canonical Kit query/source/expression values
    -> construction provenance -> named occurrence edges + original UTF-8 ranges
    -> structural admission -> one returned semantic tree and immutable sidecar
```

#### Grammar and normalization

All SELECT, statement, batch and annotated entries share one query-expression
grammar, including nested CTEs, scalar/IN/EXISTS/quantified subqueries, derived
tables and ARRAY(subquery). Parse complete query operands, not sources stripped
of projection or modifiers. INTERSECT binds above UNION/EXCEPT; UNION/EXCEPT are
left associative. Parentheses delimit operand-local CTEs, ORDER and pagination;
unparenthesized trailing clauses belong to the outer query. Each binary
setOperation has its own SetOperator/SetQuantifier and the exact direct SELECT
envelope required by Query. No syntax path repairs an invalid envelope.

| Grammar responsibility | Canonical construction and boundary |
|---|---|
| Projection | Build ordered ProjectionItem.expression/wildcard items, including mixed bare/qualified stars and explicit/implicit output aliases. Omitted quantifier and SELECT ALL normalize to SelectQuantifier.all; DISTINCT and nonempty DISTINCT ON keys remain distinct. Preserve duplicate output names and exact quoted identity. |
| Source alias | Construct DataSource.aliased around table, VALUES, derived query or joined group, with the ordered optional column list. A derived alias is optional. FROM tables have no TableRef.alias; direct mutation targets retain their separate existing contract. Never fabricate an alias. |
| FROM and joins | Comma creates a cross-join tree with lower precedence than explicit JOIN. Parenthesized groups retain grouping; an outer alias wraps the complete group. Preserve ordinary/outer/natural/USING joins and INNER/CROSS/LEFT LATERAL; condition legality remains strict. Column merge/order, alias visibility, NULL extension and correlation are binding responsibilities. |
| VALUES | Parse nonempty SQL rows of general expressions, not literals only, under the shared query/source grammar. Preserve row-major cells and parameters. Reject empty SQL rows. Shared structural validation rejects unequal raw widths only when no direct cell is fieldExpansion; binding checks final widths after parent-context expansion. Internal values([]) and values([[]]) retain Query's distinct empty/singleton meanings. |
| Composite expressions | Explicit ROW and implicit multi-item parenthesized rows construct the existing row expression. Parenthesized composite field access constructs field; composite star constructs fieldExpansion. A simple relation star operand is wholeRow(name), preserving relation identity instead of ordinary column-name priority. |
| Query tail | ORDER accepts expressions, output names/ordinals, multiple keys, direction, NULL order and collate expressions. COLLATE admits the exact quoted C/POSIX identities from SQLCollation; other names fail explicitly. Existing bare output ordinal versus explicit CAST constant distinction survives nested queries. |
| Pagination | Normalize legal LIMIT/OFFSET/FETCH FIRST/NEXT order and optional ROW/ROWS into QueryPagination. LIMIT ALL remains all; SQL counts retain expression syntax, parameters and CAST edges. Omitted FETCH count constructs fixed(1); ONLY/WITH TIES set the canonical flag. Do not pre-coerce NULL, negative or overflowing expressions into unsigned defaults. |
| Nonrecursive CTE | Preserve name, optional partial column list, complete query and materialization hint. Grammar acceptance does not evaluate demand, volatility, visibility, arity or materialization. |

FROM (VALUES ...) constructs a direct values source; a following set operator
or query tail retains a complete subquery envelope instead. FROM (SELECT ...)
retains its explicit subquery boundary. Source captures follow the constructed
source directly, including each VALUES row and cell, without inventing a query
or projection occurrence for direct VALUES syntax.

fieldExpansion is retained honestly at expression positions. A direct SELECT
or RETURNING target, top-level VALUES cell or ROW item expands according to
Query's parent-context rule; nested expressions and function arguments consume
one composite operand. In particular, a star argument to ROW_TO_JSON/TO_JSON
is not normalized to multiple arguments or a synthesized object. Ordinary
unqualified names retain column-first binding; Parser does not replace them
with wholeRow by guessing the catalog. Expansion evaluation multiplicity and
present all-NULL versus absent joined-row identity remain runtime contracts.

FETCH WITH TIES preserves the distinction between a bare unknown NULL and an
explicit bigint CAST. The accepted 18.6 observations distinguish their failure
versus unlimited behavior; Parser must not normalize away the decisive type
edge. Missing ordering, illegal clause combinations, negative counts, coercion
and overflow receive the typed error of their syntax/binding/runtime owner.
DISTINCT ON's ORDER-prefix legality similarly belongs to binding, not a
textual equality comparison in Parser.

ARRAY(subquery) constructs arraySubquery over one complete nested query. The
user's accepted PostgreSQL18.6 policy preserves inner array dimensions/lower
bounds and adds outer lower bound one. Parser performs no array accumulation,
rank repair or lower-bound rewriting. One-column admission, NULL/empty subarray
failures and typed empty results require QueryExecution evidence.

#### Capture, limits and migration

Follow Query's canonical child roles and order exactly: CTEs, projection,
source, filter, groupBy, having, DISTINCT ON, orderBy, offset, limit. Wildcards
are projection leaves. VALUES captures row nodes with valuesRow(row), then
valuesCell(column); aliases capture source(nil), sets setLeft/setRight query
children, pagination expression alternatives offset/limit, and ARRAY queries
subquery(nil). COLLATE, field and fieldExpansion use operand(0); wholeRow is a
leaf. Fixed/ALL and absent children create no expression occurrence.

Construction order can differ from that canonical order. Reorder explicit
construction references before finishing a node; never pair equal values or
search source text. Argument-star normalization retains the operand's actual
identifier range and the star wrapper's original syntax range. Parentheses,
normalization and implicit AS neither invent text spans nor leave discarded
capture nodes. Distinct repeated values across set operands, VALUES rows,
CTEs, pagination and function arguments retain separate addresses. Ranges
remain absolute half-open UTF-8 offsets through multibyte text, CRLF/comments
and batch input; earlier snapshots survive parser reuse and failure.

Consecutive ROW/ARRAY constructor children use an invocation-local frame stack.
Each frame and child is admitted before growth; open expression/constructor
nesting is balanced on success and failure. A completed child resumes the same
postfix and precedence path, so fields, casts, predicates and binary operators
cannot change scope. S08-SYN05 must parse depth-40 constructor chains with source
capture without exhausting the test worker stack, and still reject trailing
commas and one-past nesting budgets.

Charge source bytes, captures, row nodes/cells, child edges, alias/name lists,
DISTINCT keys and output arena/map entries before growth under the existing
cumulative ledger. New children enter structural and parameter traversal once;
no nested query gains a fresh budget. Every query rebuild preserves all Kit
envelope fields and optional presence, including quantifier, pagination,
accessPath, CTE hints, reduced and dataset. SPARQL retains its separate algebra
and fixed unsigned pagination; SQL-only forms fail through its typed language
boundary. Public table functions/SRF composition and recursive CTE execution
remain S10; schema-qualified table execution remains S11. Parsing qualified
names grants neither capability.

#### Falsifiable syntax and source evidence

QueryASTTests owns these planned cases; Database/QueryExecution tests own
executed types, values, scopes, failures and resource behavior. Link the
[accepted oracle summary](../../../../.codex-tools/sql-s08-evidence/accepted-summary.json),
[judgments](../../../../.codex-tools/sql-s08-evidence/judgments.json) and
[strict-NULL correction](../../../../.codex-tools/sql-s08-evidence/null-correction-observations.json)
as reference observations, never product conformance evidence. The original
32-case capture setup failure remains recorded; four corrected NULL cases
supersede only their identified ambiguous cells.

| Case | Required distinguishing evidence |
|---|---|
| S08-SYN01 / QC01–02 | Mixed-star order, duplicate/zero-row names, implicit/explicit aliases, partial/excess source aliases, omitted derived alias, group scope and comma/JOIN precedence. Replace the old mixed-star rejection with positive grammar and malformed counterparts. |
| S08-SYN02 / QC03 | Explicit/implicit/nested ROW, field selection, list expansion versus single-composite arguments and ARRAY(subquery) preserve structure and source addresses. Runtime pairs distinguish constructor/present all-NULL/absent row JSON behavior and the accepted inner-bound policy. |
| S08-SYN03 / QC04 | Every set operator/quantifier, precedence and parenthesized operand-local versus outer clauses stabilize across repeated canonical round trips; DISTINCT ON and quoted C/POSIX remain separate metadata. |
| S08-SYN04 / QC05–06 | Expression/parameter/ALL/NULL pagination, omitted FETCH count, WITH TIES and explicit NULL CAST retain meaning and provenance; CTE columns/hints survive all entry points and enclosing rebuilds. |
| S08-SYN05 / QC07 | Repeated equal children in mixed wildcard/VALUES/set/CTE/argument-star/pagination trees have unique canonical addresses and exact UTF-8 ranges; exact/one-past cumulative budgets and parser reuse fail or succeed at the selected boundary. |
| S08-SYN06 / QC08 | Builder emission and parser round trips preserve representable composite metadata and all query fields; unrepresentable literal/type metadata fails typed. Replace VALUES/set/ARRAY syntax gates only with their positive and failure evidence, preserving S10/S11 boundaries. |


### S09 grouping, aggregate and window grammar

**PLANNED; not implementation or verification evidence.** Consume the single
Kit-owned S09 grouping, SQL aggregate, analytic and window declaration model.
No parser-only aggregate tags or window evaluator are introduced. The accepted
function inventory remains [SQL F11–F13](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md).

```text
SQL tokens -> grouping / aggregate / OVER / WINDOW grammar
    -> canonical Kit declarations + construction-order source occurrences
        -> structural validation -> Engine binding and execution
```

GROUP BY is an ordered list of GroupingElement, distinct from an absent clause.
A scalar grouping expression remains one expression. An implicit parenthesized
comma row in ordinary GROUP BY normalizes to one grouping set of its component
expressions; explicit ROW retains one composite key. Empty parentheses denote
the empty grouping set. ROLLUP and CUBE retain each parenthesized compound unit
as an ordered expression list. GROUPING SETS retains ordered element lists,
including empty sets and nested ROLLUP/CUBE/GROUPING SETS. Comma composition is
retained rather than expanded by the parser, so execution owns cross products,
duplicate grouping sets and GROUPING bit identity. Integer output references
retain the existing GROUP BY reference-versus-explicit-CAST distinction.
Every ordinary, ROLLUP/CUBE unit and GROUPING SETS expression member first
consumes the complete scalar expression, including operators outside initial
parentheses. Only an implicit whole-expression tuple becomes compound grouping
keys; explicit ROW remains composite. This follows the
[PostgreSQL 18 SELECT grammar](https://www.postgresql.org/docs/18/sql-select.html).

Resolve only the closed SQL aggregate names and F13 analytic names to their
canonical nodes. Unquoted names fold through the lexer; exact quoted lower-case
catalog names and exact pg_catalog qualification select those same functions.
Analytic calls admit an absent or exact pg_catalog schema and canonicalize
that built-in identity; their closed declaration has no independent schema.
Other analytic schemas fail typed before execution. Other quoted terminal
spelling remains ordinary FunctionCall identity.
Recognized terminal names retain any exact schema in SQLAggregateCall; binding
alone admits an absent or pg_catalog schema and rejects unavailable catalogs. Every SQL aggregate producer uses AggregateFunction.sql
with SQLAggregateCall argument expressions, DISTINCT, internal ORDER BY, FILTER,
WITHIN GROUP ordering and schema. COUNT(*) is the explicit zero-argument count
form; other stars and DISTINCT star fail. STRING_AGG retains its delimiter as
an expression, including parameters and columns. GROUP_CONCAT remains the
separate existing product extension. Aggregate argument arity/type/modifier
legality remains binding-owned; grammar never discards supplied modifiers.

FILTER (WHERE expression) follows the aggregate and optional WITHIN GROUP
(ORDER BY ...); OVER follows FILTER. Internal ORDER BY is preserved for every
SQL aggregate. GROUPING has a dedicated ordered-expression node. F13 analytic
calls and ordinary aggregate OVER use WindowCall, preserving analytic argument
order, optional defaults and aggregate filter/order children. Ordered-set
window legality is a binding failure rather than a parser approximation.

OVER name is a direct named reference. OVER (...) is a specification, including
OVER (name), which copies/inherits that definition. WINDOW name AS (...) is a
definition and never canonicalizes those distinct uses together. Specifications
retain optional inherited name, PARTITION BY, ORDER BY and explicit frame.
Frames retain ROWS/RANGE/GROUPS; single-bound syntax normalizes its end to
CURRENT ROW, while BETWEEN retains both bounds. Bounds are
UNBOUNDED/CURRENT ROW/expression PRECEDING/FOLLOWING and EXCLUDE CURRENT ROW,
GROUP, TIES or NO OTHERS. Offsets remain expression nodes. An omitted frame remains absent metadata; explicit EXCLUDE NO OTHERS
normalizes to the same noOthers value as omission. Parser does not compute
frame bounds or peers.

WINDOW definitions occur after HAVING and before query-level ORDER BY.
Construction capture uses Kit's occurrence fields and ordered grouping/window
wrapper nodes, retaining each repeated argument, grouping unit, filter, sort,
partition and frame-offset source separately. Named definitions have their
original UTF-8 ranges. Calls with OVER capture one window expression containing
the exact aggregate/analytic children, without orphan aggregate captures.
Every wrapper, child edge, retained name, list and recursion level is admitted
under the same request ledger before growth. No nested frame/query resets it.
SPARQL input continues producing its own aggregates and ordinary expression
grouping; it does not acquire SQL grouping/window syntax.

| Case | Falsifiable local evidence |
|---|---|
| S09-SYN01 | Empty, compound and nested grouping elements, duplicates and ordinary output references stabilize under repeated parse/serialize/parse. |
| S09-SYN02 | Every F11/F12 name retains full arguments/schema/DISTINCT/order/FILTER/WITHIN GROUP; malformed stars, delimiters and trailing clauses fail. |
| S09-SYN03 | Every F13 name and aggregate OVER retain arguments; direct named reference differs from copied specification and WINDOW definition. |
| S09-SYN04 | Every frame mode/bound/exclusion, offset expression and implicit-default absence stabilizes; malformed syntax fails at its token. |
| S09-SYN05 | Repeated equal grouping/window/filter/order/offset children have unique canonical source addresses and exact UTF-8 ranges; parser reuse and exact/one-past limits preserve failure contracts. |
| S09-SYN06 | Fluent construction and SPARQL preservation/rejection match Builder's contract; SQL serialization budgets include all new children. |

These syntax/source tests do not establish result types, aggregate values,
frame execution, grouping legality or PostgreSQL compatibility. Those witnesses
belong to Engine and public Database tests under the same frozen S09 contract.


### S10 recursive and function-source grammar

[Query carriage](../../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s10-frozen-recursive-and-function-source-carriage) fixes all fields; [SQL C01-C12](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s10-frozen-recursion-and-table-functions) fixes meaning. Preserve WITH-wide RECURSIVE rather than discarding it. Each CTE retains optional SEARCH DEPTH/BREADTH FIRST BY keys SET sequence name and CYCLE keys SET mark name [TO constant DEFAULT constant] USING path name. CYCLE values are paired Literal constants, never row expressions or parameter slots; signed constants and incomplete pairs fail grammar. Keys and generated names retain exact folded/quoted identifier identity. Recursive dependency legality remains binding-owned.

FROM function calls retain existing exact FunctionCall names/schema/arguments/VARIADIC plus WITH ORDINALITY; existing aliased source owns all aliases. Explicit LATERAL before a function, including the leading source, normalizes to its implicit function-source semantics. Explicit lateral subqueries retain existing join kinds. Reject ROWS FROM, record column-definition lists and malformed/trailing clauses rather than accepting a prefix. SELECT-list SRF expansion remains outside this surface.

Construction capture visits every function argument through Kit's source occurrence role without duplicate function-expression captures. Existing CTE query paths remain stable; SEARCH/CYCLE constant and identifier occurrences use the Kit-defined distinct roles where present. One request ledger bounds names, lists, constants, arguments and wrapper nodes before growth. Positive parse/serialize/source tests and malformed/limit tests are required before removing the incomplete FROM gate; public execution proves the corresponding runtime path separately.

CYCLE constant parsing consumes [Builder's intrinsic constant profile](../Builder/DESIGN.md#s10-recursive-and-function-source-emission); it does not invoke Engine temporal evaluation or accept an expression in place of a Literal.


### S13 mutation and session expression syntax

The parser preserves DEFAULT as the dedicated mutation-policy expression and
INSERT OVERRIDING SYSTEM VALUE / OVERRIDING USER VALUE as its semantic identity
policy. Ordinary scalar binding rejects DEFAULT outside an admitted mutation
position. Bare CURRENT_USER, SESSION_USER and CURRENT_SCHEMA adapt to their
zero-argument built-in function calls while CURRENT_SCHEMA() remains valid.
General sequence/constraint DDL and textual transaction controls retain the S15
and S16 ownership boundary; S13 semantic block steps do not claim that grammar.
Round-trip tests verify the new syntax, strict malformed/trailing input rejection
and the existing structural/source-location budgets.
