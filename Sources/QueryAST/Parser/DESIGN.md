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
Quoted calls remain generic even when their name matches a grammar form;
unquoted COALESCE/NULLIF use their dedicated expression cases. A qualified
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
