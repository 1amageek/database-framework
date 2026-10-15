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
qualification from an exact quoted name. FunctionCall.name and DataType.custom
do not; forms whose qualification cannot be preserved by that existing String
representation fail as unsupported until S03/S11 rather than losing the distinction.

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
