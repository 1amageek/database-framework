# Builder

## Purpose and Scope

Builder owns fluent SQL/SPARQL semantic construction and the canonical SQL
serializer. Parent: [QueryAST](../DESIGN.md). Children: none. The serializer
is a planned S02 consolidation; current SQL generation remains in
[SQLQueryBuilder](SQLQueryBuilder.swift) and semantic `toSQL` paths.

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

### S01 rich-literal generation boundary

Actual current SQL generation is SQLQueryBuilder.toSQL -> expression.toSQL ->
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
