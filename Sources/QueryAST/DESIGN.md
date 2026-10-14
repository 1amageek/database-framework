# QueryAST

## Purpose and Scope

QueryAST is the SQL/SPARQL text and fluent-builder adaptation module. It
consumes DatabaseKit semantic models and owns strict parsing and canonical
SQL serialization; it does not own their meaning or execution.

- Parent: [database-framework](../../DESIGN.md).
- Children: [Parser](Parser/DESIGN.md), [Builder](Builder/DESIGN.md).
- PostgreSQL basic SQL additions are planned S00 contracts. Their complete
  feature inventory is [DatabaseKit SQL](../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md).

## Responsibilities and Boundaries

The module translates text and builder input to semantic query contracts and
translates supported SQL contracts back to canonical text. It validates
syntax and structural limits before exposing an accepted statement.
DatabaseKit owns statement/type/constraint semantics. DatabaseEngine owns
name/type binding, evaluation, transactions and authorization. QueryAST has
no storage or host authority and does not interpret SQL schema as Directory.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Package](../../DESIGN.md) | parent | Module dependency direction | Composes syntax adaptation with execution. | Do not import backend or server behavior. |
| [Parser](Parser/DESIGN.md) | child | Strict SQL/SPARQL parsing | Owns lexical/grammar acceptance and source diagnostics. | Accepted syntax is not executable capability evidence. |
| [Builder](Builder/DESIGN.md) | child | Semantic construction and canonical SQL serialization | Owns one SQL emission path. | Preserve meaning or return typed unsupported error. |
| [DatabaseKit SQL](../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md) | depends on | SQL inventory and semantic model | Fixes the compatibility profile. | AST additions must enter structural validation and codecs together. |
| [Database](../Database/DESIGN.md) | used by | Public SQL text entry | Parses before execution admission. | All public parser entry points agree on shared grammar. |
| [QueryExecution](../DatabaseEngine/QueryExecution/DESIGN.md) | used by | Canonical semantic input | Names/types bind before evaluation. | QueryAST never substitutes parser heuristics for binding. |

## Architecture

```text
SQL text -> Parser -> DatabaseKit semantic statement/block
                              ^
fluent Builder ---------------|
                              |
                              v
                   canonical SQL serializer -> SQL text

semantic input -> DatabaseEngine (outside this module)
```

## Contracts and Invariants

- One accepted inventory governs text and builder capabilities. Planned
  features are not implemented solely by adding parser tokens or AST cases.
- Parsing consumes the full intended statement/block. Unknown syntax,
  unfinished clauses, trailing tokens and unsupported constructs are typed
  errors with stable source locations, not accepted prefixes.
- One canonical serializer preserves type casts, precedence, quoted names,
  CTE hints, set operations, lateral scope, windows and all supported clauses.
  It never emits placeholders or substitutes LIKE for regex.
- Structured values, text, parsing and serialization retain structural
  limits. Nesting and node work are charged before allocation/recursion.
- SPARQL and graph extensions remain explicit product syntax and retain their
  existing semantic owner. SQL expansion must preserve their current tests.

## Runtime Flows

Parser consumes request-local lexer state, validates syntax and the final
semantic structure, then transfers an immutable statement. Builder constructs
that same structure; canonical serialization validates support and emits
bounded text which is parsed back in round-trip evidence.

## State, Ownership, and Lifecycle

Parser token position, source text and structural ledger belong to one parse
invocation. Mutable parser instances are not shared concurrently. Builder
state is value-owned; emitted text and semantic values own their storage.
No pointer, storage transaction or authorization lease is held by this module.

## Failure, Concurrency, and Constraints

Lexical, syntax, unsupported-feature and structural-limit errors remain
explicit. SQLSTATE classification is supplied by the semantic/execution
contract, preserving the parser cause and position. Configuration supplies
validated operational limits; parsing does not hard-code guessed defaults.

## Verification and Change Impact

[QueryASTTests](../../Tests/QueryASTTests) owns syntax, builder and structural
failure tests. PostgreSQL differential acceptance cases must subsequently
execute through [Database](../Database/DESIGN.md); AST shape tests alone do
not establish compatibility. Changes to syntax representation revalidate
DatabaseKit validation/codecs, Builder serialization, Database entry and
QueryExecution binding. Implementation sprints update the owned tests and
exact executable count with intentional coverage changes.
