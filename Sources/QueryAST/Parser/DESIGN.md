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

### Current facts

[SQLParser](SQLParser.swift) provides `parseSelect` and `parse`. The first
accepts a WITH-leading SELECT; statement dispatch in `parse` recognizes
SELECT/INSERT/UPDATE/DELETE and property-graph DDL but does not dispatch WITH.
Both finish with statement-terminator consumption and structural validation.
These are source facts, not a successful runtime verification of the plan.

### S01 execution evidence boundary

S01 validates positional set alignment and contextual NULL through canonical
`SelectQuery`/`DataSource.unionAll` execution. The current `parseSelectQuery`
returns after SELECT modifiers and does not parse UNION; `advance` does not
recognize double-quoted identifiers. Their implementation and text round-trip
evidence remain S02. New S01 fixtures therefore use semantic ASTs for set
operations and rich fields with reserved names, while SQLite persistence uses
nonreserved camelCase fields to execute the currently supported SQL text path.
This boundary does not claim parser support from successful AST execution.

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
Typed source diagnostics retain character/byte-offset conventions defined by
the semantic error contract. Parser instances must not concurrently mutate
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
