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

### S02 composition and completion boundary

S02 first updates [Kit semantic declarations](../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s02-frozen-syntax-to-value-boundary)
and [Wire](../../../database-kit/Sources/DatabaseWire/DESIGN.md#s02-type-and-constructor-wire-grammar),
then composes the [Parser](Parser/DESIGN.md#s02-strict-lexical-and-grammar-contract)
with [Builder's canonical constant adapter](Builder/DESIGN.md#s02-canonical-sql-and-constant-normalization).
The dependency is Parser -> Builder's pure constant adapter, never Builder ->
Parser production code; parser round trips are behavioral tests. Public builder
SQL generation has one implementation owner. Existing SQL/PGQ declarations,
SPARQL generation, typed values and standard/MultiBase guards retain their
semantic owners.

S02 proves full L01-L08 lexical/carriage/strictness behavior and executable
agreement for the already-supported public SQL subset. It does not label L04
ROW/ARRAY construction or temporal typmods fully executable from syntax-only
proofs. Unsupported runtime branches explicitly identify the S03/S06/S07/S08
owner. Source diagnostics in this sprint use UTF-8 token ranges; the binding
sidecar and binder diagnostic integration remain explicit S03 work.

### S03 composition

[Parser](Parser/DESIGN.md#s03-occurrence-source-sidecar) owns the immutable
semantic result plus sparse source sidecar; [Builder](Builder/DESIGN.md#s03-typed-text-and-qualified-serialization)
preserves unknown/typed text, exact qualification and typed NULL CAST edges.
Both consume [Kit's semantic occurrence/model contract](../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s03-semantic-carriage-and-occurrence-addresses).
Database composes source mapping with Engine failures; Engine gains no QueryAST
dependency. The same returned semantic tree/provenance is used after normalization.
S03-C10/C11 prove occurrence-safe local positions and bounded retained state;
S16 owns final remote diagnostic presentation.

## Runtime Flows

### S08 relational composition

The [Query carriage authority](../../../database-kit/Sources/DatabaseKit/Query/DESIGN.md#s08-frozen-relational-query-carriage)
supplies the single projection/source/set/pagination model. Its consuming
[Parser contract](Parser/DESIGN.md#s08-relational-grammar-and-construction-provenance)
and [Builder contract](Builder/DESIGN.md#s08-relational-emission-and-lossless-construction)
own syntax and emission respectively. The module composes those children;
it does not infer relation scope, composite presence, CTE demand or SQL
comparison from accepted syntax. Source occurrences survive every new child
edge and SPARQL remains subject to its own language validation. S08 acceptance
requires the children’s runtime witnesses and the public Database execution
path, rather than a parser-only round trip.

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

### S01 rich-value text boundary

S01 admits rich semantic literals and bound parameters without adding their
SQL lexical syntax. Builder's [throwing generation boundary](Builder/DESIGN.md#s01-rich-literal-generation-boundary)
rejects unsupported rich emission explicitly before output. Parser continues
accepting its current syntax and does not consume diagnostic
Literal.description. Full rich lexical forms and canonical serializer
consolidation belong to S02; S01 typed carriage is not evidence those text
forms are supported.

### S04 composition

[Parser](Parser/DESIGN.md#s04-conditional-and-quantified-grammar) and
[Builder](Builder/DESIGN.md#s04-conditional-and-quantified-serialization)
compose the [S04 Kit semantics](../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md#s04-semantic-additions-and-scope).
Syntax/source/serialization proof precedes execution integration; parsing
array-valued ANY/ALL does not claim S07 constructors or S08 ROW semantics.

### S05 string and pattern adaptation

[Parser](Parser/DESIGN.md#s05-string-and-pattern-syntax) and
[Builder](Builder/DESIGN.md#s05-exact-pattern-emission) consume the Kit-owned
pattern/concatenation declarations and canonical string-function calls. They
preserve exact generic names, explicit escape meaning and semantic child source
positions. Row-function execution remains S10-owned and fails explicitly at the
unsupported FROM entry rather than producing a scalar substitute.


### S06 temporal adaptation

[Parser](Parser/DESIGN.md#s06-temporal-syntax-and-provenance) owns temporal special
grammar and its normalized source occurrences;
[Builder](Builder/DESIGN.md#s06-temporal-emission) owns bounded exact emission.
They compose the Kit-owned current-value, temporal value and interval descriptor
contracts without interpreting clocks, zones, calendar arithmetic or typmods.
S06-SYN01...06 are the local syntax/identity obligations; Database public SQL and
QueryExecution own executable temporal acceptance.


### S09 grouping and window composition

**PLANNED; not implementation or verification evidence.**
[Parser](Parser/DESIGN.md#s09-grouping-aggregate-and-window-grammar) and
[Builder](Builder/DESIGN.md#s09-grouping-aggregate-and-window-emission) compose
Kit-owned grouping elements, SQLAggregateCall, WindowCall and named-window
metadata. Source occurrence capture retains all new children and exact names;
serialization preserves direct OVER references versus copied specifications.
Engine owns grouping expansion, aggregate legality/results, named inheritance,
partition peers, frame bounds and evaluation. SQL-only additions remain typed
SPARQL failures. S09-SYN01...06 plus public execution evidence close adaptation;
syntax acceptance alone does not close SQL Q11/Q14 or F11–F13.
