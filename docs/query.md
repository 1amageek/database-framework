# Query And Fusion

Queries are split by responsibility so parsing, planning, index access, and
storage are independently replaceable.

## Query Flow

~~~text
KeyPath / SQL / SPARQL input
            |
            v
        QueryAST / QueryIR
            |
            +--> scalar and index planning
            +--> graph pattern execution
            +--> fusion access paths
            |
            v
        StorageKit transaction
~~~

## Query Families

| Family | Owner | Purpose |
|---|---|---|
| KeyPath queries | DatabaseEngine and index modules | type-safe application queries |
| SQL | QueryAST and scalar/index executors | relational-style predicates and projections |
| SPARQL | GraphIndex and QueryAST | graph patterns, filters, aggregates |
| Fusion | DatabaseEngine plus index modules | combine vector, text, rank, bitmap, and spatial results |

The planner selects an access path from registered index descriptors. An index
module owns its read bridge and execution logic; DatabaseEngine owns common
binding, pagination, and transaction contracts.

## SQL And SPARQL

SQL and SPARQL share transport and selected expression structures where their
semantics agree. Their evaluators remain separate where semantics differ:

- SQL uses closed-world and NULL behavior.
- SPARQL uses open-world and unbound-variable behavior.
- Graph pattern matching remains in GraphIndex.
- Scalar and relational access remains in scalar/index modules.

This separation prevents a SQL translation from silently changing SPARQL
semantics.

## Fusion

Fusion combines staged, context-free inputs using the public `FusionQuery`
contract. DatabaseKit owns the plan; DatabaseEngine owns candidate flow,
transaction lifetime, score composition, and output. Each index module owns
only its physical read algorithm. The complete admission, authorization,
session, failure, and performance contract is defined by
[`fusion-execution-design.md`](fusion-execution-design.md).

~~~swift
let query = FusionQuery<Document> {
    Filter(Document.fields.isPublished, equals: true)
    Search(Document.fields.content)
        .terms(["database"])
        .limit(20)
    Rank(Document.fields.popularity)
        .order(.descending)
}
.strategy(.weighted([0, 1]))

let response = try await context.execute(query)
~~~

The production physical reader currently implemented is full-text `Search`.
`Filter` and `Rank` execute through the canonical relational engine. Other
feature inputs already lower to QueryIR but fail preflight with a typed error
until their owning module supplies and verifies its physical reader. There is
no silent fallback to an unrestricted or approximate implementation.

Field authorization completes before index-selection errors or feature
availability are reported. `Search(field)` requires an exact single-field
full-text index because a multi-field full-text index stores combined postings
and cannot correctly represent a field-isolated search.

Execution receives one sealed, authorized Fusion execution bound to one read
session. It does not receive a raw query plus an ambient authorization value,
and feature readers receive only parent-issued index read leases.

## Error Behavior

Optimization strategy changes are allowed only when the planner can prove that
the alternative is valid. Runtime errors such as decoding failures, storage
errors, and unsupported operations propagate to the caller. They are not
converted into an empty result or an invisible fallback.
