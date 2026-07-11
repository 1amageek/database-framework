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

Fusion combines independent index queries using the public FusionQuery and
FusionBuilder contracts. Typical combinations include full-text search with
vector similarity or a bitmap filter with a ranked result.

~~~swift
let results = try await context.fuse(Document.self) {
    Parallel {
        Search(\.content).terms(["database"])
        Similar(\.embedding, dimensions: 1536)
            .nearest(to: queryVector, k: 10)
    }
}.execute()
~~~

Concrete query types are provided by index modules. DatabaseEngine does not
hard-code feature-specific index cases.

## Error Behavior

Optimization strategy changes are allowed only when the planner can prove that
the alternative is valid. Runtime errors such as decoding failures, storage
errors, and unsupported operations propagate to the caller. They are not
converted into an empty result or an invisible fallback.
