# QueryAST

SQL/SPARQL syntax adapter over the canonical query models owned by
`DatabaseKit`. It provides parsing, construction, and serialization without
owning execution plans or runtime semantics.

## Overview

QueryAST provides the following capabilities:

- **SQL/SPARQL Parser**: Build AST from query strings
- **Query Builder**: Programmatic query construction
- **Serializer**: Generate SQL/SPARQL strings from AST
- **Query Analysis**: Variable reference and aggregate function detection
- **SQL/PGQ Support**: Graph pattern matching (ISO/IEC 9075-16:2023)

## Architecture

```text
SQL text -> Parser/SQLParser -> DatabaseKit semantic values
                                      ^              |
Builder/SQLQueryBuilder ---------------+              v
                                  Builder/SQLSerializer
                                      -> bounded SQLTextWriter -> owned SQL text

SPARQL text -> Parser/SPARQLParser -> DatabaseKit SPARQL models
Builder/SPARQLQueryBuilder ----------> DatabaseKit SPARQL serialization
```

`QueryIRExport.swift` re-exports `DatabaseKit`. SQL/PGQ, SPARQL and shared
query models remain in that package; QueryAST's implementation directories
are `Parser/` and `Builder/`.

### Canonical SQL Serialization

`SQLSerializing` declares the pure serialization contract. Its immutable
implementation, `SQLSerializer(limits:)`, accepts `QueryStructuralLimits`
(default: `.default`). Each `serialize` invocation uses one bounded writer
for nested values and, for `[QueryStatement]`, the complete statement block.
Depth, node and collection budgets are checked while traversing; escaped
UTF-8 output is checked against `maximumRetainedBytes` before growth.

| Public entry | Result |
|---|---|
| `SQLParser.parseSelect(_:)` | `SelectQuery` |
| `SQLParser.parse(_:)` / `parseStatements(_:)` | `QueryStatement` / `[QueryStatement]` |
| `SQLQueryBuilder.buildAST()` | `SelectQuery` |
| `SQLQueryBuilder.toSQL()` | Delegates to `SQLSerializer().serialize(buildAST())` |
| `SQLSerializer.serialize(_:)` | SQL for statements, SELECT, expressions, literals, types, sources, aggregates and SQL/PGQ roots |

All serialization roots throw `SQLSerializationError` for unsupported
language, source, metadata, type or literal, invalid structures, and resource
limits. They never use diagnostic `description` text as executable SQL.

## Key Types

### Query Representation

These declarations are owned by `DatabaseKit` and re-exported for parser and
builder users.

| Type | Description |
|------|-------------|
| `SelectQuery` | AST representation of SELECT queries |
| `Expression` | Expressions (comparison, arithmetic, logical, aggregation, etc.) |
| `DataSource` | FROM clause data sources (tables, JOINs, subqueries) |
| `Projection` | SELECT clause projections (columns, expressions, *) |
| `Literal` | Literal values (strings, numbers, dates, etc.) |

### SQL/PGQ (Graph Queries)

| Type | Description |
|------|-------------|
| `GraphTableSource` | GRAPH_TABLE clause |
| `MatchPattern` | MATCH clause patterns |
| `PathPattern` | Path patterns (nodes, edges, quantifiers) |
| `NodePattern` | Node patterns |
| `EdgePattern` | Edge patterns |

### SPARQL

| Type | Description |
|------|-------------|
| `GraphPattern` | SPARQL graph patterns (BGP, OPTIONAL, UNION, etc.) |
| `PropertyPath` | Property paths (sequence, alternative, repetition) |
| `SPARQLTerm` | RDF terms (IRI, literal, variable, blank node) |
| `TriplePattern` | Triple patterns |

## Usage Examples

### Parsing SQL Queries

```swift
import QueryAST

let parser = SQLParser()
let query = try parser.parseSelect("""
    SELECT u.name, COUNT(o.id) as order_count
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.status = 'active'
    GROUP BY u.name
    ORDER BY order_count DESC
    LIMIT 10
""")

// Traverse the AST
print(query.projection)  // Projection
print(query.source)      // FROM clause
print(query.filter)      // WHERE clause
```

### Programmatic Query Construction

```swift
let query = SelectQuery(
    projection: .items([
        ProjectionItem(.column(ColumnRef(table: "u", column: "name"))),
        ProjectionItem(
            .aggregate(.count(.column(ColumnRef(table: "o", column: "id")), distinct: false)),
            alias: "order_count"
        )
    ]),
    source: .join(JoinClause(
        type: .left,
        left: .table(TableRef(table: "users", alias: "u")),
        right: .table(TableRef(table: "orders", alias: "o")),
        condition: .on(.equal(
            .column(ColumnRef(table: "u", column: "id")),
            .column(ColumnRef(table: "o", column: "user_id"))
        ))
    )),
    filter: .equal(
        .column(ColumnRef(table: "u", column: "status")),
        .literal(.string("active"))
    ),
    groupBy: [.column(ColumnRef(table: "u", column: "name"))],
    orderBy: [SortKey(.column(ColumnRef(column: "order_count")), direction: .descending)],
    limit: 10
)

// Convert to SQL
let serializer = SQLSerializer()
print(try serializer.serialize(query))

// Fluent construction uses the same serializer.
let builder = SQLQueryBuilder(from: "users").select("name")
print(try builder.toSQL())
```

### Parsing SPARQL Queries

```swift
let parser = SPARQLParser()
let query = try parser.parse("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?name ?email
    WHERE {
        ?person foaf:name ?name .
        OPTIONAL { ?person foaf:mbox ?email }
        FILTER(CONTAINS(?name, "Alice"))
    }
    ORDER BY ?name
    LIMIT 100
""")
```

### SQL/PGQ Graph Queries

```swift
// Graph pattern matching using GRAPH_TABLE
let graphQuery = GraphTableSource.match(
    graph: "social_network",
    from: NodePattern(variable: "p1", labels: ["Person"]),
    via: EdgePattern(variable: "f", labels: ["FRIEND"], direction: .outgoing),
    to: NodePattern(variable: "p2", labels: ["Person"])
).returning([
    (.column(ColumnRef(table: "p1", column: "name")), "person"),
    (.column(ColumnRef(table: "p2", column: "name")), "friend")
])

print(try SQLSerializer().serialize(graphQuery))
// GRAPH_TABLE("social_network", MATCH ("p1":"Person")-["f":"FRIEND"]->("p2":"Person") COLUMNS ("p1"."name" AS "person", "p2"."name" AS "friend"))
```

## Security

### SQL Injection Prevention

The canonical writer always double-quotes exact logical identifiers and
doubles embedded quotes. SQL strings double single quotes. Empty or
U+0000-containing identifiers and U+0000-containing strings fail explicitly.
Values enter through typed literals or parameters rather than raw SQL fragments.

```swift
let serializer = SQLSerializer()
try serializer.serialize(Expression.column(ColumnRef("user name"))) // "\"user name\""
try serializer.serialize(Expression.column(ColumnRef("table\"name"))) // "\"table\"\"name\""
try serializer.serialize(Literal.string("O'Brien")) // "'O''Brien'"

// SPARQL escaping remains owned by DatabaseKit.
try SPARQLEscape.ncName("validName")
SPARQLEscape.iri("http://example.org/path")
```

### Reference Standards

- **SQL Identifiers**: ISO/IEC 9075:2023 Section 5.2 (Delimited Identifier)
- **SQL Strings**: ISO/IEC 9075:2023 Section 5.3 (Character String Literal)
- **SPARQL NCName**: W3C XML Namespaces 1.0
- **SPARQL IRI**: RFC 3987, SPARQL 1.1 Section 19.5

## Query Analysis

```swift
let query = try SQLParser().parseSelect("SELECT name, email FROM users WHERE age > 18")

// Get referenced columns
let columns = query.referencedColumns
// ["name", "email", "age"]

// Check if query contains aggregation
let hasAgg = query.hasAggregation
// false

// Get variable references (SPARQL)
let sparqlQuery = try SPARQLParser().parse("SELECT ?name WHERE { ?s foaf:name ?name }")
let vars = sparqlQuery.referencedVariables
// ["name", "s"]
```

Execution planning and cost models are owned by `DatabaseEngine`; they are not
part of the `QueryAST` syntax product.

## Supported SQL Features

### SELECT Clause
- [x] Columns, expressions, aliases
- [x] DISTINCT / ALL
- [x] Aggregate functions (COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, ARRAY_AGG)
- [ ] Window functions (OVER, PARTITION BY): later profile sprint

### FROM Clause
- [x] Table references (schema-qualified, aliases)
- [x] JOIN (INNER, LEFT, RIGHT, FULL, CROSS, NATURAL, LATERAL)
- [x] Subqueries
- [x] GRAPH_TABLE (SQL/PGQ)
- [x] VALUES

### WHERE Clause
- [x] Comparison operators
- [x] Logical operators (AND, OR, NOT)
- [x] LIKE, BETWEEN, IN
- [x] IS NULL / IS NOT NULL
- [x] EXISTS / NOT EXISTS
- [x] Subqueries

### Other
- [x] GROUP BY / HAVING
- [x] ORDER BY (ASC, DESC, NULLS FIRST/LAST)
- [x] LIMIT / OFFSET
- [x] WITH (CTE)
- [ ] Set operations (UNION, INTERSECT, EXCEPT): represented source forms
  throw a typed unsupported-source error until their grammar is implemented

Parsing and serialization establish syntax and retained semantic meaning;
they do not establish runtime support. ARRAY/ROW constructors, temporal
conversion policy, and later statement forms remain with their execution
owners. The accepted profile and staged boundaries are defined in
[DatabaseKit SQL design](../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md).

## Supported SPARQL Features

Reference: [W3C SPARQL 1.1 Query Language](https://www.w3.org/TR/sparql11-query/)

### Query Forms
- [x] SELECT
- [x] CONSTRUCT
- [x] ASK
- [x] DESCRIBE

### Graph Patterns
- [x] Basic Graph Pattern (BGP)
- [x] OPTIONAL
- [x] UNION
- [x] MINUS
- [x] FILTER
- [x] BIND
- [x] VALUES
- [x] SERVICE (FEDERATED)
- [x] GRAPH (Named Graph)

### Property Paths
- [x] Sequence (/)
- [x] Alternative (|)
- [x] Inverse path (^)
- [x] Zero or more (*)
- [x] One or more (+)
- [x] Zero or one (?)
- [x] Negation (!)
- [x] Range ({n,m})

### Solution Modifiers
- [x] ORDER BY
- [x] LIMIT / OFFSET
- [x] DISTINCT / REDUCED
- [x] GROUP BY / HAVING
- [x] Aggregate functions (COUNT, SUM, AVG, MIN, MAX, SAMPLE, GROUP_CONCAT)

### FILTER Built-in Functions (Section 17.4)

`SPARQLParser` recognizes the SPARQL 1.1 functional forms, RDF-term,
string, numeric, date/time, and hash functions represented by `Expression`.
This includes nested calls and the arity-specific forms for `SUBSTR`,
`REPLACE`, `STRDT`, and `STRLANG`. SPARQL 1.2 functions represented by the
current AST, including `STRLANGDIR`, are parsed under the same bounded
structural limits.

Parsing establishes syntax and AST structure only. Runtime function semantics
belong to GraphIndex's SPARQL expression evaluator.

### Aggregate Functions (Section 17.5)

The parser emits `Expression.aggregate` for every aggregate represented by
`AggregateFunction`.

| Function | Syntax | AST Node | Parser |
|----------|--------|----------|--------|
| `COUNT([DISTINCT] expr \| *)` | `DISTINCT`, `*` | `.aggregate(.count(...))` | done |
| `SUM([DISTINCT] expr)` | `DISTINCT` | `.aggregate(.sum(...))` | done |
| `AVG([DISTINCT] expr)` | `DISTINCT` | `.aggregate(.avg(...))` | done |
| `MIN(expr)` | — | `.aggregate(.min(...))` | done |
| `MAX(expr)` | — | `.aggregate(.max(...))` | done |
| `SAMPLE(expr)` | — | `.aggregate(.sample(...))` | done |
| `GROUP_CONCAT([DISTINCT] expr [; SEPARATOR="s"])` | `DISTINCT`, `SEPARATOR` | `.aggregate(.groupConcat(...))` | done |

## Testing

Use the package-owned [Xcode harness](../../scripts/xcode-test-harness)
for focused `QueryASTTests` verification. It owns the fixed toolchain,
external timeouts, isolated source copy, testing-runtime injection and result
collection. Trait selection and exact reviewed counts follow the package's
[verification contract](../../AGENTS.md).

## Reference Standards

- [ISO/IEC 9075:2023](https://www.iso.org/standard/76583.html) - SQL Standard
- [ISO/IEC 9075-16:2023](https://www.iso.org/standard/76588.html) - SQL/PGQ (Property Graph Queries)
- [W3C SPARQL 1.1](https://www.w3.org/TR/sparql11-query/) - SPARQL Query Language
- [W3C RDF-star](https://w3c.github.io/rdf-star/) - RDF-star and SPARQL-star
