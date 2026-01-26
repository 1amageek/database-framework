# QueryAST

Abstract Syntax Tree (AST) module for SQL/SPARQL queries. Provides query parsing, transformation, and serialization.

## Overview

QueryAST provides the following capabilities:

- **SQL/SPARQL Parser**: Build AST from query strings
- **Query Builder**: Programmatic query construction
- **Serializer**: Generate SQL/SPARQL strings from AST
- **Query Analysis**: Variable reference and aggregate function detection
- **SQL/PGQ Support**: Graph pattern matching (ISO/IEC 9075-16:2023)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         QueryAST                                 │
├─────────────────────────────────────────────────────────────────┤
│  Parser/                   │  Builder/                          │
│  ├── SQLParser             │  ├── SQLQueryBuilder               │
│  └── SPARQLParser          │  └── SPARQLQueryBuilder            │
├────────────────────────────┼────────────────────────────────────┤
│  SQL/                      │  SPARQL/                           │
│  ├── MatchPattern          │  ├── GraphPattern                  │
│  ├── PathPattern           │  ├── PropertyPath                  │
│  └── GraphTable            │  └── SPARQLTerm                    │
├────────────────────────────┴────────────────────────────────────┤
│  Core Types                                                      │
│  ├── Expression            ├── DataSource                       │
│  ├── SelectQuery           ├── Literal                          │
│  └── QueryStatement        └── Projection                       │
├─────────────────────────────────────────────────────────────────┤
│  Utils/                                                          │
│  └── SQLEscape (identifier/string escaping)                      │
└─────────────────────────────────────────────────────────────────┘
```

## Key Types

### Query Representation

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
print(query.toSQL())
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

print(graphQuery.toSQL())
// GRAPH_TABLE(social_network,
//   MATCH (p1:Person)-[f:FRIEND]->(p2:Person)
//   COLUMNS (p1.name AS person, p2.name AS friend)
// )
```

## Security

### SQL Injection Prevention

All identifiers and string literals are properly escaped:

```swift
// Identifier escaping (double quotes)
SQLEscape.identifier("user name")  // "\"user name\""
SQLEscape.identifier("table\"name") // "\"table\"\"name\""

// String literal escaping (single quotes)
SQLEscape.string("O'Brien")  // "'O''Brien'"

// SPARQL NCName validation
try SPARQLEscape.ncName("validName")  // "validName"
try SPARQLEscape.ncName("invalid name")  // throws SPARQLEscapeError.invalidNCName

// IRI escaping
SPARQLEscape.iri("http://example.org/path")  // "<http://example.org/path>"
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

## Query Optimization (Experimental)

```swift
// Generate query plan
let plan = QueryPlan(query: query, indexes: availableIndexes)

// Cost estimation
print(plan.estimatedCost)

// Get optimized plan
let optimizedPlan = plan.optimized()
```

## Supported SQL Features

### SELECT Clause
- [x] Columns, expressions, aliases
- [x] DISTINCT / ALL
- [x] Aggregate functions (COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, ARRAY_AGG)
- [x] Window functions (OVER, PARTITION BY)

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
- [x] Set operations (UNION, INTERSECT, EXCEPT)

## Supported SPARQL Features

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
- [x] Aggregate functions

## Testing

```bash
# Run QueryAST tests
swift test --filter QueryASTTests

# Run specific test suites
swift test --filter "SQLParserTests"
swift test --filter "SPARQLParserTests"
swift test --filter "GraphTableTests"
```

## Reference Standards

- [ISO/IEC 9075:2023](https://www.iso.org/standard/76583.html) - SQL Standard
- [ISO/IEC 9075-16:2023](https://www.iso.org/standard/76588.html) - SQL/PGQ (Property Graph Queries)
- [W3C SPARQL 1.1](https://www.w3.org/TR/sparql11-query/) - SPARQL Query Language
- [W3C RDF-star](https://w3c.github.io/rdf-star/) - RDF-star and SPARQL-star
