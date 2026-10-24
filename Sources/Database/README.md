# Database Module

The Database module is the umbrella API for database-framework. Package traits
select its optional dependencies and re-exports; `Database` itself is not a
trait.

## Features

- **Stable umbrella import**: `import Database` is unchanged across trait sets
- **Trait-selected indexes**: Only enabled index modules enter the dependency graph
- **SQL Query Support**: Execute SQL and IR through `QueryConnection.execute`
- **Graph query support**: `GraphIndexes` adds GraphIndex, OntologyIndex, SPARQL execution, and scalar support
- **Unified API**: Consistent interface across all index types

## Modules

The core umbrella always re-exports DatabaseKit, StorageKit, DatabaseEngine,
DatabaseRuntime, and QueryAST. Each index module is re-exported only when its
package trait is active. `GraphIndexes` also enables `ScalarIndexes` and
re-exports OntologyIndex because those capabilities are required by graph and
SPARQL execution.

For example, a consuming package selects a graph composition with:

```swift
.package(
    url: "https://github.com/1amageek/database-framework.git",
    from: "26.0814.0",
    traits: ["GraphIndexes"]
)
```

The package has no default traits. `GraphIndexes` enables `ScalarIndexes`; the
umbrella also includes GraphIndex and OntologyIndex for that composition.
`Relationships`, storage backends, and `MultiBase` remain independent
choices.

`Database` does not re-export remote operation execution or native hosting.
Those products belong to the independent `database-server` repository; an
in-process application does not depend on them.

Backend facade availability is both trait- and platform-dependent:

| Adapter | Trait | Platforms |
|---|---|---|
| FoundationDB | `FoundationDB` | macOS, Linux |
| SQLite | `SQLite` | macOS, iOS, Linux |
| PostgreSQL | `PostgreSQL` | macOS, iOS, Linux |

The SQLite and PostgreSQL facades create one engine and pass its root directly
to `DBConfiguration(storageEngine:)`. FoundationDB instead requires an
`FDBDatabaseConfiguration` with a non-empty application-selected Directory
path; the facade resolves that Directory once and injects its retained
`Subspace`. Schema, metadata, data, and index paths are tuple-derived below the
injected root without a hot-path Directory lookup. With `MultiBase`, a multi-domain
composition instead supplies `DatabaseStorageTopology` through the
trait-specific initializer. The default facade does not create a hidden
one-domain topology. Opening failure, `DBContainer.shutdown()`, and
deinitialization share one exactly-once shutdown path.

## SQL Query Execution

Open a logical connection from the existing context. SQL text, QueryStatement and SQLBlock share execution, canonical results and original authorization. Connection closure drains its work; the container continues to own the engine.

### Basic SQL Queries

```swift
import Database

let sql: SQL = """
SELECT * FROM User
WHERE age > 18 AND email LIKE '%@example.com'
"""
let connection = try await context.openConnection()
defer { await connection.close() }
let users = try await connection.execute(sql)
```

### Supported SQL Features

- `SELECT` statements with projection
- `SELECT` without `FROM`
- `WHERE` clauses with predicates
- `JOIN`, `NATURAL JOIN`, and `LATERAL` sources
- `GROUP BY`, `HAVING`, and canonical aggregate functions
- `LIMIT` and `OFFSET`
- `ORDER BY`
- `IN`, `NOT IN`, scalar, and correlated subqueries
- named and positional parameters
- String literals and numeric literals

The parser produces the canonical `SelectQuery` contract and DatabaseEngine
executes that contract directly. Unsupported statements and expressions fail
explicitly; there is no secondary query-builder bridge or silent downgrade.

## SPARQL() SQL Function

The `SPARQL()` function enables hybrid SQL/SPARQL queries, allowing you to combine relational filtering with graph pattern matching.

### Syntax

```sql
SPARQL(TypeName, 'SPARQL_QUERY' [, 'VARIABLE'])
```

**Parameters**:
- `TypeName`: The `@Persistable` type with a graph index
- `SPARQL_QUERY`: A valid SPARQL SELECT query (string literal)
- `VARIABLE` (optional): Variable name to extract from multi-variable results

**Returns**: An array of canonical `FieldValue` scalars from the selected
SPARQL binding. RDF IRIs and literals remain `RDFTerm` values; the function
does not coerce an IRI to a SQL `String`.

### Basic Example

```swift
import Database

// Define models
@Persistable
struct User {
    #Directory<User>("app", "users")
    var id: String = ""
    var resource: RDFTerm = .iri(.xsdString)
    var name: String = ""
}

@Persistable
struct Triple {
    #Directory<Triple>("app", "triples")
    var id: String = ""
    var subject: RDFTerm
    var predicate: RDFTerm
    var object: RDFTerm

    #Index(.graph(
        name: "Triple_rdf",
        definition: .rdf(
            subject: \Triple.subject,
            predicate: \Triple.predicate,
            object: \Triple.object,
            graph: nil
        )
    ))
}

// Execute hybrid query
let sql: SQL = """
SELECT * FROM User
WHERE resource IN (
    SPARQL(Triple, 'SELECT ?user WHERE { ?user <urn:predicate:follows> <urn:user:alice> }')
)
"""
let connection = try await context.openConnection()
defer { await connection.close() }
let users = try await connection.execute(sql)
// Returns all users who follow alice
```

`User.resource` and the selected SPARQL variable are both canonical RDF terms.
If an application also stores a textual identifier, it must maintain that
field explicitly; hybrid query execution does not erase RDF identity by
converting the term to text.

### Advanced Usage

#### Multiple SPARQL Subqueries

Combine multiple graph patterns with SQL logic:

```swift
let sql: SQL = """
SELECT * FROM User
WHERE resource IN (SPARQL(Follow, 'SELECT ?follower WHERE { ?follower <urn:predicate:follows> ?user }'))
  AND resource IN (SPARQL(Interest, 'SELECT ?user WHERE { ?user <urn:predicate:likes> "technology" }'))
  AND age > 18
"""
// Returns adult users who follow someone AND like technology
```

#### Explicit Variable Selection

When SPARQL returns multiple variables, specify which one to use:

```swift
let sql: SQL = """
SELECT * FROM User
WHERE resource IN (
    SPARQL(Follow, 'SELECT ?follower ?following WHERE { ?follower <urn:predicate:follows> ?following }', '?follower')
)
"""
// Extract only the ?follower variable from the SPARQL result
```

#### Complex Graph Patterns

Use SPARQL's expressive pattern matching:

```swift
let sql: SQL = """
SELECT * FROM Product
WHERE resource IN (
    SPARQL(Recommendation, 'SELECT ?product WHERE {
        ?user <urn:predicate:purchased> ?prev_product .
        ?prev_product <urn:predicate:category> ?cat .
        ?product <urn:predicate:category> ?cat .
        ?product <urn:predicate:rating> ?rating .
        FILTER(?rating > 4.0)
    }')
)
AND price < 100
"""
// Find highly-rated products in categories the user has purchased from, under $100
```

### Error Handling

SQL failures retain a typed diagnostic and their underlying execution error:

```swift
let connection = try await context.openConnection()
defer { await connection.close() }
do {
    let result = try await connection.execute(sql)
    for try await row in result.rows {
        print(row.values)
    }
} catch let failure as SQLExecutionFailure {
    print(failure.diagnostic)
    throw failure
}
```

### Implementation Details

#### Execution Flow

1. **SQL Parsing**: Parse SQL string to `SelectQuery`
2. **SPARQL Detection**: Traverse expression tree to find `SPARQL()` function calls
3. **SPARQL Execution**: Execute SPARQL subqueries within parent transaction
   - Resolve type name to schema entity
   - Find graph index descriptor
   - Resolve the schema-declared graph index descriptor
   - Execute SPARQL query against graph index
4. **Result Inlining**: Replace `SPARQL()` calls with literal arrays
5. **Query Execution**: Execute the rewritten canonical `SelectQuery`

#### Transaction Isolation

- SPARQL subqueries execute within the **same transaction** as the parent SQL query
- Ensures **consistent snapshot** across SQL and SPARQL operations
- All operations are **ACID compliant**
- Rewriting does not open a nested storage transaction

#### Performance Characteristics

- **Index-backed**: SPARQL queries use graph index for fast traversal
- **Single transaction**: No additional round-trips for SPARQL execution
- **Result inlining**: Each `SPARQL()` occurrence is evaluated once and its
  canonical scalar results are reused by the parent relational execution
- **Scalability**: Tested with 100+ result items per query

### Limitations

1. **Single-variable projection only**
   - `IN` predicate requires scalar values
   - Multi-variable SPARQL results must use explicit variable selection
   - Rationale: SQL `IN (...)` expects a list of scalars, not tuples

2. **No dynamic directory support**
   - Types with dynamic directory partitions cannot be used
   - Example: `#Directory<Order>("orders", \.tenantId)` is not supported
   - Rationale: SPARQL function needs static directory path resolution

### Use Cases

#### Social Network Queries

```swift
// Find mutual followers
let sql: SQL = """
SELECT * FROM User
WHERE resource IN (
    SPARQL(Follow, 'SELECT ?user WHERE {
        <urn:user:alice> <urn:predicate:follows> ?user .
        ?user <urn:predicate:follows> <urn:user:alice>
    }')
)
"""

// Find influencers (users with many followers)
let sql: SQL = """
SELECT * FROM User
WHERE resource IN (
    SPARQL(Follow, 'SELECT ?user WHERE {
        ?follower <urn:predicate:follows> ?user
    } GROUP BY ?user HAVING (COUNT(?follower) > 1000)')
)
```

#### Knowledge Graph Queries

```swift
// Find related articles
let sql: SQL = """
SELECT * FROM Article
WHERE resource IN (
    SPARQL(ArticleGraph, 'SELECT ?article WHERE {
        <urn:article:123> <urn:predicate:cites> ?cited .
        ?article <urn:predicate:cites> ?cited
    }')
)
AND publishDate > '2024-01-01'
```

#### Access Control

```swift
// Find accessible resources
let sql: SQL = """
SELECT * FROM Resource
WHERE resource IN (
    SPARQL(Permission, 'SELECT ?resource WHERE {
        ?user <urn:predicate:memberOf> ?group .
        ?group <urn:predicate:canAccess> ?resource
    }')
)
AND type = 'document'
```

## API Reference

### Common execution

```swift
let connection = try await context.openConnection()
defer { await connection.close() }
let minimumAge = 18
let result = try await connection.execute(
    "SELECT * FROM User WHERE age > \(minimumAge)"
)
for try await row in result.rows {
    print(row.values)
}
```

The same execute family accepts QueryStatement, SQLBlock and explicit QueryInput.
Interpolation binds parameters. Dynamic SQL uses SQL(text:parameters:). A block
retains each statement's result and schema in order; command, Boolean, RDF and
COPY outcomes keep their own kinds. The default is complete execution; explicit
paging preserves the read-only snapshot contract. No result requires close().

See [QueryExecution](QueryExecution/DESIGN.md) for execution, error, budget and
lifetime contracts. The language-specific collecting kernels and model-adaptation
path are DatabaseExecution SPI for framework composition and owner tests.

### Error Types

#### SPARQLFunctionError

Errors that occur during SPARQL() function execution.

```swift
public enum SPARQLFunctionError: Error, Sendable, Equatable, CustomStringConvertible {
    case invalidArguments(String)
    case typeNotFound(String)
    case graphIndexNotFound(String)
    case ambiguousGraphIndexes(typeName: String, candidates: [String])
    case missingVariable(String)
    case multipleVariablesNotSupported
}
```

#### SQLExecutionError

Errors that occur during SQL string execution.

```swift
public enum SQLExecutionError: Error, Sendable, CustomStringConvertible {
    case unsupportedStatement(String)
}
```

## Testing

The Database module includes comprehensive integration tests for the SPARQL() function:

- Basic IN predicate with SPARQL()
- SPARQL() with complex WHERE clause
- Multiple SPARQL() calls in same query
- Error handling (type not found, no graph index, etc.)
- Explicit variable selection
- Empty result sets
- Performance with large result sets
- Shared transaction behavior for SPARQL rewriting and the parent SQL read
- Canonical RDF-term identity without implicit string coercion
- `ORDER BY` and `LIMIT` after SPARQL result inlining

Run tests:
```bash
xcodebuild test -scheme database-framework-Package -destination 'platform=macOS,arch=arm64' -only-testing:DatabaseTests/SPARQLFunctionIntegrationTests
```

## Best Practices

### 1. Use SPARQL for Graph Patterns

Use SPARQL() when your query involves graph relationships:

```swift
// ✅ Good: Graph traversal
WHERE resource IN (SPARQL(Follow, 'SELECT ?follower WHERE { ?follower <urn:predicate:follows> ?user }'))

// ❌ Not ideal: Simple equality (use SQL instead)
WHERE resource IN (SPARQL(User, 'SELECT ?user WHERE { ?user <urn:predicate:status> "active" }'))
```

### 2. Combine with SQL for Filtering

Use SQL for type-specific filtering, SPARQL for relationships:

```swift
// ✅ Good: Hybrid approach
SELECT * FROM User
WHERE age > 18  -- SQL filter
  AND status = 'active'  -- SQL filter
  AND resource IN (SPARQL(Follow, '...'))  -- Graph pattern
```

### 3. Index Graph Edges Properly

Ensure graph indexes match your query patterns:

```swift
@Persistable
struct Follow {
    var id: String
    var follower: RDFTerm
    var predicate: RDFTerm
    var following: RDFTerm

    // ✅ Good: Index matches query direction
    #Index(.graph(
        name: "Follow_rdf",
        definition: .rdf(
            subject: \Follow.follower,
            predicate: \Follow.predicate,
            object: \Follow.following,
            graph: nil
        )
    ))
}
```

### 4. Handle Errors Explicitly

Propagate typed execution failures and inspect their diagnostic when presenting
an error. Cancellation, resource exhaustion and transaction failure must not
be converted into an empty result.

## See Also

- [GraphIndex Module](../GraphIndex/README.md) - Graph index implementation
- [QueryAST Module](../QueryAST/README.md) - SQL and SPARQL parsing
- [DatabaseEngine Module](../DatabaseEngine/README.md) - Core engine functionality
