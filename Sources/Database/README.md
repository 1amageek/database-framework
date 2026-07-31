# Database Module

The Database module is the umbrella API for database-framework. Package traits
select its optional dependencies and re-exports; `Database` itself is not a
trait.

## Features

- **Stable umbrella import**: `import Database` is unchanged across trait sets
- **Trait-selected indexes**: Only enabled index modules enter the dependency graph
- **SQL Query Support**: Execute SQL queries with `executeSQL(_:as:)`
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
    from: "26.0731.2",
    traits: ["GraphIndexes"]
)
```

An explicit trait list without `.defaults` replaces the default full native
host profile (`FoundationDB` plus `AllRuntimeFeatures`). `GraphIndexes` enables
`ScalarIndexes`; the umbrella also includes GraphIndex and OntologyIndex for
that composition. `Relationships` remains an independent choice.

Backend facade availability is both trait- and platform-dependent:

| Adapter | Trait | Platforms |
|---|---|---|
| FoundationDB | `FoundationDB` | macOS, Linux |
| SQLite | `SQLite` | macOS, iOS, Linux |
| PostgreSQL | `PostgreSQL` | macOS, iOS, Linux |

Every facade ultimately creates an engine and passes it to the
backend-neutral `DBConfiguration(storageEngine:)` contract. The resulting
container owns that engine. Opening failure, `DBContainer.shutdown()`, and
deinitialization share one exactly-once shutdown path.

## SQL Query Execution

The Database module extends `DatabaseContext` with SQL query execution capabilities.

### Basic SQL Queries

```swift
import Database

let sql = """
SELECT * FROM User
WHERE age > 18 AND email LIKE '%@example.com'
"""
let users = try await context.executeSQL(sql, as: User.self)
```

### Supported SQL Features

- `SELECT` statements with projection
- `WHERE` clauses with predicates
- `LIMIT` and `OFFSET`
- `IN` predicates
- String literals and numeric literals

**Note**: The current implementation uses a query bridge to convert SQL to type-safe `Query<T>`. Some SQL features (like `ORDER BY`) may have limited support depending on the bridge implementation.

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

**Returns**: Array of scalar values (typically IDs) matching the SPARQL pattern

### Basic Example

```swift
import Database

// Define models
@Persistable
struct User {
    #Directory<User>("app", "users")
    var id: String = ""
    var name: String = ""
}

@Persistable
struct Follow {
    #Directory<Follow>("app", "follows")
    var id: String = ""
    var follower: String = ""
    var predicate: String = "follows"
    var following: String = ""

    #Index(
        .propertyGraph(strategy: .tripleStore),
        from: \Follow.follower,
        edge: \Follow.predicate,
        to: \Follow.following
    )
}

// Execute hybrid query
let sql = """
SELECT * FROM User
WHERE id IN (SPARQL(Follow, 'SELECT ?follower WHERE { ?follower "follows" "alice" }'))
"""
let users = try await context.executeSQL(sql, as: User.self)
// Returns all users who follow alice
```

### Advanced Usage

#### Multiple SPARQL Subqueries

Combine multiple graph patterns with SQL logic:

```swift
let sql = """
SELECT * FROM User
WHERE id IN (SPARQL(Follow, 'SELECT ?follower WHERE { ?follower "follows" ?user }'))
  AND id IN (SPARQL(Interest, 'SELECT ?user WHERE { ?user "likes" "technology" }'))
  AND age > 18
"""
// Returns adult users who follow someone AND like technology
```

#### Explicit Variable Selection

When SPARQL returns multiple variables, specify which one to use:

```swift
let sql = """
SELECT * FROM User
WHERE id IN (
    SPARQL(Follow, 'SELECT ?follower ?following WHERE { ?follower "follows" ?following }', '?follower')
)
"""
// Extract only the ?follower variable from the SPARQL result
```

#### Complex Graph Patterns

Use SPARQL's expressive pattern matching:

```swift
let sql = """
SELECT * FROM Product
WHERE id IN (
    SPARQL(Recommendation, 'SELECT ?product WHERE {
        ?user "purchased" ?prev_product .
        ?prev_product "category" ?cat .
        ?product "category" ?cat .
        ?product "rating" ?rating .
        FILTER(?rating > 4.0)
    }')
)
AND price < 100
"""
// Find highly-rated products in categories the user has purchased from, under $100
```

### Error Handling

The SPARQL() function provides type-safe error handling:

```swift
do {
    let users = try await context.executeSQL(sql, as: User.self)
} catch let error as SPARQLFunctionError {
    switch error {
    case .typeNotFound(let typeName):
        print("Type '\(typeName)' not found in schema")
    case .graphIndexNotFound(let typeName):
        print("Type '\(typeName)' has no graph index")
    case .multipleVariablesNotSupported:
        print("Query returns multiple variables - specify which one to use")
    case .invalidArguments(let message):
        print("Invalid SPARQL() arguments: \(message)")
    case .missingVariable(let varName):
        print("Variable '\(varName)' not found in SPARQL results")
    case .invalidGraphIndex(let typeName):
        print("Invalid graph index for type '\(typeName)'")
    }
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
5. **Query Execution**: Convert rewritten `SelectQuery` to `Query<T>` and execute

#### Transaction Isolation

- SPARQL subqueries execute within the **same transaction** as the parent SQL query
- Ensures **consistent snapshot** across SQL and SPARQL operations
- All operations are **ACID compliant**
- No transaction overhead for SPARQL execution

#### Performance Characteristics

- **Index-backed**: SPARQL queries use graph index for fast traversal
- **Single transaction**: No additional round-trips for SPARQL execution
- **Result inlining**: SPARQL results are cached within transaction scope
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

3. **Query bridge limitations**
   - Some SQL features may not be supported in the current bridge
   - `ORDER BY` support depends on bridge implementation
   - Use standard `fetch()` API for complex sorting requirements

### Use Cases

#### Social Network Queries

```swift
// Find mutual followers
let sql = """
SELECT * FROM User
WHERE id IN (
    SPARQL(Follow, 'SELECT ?user WHERE {
        "alice" "follows" ?user .
        ?user "follows" "alice"
    }')
)
"""

// Find influencers (users with many followers)
let sql = """
SELECT * FROM User
WHERE id IN (
    SPARQL(Follow, 'SELECT ?user WHERE {
        ?follower "follows" ?user
    } GROUP BY ?user HAVING (COUNT(?follower) > 1000)')
)
```

#### Knowledge Graph Queries

```swift
// Find related articles
let sql = """
SELECT * FROM Article
WHERE id IN (
    SPARQL(ArticleGraph, 'SELECT ?article WHERE {
        "article:123" "cites" ?cited .
        ?article "cites" ?cited
    }')
)
AND publishDate > '2024-01-01'
```

#### Access Control

```swift
// Find accessible resources
let sql = """
SELECT * FROM Resource
WHERE id IN (
    SPARQL(Permission, 'SELECT ?resource WHERE {
        ?user "member_of" ?group .
        ?group "can_access" ?resource
    }')
)
AND type = 'document'
```

## API Reference

### DatabaseContext Extensions

#### executeSQL(_:as:)

Executes a SQL query string and returns typed results.

```swift
public func executeSQL<T: Persistable>(
    _ sql: String,
    as type: T.Type
) async throws -> [T]
```

**Parameters**:
- `sql`: SQL query string
- `type`: The Persistable type to fetch

**Returns**: Array of matching models

**Throws**:
- `SQLParseError`: Invalid SQL syntax
- `SPARQLFunctionError`: SPARQL execution errors
- `CanonicalReadError`: Query conversion errors
- typed errors from the selected `StorageEngine`

**Example**:
```swift
let users = try await context.executeSQL(
    "SELECT * FROM User WHERE age > 18",
    as: User.self
)
```

### Error Types

#### SPARQLFunctionError

Errors that occur during SPARQL() function execution.

```swift
public enum SPARQLFunctionError: Error, Sendable, CustomStringConvertible {
    case invalidArguments(String)
    case typeNotFound(String)
    case graphIndexNotFound(String)
    case invalidGraphIndex(String)
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

Run tests:
```bash
xcodebuild test -scheme DatabaseCoreFocused -destination 'platform=macOS,arch=arm64' -only-testing:DatabaseTests/SPARQLFunctionIntegrationTests
```

## Best Practices

### 1. Use SPARQL for Graph Patterns

Use SPARQL() when your query involves graph relationships:

```swift
// ✅ Good: Graph traversal
WHERE id IN (SPARQL(Follow, 'SELECT ?follower WHERE { ?follower "follows" ?user }'))

// ❌ Not ideal: Simple equality (use SQL instead)
WHERE id IN (SPARQL(User, 'SELECT ?user WHERE { ?user "status" "active" }'))
```

### 2. Combine with SQL for Filtering

Use SQL for type-specific filtering, SPARQL for relationships:

```swift
// ✅ Good: Hybrid approach
SELECT * FROM User
WHERE age > 18  -- SQL filter
  AND status = 'active'  -- SQL filter
  AND id IN (SPARQL(Follow, '...'))  -- Graph pattern
```

### 3. Index Graph Edges Properly

Ensure graph indexes match your query patterns:

```swift
@Persistable
struct Follow {
    var follower: String
    var predicate: String = "follows"
    var following: String

    // ✅ Good: Index matches query direction
    #Index(
        .propertyGraph(strategy: .adjacency),
        from: \Follow.follower,
        edge: \Follow.predicate,
        to: \Follow.following
    )
}
```

### 4. Handle Errors Explicitly

Don't use `try?` - handle SPARQL errors explicitly:

```swift
// ❌ Bad
let users = try? await context.executeSQL(sql, as: User.self)

// ✅ Good
do {
    let users = try await context.executeSQL(sql, as: User.self)
} catch let error as SPARQLFunctionError {
    // Handle specific SPARQL errors
    logger.error("SPARQL query failed: \(error)")
    throw error
}
```

## See Also

- [GraphIndex Module](../GraphIndex/README.md) - Graph index implementation
- [QueryAST Module](../QueryAST/README.md) - SQL and SPARQL parsing
- [DatabaseEngine Module](../DatabaseEngine/README.md) - Core engine functionality
