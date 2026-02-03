# DatabaseCLI

**Interactive Database CLI powered by TypeCatalog**

An interactive command-line interface for FoundationDB, equivalent to PostgreSQL's `psql`. Access and manipulate data dynamically using TypeCatalog (schema metadata) stored in FDB, without requiring compiled `@Persistable` types.

## Features

- **No Type Definitions Required**: Dynamic data access using TypeCatalog
- **Schema Introspection**: PostgreSQL-style `\dt`, `\d` equivalent commands
- **CRUD Operations**: insert/get/update/delete
- **Query Support**: Filter, sort, and limit
- **Graph Queries**: SPARQL support
- **Dynamic Directories**: Partition specification for multi-tenant types
- **Raw FDB Access**: Low-level key-value operations

## Getting Started

### Standalone Mode (TypeCatalog-only)

```swift
import DatabaseCLI
import FoundationDB
import DatabaseEngine

let database = try FDBClient.openDatabase()
let registry = SchemaRegistry(database: database)
let catalogs = try await registry.loadAll()
let repl = DatabaseREPL(database: database, catalogs: catalogs)
try await repl.run()
```

### Embedded Mode (with FDBContainer)

```swift
let container = try await FDBContainer(for: schema)
let repl = try await DatabaseREPL(container: container)
try await repl.run()
```

Embedded mode is required for version history (`history` command).

## Command Reference

### Schema Info

#### `schema list`
List all registered types (PostgreSQL: `\dt`)

```bash
database> schema list
Registered types:
  User  (3 fields, 2 indexes)
  Order  (5 fields, 3 indexes)
  RDFTriple  (3 fields, 1 indexes)
```

#### `schema show <TypeName>`
Show type fields, types, and indexes (PostgreSQL: `\d tablename`)

```bash
database> schema show User
User
  Fields:
    id: string
    name: string
    age: int64
  Indexes:
    name_idx (scalar) [name]
    age_idx (scalar) [age]
  Directory: ["users"]
```

For types with dynamic directories:

```bash
database> schema show Order
Order
  Fields:
    id: string
    tenantId: string
    amount: int64
  Indexes:
    amount_idx (scalar) [amount]
  Directory: ["orders", <tenantId>]
    - Static: "orders"
    - Dynamic: tenantId (use --partition tenantId=<value>)
```

### Data Operations

#### `insert <TypeName> <json> [--partition field=value ...]`
Insert a record

**⚠️ Important**: CLI writes do NOT update indexes.

```bash
database> insert User {"id": "user-001", "name": "Alice", "age": 30}
WARNING: CLI writes do NOT update indexes.
Inserted record into 'User'

# With partition (for dynamic directory types)
database> insert Order {"id": "order-001", "amount": 1000} --partition tenantId=tenant_123
WARNING: CLI writes do NOT update indexes.
Inserted record into 'Order'
```

#### `get <TypeName> <id> [--partition field=value ...]`
Get a record by ID (PostgreSQL: `SELECT * WHERE id = ?`)

```bash
database> get User user-001
{"id": "user-001", "name": "Alice", "age": 30}

# With partition
database> get Order order-001 --partition tenantId=tenant_123
{"id": "order-001", "tenantId": "tenant_123", "amount": 1000}
```

#### `update <TypeName> <id> <json> [--partition field=value ...]`
Update a record (merge fields)

**⚠️ Important**: CLI writes do NOT update indexes.

```bash
database> update User user-001 {"age": 31}
WARNING: CLI writes do NOT update indexes.
Updated record 'user-001' in 'User'

# Update multiple fields
database> update User user-001 {"age": 32, "name": "Alice Smith"}
WARNING: CLI writes do NOT update indexes.
Updated record 'user-001' in 'User'
```

#### `delete <TypeName> <id> [--partition field=value ...]`
Delete a record

```bash
database> delete User user-001
Deleted record 'user-001' from 'User'
```

### Query

#### `find <TypeName> [--where field op value] [--sort field [desc]] [--limit N] [--partition field=value ...]`

**Supported Operators**: `==`, `!=`, `>`, `<`, `>=`, `<=`

```bash
# List all records
database> find User
Found 3 record(s):
{"id": "user-001", "name": "Alice", "age": 30}
{"id": "user-002", "name": "Bob", "age": 25}
{"id": "user-003", "name": "Charlie", "age": 35}

# Limit results
database> find User --limit 2
Found 2 record(s):
{"id": "user-001", "name": "Alice", "age": 30}
{"id": "user-002", "name": "Bob", "age": 25}

# Filter
database> find User --where age > 30
Found 1 record(s):
{"id": "user-003", "name": "Charlie", "age": 35}

# String filter
database> find User --where name == "Alice"
Found 1 record(s):
{"id": "user-001", "name": "Alice", "age": 30}

# Sort
database> find User --sort age desc
Found 3 record(s):
{"id": "user-003", "name": "Charlie", "age": 35}
{"id": "user-001", "name": "Alice", "age": 30}
{"id": "user-002", "name": "Bob", "age": 25}

# Combined conditions
database> find User --where age > 20 --sort name --limit 5
Found 3 record(s):
{"id": "user-001", "name": "Alice", "age": 30}
{"id": "user-002", "name": "Bob", "age": 25}
{"id": "user-003", "name": "Charlie", "age": 35}

# With partition
database> find Order --limit 10 --partition tenantId=tenant_123
Found 2 record(s):
{"id": "order-001", "tenantId": "tenant_123", "amount": 1000}
{"id": "order-002", "tenantId": "tenant_123", "amount": 2000}
```

### Graph Queries

#### `graph <TypeName> [from=<value>] [edge=<value>] [to=<value>] [--limit N]`
Execute a graph traversal query

```bash
# Get all edges
database> graph RDFTriple --limit 10
Graph query results (10 edges)
  ex:Toyota --[rdf:type]--> ex:Company
  ex:Toyota --[ex:founded]--> 1937
  ex:Alice --[ex:knows]--> ex:Bob

# Filter by from
database> graph RDFTriple from=ex:Toyota
Graph query results (5 edges)
  ex:Toyota --[rdf:type]--> ex:Company
  ex:Toyota --[ex:founded]--> 1937

# Filter by edge
database> graph RDFTriple edge=rdf:type
Graph query results (3 edges)
  ex:Toyota --[rdf:type]--> ex:Company
  ex:Honda --[rdf:type]--> ex:Company

# Filter by from + edge
database> graph RDFTriple from=ex:Toyota edge=rdf:type
Graph query results (1 edges)
  ex:Toyota --[rdf:type]--> ex:Company

# Filter by to
database> graph RDFTriple to=ex:Company
Graph query results (2 edges)
  ex:Toyota --[rdf:type]--> ex:Company
  ex:Honda --[rdf:type]--> ex:Company
```

#### `sparql <TypeName> <SPARQL query>`
Execute a SPARQL SELECT query

```bash
# Basic SELECT
database> sparql RDFTriple SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10
SPARQL results (10 bindings, ?s, ?p, ?o)
?s         | ?p          | ?o
-----------+-------------+------------
ex:Toyota  | rdf:type    | ex:Company
ex:Toyota  | ex:founded  | 1937
ex:Alice   | ex:knows    | ex:Bob

Statistics: 1 patterns, 1 scans, 2.34ms

# With filter
database> sparql RDFTriple SELECT ?s WHERE { ?s <rdf:type> "ex:Company" }
SPARQL results (2 bindings, ?s)
?s
-----------
ex:Toyota
ex:Honda

Statistics: 1 patterns, 1 scans, 1.23ms

# Multiple patterns
database> sparql RDFTriple SELECT ?company ?year WHERE { ?company <rdf:type> "ex:Company" . ?company <ex:founded> ?year }
SPARQL results (2 bindings, ?company, ?year)
?company   | ?year
-----------+------
ex:Toyota  | 1937
ex:Honda   | 1948

Statistics: 2 patterns, 2 scans, 3.45ms
```

### Version History

**Note**: Available only in embedded mode (with `FDBContainer`)

#### `history <TypeName> <id> [--limit N]`
Show version history

```bash
database> history User user-001 --limit 5
(Not available in standalone mode)
Version history queries require compiled @Persistable types.
Use embedded mode with FDBContainer for version history.
```

### Destructive Operations

#### `clear <TypeName> [--force]`
Clear all data for a type

**Safety Gates** (without `--force`):
1. Show record counts and interactive confirmation prompt (y/N)
2. 10-second countdown before execution (Ctrl+C to abort)

```bash
database> clear User
  User: 3 records
Delete ALL data for User? (y/N): y
Executing in 10 seconds... (Ctrl+C to abort)
  10... 9... 8... 7... 6... 5... 4... 3... 2... 1...
Cleared all data for 'User'

# Skip confirmation
database> clear User --force
Cleared all data for 'User'
```

#### `clear --all [--force]`
Clear all data for all types

```bash
database> clear --all
The following types will be cleared:
  User: 3 records
  Order: 10 records
  RDFTriple: 50 records
Delete ALL data for ALL types? (y/N): y
Executing in 10 seconds... (Ctrl+C to abort)
  10... 9... 8... 7... 6... 5... 4... 3... 2... 1...
Cleared all types

# Skip confirmation
database> clear --all --force
Cleared all types
```

### Raw FDB Access

#### `raw get <key>`
Get value for a key

```bash
database> raw get mykey
Key: mykey
Value (11 bytes):
hello world

# Tuple format
database> raw get ("mykey", 123)
Key: ("mykey", 123)
Value (15 bytes):
some data here
```

#### `raw set <key> <value>`
Set a key-value pair

```bash
database> raw set mykey hello world
Set key 'mykey' (11 bytes)

# Tuple format
database> raw set ("mykey", 123) some data
Set key '("mykey", 123)' (9 bytes)
```

#### `raw delete <key>`
Delete a key

```bash
database> raw delete mykey
Deleted key 'mykey'
```

#### `raw range <prefix> [limit N]`
Scan keys with prefix

```bash
database> raw range _cli
Found 3 key(s):
  ("_cli", "session", 1) = 45 bytes
  ("_cli", "session", 2) = 45 bytes
  ("_cli", "config") = 12 bytes

# With limit
database> raw range _cli limit 2
Found 2 key(s):
  ("_cli", "session", 1) = 45 bytes
  ("_cli", "session", 2) = 45 bytes
```

### Other Commands

#### `help [topic]`
Show help

```bash
# General help
database> help
database - FoundationDB Interactive CLI

Schema Info:
  schema list                        List all types
  schema show <TypeName>             Show type fields, types, and indexes
...

# Topic-specific help
database> help schema
Schema Commands:
  schema list                  List all registered types
  schema show <TypeName>       Show type fields, types, and indexes
...

database> help find
Find Commands:
  find <TypeName>                              List all records
  find <TypeName> --limit N                    List with limit
...

database> help graph
Graph Commands:
  graph <TypeName> [from=<value>] [edge=<value>] [to=<value>] [--limit N]
...

database> help data
Data Commands:
  insert <TypeName> <json>     Insert a record
  get <TypeName> <id>          Get a record by ID
...

database> help raw
Raw Commands:
  raw get <key>               Get value for a key
  raw set <key> <value>       Set a key-value pair
...
```

Available help topics: `schema`, `find`, `graph`, `data`, `history`, `clear`, `raw`

#### `quit` / `exit`
Exit the CLI

```bash
database> quit
Goodbye!
```

## Partitions (Dynamic Directories)

For multi-tenant types or types with dynamic directory components, use the `--partition` option to specify partition values.

### Checking if a Type Requires Partitions

```bash
database> schema show Order
Order
  ...
  Directory: ["orders", <tenantId>]
    - Static: "orders"
    - Dynamic: tenantId (use --partition tenantId=<value>)
```

### Partition Examples

```bash
# Get
get Order order-001 --partition tenantId=tenant_123

# Insert
insert Order {"id": "order-002", "amount": 2000} --partition tenantId=tenant_123

# Update
update Order order-001 {"amount": 1500} --partition tenantId=tenant_123

# Delete
delete Order order-001 --partition tenantId=tenant_123

# Query
find Order --limit 10 --partition tenantId=tenant_123
find Order --where amount > 1000 --partition tenantId=tenant_123
```

### Multiple Partition Fields

```bash
# For types with multiple dynamic fields
get MyType record-001 --partition orgId=org_456 --partition teamId=team_789
```

## PostgreSQL Comparison

| DatabaseCLI | PostgreSQL | Description |
|------------|-----------|-------------|
| `schema list` | `\dt` | List tables |
| `schema show User` | `\d users` | Show table definition |
| `get User user-001` | `SELECT * FROM users WHERE id = 'user-001'` | Get by ID |
| `find User --where age > 30` | `SELECT * FROM users WHERE age > 30` | Conditional query |
| `find User --sort age desc` | `SELECT * FROM users ORDER BY age DESC` | Sort |
| `find User --limit 10` | `SELECT * FROM users LIMIT 10` | Limit |
| `insert User {...}` | `INSERT INTO users VALUES (...)` | Insert |
| `update User user-001 {...}` | `UPDATE users SET ... WHERE id = 'user-001'` | Update |
| `delete User user-001` | `DELETE FROM users WHERE id = 'user-001'` | Delete |
| `clear User` | `TRUNCATE users` | Clear all |

## Important Notes

### ⚠️ CLI Writes Do NOT Update Indexes

The `insert` and `update` commands write directly to the data store but do NOT update indexes. This is because:

1. **TypeCatalog-only operation**: No compiled `@Persistable` types available
2. **IndexMaintainer unavailable**: Index maintenance logic requires typed context
3. **Debug/development use**: CLI is primarily for development and debugging

**For production data, use `FDBContext.save()` to ensure index consistency.**

### Operation Modes

| Mode | TypeCatalog | FDBContainer | Version History | Index Updates |
|------|-------------|--------------|-----------------|---------------|
| Standalone | ✅ | ❌ | ❌ | ❌ |
| Embedded | ✅ | ✅ | ✅ | ❌ |

## Architecture

```
DatabaseCLICore/
├── Core/
│   ├── DatabaseREPL.swift          # REPL loop
│   ├── CommandRouter.swift         # Command parsing and dispatch
│   └── CatalogDataAccess.swift     # TypeCatalog-based data access
├── Commands/
│   ├── DataCommands.swift          # insert/get/update/delete
│   ├── FindCommands.swift          # find + filter/sort
│   ├── SchemaInfoCommands.swift    # schema list/show
│   ├── GraphCommands.swift         # graph/sparql
│   ├── HistoryCommands.swift       # history (embedded mode only)
│   ├── ClearCommand.swift          # clear (destructive)
│   └── RawCommands.swift           # raw FDB access
├── Cluster/
│   └── LocalCluster.swift
└── Util/
    ├── CLIError.swift              # Error definitions
    ├── JSONParser.swift            # JSON parsing
    └── Output.swift                # Output formatting
```

## Related Files

- `Sources/DatabaseEngine/Registry/TypeCatalog.swift` - Schema metadata
- `Sources/DatabaseEngine/Registry/SchemaRegistry.swift` - Catalog persistence
- `Sources/DatabaseEngine/Registry/DynamicProtobufDecoder.swift` - Dynamic decoding
- `Sources/DatabaseEngine/Registry/DynamicProtobufEncoder.swift` - Dynamic encoding
