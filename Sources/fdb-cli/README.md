# fdb-cli

An interactive CLI tool for a dynamic schema database running on FoundationDB.

## Overview

fdb-cli is a CLI that allows you to define and manipulate schemas dynamically at runtime without defining schemas at compile time. It supports 12 index types and provides various search features including vector search, full-text search, and graph queries.

## Starting the CLI

```bash
swift run fdb-cli
```

## Command List

| Command | Purpose |
|---------|---------|
| `admin` | Schema and index management |
| `insert` | Insert data |
| `get` | Retrieve by ID |
| `update` | Update data |
| `delete` | Delete data |
| `find` | Unified search |
| `graph` | Graph operations |
| `history` | History operations |
| `help` | Display help |
| `exit` | Exit |

---

## Schema Definition

### Basic Syntax

```bash
admin schema define <Schema> <field>:<type>[#modifier][@relationship] ... [--options]
```

### Field Types

| Type | Description | Example |
|------|-------------|---------|
| `string` | String | `name:string` |
| `int` | Integer | `age:int` |
| `double` | Floating point | `price:double` |
| `bool` | Boolean | `active:bool` |
| `date` | Date/time (ISO8601) | `createdAt:date` |
| `[string]` | String array | `tags:[string]` |
| `[double]` | Numeric array (vector) | `embedding:[double]` |

### Index Modifiers `#`

| Modifier | Index | Description |
|----------|-------|-------------|
| `#indexed` | ScalarIndex | Equality and range queries |
| `#unique` | ScalarIndex (unique) | With uniqueness constraint |
| `#vector(dim=N,metric=M)` | VectorIndex | K-NN similarity search |
| `#fulltext(tokenizer=T)` | FullTextIndex | Full-text search |
| `#bitmap` | BitmapIndex | For low cardinality fields |
| `#rank` | RankIndex | Ranking |
| `#leaderboard(window=W)` | LeaderboardIndex | Time-windowed ranking |

### Relationship Modifiers `@`

```bash
@relationship(TargetSchema,deleteRule)
```

**Delete Rules:**
- `cascade` - Delete children when parent is deleted
- `nullify` - Set FK to null when parent is deleted
- `deny` - Reject parent deletion if children exist
- `noAction` - Do nothing

### Compound Options `--`

| Option | Description |
|--------|-------------|
| `--spatial name(fields=lat,lon)` | Geospatial index |
| `--graph name(from=F,to=T)` | Graph index |
| `--aggregate name(type=T,field=F,by=B)` | Aggregation index |
| `--version name(retention=R)` | Version management |
| `--composite name(fields=F1,F2)` | Composite index |
| `--permuted name(source=S,order=1,0,2)` | Permuted index |

---

## Schema Definition Examples

### Basic User Schema

```bash
admin schema define User \
  id:string \
  name:string#indexed \
  email:string#unique \
  age:int#indexed \
  status:string#bitmap
```

### Document with Vector Search

```bash
admin schema define Document \
  id:string \
  title:string#indexed \
  content:string#fulltext(tokenizer=stem) \
  embedding:[double]#vector(dim=384,metric=cosine)
```

### Store with Geospatial Support

```bash
admin schema define Store \
  id:string \
  name:string#indexed \
  category:string#bitmap \
  lat:double \
  lon:double \
  --spatial location(fields=lat,lon)
```

### Graph (Social Network)

```bash
admin schema define Follow \
  id:string \
  follower:string#indexed \
  followee:string#indexed \
  --graph edges(from=follower,to=followee)
```

### Relationships (E-commerce Site)

```bash
admin schema define Customer \
  id:string \
  name:string#indexed \
  email:string#unique

admin schema define Order \
  id:string \
  customerId:string@relationship(Customer,cascade) \
  amount:double#indexed \
  status:string#bitmap
```

### Aggregation Index

```bash
admin schema define Sale \
  id:string \
  region:string#bitmap \
  amount:double \
  --aggregate region_sum(type=sum,field=amount,by=region) \
  --aggregate region_count(type=count,by=region)
```

---

## Data Operations

### Insert

```bash
insert <Schema> <json>

# Example
insert User {"id": "u1", "name": "Alice", "age": 25, "email": "alice@example.com", "status": "active"}
```

### Retrieve

```bash
get <Schema> <id>

# Example
get User u1
```

### Update

```bash
update <Schema> <id> <json>

# Example
update User u1 {"age": 26}
```

### Delete

```bash
delete <Schema> <id>

# Example
delete User u1
```

---

## Search Commands (find)

### Scalar Search

```bash
find <Schema> where <field> <op> <value> [limit N]

# Operators: =, !=, >, <, >=, <=

# Examples
find User where age > 20
find User where age >= 20 limit 10
find User where email = "alice@example.com"
```

### Vector Search

```bash
find <Schema> --vector <field> <vector> --k <N> [--metric <M>]

# Examples
find Document --vector embedding [0.1,0.2,0.3,...] --k 10
find Document --vector embedding [0.1,0.2,0.3,...] --k 10 --metric cosine
```

### Full-Text Search

```bash
find <Schema> --text <field> "<query>" [--phrase] [--fuzzy N]

# Examples
find Article --text body "machine learning"
find Article --text body "machine learning" --phrase
find Article --text body "machin" --fuzzy 2
```

### Geospatial Search

```bash
# Proximity search
find <Schema> --near <lat> <lon> --radius <distance>

# Bounding box
find <Schema> --bbox <minLat> <minLon> <maxLat> <maxLon>

# Examples
find Store --near 35.68 139.73 --radius 5km
find Store --bbox 35.65 139.70 35.70 139.80
```

### Bitmap Search

```bash
find <Schema> --bitmap <field> = <value> [--count]

# Examples
find User --bitmap status = active
find User --bitmap status in [active,pending]
find User --bitmap status = active --count
```

### Ranking Search

```bash
find <Schema> --rank <field> --top N
find <Schema> --rank <field> --of <id>
find <Schema> --rank <field> --count

# Examples
find Player --rank score --top 100
find Player --rank score --of player123
```

### Leaderboard Search

```bash
find <Schema> --leaderboard <indexName> --top N
find <Schema> --leaderboard <indexName> --of <id>
find <Schema> --leaderboard <indexName> --windows

# Examples
find GameScore --leaderboard daily --top 100
find GameScore --leaderboard daily --of player123
```

### Aggregation Query

```bash
find <Schema> --aggregate <indexName>

# Examples
find Sale --aggregate region_sum
find Sale --aggregate region_count
```

### JOIN Query

```bash
find <Schema> --join <relationName>

# Example
find Order --join customer
```

---

## Graph Commands

```bash
# Get adjacent nodes
graph <Schema> from=<node>
graph <Schema> to=<node>

# Traversal (BFS)
graph <Schema> from=<node> --depth <N>

# Shortest path
graph <Schema> --path from=<node> to=<node>

# Algorithms
graph <Schema> --pagerank --top <N>

# Examples
graph Follow from=alice
graph Follow to=alice
graph Follow from=alice --depth 3
graph Follow --path from=alice to=frank
graph Follow --pagerank --top 10
```

---

## History Commands

```bash
# List history
history <Schema> <id>

# Get specific version
history <Schema> <id> --at <version>

# Show diff
history <Schema> <id> --diff <v1> <v2>

# Rollback
history <Schema> <id> --rollback <version>

# Examples
history Document doc123
history Document doc123 --at 2
history Document doc123 --diff 1 2
history Document doc123 --rollback 2
```

---

## Administration Commands

### Schema Management

```bash
# List schemas
admin schema list

# Schema details
admin schema show <Name>

# Delete schema
admin schema drop <Name>
```

### Index Management

```bash
# Add index
admin index add <Schema> <field>#<type>

# List indexes
admin index list <Schema>

# Drop index
admin index drop <Schema> <indexName>

# Rebuild index
admin index rebuild <Schema> <indexName>
```

---

## Supported Index List

| Index | Purpose | Modifier/Option |
|-------|---------|-----------------|
| ScalarIndex | Equality and range queries | `#indexed`, `#unique` |
| VectorIndex | K-NN similarity search | `#vector(dim=N,metric=M)` |
| FullTextIndex | Full-text search with BM25 | `#fulltext(tokenizer=T)` |
| SpatialIndex | Geospatial queries | `--spatial` |
| RankIndex | Ranking and Top-K | `#rank` |
| PermutedIndex | Compound index reordering | `--permuted` |
| GraphIndex | Graph traversal | `--graph` |
| AggregationIndex | Aggregations (COUNT/SUM/AVG) | `--aggregate` |
| VersionIndex | History and audit trails | `--version` |
| BitmapIndex | Set operations | `#bitmap` |
| LeaderboardIndex | Time-windowed ranking | `#leaderboard(window=W)` |
| RelationshipIndex | FK and relationships | `@relationship(T,rule)` |

---

## Constraints and Validation

### Unique Constraint

Fields with the `#unique` modifier cannot have duplicate values inserted or updated.

```bash
admin schema define User id:string email:string#unique

insert User {"id": "u1", "email": "test@example.com"}
insert User {"id": "u2", "email": "test@example.com"}  # Error: unique constraint violation
```

### Referential Integrity

Fields with the `@relationship` modifier require the referenced record to exist.

```bash
admin schema define Customer id:string name:string
admin schema define Order id:string customerId:string@relationship(Customer,cascade)

insert Order {"id": "o1", "customerId": "c999"}  # Error: Customer c999 does not exist
```

### Delete Rules

Controls how child records are handled when parent records are deleted.

- **cascade**: Automatically delete child records
- **nullify**: Set child record FK to null
- **deny**: Reject deletion if child records exist
- **noAction**: Do nothing (manual management)
