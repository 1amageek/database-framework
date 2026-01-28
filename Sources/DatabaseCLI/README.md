# DatabaseCLI

An interactive database CLI built on FoundationDB. Provides dynamic schema definitions, 13 index types, graph traversal, full-text search, vector similarity search, and more.

## Prerequisites

FoundationDB must be installed.

```bash
# macOS
brew install foundationdb

# Linux
# https://apple.github.io/foundationdb/getting-started-linux.html
```

`fdbserver` and `fdbcli` must be on PATH or at one of:

- `/usr/local/libexec/fdbserver`, `/opt/homebrew/libexec/fdbserver`
- `/usr/local/bin/fdbcli`, `/opt/homebrew/bin/fdbcli`

## Local Cluster Management

### Initialization

Creates a `.database/` directory in the current project and starts a local fdbserver.

```bash
database init
database init --port 4691    # specify port
```

`.database/` structure:

```
.database/
├── fdb.cluster    connection info (local:<id>@127.0.0.1:<port>)
├── fdb.pid        server process ID
├── data/          data files
└── logs/          server logs
```

`init` checks port availability before starting. If the port is in use, it returns a `portInUse` error.

### Start / Stop

```bash
database start     # start server from existing .database/
database stop      # stop the server
database status    # show status (port, PID, process liveness)
```

`start` behavior:

- Errors if `.database/` does not exist (run `database init` first)
- If the server is already running, exits successfully with a message
- Automatically cleans up stale PID files (process dead but PID file remains)
- Checks port availability before starting
- Does not call `configureDatabase` (already done during `init`)

`status` output example:

```
Cluster file: /path/to/project/.database/fdb.cluster
Port:         4690
Status:       running (PID: 12345)
```

### Cluster File Auto-Detection

When launching the REPL (`database` with no arguments), the CLI walks up from the current directory looking for `.database/fdb.cluster`. If not found, it connects to the system default FoundationDB.

```bash
cd /path/to/project
database              # auto-detects .database/fdb.cluster
```

## REPL

```bash
database              # launch interactive shell
```

```
database> help                    list all commands
database> help <topic>            topic help (admin, find, graph, history, data, raw)
database> quit                    exit
```

## Schema Definition

### Basic Syntax

```
admin schema define <Name> <field>...
```

Field format: `name:type[?][#modifier][@relationship]`

### Field Types

| Type | Description |
|------|-------------|
| `string` | String |
| `int` | Integer |
| `double` | Floating point |
| `bool` | Boolean |
| `date` | Date (ISO 8601) |
| `[string]` | String array |
| `[double]` | Float array |

Append `?` for optional fields: `email:string?`

### Modifiers

| Modifier | Description |
|----------|-------------|
| `#indexed` | Scalar index for range/equality queries |
| `#unique` | Unique constraint + scalar index |
| `#bitmap` | Bitmap index (low-cardinality fields) |
| `#rank` | Rank index |

Parameterized modifiers:

```
# Vector index
#vector(dim=384)
#vector(dim=384,metric=cosine,algorithm=hnsw,m=16,ef=200)

# Full-text search index
#fulltext(tokenizer=simple)
#fulltext(tokenizer=stem,positions=true)

# Leaderboard index
#leaderboard(window=daily,count=7)
#leaderboard(window=hourly,count=24,by=category)
```

### Compound Options

```
# Spatial index
--spatial indexName(fields=lat,lon)
--spatial indexName(fields=lat,lon,encoding=s2,level=16)

# Graph index
--graph indexName(from=source,to=target)
--graph indexName(from=source,to=target,label=type,strategy=adjacency)

# Aggregation index
--aggregate indexName(type=sum,field=amount,by=category)
--aggregate indexName(type=count,by=status)

# Version index
--version indexName(retention=keepAll)
--version indexName(retention=keepLast:10)
--version indexName(retention=keepDays:30)

# Composite index
--composite indexName(fields=field1,field2,field3)

# Permuted index
--permuted indexName(source=field,order=0,1,2)
```

### Relationships

```
fieldName:string@relationship(TargetSchema,rule)
```

Rules: `cascade`, `nullify`, `deny`, `noAction`

### Examples

```
database> admin schema define User id:string name:string#indexed age:int#indexed email:string#unique

database> admin schema define Document id:string title:string content:string#fulltext(tokenizer=stem) embedding:[double]#vector(dim=384)

database> admin schema define Store id:string name:string lat:double lon:double --spatial location(fields=lat,lon)

database> admin schema define Follow id:string follower:string followee:string --graph edges(from=follower,to=followee)

database> admin schema define Order id:string customerId:string@relationship(Customer,cascade) amount:double#indexed status:string#bitmap --aggregate orderStats(type=sum,field=amount,by=customerId)

database> admin schema define Score id:string playerId:string score:int#rank --leaderboard daily(window=daily,count=7)

database> admin schema define Article id:string title:string body:string --version history(retention=keepAll)
```

### Schema Management

```
database> admin schema list              list all schemas
database> admin schema show User         show schema details
database> admin schema drop User         drop a schema
```

### Index Management

```
database> admin index add User name#indexed         add an index
database> admin index list User                      list indexes
database> admin index drop User idx_name             drop an index
database> admin index rebuild User idx_name          rebuild an index
```

## Data Operations

### Insert

```
database> insert User {"id": "u1", "name": "Alice", "age": 30, "email": "alice@example.com"}
```

The `id` field is required. Values are auto-coerced to schema-defined types. Unique constraints and relationship integrity are validated.

### Get

```
database> get User u1
```

### Update

```
database> update User u1 {"age": 31}
```

Merges with the existing record. The `id` field cannot be changed.

### Delete

```
database> delete User u1
```

Follows relationship rules: `cascade` deletes referenced records, `deny` rejects deletion if references exist, `nullify` sets referencing fields to null.

## Search

### Scalar Queries

```
database> find User where age > 25
database> find User where name = "Alice"
database> find User where age >= 20 limit 10
database> find User where age > 20 order-by age desc
```

Operators: `=`, `!=`, `>`, `<`, `>=`, `<=`

### Vector Similarity Search

```
database> find Document --vector embedding [0.1, 0.2, 0.3, ...] --k 10
database> find Document --vector embedding 0.1 0.2 0.3 --k 5 --metric cosine
```

Metrics: `cosine`, `euclidean`, `dotProduct`

### Full-Text Search

```
database> find Document --text content "search query"
database> find Document --text content "exact phrase" --phrase
database> find Document --text content "fuzzy" --fuzzy 2
database> find Document --text content "search" limit 20
```

### Spatial Search

```
database> find Store --near 35.6812 139.7671 --radius 1000
database> find Store --near 35.6812 139.7671 --radius 5km
database> find Store --bbox 35.0 139.0 36.0 140.0
```

Distance units: number (meters), `5km`, `1000m`

### Bitmap Queries

```
database> find Order --bitmap status = "shipped"
database> find Order --bitmap status != "cancelled"
database> find Order --bitmap status in shipped,delivered
database> find Order --bitmap status = "shipped" --count
```

### Rank Queries

```
database> find Score --rank score --top 10
database> find Score --rank score --of u1
database> find Score --rank score --count
database> find Score --rank score --range 1-10
```

### Leaderboard Queries

```
database> find Score --leaderboard daily --top 10
database> find Score --leaderboard daily --of u1
database> find Score --leaderboard daily --windows
```

### Aggregation Queries

```
database> find Order --aggregate orderStats
```

### Join Queries

```
database> find Order --join customer
```

## Graph Operations

### Edge Traversal

```
database> graph Follow from=u1              outgoing edges from u1
database> graph Follow to=u2                incoming edges to u2
database> graph Follow from=u1 --depth 3    BFS traversal (depth 3)
```

### Shortest Path

```
database> graph Follow --path from=u1 to=u5
```

### Algorithms

```
database> graph Follow --pagerank --top 10     PageRank (top 10)
```

## Version History

Available on schemas with a `--version` index.

```
database> history Article a1                     list all versions
database> history Article a1 --at v3             get record at version 3
database> history Article a1 --diff v1 v2        diff two versions
database> history Article a1 --rollback v2       rollback to version 2
```

## Raw FDB Access

Direct access to FoundationDB key-value pairs.

```
database> raw get mykey
database> raw set mykey myvalue
database> raw delete mykey
database> raw range prefix limit 10
```

Tuple-format keys:

```
database> raw get ("users", "u1")
database> raw range ("users",) limit 5
```

## Supported Index Types

| Index | Purpose |
|-------|---------|
| Scalar | Equality and range queries |
| Vector | Vector similarity search (HNSW / Flat) |
| FullText | Full-text search (simple / stem / ngram / keyword) |
| Spatial | Geospatial queries (S2 / Morton) |
| Rank | Ranking and Top-N |
| Bitmap | Low-cardinality filters |
| Graph | Graph traversal, shortest path, PageRank |
| Aggregation | Aggregates (count / sum / avg / min / max / distinct / percentile) |
| Version | Version history and rollback |
| Leaderboard | Time-windowed leaderboards |
| Permuted | Permuted index access patterns |
| Relationship | Foreign key constraints (cascade / nullify / deny) |
| Composite | Composite multi-field indexes |

## Troubleshooting

### fdbserver not found

```
ERROR: fdbserver not found
```

Install FoundationDB:

```bash
brew install foundationdb     # macOS
```

### Port already in use

```
ERROR: Port 4690 is already in use.
```

Specify a different port or stop the conflicting process:

```bash
database init --port 4691
```

### Connection failed

```
ERROR: Failed to connect to FoundationDB
```

Check if the server is running:

```bash
database status
database start      # start if stopped
```

### Stale PID file

If `database status` shows `stopped (stale PID file)`, the stale PID file is automatically cleaned up. Run `database start` to restart.
