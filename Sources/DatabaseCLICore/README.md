# DatabaseCLICore

`DatabaseCLICore` provides read-only schema and raw-storage inspection for the
FoundationDB administration CLI. It does not provide a second database runtime.

## Responsibility

```text
StorageEngine
    |
    +-- DatabaseFormatCatalog.loadRequired()
    +-- SchemaRegistry.loadAll()
    |
    v
CatalogDataAccess
    |
    +-- SchemaInfoCommands
    +-- bounded RawCommands
    +-- DatabaseREPL
```

`CatalogDataAccess.open(database:)` verifies that the database contains the
canonical persisted format descriptor before exposing catalog inspection.
Schema entries are loaded from the catalog written by `DBContainer`.

## Supported Commands

| Command | Behavior |
|---|---|
| `schema list` | Lists catalog entities |
| `schema show <TypeName>` | Shows fields, indexes, and directory components |
| `raw get <key>` | Reads one raw key |
| `raw range <prefix> [limit N]` | Reads at most 10,000 keys sharing a prefix |
| `help [schema|raw]` | Shows command help |
| `quit` / `exit` | Leaves the REPL |

Raw commands are intentionally read-only. Model mutation, query, graph,
ontology, maintenance, and job operations belong to the authenticated
`DatabaseWire` server path, where authorization, limits, idempotency,
preconditions, model encoding, relationship rules, and index maintenance are
enforced.

## Embedding the REPL

```swift
import DatabaseCLICore
import DatabaseEngine

let repl = try await DatabaseREPL(container: container)
try await repl.run()
```

The standalone initializer accepts any `StorageEngine`, but the current
executable supplies FoundationDB:

```swift
let repl = try await DatabaseREPL(database: engine)
try await repl.run()
```

## Source Layout

| Path | Responsibility |
|---|---|
| `Core/CatalogDataAccess.swift` | Validated format and schema-catalog access |
| `Core/DatabaseREPL.swift` | Interactive read-evaluate-print loop |
| `Core/CommandRouter.swift` | Tokenization and command routing |
| `Commands/SchemaInfoCommands.swift` | Schema inspection |
| `Commands/RawCommands.swift` | Bounded raw key inspection |
| `Cluster/` | Local FoundationDB cluster discovery and setup |
| `Util/` | Typed CLI errors and output formatting |
