# Database CLI

The `database` executable is a FoundationDB administration and inspection
tool for macOS and Linux. The default full-host profile includes the
`FoundationDB` trait.

```bash
swift build --product database
```

## Commands

| Command | Responsibility |
|---|---|
| `database init [--port N]` | Creates a local `.database/fdb.cluster` configuration |
| `database status` | Displays the discovered local cluster configuration |
| `database schema list` | Lists catalog entities |
| `database schema show <TypeName>` | Displays fields, indexes, and directory components |
| `database raw get <key>` | Reads one raw key |
| `database raw range <prefix> [--limit N]` | Reads a bounded key range |
| `database` | Starts the interactive inspection REPL |

The executable discovers `.database/fdb.cluster` by walking upward from the
current working directory. If no project-local cluster is found, it uses the
FoundationDB default cluster configuration.

## Security and Consistency Boundary

This CLI does not mutate model data and does not implement query, graph,
ontology, maintenance, or job semantics. Those operations use the authenticated
`DatabaseWire` endpoint so the database runtime can enforce authorization,
limits, idempotency, preconditions, canonical persistence, relationship rules,
and index maintenance in one transaction.

Raw reads are diagnostic. Their bytes are not a stable replacement for the
model or DatabaseWire APIs.

See [`DatabaseCLICore`](../DatabaseCLICore/README.md) for the embeddable REPL
contract.
