# GraphIndex

Unified graph edge and RDF triple indexing with configurable storage strategies.

## Overview

GraphIndex provides a unified solution for both general graph edges and RDF triples. It supports multiple storage strategies with different trade-offs between write cost and query flexibility.

**Algorithms**:
- **Adjacency (2-index)**: Outgoing and incoming edge indexes for simple traversal
- **TripleStore (3-index)**: SPO/POS/OSP indexes for SPARQL-like queries
- **Hexastore (6-index)**: All permutations for maximum query flexibility

**Storage Layout** (varies by strategy):
```
adjacency (2 indexes):
  [0=out]/[edge]/[from]/[to] = ''
  [1=in]/[edge]/[to]/[from] = ''

tripleStore (3 indexes):
  [2=spo]/[from]/[edge]/[to] = ''
  [3=pos]/[edge]/[to]/[from] = ''
  [4=osp]/[to]/[from]/[edge] = ''

hexastore (6 indexes):
  [2=spo]/[from]/[edge]/[to] = ''
  [3=pos]/[edge]/[to]/[from] = ''
  [4=osp]/[to]/[from]/[edge] = ''
  [5=sop]/[from]/[to]/[edge] = ''
  [6=pso]/[edge]/[from]/[to] = ''
  [7=ops]/[to]/[edge]/[from] = ''
```

## Terminology

GraphIndex unifies different terminology from graph and RDF domains:

| Domain | Field 1 | Field 2 | Field 3 |
|--------|---------|---------|---------|
| Graph | Source | Label | Target |
| RDF | Subject | Predicate | Object |
| Unified | From | Edge | To |

## Use Cases

### 1. Social Network (Follows)

**Scenario**: Track who follows whom in a social network.

```swift
@Persistable
struct Follow {
    var id: String = ULID().ulidString
    var follower: String = ""
    var followee: String = ""

    #Index<Follow>(
        type: GraphIndexKind.adjacency(
            source: \.follower,
            target: \.followee
        )
    )
}

// Find who Alice follows
let aliceFollows = try await context.graph(Follow.self)
    .defaultIndex()
    .from("alice")
    .execute()

// Find Alice's followers
let aliceFollowers = try await context.graph(Follow.self)
    .defaultIndex()
    .to("alice")
    .execute()
```

**Performance**: O(log n + k) where k = result count.

### 2. RDF Knowledge Graph

**Scenario**: Store and query RDF triples.

```swift
@Persistable
struct Statement {
    var id: String = ULID().ulidString
    var subject: String = ""
    var predicate: String = ""
    var object: String = ""

    #Index<Statement>(
        type: GraphIndexKind.rdf(
            subject: \.subject,
            predicate: \.predicate,
            object: \.object,
            strategy: .tripleStore
        )
    )
}

// SPARQL-like: ?s knows ?o (find all "knows" relationships)
let knowsRelations = try await context.graph(Statement.self)
    .index(\.subject, \.predicate, \.object)
    .edge("knows")
    .execute()

// Find all facts about "Alice"
let aliceFacts = try await context.graph(Statement.self)
    .index(\.subject, \.predicate, \.object)
    .from("Alice")
    .execute()
```

### 3. High-Performance Knowledge Graph

**Scenario**: Read-heavy knowledge base with diverse query patterns.

```swift
@Persistable
struct KnowledgeTriple {
    var id: String = ULID().ulidString
    var entity: String = ""
    var relation: String = ""
    var value: String = ""

    #Index<KnowledgeTriple>(
        type: GraphIndexKind.knowledgeGraph(
            entity: \.entity,
            relation: \.relation,
            value: \.value
        )  // Uses hexastore strategy
    )
}

// All queries optimal with hexastore:
// - Find by entity
// - Find by relation
// - Find by value
// - Any combination
```

### 4. Multi-Hop Graph Traversal

**Scenario**: Find friends-of-friends using BFS traversal.

```swift
let traverser = GraphTraverser<Follow>(
    database: container.database,
    subspace: indexSubspace
)

// Find all users within 3 hops of Alice
for try await (depth, userID) in traverser.traverse(
    from: "alice",
    maxDepth: 3,
    label: nil,  // All edge types
    direction: .outgoing
) {
    print("Depth \(depth): \(userID)")
}

// Paginated traversal with deterministic cursor
let page = try await traverser.traversePaginated(
    from: "alice",
    maxDepth: 2,
    pageSize: 100
)
print("Found \(page.nodes.count) nodes, complete: \(page.isComplete)")
```

### 5. Labeled Multi-Graph

**Scenario**: Multiple relationship types between nodes.

```swift
@Persistable
struct Relationship {
    var id: String = ULID().ulidString
    var source: String = ""
    var target: String = ""
    var type: String = ""  // "follows", "blocks", "likes"

    #Index<Relationship>(
        type: GraphIndexKind.adjacency(
            source: \.source,
            target: \.target,
            label: \.type
        )
    )
}

// Find only "follows" relationships from Alice
let aliceFollows = try await context.graph(Relationship.self)
    .defaultIndex()
    .from("alice")
    .edge("follows")
    .execute()

// Find all "blocks" targeting Bob
let bobBlockers = try await context.graph(Relationship.self)
    .defaultIndex()
    .edge("blocks")
    .to("bob")
    .execute()
```

## Design Patterns

### Strategy Selection Guide

| Strategy | Indexes | Write Cost | Query Patterns | Use Case |
|----------|---------|------------|----------------|----------|
| `adjacency` | 2 | Low | from→to, to→from | Social graphs, simple traversal |
| `tripleStore` | 3 | Medium | SPO, POS, OSP | RDF stores, SPARQL queries |
| `hexastore` | 6 | High | All combinations | Read-heavy, diverse patterns |

**Decision Matrix**:
```
┌────────────────────────────────────────────────────────────────┐
│                    Which strategy should I use?                │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  Simple social graph (follow/friend)?                          │
│  Only need from→to and to→from queries?   ──────▶  .adjacency  │
│                                                                │
│  RDF triple store?                                             │
│  SPARQL-like queries?                     ──────▶  .tripleStore│
│  Need ?P?, ?PO, ??O patterns?                                  │
│                                                                │
│  Knowledge graph with many query types?                        │
│  Read-heavy workload?                     ──────▶  .hexastore  │
│  Need optimal performance for any pattern?                     │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### Query Pattern to Index Mapping

| Query Pattern | adjacency | tripleStore | hexastore |
|--------------|-----------|-------------|-----------|
| `(from, edge, to)` | out | SPO | SPO |
| `(from, edge, ?)` | out | SPO | SPO |
| `(from, ?, to)` | scan | OSP | SOP |
| `(?, edge, to)` | in | POS | POS |
| `(from, ?, ?)` | out | SPO | SPO |
| `(?, edge, ?)` | scan | POS | PSO |
| `(?, ?, to)` | in | OSP | OSP |
| `(?, ?, ?)` | scan | SPO | SPO |

**Note**: "scan" means the pattern requires scanning multiple index entries.

### Hexastore Reference

Based on the paper: Weiss, C., Karras, P., & Bernstein, A. (2008). "Hexastore: sextuple indexing for semantic web data management"

```
All 6 permutations:
  SPO: Subject → Predicate → Object
  SOP: Subject → Object → Predicate
  PSO: Predicate → Subject → Object
  POS: Predicate → Object → Subject
  OSP: Object → Subject → Predicate
  OPS: Object → Predicate → Subject

Query example: Find all X where (X, knows, Bob)
  - Use POS index: predicate="knows", object="Bob" → subjects
  - Single index scan, no filtering needed
```

### GraphTraverser Usage

The `GraphTraverser` class provides efficient graph traversal:

```swift
// Create traverser
let traverser = GraphTraverser<Edge>(
    database: database,
    subspace: indexSubspace
)

// 1-hop neighbors (AsyncThrowingStream)
for try await targetID in traverser.neighbors(
    from: "alice",
    label: "follows",
    direction: .outgoing,
    mode: .snapshot  // Use snapshot for large traversals
) {
    print("Alice follows: \(targetID)")
}

// Multi-hop BFS (up to 3 hops, max 10000 nodes)
for try await (depth, nodeID) in traverser.traverse(
    from: "alice",
    maxDepth: 3,
    label: nil,
    direction: .outgoing,
    maxNodes: 10000
) {
    print("Depth \(depth): \(nodeID)")
}

// Paginated traversal with cursor
let page = try await traverser.traversePaginated(
    from: "alice",
    maxDepth: 3,
    pageSize: 100,
    cursor: nil  // Or cursor from previous call
)

if let nextCursor = page.nextCursor {
    // Resume from where we left off
    let nextPage = try await traverser.traversePaginated(
        from: "alice",
        maxDepth: 3,
        pageSize: 100,
        cursor: nextCursor
    )
}
```

### Sparse Index (Optional Relationships)

GraphIndex supports sparse indexing for optional relationships:

```swift
@Persistable
struct OptionalFollow {
    var id: String = ULID().ulidString
    var follower: String = ""
    var followee: String? = nil  // Optional target

    #Index<OptionalFollow>(
        type: GraphIndexKind.adjacency(
            source: \.follower,
            target: \.followee
        )
    )
}

// Edges with nil followee are NOT indexed
// Only edges with non-nil values appear in queries
```

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| Adjacency strategy | ✅ Complete | 2 indexes (out/in) |
| TripleStore strategy | ✅ Complete | 3 indexes (SPO/POS/OSP) |
| Hexastore strategy | ✅ Complete | 6 indexes (all permutations) |
| SPARQL-like queries | ✅ Complete | Pattern matching with wildcards |
| 1-hop neighbors | ✅ Complete | Direct edge traversal |
| Multi-hop BFS | ✅ Complete | Configurable depth and limits |
| Bounded traversal | ✅ Complete | Pagination with continuation tokens |
| Sparse index (nil) | ✅ Complete | nil values not indexed |
| Bidirectional BFS | ⚠️ Partial | Basic support |
| Shortest path | ❌ Not implemented | Planned |
| PageRank | ❌ Not implemented | Planned |

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Insert (adjacency) | O(1) | 2 key writes |
| Insert (tripleStore) | O(1) | 3 key writes |
| Insert (hexastore) | O(1) | 6 key writes |
| Delete | O(strategy) | Same as insert |
| Point query | O(log n) | Single index lookup |
| Prefix scan | O(log n + k) | k = result count |
| 1-hop neighbors | O(log n + d) | d = degree of node |
| Multi-hop BFS | O(V + E) | V = visited nodes |

### Storage Overhead

| Strategy | Keys per Edge | Relative Size |
|----------|---------------|---------------|
| adjacency | 2 | 1x (baseline) |
| tripleStore | 3 | 1.5x |
| hexastore | 6 | 3x |

### FDB Considerations

- **Key size**: Sum of from/edge/to field sizes
- **Empty values**: All strategies store empty values
- **Transaction limits**: Multi-hop traversals use multiple transactions

## Benchmark Results

Run with: `swift test --filter GraphIndexPerformanceTests`

### Indexing

| Edges | Strategy | Insert Time | Throughput |
|-------|----------|-------------|------------|
| 100 | adjacency | ~20ms | ~5,000/s |
| 100 | tripleStore | ~30ms | ~3,300/s |
| 100 | hexastore | ~60ms | ~1,700/s |
| 1,000 | adjacency | ~200ms | ~5,000/s |
| 1,000 | tripleStore | ~300ms | ~3,300/s |
| 1,000 | hexastore | ~600ms | ~1,700/s |

### Query

| Edges | Query Type | Strategy | Latency (p50) |
|-------|------------|----------|---------------|
| 1,000 | from=X | adjacency | ~3ms |
| 1,000 | from=X | tripleStore | ~3ms |
| 1,000 | edge=X | adjacency | ~10ms (scan) |
| 1,000 | edge=X | tripleStore | ~3ms (POS index) |
| 1,000 | to=X | adjacency | ~3ms |

### Traversal

| Nodes | Edges | Max Depth | Latency (p50) |
|-------|-------|-----------|---------------|
| 100 | 500 | 2 | ~50ms |
| 100 | 500 | 3 | ~100ms |
| 1,000 | 5,000 | 2 | ~200ms |
| 1,000 | 5,000 | 3 | ~500ms |

*Benchmarks run on M1 Mac with local FoundationDB cluster.*

## References

- [Hexastore Paper](https://dl.acm.org/doi/10.14778/1453856.1453965) - Weiss, Karras, & Bernstein (2008)
- [RDF Triple Stores](https://en.wikipedia.org/wiki/Triplestore) - Wikipedia
- [SPARQL](https://www.w3.org/TR/sparql11-query/) - W3C Query Language
- [Graph Databases](https://neo4j.com/developer/graph-database/) - Neo4j Introduction
- [BFS Algorithm](https://en.wikipedia.org/wiki/Breadth-first_search) - Wikipedia
