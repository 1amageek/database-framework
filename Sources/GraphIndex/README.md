# GraphIndex

Unified graph edge and RDF triple indexing with configurable storage strategies and OWL DL reasoning.

## Overview

GraphIndex provides a unified solution for both general graph edges and RDF triples. It supports multiple storage strategies with different trade-offs between write cost and query flexibility.

**Key Features**:
- **Multi-strategy storage**: Adjacency, TripleStore, Hexastore
- **Graph algorithms**: PageRank, shortest path, community detection, cycle detection
- **OWL DL reasoning**: Automatic classification, subsumption checking, instance reasoning

**Storage Strategies**:
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
    direction: .outgoing
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

## Graph Algorithms

GraphIndex includes production-ready graph algorithms with academic rigor.

### Cycle Detection

Detect cycles in directed graphs using DFS three-color algorithm.

```swift
let detector = CycleDetector<Edge>(
    database: database,
    subspace: indexSubspace
)

// Check if graph has any cycle
let hasCycle = try await detector.hasCycle(edgeLabel: "depends_on")

// Find all cycles (with limit)
let cycleInfo = try await detector.findCycles(edgeLabel: "depends_on", maxCycles: 10)
for cycle in cycleInfo.cycles {
    print("Cycle: \(cycle.joined(separator: " -> "))")
}

// Validate DAG constraint before inserting edge
let wouldCycle = try await detector.wouldCreateCycle(
    from: "A",
    to: "B",
    edgeLabel: "depends_on"
)
```

**Reference**: CLRS "Introduction to Algorithms" Chapter 22

### Topological Sort

Compute linear ordering for DAGs using Kahn's algorithm.

```swift
let sorter = TopologicalSorter<Edge>(
    database: database,
    subspace: indexSubspace
)

// Get topological order
let result = try await sorter.sort(edgeLabel: "depends_on")
if let order = result.order {
    print("Build order: \(order.joined(separator: " -> "))")
} else {
    print("Cycle detected in: \(result.cyclicNodes)")
}

// Get transitive dependencies
let deps = try await sorter.dependencies(of: "module_A", edgeLabel: "depends_on")

// Get critical path (longest path)
let critical = try await sorter.criticalPath(edgeLabel: "depends_on")
```

**Reference**: Kahn (1962)

### Shortest Path

Find shortest paths using unidirectional or bidirectional BFS.

```swift
let finder = ShortestPathFinder<Edge>(
    database: database,
    subspace: indexSubspace
)

// Find shortest path between two nodes
let result = try await finder.findShortestPath(
    from: "alice",
    to: "bob",
    edgeLabel: nil  // All edge types
)

if let path = result.path {
    print("Path: \(path.nodeIDs.joined(separator: " -> "))")
    print("Distance: \(path.distance)")
}

// Find all shortest paths
let allPaths = try await finder.findAllShortestPaths(
    from: "alice",
    to: "bob"
)

// Check connectivity
let connected = try await finder.isConnected(from: "alice", to: "bob")
```

### Weighted Shortest Path

Find shortest paths in weighted graphs using Dijkstra's algorithm.

```swift
let dijkstra = WeightedShortestPath<Edge>(
    database: database,
    subspace: indexSubspace
)

// Single path with weights
let result = try await dijkstra.findPath(
    from: "A",
    to: "Z",
    weightExtractor: { edge in edge.weight }
)

// Single-source shortest paths (SSSP)
let sssp = try await dijkstra.singleSource(
    from: "A",
    weightExtractor: { edge in edge.weight }
)
```

**Complexity**: O((V + E) log V)

### PageRank

Compute importance scores using power iteration.

```swift
let pagerank = PageRankComputer<Edge>(
    database: database,
    subspace: indexSubspace,
    configuration: .default  // damping=0.85, iterations=100
)

let result = try await pagerank.compute(edgeLabel: nil)

// Get top ranked nodes
let top10 = result.topK(10)
for (node, score) in top10 {
    print("\(node): \(score)")
}

// Get specific node's rank
if let rank = result.rank(for: "alice") {
    print("Alice is ranked #\(rank)")
}
```

**Reference**: Page, Brin et al. (1999)

### Community Detection

Detect communities using Label Propagation Algorithm.

```swift
let detector = CommunityDetector<Edge>(
    database: database,
    subspace: indexSubspace,
    configuration: .default
)

let result = try await detector.detect(edgeLabel: nil)

// Get node's community
if let community = result.community(for: "alice") {
    print("Alice is in community: \(community)")
}

// Get community members
let members = result.communityMembers(for: communityID)

// Get largest communities
let largest = result.largestCommunities(k: 5)
```

**Reference**: Raghavan et al. (2007)

## OWL DL Reasoning

GraphIndex includes a complete OWL DL reasoner based on the Tableaux algorithm (SHOIN(D)).

### Overview

The reasoner provides:
- **Ontology validation**: Check OWL DL conformance
- **Consistency checking**: Verify ontology has a valid model
- **Classification**: Compute complete class hierarchy with inferred subsumptions
- **Instance reasoning**: Check individual membership and types

**Reference**: Baader, F., et al. (2003). "The Description Logic Handbook"

### Creating an Ontology

```swift
import GraphIndex

var ontology = OWLOntology(iri: "http://example.org/animals")

// Define classes
ontology.classes.append(OWLClass(iri: "ex:Animal"))
ontology.classes.append(OWLClass(iri: "ex:Mammal"))
ontology.classes.append(OWLClass(iri: "ex:Dog"))
ontology.classes.append(OWLClass(iri: "ex:Cat"))
ontology.classes.append(OWLClass(iri: "ex:Canine"))

// Define class hierarchy (rdfs:subClassOf)
ontology.axioms.append(.subClassOf(
    sub: .named("ex:Mammal"),
    sup: .named("ex:Animal")
))
ontology.axioms.append(.subClassOf(
    sub: .named("ex:Dog"),
    sup: .named("ex:Mammal")
))
ontology.axioms.append(.subClassOf(
    sub: .named("ex:Cat"),
    sup: .named("ex:Mammal")
))

// Define equivalence (owl:equivalentClass)
ontology.axioms.append(.equivalentClasses([
    .named("ex:Canine"),
    .named("ex:Dog")
]))

// Define disjoint classes (owl:disjointWith)
ontology.axioms.append(.disjointClasses([
    .named("ex:Dog"),
    .named("ex:Cat")
]))
```

### Automatic Classification

```swift
let reasoner = OWLReasoner(ontology: ontology)

// Compute complete class hierarchy (including inferred relationships)
var hierarchy = reasoner.classify()

// Query super-classes (transitive closure)
let dogSupers = hierarchy.superClasses(of: "ex:Dog")
// → ["ex:Mammal", "ex:Animal", "owl:Thing"]

// Query sub-classes
let mammalSubs = hierarchy.subClasses(of: "ex:Mammal")
// → ["ex:Dog", "ex:Cat", "ex:Canine"]

// Query equivalent classes
let dogEquiv = hierarchy.equivalentClasses(of: "ex:Dog")
// → ["ex:Canine"]

// Query direct parents only
let directSupers = hierarchy.directSuperClasses(of: "ex:Dog")
// → ["ex:Mammal"]
```

### Subsumption and Equivalence Checking

```swift
// Check if Animal subsumes Dog (Dog ⊑ Animal)
let result = reasoner.subsumes(
    superClass: .named("ex:Animal"),
    subClass: .named("ex:Dog")
)
print(result.value)  // true

// Check equivalence
let equiv = reasoner.areEquivalent(
    .named("ex:Dog"),
    .named("ex:Canine")
)
print(equiv.value)  // true

// Check disjointness
let disjoint = reasoner.areDisjoint(
    .named("ex:Dog"),
    .named("ex:Cat")
)
print(disjoint.value)  // true
```

### Instance Reasoning

```swift
// Add individuals
ontology.individuals.append(OWLNamedIndividual(iri: "ex:fido"))
ontology.axioms.append(.classAssertion(
    individual: "ex:fido",
    classExpr: .named("ex:Dog")
))

let reasoner = OWLReasoner(ontology: ontology)

// Check instance membership
let isDog = reasoner.isInstanceOf(
    individual: "ex:fido",
    classExpr: .named("ex:Dog")
)
print(isDog.value)  // true

// Check inferred membership
let isMammal = reasoner.isInstanceOf(
    individual: "ex:fido",
    classExpr: .named("ex:Mammal")
)
print(isMammal.value)  // true (inferred via class hierarchy)

// Get all types of an individual
let types = reasoner.types(of: "ex:fido")
// → ["ex:Dog", "ex:Canine", "ex:Mammal", "ex:Animal", "owl:Thing"]
```

### Property Reasoning

```swift
// Define transitive property
var ancestorOf = OWLObjectProperty(iri: "ex:ancestorOf")
ancestorOf.characteristics.insert(.transitive)
ontology.objectProperties.append(ancestorOf)

// Define inverse property
var descendantOf = OWLObjectProperty(iri: "ex:descendantOf")
descendantOf.inverseOf = "ex:ancestorOf"
ontology.objectProperties.append(descendantOf)

let reasoner = OWLReasoner(ontology: ontology)

// Query property characteristics
print(reasoner.isTransitive("ex:ancestorOf"))  // true
print(reasoner.inverseProperty(of: "ex:ancestorOf"))  // "ex:descendantOf"

// Reachable individuals via transitive property
let reachable = reasoner.reachableIndividuals(
    from: "ex:alice",
    via: "ex:ancestorOf",
    includeInferred: true
)
```

### Supported OWL Constructs

| Construct | Status | Description |
|-----------|--------|-------------|
| **Class Axioms** | | |
| `rdfs:subClassOf` | ✅ | Class hierarchy |
| `owl:equivalentClass` | ✅ | Class equivalence |
| `owl:disjointWith` | ✅ | Class disjointness |
| `owl:disjointUnion` | ✅ | Disjoint union |
| **Class Expressions** | | |
| Named class | ✅ | `ex:Person` |
| `owl:intersectionOf` | ✅ | `A ⊓ B` |
| `owl:unionOf` | ✅ | `A ⊔ B` |
| `owl:complementOf` | ✅ | `¬A` |
| `owl:oneOf` | ✅ | Nominals `{a, b, c}` |
| `owl:someValuesFrom` | ✅ | `∃R.C` |
| `owl:allValuesFrom` | ✅ | `∀R.C` |
| `owl:hasValue` | ✅ | `∃R.{a}` |
| `owl:minCardinality` | ✅ | `≥n R.C` |
| `owl:maxCardinality` | ✅ | `≤n R.C` |
| `owl:exactCardinality` | ✅ | `=n R.C` |
| **Property Axioms** | | |
| `rdfs:subPropertyOf` | ✅ | Property hierarchy |
| `owl:inverseOf` | ✅ | Inverse properties |
| `owl:TransitiveProperty` | ✅ | Transitivity |
| `owl:SymmetricProperty` | ✅ | Symmetry |
| `owl:FunctionalProperty` | ✅ | Functionality |
| `owl:InverseFunctionalProperty` | ✅ | Inverse functionality |
| `owl:propertyChain` | ✅ | Property chains |
| **Individual Axioms** | | |
| `rdf:type` | ✅ | Class assertion |
| Object property assertion | ✅ | Property assertion |
| `owl:sameAs` | ✅ | Individual equality |
| `owl:differentFrom` | ✅ | Individual difference |

### Reasoning Configuration

```swift
let config = OWLReasoner.Configuration(
    maxExpansionSteps: 10000,      // Safety limit for tableaux
    enableIncrementalReasoning: true,
    cacheClassification: true,     // Cache subsumption results
    timeout: 60.0                  // Timeout in seconds
)

let reasoner = OWLReasoner(ontology: ontology, configuration: config)
```

### OWL DL Validation

```swift
// Check OWL DL conformance
let (isValid, violations) = reasoner.validateOWLDL()

if !isValid {
    for violation in violations {
        print("Violation: \(violation)")
    }
}

// Check ontology structure
let errors = reasoner.validateStructure()
```

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| **Storage Strategies** | | |
| Adjacency strategy | ✅ Complete | 2 indexes (out/in) |
| TripleStore strategy | ✅ Complete | 3 indexes (SPO/POS/OSP) |
| Hexastore strategy | ✅ Complete | 6 indexes (all permutations) |
| **Query** | | |
| SPARQL-like queries | ✅ Complete | Pattern matching with wildcards |
| Path pattern queries | ✅ Complete | Variable-length paths |
| Algorithm queries | ✅ Complete | PageRank, Community, etc. |
| **Traversal** | | |
| 1-hop neighbors | ✅ Complete | Direct edge traversal |
| Multi-hop BFS | ✅ Complete | Configurable depth and limits |
| Paginated traversal | ✅ Complete | Deterministic cursor-based |
| Bidirectional BFS | ✅ Complete | O(b^(d/2)) optimization |
| **Algorithms** | | |
| Cycle detection | ✅ Complete | DFS three-color |
| Topological sort | ✅ Complete | Kahn's algorithm |
| Shortest path | ✅ Complete | Unidirectional & bidirectional BFS |
| Weighted shortest path | ✅ Complete | Dijkstra's algorithm |
| PageRank | ✅ Complete | Power iteration |
| Community detection | ✅ Complete | Label propagation |
| **OWL DL Reasoning** | | |
| Ontology validation | ✅ Complete | OWL DL conformance check |
| Consistency checking | ✅ Complete | Tableaux algorithm |
| Classification | ✅ Complete | Automatic class hierarchy |
| Subsumption checking | ✅ Complete | With explanation |
| Instance reasoning | ✅ Complete | Type inference |
| Property reasoning | ✅ Complete | Transitive, inverse, chains |
| Class expressions | ✅ Complete | Full SHOIN(D) support |
| **Other** | | |
| Sparse index (nil) | ✅ Complete | nil values not indexed |
| Limit handling | ✅ Complete | isComplete, limitReason |

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

### Storage & Query
- [Hexastore Paper](https://dl.acm.org/doi/10.14778/1453856.1453965) - Weiss, Karras, & Bernstein (2008)
- [RDF Triple Stores](https://en.wikipedia.org/wiki/Triplestore) - Wikipedia
- [SPARQL](https://www.w3.org/TR/sparql11-query/) - W3C Query Language

### OWL & Description Logics
- [The Description Logic Handbook](https://www.cambridge.org/core/books/description-logic-handbook/ABADB9C15A29EA93DF26B35BD4C5C73E) - Baader, Calvanese, et al. (2003)
- [OWL 2 Web Ontology Language](https://www.w3.org/TR/owl2-overview/) - W3C Recommendation
- [OWL 2 Profiles](https://www.w3.org/TR/owl2-profiles/) - W3C (RL, EL, QL)
- [Tableaux Decision Procedure for SHOIQ](https://www.cs.ox.ac.uk/people/ian.horrocks/Publications/download/2007/HoSa07a.pdf) - Horrocks & Sattler (2007)
- [Hypertableau Reasoning](https://www.cs.ox.ac.uk/people/boris.motik/pubs/msh09hypertableau.pdf) - Motik, Shearer, Horrocks (2009)

### Algorithms
- [Introduction to Algorithms (CLRS)](https://mitpress.mit.edu/books/introduction-algorithms) - Cormen et al. (Chapter 22: Graph Algorithms)
- [Topological Sorting](https://dl.acm.org/doi/10.1145/368996.369025) - Kahn (1962)
- [PageRank](http://ilpubs.stanford.edu:8090/422/) - Page, Brin et al. (1999)
- [Label Propagation](https://arxiv.org/abs/0709.2938) - Raghavan, Albert, Kumara (2007)
- [Dijkstra's Algorithm](https://en.wikipedia.org/wiki/Dijkstra%27s_algorithm) - Dijkstra (1959)
- [Bidirectional Search](https://en.wikipedia.org/wiki/Bidirectional_search) - Pohl (1971)

### General
- [Graph Databases](https://neo4j.com/developer/graph-database/) - Neo4j Introduction
- [BFS Algorithm](https://en.wikipedia.org/wiki/Breadth-first_search) - Wikipedia
