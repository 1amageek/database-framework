// GraphQueryBuilder+Reasoning.swift
// GraphIndex - Query extension for OWL reasoning
//
// Extends GraphQueryBuilder to support OWL DL inference during queries.
//
// Following CLAUDE.md extension pattern: new methods via extension,
// not modifying core FDBContext or GraphQueryBuilder.
//
// Inference strategies:
//   1. Sub-property expansion: query all sub-roles of the bound edge
//   2. Inverse expansion: query owl:inverseOf with swapped from/to
//   3. Transitive closure: BFS from results until fixpoint
//
// Reference: Horrocks & Sattler (2007), Section 3 — role hierarchy semantics

import Foundation
import Core
import Graph
import DatabaseEngine
import StorageKit

// MARK: - EdgeKey (efficient deduplication)

/// Struct for efficient edge deduplication (replaces Set<[String]>).
/// Three fixed fields hash more efficiently than Array<String>.
private struct EdgeKey: Hashable {
    let from: String
    let edge: String
    let to: String
}

// MARK: - ReasoningGraphQueryBuilder

/// Graph query builder with OWL DL reasoning support
///
/// Extends graph queries with inference capabilities:
/// - Transitive property expansion
/// - Inverse property inference
/// - Role hierarchy traversal
/// - Type inference
///
/// **Example**:
/// ```swift
/// let reasoner = OWLReasoner(ontology: ontology)
/// let results = try await context.graph(Statement.self)
///     .defaultIndex()
///     .withReasoning(reasoner)
///     .from("ex:Alice")
///     .edge("ex:knows")  // Will also follow subproperties
///     .withTransitiveClosure()
///     .execute()
/// ```
public struct ReasoningGraphQueryBuilder<Item: Persistable> {

    // MARK: - Properties

    private let base: GraphQueryBuilder<Item>
    private let reasoner: OWLReasoner

    /// Pre-computed ontology context (shared with SPARQL executor).
    /// Built once from the reasoner's ontology, avoiding repeated
    /// RoleHierarchy construction on each execute().
    private let ontologyContext: OntologyContext

    /// Whether to include inferred relationships
    private var includeInferred: Bool = true

    /// Whether to expand transitive properties
    private var expandTransitive: Bool = true

    /// Whether to include inverse property assertions
    private var includeInverse: Bool = true

    /// Whether to include sub-property assertions
    private var includeSubProperties: Bool = true

    /// Maximum depth for transitive closure
    private var maxTransitiveDepth: Int = 10

    /// Tracked pattern values for inference expansion
    private var trackedFrom: String?
    private var trackedEdge: String?
    private var trackedTo: String?

    // MARK: - Initialization

    /// Initialize with base query builder and reasoner
    ///
    /// - Parameters:
    ///   - base: The underlying GraphQueryBuilder
    ///   - reasoner: The OWL reasoner to use for inference
    public init(base: GraphQueryBuilder<Item>, reasoner: OWLReasoner) {
        self.base = base
        self.reasoner = reasoner
        self.ontologyContext = OntologyContext(ontology: reasoner.ontology)
    }

    /// Internal init that reuses a pre-computed OntologyContext
    private init(base: GraphQueryBuilder<Item>, reasoner: OWLReasoner, ontologyContext: OntologyContext) {
        self.base = base
        self.reasoner = reasoner
        self.ontologyContext = ontologyContext
    }

    // MARK: - Configuration

    /// Enable or disable inference
    public func withInference(_ enabled: Bool = true) -> Self {
        var copy = self
        copy.includeInferred = enabled
        return copy
    }

    /// Enable or disable transitive closure expansion
    public func withTransitiveClosure(_ enabled: Bool = true, maxDepth: Int = 10) -> Self {
        var copy = self
        copy.expandTransitive = enabled
        copy.maxTransitiveDepth = maxDepth
        return copy
    }

    /// Enable or disable inverse property inference
    public func withInverseInference(_ enabled: Bool = true) -> Self {
        var copy = self
        copy.includeInverse = enabled
        return copy
    }

    /// Enable or disable sub-property inference
    public func withSubPropertyInference(_ enabled: Bool = true) -> Self {
        var copy = self
        copy.includeSubProperties = enabled
        return copy
    }

    // MARK: - Pattern Delegation

    /// Set from/subject pattern
    public func from(_ value: String) -> Self {
        var copy = Self(base: base.from(value), reasoner: reasoner, ontologyContext: ontologyContext)
        copy.copySettings(from: self)
        copy.trackedFrom = value
        copy.trackedEdge = trackedEdge
        copy.trackedTo = trackedTo
        return copy
    }

    /// Set edge/predicate pattern
    public func edge(_ value: String) -> Self {
        var copy = Self(base: base.edge(value), reasoner: reasoner, ontologyContext: ontologyContext)
        copy.copySettings(from: self)
        copy.trackedFrom = trackedFrom
        copy.trackedEdge = value
        copy.trackedTo = trackedTo
        return copy
    }

    /// Set to/object pattern
    public func to(_ value: String) -> Self {
        var copy = Self(base: base.to(value), reasoner: reasoner, ontologyContext: ontologyContext)
        copy.copySettings(from: self)
        copy.trackedFrom = trackedFrom
        copy.trackedEdge = trackedEdge
        copy.trackedTo = value
        return copy
    }

    /// Set result limit
    public func limit(_ count: Int) -> Self {
        var copy = Self(base: base.limit(count), reasoner: reasoner, ontologyContext: ontologyContext)
        copy.copySettings(from: self)
        copy.trackedFrom = trackedFrom
        copy.trackedEdge = trackedEdge
        copy.trackedTo = trackedTo
        return copy
    }

    /// Copy inference settings from another builder
    private mutating func copySettings(from other: Self) {
        includeInferred = other.includeInferred
        expandTransitive = other.expandTransitive
        includeInverse = other.includeInverse
        includeSubProperties = other.includeSubProperties
        maxTransitiveDepth = other.maxTransitiveDepth
    }

    // MARK: - Execution

    /// Execute query with reasoning
    ///
    /// Returns edges including both explicit and inferred relationships.
    /// Inference is applied in three phases:
    /// 1. Sub-property expansion: query all sub-roles of the bound edge
    /// 2. Inverse expansion: query owl:inverseOf with swapped from/to
    /// 3. Transitive closure: BFS from results until fixpoint
    public func execute() async throws -> [GraphQueryBuilder<Item>.GraphEdge] {
        // Get base results
        let baseResults = try await base.execute()

        if !includeInferred {
            return baseResults
        }

        // Deduplication set using struct for efficient hashing
        var seen = Set<EdgeKey>()
        var allResults = baseResults
        for r in baseResults {
            seen.insert(EdgeKey(from: r.from, edge: r.edge, to: r.to))
        }

        guard let edgeIRI = trackedEdge else {
            // No edge bound — cannot infer without knowing the property
            return allResults
        }

        // Phase 1: Sub-property expansion
        if includeSubProperties {
            let subRoles = ontologyContext.subProperties(of: edgeIRI)
            for subRole in subRoles {
                var subQuery = base.edge(subRole)
                if let f = trackedFrom { subQuery = subQuery.from(f) }
                if let t = trackedTo { subQuery = subQuery.to(t) }
                let subResults = try await subQuery.execute()
                for r in subResults {
                    // Report the edge using the queried predicate (original edgeIRI)
                    let key = EdgeKey(from: r.from, edge: edgeIRI, to: r.to)
                    if seen.insert(key).inserted {
                        allResults.append(GraphQueryBuilder<Item>.GraphEdge(
                            from: r.from, edge: edgeIRI, to: r.to
                        ))
                    }
                }
            }
        }

        // Phase 2: Inverse expansion
        if includeInverse {
            if let inverseRole = ontologyContext.inverseProperty(of: edgeIRI) {
                // Query the inverse with swapped from/to
                var invQuery = base.edge(inverseRole)
                if let f = trackedFrom { invQuery = invQuery.to(f) }
                if let t = trackedTo { invQuery = invQuery.from(t) }
                let invResults = try await invQuery.execute()
                for r in invResults {
                    // Map back: inverse means swap from/to
                    let key = EdgeKey(from: r.to, edge: edgeIRI, to: r.from)
                    if seen.insert(key).inserted {
                        allResults.append(GraphQueryBuilder<Item>.GraphEdge(
                            from: r.to, edge: edgeIRI, to: r.from
                        ))
                    }
                }
            }
        }

        // Phase 3: Transitive closure
        if expandTransitive && ontologyContext.isTransitive(edgeIRI) {
            allResults = try await expandTransitiveClosure(
                baseResults: allResults,
                edgeIRI: edgeIRI,
                seen: &seen
            )
        }

        return allResults
    }

    /// Transitive closure expansion via BFS + adjacency-based Warshall
    ///
    /// Two-phase algorithm:
    /// 1. **BFS discovery**: Explore outgoing edges from frontier nodes,
    ///    expanding the explicit edge set depth by depth.
    /// 2. **Warshall fixed-point**: Compute full transitive closure over
    ///    the adjacency list until no new pairs are discovered.
    ///
    /// Complexity: O(D × F × Q + N³) where D = depth, F = frontier size,
    /// Q = query cost, N = number of distinct nodes. The Warshall phase
    /// is O(N³) worst-case but operates on a compact adjacency map.
    ///
    /// Reference: Warshall, S. (1962). "A Theorem on Boolean Matrices"
    private func expandTransitiveClosure(
        baseResults: [GraphQueryBuilder<Item>.GraphEdge],
        edgeIRI: String,
        seen: inout Set<EdgeKey>
    ) async throws -> [GraphQueryBuilder<Item>.GraphEdge] {
        var allResults = baseResults

        // Phase 1: BFS to discover all explicit edges reachable from current results
        var frontier = Set<String>()
        for r in allResults where r.edge == edgeIRI {
            frontier.insert(r.to)
        }

        // Adjacency map: source → set of targets (for Warshall phase)
        var adjacency: [String: Set<String>] = [:]
        for r in allResults where r.edge == edgeIRI {
            adjacency[r.from, default: []].insert(r.to)
        }

        var depth = 0
        while !frontier.isEmpty && depth < maxTransitiveDepth {
            var nextFrontier = Set<String>()

            for node in frontier {
                let results = try await base.from(node).edge(edgeIRI).execute()
                for r in results {
                    let key = EdgeKey(from: r.from, edge: edgeIRI, to: r.to)
                    if seen.insert(key).inserted {
                        allResults.append(GraphQueryBuilder<Item>.GraphEdge(
                            from: r.from, edge: edgeIRI, to: r.to
                        ))
                        adjacency[r.from, default: []].insert(r.to)
                        nextFrontier.insert(r.to)
                    }
                }
            }

            frontier = nextFrontier
            depth += 1
        }

        // Phase 2: Warshall fixed-point on adjacency map
        // For each intermediate node k, for each pair (i, k) and (k, j), add (i, j).
        // Repeat until no new edges are added.
        // nodes is fixed: Warshall adds only new edges between existing nodes,
        // no new nodes are introduced, so recomputation is unnecessary.
        let nodes = Array(Set(adjacency.keys).union(adjacency.values.flatMap { $0 }))
        var changed = true
        while changed {
            changed = false
            for k in nodes {
                guard let kTargets = adjacency[k] else { continue }
                for i in nodes {
                    guard let iTargets = adjacency[i], iTargets.contains(k) else { continue }
                    for j in kTargets {
                        let key = EdgeKey(from: i, edge: edgeIRI, to: j)
                        if seen.insert(key).inserted {
                            allResults.append(GraphQueryBuilder<Item>.GraphEdge(
                                from: i, edge: edgeIRI, to: j
                            ))
                            adjacency[i, default: []].insert(j)
                            changed = true
                        }
                    }
                }
            }
        }

        return allResults
    }

    // MARK: - Reasoning-Specific Queries

    /// Find all individuals of a given type (including inferred types)
    ///
    /// - Parameter classIRI: The class IRI
    /// - Returns: Set of individual IRIs
    public func instancesOf(_ classIRI: String) -> Set<String> {
        let classExpr = OWLClassExpression.named(classIRI)
        return reasoner.instances(of: classExpr)
    }

    /// Find all types of an individual (including inferred types)
    ///
    /// - Parameter individualIRI: The individual IRI
    /// - Returns: Set of class IRIs
    public func typesOf(_ individualIRI: String) -> Set<String> {
        reasoner.types(of: individualIRI)
    }

    /// Check if an edge exists (including inferred edges)
    ///
    /// - Parameters:
    ///   - from: Source individual IRI
    ///   - edge: Property IRI
    ///   - to: Target individual IRI
    /// - Returns: true if the edge exists or can be inferred
    public func edgeExists(from: String, edge: String, to: String) -> Bool {
        let reachable = reasoner.reachableIndividuals(
            from: from,
            via: edge,
            includeInferred: includeInferred
        )
        return reachable.contains(to)
    }
}

// MARK: - GraphQueryBuilder Extension

extension GraphQueryBuilder {
    /// Add reasoning support to this query builder
    ///
    /// - Parameter reasoner: The OWL reasoner to use
    /// - Returns: A ReasoningGraphQueryBuilder with inference capabilities
    ///
    /// **Example**:
    /// ```swift
    /// let results = try await builder
    ///     .withReasoning(reasoner)
    ///     .from("ex:Alice")
    ///     .edge("ex:ancestorOf")
    ///     .withTransitiveClosure()
    ///     .execute()
    /// ```
    public func withReasoning(_ reasoner: OWLReasoner) -> ReasoningGraphQueryBuilder<T> {
        ReasoningGraphQueryBuilder(base: self, reasoner: reasoner)
    }
}

// MARK: - Reasoning Query Result

/// Extended query result with reasoning information
public struct ReasoningQueryResult<T: Sendable>: Sendable {
    /// The query results
    public let results: [T]

    /// Statistics about inference
    public let inferenceStatistics: InferenceStatistics

    public struct InferenceStatistics: Sendable {
        public let explicitCount: Int
        public let inferredCount: Int
        public let totalCount: Int
        public let inferenceTime: TimeInterval

        public init(explicitCount: Int, inferredCount: Int, inferenceTime: TimeInterval) {
            self.explicitCount = explicitCount
            self.inferredCount = inferredCount
            self.totalCount = explicitCount + inferredCount
            self.inferenceTime = inferenceTime
        }
    }
}
