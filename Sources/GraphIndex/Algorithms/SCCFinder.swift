// SCCFinder.swift
// GraphIndex - Strongly Connected Components using Tarjan's Algorithm
//
// Finds all strongly connected components in a directed graph.
// Reference: Tarjan, R. E. (1972). "Depth-first search and linear graph algorithms"
// SIAM Journal on Computing, 1(2), 146-160.

import StorageKit
import DatabaseKit
import DatabaseEngine
import Synchronization

/// Configuration for SCC algorithm
public struct SCCConfiguration: Sendable {
    /// Maximum number of components to find
    public var maxComponents: Int

    /// Maximum number of nodes to explore
    public var maxNodes: Int

    /// Batch size for reading edges
    public var batchSize: Int

    /// Default configuration
    public static let `default` = SCCConfiguration(
        maxComponents: 10000,
        maxNodes: 100_000,
        batchSize: 100
    )

    public init(
        maxComponents: Int = 10000,
        maxNodes: Int = 100_000,
        batchSize: Int = 100
    ) {
        self.maxComponents = Swift.max(1, maxComponents)
        self.maxNodes = Swift.max(1, maxNodes)
        self.batchSize = Swift.max(1, batchSize)
    }
}

/// Result of SCC computation
public struct SCCResult: Sendable {
    /// All strongly connected components (each component is a list of node IDs)
    public let components: [[GraphIdentity]]

    /// Mapping from node ID to component index
    public let nodeToComponent: [GraphIdentity: Int]

    /// Whether the graph is a DAG (all components have size 1)
    public var isDAG: Bool {
        components.allSatisfy { $0.count == 1 }
    }

    /// Size of the largest component
    public var largestComponentSize: Int {
        components.map { $0.count }.max() ?? 0
    }

    /// Number of components
    public var componentCount: Int {
        components.count
    }

    /// Number of nodes explored
    public let nodesExplored: Int

    /// Duration in nanoseconds
    public let durationNs: UInt64

    /// Reason if search was incomplete
    public let limitReason: LimitReason?

    /// Whether all components were found.
    public var isComplete: Bool { limitReason == nil }

    public init(
        components: [[GraphIdentity]],
        nodeToComponent: [GraphIdentity: Int],
        nodesExplored: Int,
        durationNs: UInt64,
        limitReason: LimitReason? = nil
    ) {
        self.components = components
        self.nodeToComponent = nodeToComponent
        self.nodesExplored = nodesExplored
        self.durationNs = durationNs
        self.limitReason = limitReason
    }
}

/// Condensation graph (DAG of SCCs)
public struct CondensationGraph: Sendable {
    /// Edges between components (from component index -> to component indices)
    public let edges: [Int: Set<Int>]

    /// Component sizes
    public let componentSizes: [Int]

    /// Number of edges in condensation graph
    public var edgeCount: Int {
        edges.values.reduce(0) { $0 + $1.count }
    }
}

/// Finds Strongly Connected Components in a directed graph
///
/// Uses Tarjan's algorithm with O(V+E) time complexity.
///
/// **Thread-safety**: Safe to use concurrently.
///
/// **Usage**:
/// ```swift
/// let sccFinder = SCCFinder(container: container, scanner: scanner)
/// let result = try await sccFinder.findSCCs(for: Edge.self)
/// for (index, component) in result.components.enumerated() {
///     print("Component \(index): \(component)")
/// }
/// ```
public final class SCCFinder: Sendable {

    // MARK: - Properties

    private let snapshot: GraphReadSnapshot
    private let scanner: GraphEdgeScanner
    private let configuration: SCCConfiguration
    private let workBudget: GraphAlgorithmWorkBudget?

    // MARK: - Initialization

    package init(
        snapshot: GraphReadSnapshot,
        scanner: GraphEdgeScanner,
        configuration: SCCConfiguration = .default
    ) {
        self.snapshot = snapshot
        self.scanner = scanner
        self.configuration = configuration
        self.workBudget = snapshot.workBudget
    }

    // MARK: - Public API

    /// Find all strongly connected components
    ///
    /// - Parameter edgeLabel: Optional edge label filter
    /// - Returns: SCC result with all components
    public func findSCCs(edgeLabel: GraphIdentity? = nil) async throws -> SCCResult {
        try await computeSCCs(edgeLabel: edgeLabel).result
    }

    private struct SCCComputation: Sendable {
        let result: SCCResult
        let graph: MaterializedGraphSnapshot
    }

    private func computeSCCs(
        edgeLabel: GraphIdentity?
    ) async throws -> SCCComputation {
        let startTime = snapshot.clock.now()

        let load = try await MaterializedGraphSnapshotBuilder.load(
            scanner: scanner,
            edgeLabel: edgeLabel,
            snapshot: snapshot,
            maximumNodes: configuration.maxNodes
        )
        if let limitReason = load.limitReason {
            return SCCComputation(
                result: SCCResult(
                    components: [],
                    nodeToComponent: [:],
                    nodesExplored: load.graph.nodes.count,
                    durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                    limitReason: limitReason
                ),
                graph: load.graph
            )
        }

        let result = try await runTarjan(
            graph: load.graph,
            initialLimitReason: load.limitReason
        )

        let endTime = snapshot.clock.now()
        let durationNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds

        return SCCComputation(
            result: SCCResult(
                components: result.components,
                nodeToComponent: result.nodeToComponent,
                nodesExplored: result.nodesExplored,
                durationNs: durationNs,
                limitReason: result.limitReason
            ),
            graph: load.graph
        )
    }

    /// Check if two nodes are in the same SCC (strongly connected)
    ///
    /// - Parameters:
    ///   - from: Source node
    ///   - to: Target node
    ///   - edgeLabel: Optional edge label filter
    /// - Returns: True if nodes are strongly connected
    public func isStronglyConnected(
        from: GraphIdentity,
        to: GraphIdentity,
        edgeLabel: GraphIdentity? = nil
    ) async throws -> Bool {
        let load = try await MaterializedGraphSnapshotBuilder.load(
            scanner: scanner,
            edgeLabel: edgeLabel,
            snapshot: snapshot,
            maximumNodes: configuration.maxNodes
        )
        if let limitReason = load.limitReason {
            throw SCCError.incomplete(limitReason)
        }
        // First check if there's a path from -> to
        let forwardPath = try await hasPath(
            from: from,
            to: to,
            graph: load.graph
        )
        if let limitReason = forwardPath.limitReason {
            throw SCCError.incomplete(limitReason)
        }
        guard forwardPath.exists else { return false }

        // Then check if there's a path to -> from
        let backwardPath = try await hasPath(
            from: to,
            to: from,
            graph: load.graph
        )
        if let limitReason = backwardPath.limitReason {
            throw SCCError.incomplete(limitReason)
        }
        return backwardPath.exists
    }

    /// Build the condensation graph (DAG of SCCs)
    ///
    /// - Parameter edgeLabel: Optional edge label filter
    /// - Returns: Condensation graph structure
    public func condensationGraph(edgeLabel: GraphIdentity? = nil) async throws -> CondensationGraph {
        let computation = try await computeSCCs(edgeLabel: edgeLabel)
        let sccResult = computation.result
        if let limitReason = sccResult.limitReason {
            throw SCCError.incomplete(limitReason)
        }

        var edges: [Int: Set<Int>] = [:]

        // Initialize edge sets for all components
        for index in 0..<sccResult.componentCount {
            edges[index] = []
        }

        for edge in computation.graph.edges {
            guard try consumeWork() else {
                throw incompleteWorkError()
            }
            guard let componentIndex = sccResult.nodeToComponent[edge.source],
                  let neighborComponent = sccResult.nodeToComponent[edge.target] else {
                throw SCCError.inconsistentState(
                    "condensation edge references an unknown component"
                )
            }
            if neighborComponent != componentIndex {
                guard edges[componentIndex] != nil else {
                    throw SCCError.inconsistentState(
                        "missing condensation component \(componentIndex)"
                    )
                }
                edges[componentIndex]?.insert(neighborComponent)
            }
        }

        let componentSizes = sccResult.components.map { $0.count }

        return CondensationGraph(
            edges: edges,
            componentSizes: componentSizes
        )
    }

    // MARK: - Tarjan's Algorithm

    /// Internal state for Tarjan's algorithm.
    private struct TarjanState {
        var index: Int = 0
        var nodeIndex: [GraphIdentity: Int] = [:]
        var nodeLowLink: [GraphIdentity: Int] = [:]
        var onStack: Set<GraphIdentity> = []
        var stack: [GraphIdentity] = []
        var components: [[GraphIdentity]] = []
        var nodeToComponent: [GraphIdentity: Int] = [:]
        var nodesExplored: Int = 0
        var limitReason: LimitReason?
        var reachedComponentLimit = false

        // Neighbors cache to avoid repeated lookups
        var neighborsCache: [GraphIdentity: [GraphIdentity]] = [:]
    }

    private struct TarjanResult {
        let components: [[GraphIdentity]]
        let nodeToComponent: [GraphIdentity: Int]
        let nodesExplored: Int
        let limitReason: LimitReason?
    }

    private func runTarjan(
        graph: MaterializedGraphSnapshot,
        initialLimitReason: LimitReason?
    ) async throws -> TarjanResult {
        let nodes = graph.nodes
        var neighborsCache: [GraphIdentity: [GraphIdentity]] = [:]
        var nodesExplored = 0

        for node in nodes.sorted() {
            guard try consumeWork() else {
                return TarjanResult(
                    components: [],
                    nodeToComponent: [:],
                    nodesExplored: nodesExplored,
                    limitReason: initialLimitReason ?? workBudget?.limitReason
                )
            }

            var neighbors: [GraphIdentity] = []
            for edge in graph.outgoingNeighbors(of: node) {
                guard try consumeWork() else {
                    return TarjanResult(
                        components: [],
                        nodeToComponent: [:],
                        nodesExplored: nodesExplored,
                        limitReason: initialLimitReason ?? workBudget?.limitReason
                    )
                }
                if nodes.contains(edge.target) {
                    neighbors.append(edge.target)
                }
            }

            neighborsCache[node] = neighbors
            nodesExplored += 1
        }

        var state = TarjanState()
        state.neighborsCache = neighborsCache
        state.nodesExplored = nodesExplored
        state.limitReason = initialLimitReason

        // Run Tarjan's DFS
        for node in nodes.sorted() {
            guard state.nodeIndex[node] == nil else { continue }
            guard state.components.count < configuration.maxComponents else {
                recordComponentLimit(state: &state)
                break
            }
            try strongConnect(node, state: &state)
            if state.reachedComponentLimit { break }
        }

        return TarjanResult(
            components: state.components,
            nodeToComponent: state.nodeToComponent,
            nodesExplored: state.nodesExplored,
            limitReason: state.limitReason
        )
    }

    /// Core of Tarjan's algorithm - iterative version to avoid stack overflow
    private func strongConnect(_ start: GraphIdentity, state: inout TarjanState) throws {
        // Use explicit stack to avoid recursion depth issues
        var callStack: [(node: GraphIdentity, phase: Int, neighborIndex: Int)] = [(start, 0, 0)]

        while !callStack.isEmpty {
            let (node, phase, neighborIndex) = callStack.removeLast()

            switch phase {
            case 0:
                // Initialize node
                state.nodeIndex[node] = state.index
                state.nodeLowLink[node] = state.index
                state.index += 1
                state.stack.append(node)
                state.onStack.insert(node)

                // Move to phase 1 (process neighbors)
                callStack.append((node, 1, 0))

            case 1:
                // Process neighbors
                guard let neighbors = state.neighborsCache[node] else {
                    throw SCCError.inconsistentState(
                        "missing cached neighbors for Tarjan node"
                    )
                }

                if neighborIndex < neighbors.count {
                    let neighbor = neighbors[neighborIndex]

                    if state.nodeIndex[neighbor] == nil {
                        // Neighbor not visited, recurse
                        callStack.append((node, 2, neighborIndex))  // Resume after recursive call
                        callStack.append((neighbor, 0, 0))  // Start DFS on neighbor
                    } else if state.onStack.contains(neighbor) {
                        // Neighbor is on stack, update lowlink
                        guard let lowLink = state.nodeLowLink[node],
                              let neighborIndex = state.nodeIndex[neighbor] else {
                            throw SCCError.inconsistentState("missing Tarjan index state")
                        }
                        state.nodeLowLink[node] = Swift.min(lowLink, neighborIndex)
                        // Continue to next neighbor
                        callStack.append((node, 1, neighborIndex + 1))
                    } else {
                        // Continue to next neighbor
                        callStack.append((node, 1, neighborIndex + 1))
                    }
                } else {
                    // All neighbors processed, check if root of SCC
                    guard let lowLink = state.nodeLowLink[node],
                          let nodeIndex = state.nodeIndex[node] else {
                        throw SCCError.inconsistentState("missing Tarjan root state")
                    }
                    if lowLink == nodeIndex {
                        guard state.components.count < configuration.maxComponents else {
                            recordComponentLimit(state: &state)
                            callStack.removeAll(keepingCapacity: false)
                            continue
                        }

                        // Start new SCC
                        var component: [GraphIdentity] = []
                        repeat {
                            guard let w = state.stack.popLast() else {
                                throw SCCError.inconsistentState("Tarjan stack underflow")
                            }
                            state.onStack.remove(w)
                            component.append(w)
                            state.nodeToComponent[w] = state.components.count
                        } while component.last != node

                        state.components.append(component)
                    }
                }

            case 2:
                // Resume after recursive call
                guard let neighbors = state.neighborsCache[node] else {
                    throw SCCError.inconsistentState(
                        "missing cached neighbors for resumed Tarjan node"
                    )
                }
                guard neighbors.indices.contains(neighborIndex) else {
                    throw SCCError.inconsistentState("invalid Tarjan neighbor index")
                }
                let neighbor = neighbors[neighborIndex]

                // Update lowlink from child
                guard let lowLink = state.nodeLowLink[node],
                      let neighborLowLink = state.nodeLowLink[neighbor] else {
                    throw SCCError.inconsistentState("missing Tarjan low-link state")
                }
                state.nodeLowLink[node] = Swift.min(lowLink, neighborLowLink)

                // Continue to next neighbor
                callStack.append((node, 1, neighborIndex + 1))

            default:
                break
            }
        }
    }

    // MARK: - Helper Methods

    private func recordComponentLimit(state: inout TarjanState) {
        state.reachedComponentLimit = true
        guard state.limitReason == nil else { return }
        state.limitReason = .maxResultsReached(
            returned: state.components.count,
            limit: configuration.maxComponents
        )
    }

    private func consumeWork(_ units: UInt64 = 1) throws -> Bool {
        try workBudget?.consume(units) ?? true
    }

    private func incompleteWorkError() -> SCCError {
        guard let reason = workBudget?.limitReason else {
            return .inconsistentState("work budget stopped without a limit reason")
        }
        return .incomplete(reason)
    }

    private struct PathCheck: Sendable {
        let exists: Bool
        let limitReason: LimitReason?
    }

    /// Check if there's a path from source to target (BFS)
    private func hasPath(
        from source: GraphIdentity,
        to target: GraphIdentity,
        graph: MaterializedGraphSnapshot
    ) async throws -> PathCheck {
        if source == target { return PathCheck(exists: true, limitReason: nil) }

        let maxNodes = configuration.maxNodes

        var visited = Set<GraphIdentity>()
        var queue = [source]
        var queueIndex = 0
        visited.insert(source)

        while queueIndex < queue.count {
            guard try consumeWork() else {
                return PathCheck(
                    exists: false,
                    limitReason: workBudget?.limitReason
                )
            }

            let current = queue[queueIndex]
            queueIndex += 1

            for neighbor in graph.outgoingNeighbors(of: current) {
                guard try consumeWork() else {
                    return PathCheck(
                        exists: false,
                        limitReason: workBudget?.limitReason
                    )
                }
                if neighbor.target == target {
                    return PathCheck(exists: true, limitReason: nil)
                }

                if !visited.contains(neighbor.target) {
                    visited.insert(neighbor.target)
                    queue.append(neighbor.target)

                    // Limit search to prevent infinite loops.
                    if visited.count > maxNodes {
                        return PathCheck(
                            exists: false,
                            limitReason: .maxNodesReached(
                                explored: maxNodes,
                                limit: maxNodes
                            )
                        )
                    }
                }
            }
        }

        return PathCheck(exists: false, limitReason: nil)
    }
}

// MARK: - Transaction-scoped Query

/// Public SCC query that creates the finder only inside a transaction scope.
///
/// A storage transaction never escapes this type, so every read performed by
/// one method observes exactly one read version.
public struct StronglyConnectedComponentsQuery<Edge: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let index: DeclaredPropertyGraphIndex
    private let configuration: SCCConfiguration

    package init(
        queryContext: IndexQueryContext,
        index: DeclaredPropertyGraphIndex,
        configuration: SCCConfiguration
    ) {
        self.queryContext = queryContext
        self.index = index
        self.configuration = configuration
    }

    public func find(edgeLabel: String? = nil) async throws -> SCCResult {
        return try await withFinder(
            missing: {
                SCCResult(
                    components: [],
                    nodeToComponent: [:],
                    nodesExplored: 0,
                    durationNs: 0
                )
            }
        ) { finder in
            try await finder.findSCCs(
                edgeLabel: edgeLabel.map(GraphIdentity.identifier)
            )
        }
    }

    public func containsSameComponent(
        _ source: String,
        _ target: String,
        edgeLabel: String? = nil
    ) async throws -> Bool {
        return try await withFinder(missing: { false }) { finder in
            try await finder.isStronglyConnected(
                from: .identifier(source),
                to: .identifier(target),
                edgeLabel: edgeLabel.map(GraphIdentity.identifier)
            )
        }
    }

    public func condensationGraph(
        edgeLabel: String? = nil
    ) async throws -> CondensationGraph {
        return try await withFinder(
            missing: {
                CondensationGraph(edges: [:], componentSizes: [])
            }
        ) { finder in
            try await finder.condensationGraph(
                edgeLabel: edgeLabel.map(GraphIdentity.identifier)
            )
        }
    }

    private func withFinder<Result: Sendable>(
        missing: @Sendable @escaping () -> Result,
        _ operation: @Sendable @escaping (SCCFinder) async throws -> Result
    ) async throws -> Result {
        return try await queryContext.withTransaction { transaction in
            guard let resolvedIndex = try await PropertyGraphIndexResolver
                .resolve(
                    index,
                    for: Edge.self,
                    in: queryContext,
                    transaction: transaction
                ) else {
                return missing()
            }
            let snapshot = GraphReadSnapshot(
                transaction: transaction,
                monotonicClock: queryContext.context.container.monotonicClock
            )
            let finder = SCCFinder(
                snapshot: snapshot,
                scanner: resolvedIndex.scanner(snapshot: snapshot),
                configuration: configuration
            )
            return try await operation(finder)
        }
    }
}

extension DatabaseContext {
    /// Select the entity's only property-graph index for SCC operations.
    public func stronglyConnectedComponents<Edge: Persistable>(
        for type: Edge.Type,
        configuration: SCCConfiguration = .default
    ) throws -> StronglyConnectedComponentsQuery<Edge> {
        StronglyConnectedComponentsQuery(
            queryContext: indexQueryContext,
            index: try PropertyGraphIndexResolver.unique(
                for: type,
                in: indexQueryContext
            ),
            configuration: configuration
        )
    }

    /// Select an entity-owned property-graph index by exact name.
    public func stronglyConnectedComponents<Edge: Persistable>(
        for type: Edge.Type,
        indexNamed indexName: String,
        configuration: SCCConfiguration = .default
    ) throws -> StronglyConnectedComponentsQuery<Edge> {
        StronglyConnectedComponentsQuery(
            queryContext: indexQueryContext,
            index: try PropertyGraphIndexResolver.exact(
                named: indexName,
                for: type,
                in: indexQueryContext
            ),
            configuration: configuration
        )
    }
}

// MARK: - SCC Errors

/// Errors for SCC operations
public enum SCCError: Error, CustomStringConvertible, Sendable {
    case graphIndexNotFound
    case incomplete(LimitReason)
    case inconsistentState(String)

    public var description: String {
        switch self {
        case .graphIndexNotFound:
            return "No graph index found on the type. Add a GraphIndexKind to the type."
        case .incomplete(let reason):
            return "SCC computation is incomplete: \(reason)"
        case .inconsistentState(let message):
            return "SCC internal state is inconsistent: \(message)"
        }
    }
}
