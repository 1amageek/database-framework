// SCCFinder.swift
// GraphIndex - Strongly Connected Components using Tarjan's Algorithm
//
// Finds all strongly connected components in a directed graph.
// Reference: Tarjan, R. E. (1972). "Depth-first search and linear graph algorithms"
// SIAM Journal on Computing, 1(2), 146-160.

import Foundation
import FoundationDB
import Core
import DatabaseEngine
import Graph
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
        self.maxComponents = maxComponents
        self.maxNodes = maxNodes
        self.batchSize = batchSize
    }
}

/// Reason for incomplete SCC search
public enum SCCLimitReason: Sendable, Equatable {
    case maxNodesReached
    case maxComponentsReached
    case timeout
}

/// Result of SCC computation
public struct SCCResult: Sendable {
    /// All strongly connected components (each component is a list of node IDs)
    public let components: [[String]]

    /// Mapping from node ID to component index
    public let nodeToComponent: [String: Int]

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

    /// Whether all components were found
    public let isComplete: Bool

    /// Reason if search was incomplete
    public let limitReason: SCCLimitReason?

    public init(
        components: [[String]],
        nodeToComponent: [String: Int],
        nodesExplored: Int,
        durationNs: UInt64,
        isComplete: Bool,
        limitReason: SCCLimitReason? = nil
    ) {
        self.components = components
        self.nodeToComponent = nodeToComponent
        self.nodesExplored = nodesExplored
        self.durationNs = durationNs
        self.isComplete = isComplete
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

    nonisolated(unsafe) private let database: any DatabaseProtocol
    private let scanner: GraphEdgeScanner
    private let configuration: SCCConfiguration

    // MARK: - Initialization

    public init(
        database: any DatabaseProtocol,
        scanner: GraphEdgeScanner,
        configuration: SCCConfiguration = .default
    ) {
        self.database = database
        self.scanner = scanner
        self.configuration = configuration
    }

    // MARK: - Public API

    /// Find all strongly connected components
    ///
    /// - Parameter edgeLabel: Optional edge label filter
    /// - Returns: SCC result with all components
    public func findSCCs(edgeLabel: String? = nil) async throws -> SCCResult {
        let startTime = DispatchTime.now()

        // Collect all nodes first
        let allNodes = try await collectAllNodes(edgeLabel: edgeLabel)

        // Run Tarjan's algorithm
        let result = try await runTarjan(nodes: allNodes, edgeLabel: edgeLabel)

        let endTime = DispatchTime.now()
        let durationNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds

        return SCCResult(
            components: result.components,
            nodeToComponent: result.nodeToComponent,
            nodesExplored: result.nodesExplored,
            durationNs: durationNs,
            isComplete: result.isComplete,
            limitReason: result.limitReason
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
        from: String,
        to: String,
        edgeLabel: String? = nil
    ) async throws -> Bool {
        // First check if there's a path from -> to
        let forwardPath = try await hasPath(from: from, to: to, edgeLabel: edgeLabel)
        guard forwardPath else { return false }

        // Then check if there's a path to -> from
        let backwardPath = try await hasPath(from: to, to: from, edgeLabel: edgeLabel)
        return backwardPath
    }

    /// Build the condensation graph (DAG of SCCs)
    ///
    /// - Parameter edgeLabel: Optional edge label filter
    /// - Returns: Condensation graph structure
    public func condensationGraph(edgeLabel: String? = nil) async throws -> CondensationGraph {
        let sccResult = try await findSCCs(edgeLabel: edgeLabel)

        // Collect condensation edges inside the transaction
        let condensationEdges: [Int: Set<Int>] = try await database.withTransaction(configuration: .default) { transaction in
            var edges: [Int: Set<Int>] = [:]

            // Initialize edge sets for all components
            for i in 0..<sccResult.componentCount {
                edges[i] = []
            }

            // For each edge in the original graph, add corresponding condensation edge
            for (node, componentIdx) in sccResult.nodeToComponent {
                let neighbors = try await self.scanner.scanAllOutgoing(
                    from: node,
                    edgeLabel: edgeLabel,
                    transaction: transaction
                )

                for neighbor in neighbors {
                    if let neighborComponent = sccResult.nodeToComponent[neighbor.target] {
                        // Only add edge if components are different
                        if neighborComponent != componentIdx {
                            edges[componentIdx]?.insert(neighborComponent)
                        }
                    }
                }
            }

            return edges
        }

        let componentSizes = sccResult.components.map { $0.count }

        return CondensationGraph(
            edges: condensationEdges,
            componentSizes: componentSizes
        )
    }

    // MARK: - Tarjan's Algorithm

    /// Internal state for Tarjan's algorithm
    ///
    /// **Thread-Safety Note**: Marked as `@unchecked Sendable` because:
    /// 1. The state is created at the start of `runTarjan`
    /// 2. All mutations occur synchronously within that single method call
    /// 3. The state is never shared across concurrent contexts
    /// 4. After `runTarjan` returns, the state is no longer accessible
    ///
    /// This is safe because the entire lifecycle (create → mutate → consume)
    /// happens within a single synchronous execution path.
    private final class TarjanState: @unchecked Sendable {
        var index: Int = 0
        var nodeIndex: [String: Int] = [:]
        var nodeLowLink: [String: Int] = [:]
        var onStack: Set<String> = []
        var stack: [String] = []
        var components: [[String]] = []
        var nodeToComponent: [String: Int] = [:]
        var nodesExplored: Int = 0
        var isComplete: Bool = true
        var limitReason: SCCLimitReason?

        // Neighbors cache to avoid repeated lookups
        var neighborsCache: [String: [String]] = [:]
    }

    private struct TarjanResult {
        let components: [[String]]
        let nodeToComponent: [String: Int]
        let nodesExplored: Int
        let isComplete: Bool
        let limitReason: SCCLimitReason?
    }

    private func runTarjan(
        nodes: Set<String>,
        edgeLabel: String?
    ) async throws -> TarjanResult {
        let state = TarjanState()

        // Pre-fetch all edges
        try await database.withTransaction(configuration: .default) { transaction in
            for node in nodes {
                if state.nodesExplored >= self.configuration.maxNodes {
                    state.isComplete = false
                    state.limitReason = .maxNodesReached
                    break
                }

                let neighbors = try await self.scanner.scanAllOutgoing(
                    from: node,
                    edgeLabel: edgeLabel,
                    transaction: transaction
                )

                state.neighborsCache[node] = neighbors.map { $0.target }
                state.nodesExplored += 1
            }
        }

        // Run Tarjan's DFS
        for node in nodes {
            if state.components.count >= configuration.maxComponents {
                state.isComplete = false
                state.limitReason = .maxComponentsReached
                break
            }

            if state.nodeIndex[node] == nil {
                strongConnect(node, state: state)
            }
        }

        return TarjanResult(
            components: state.components,
            nodeToComponent: state.nodeToComponent,
            nodesExplored: state.nodesExplored,
            isComplete: state.isComplete,
            limitReason: state.limitReason
        )
    }

    /// Core of Tarjan's algorithm - iterative version to avoid stack overflow
    private func strongConnect(_ start: String, state: TarjanState) {
        // Use explicit stack to avoid recursion depth issues
        var callStack: [(node: String, phase: Int, neighborIndex: Int)] = [(start, 0, 0)]

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
                let neighbors = state.neighborsCache[node] ?? []

                if neighborIndex < neighbors.count {
                    let neighbor = neighbors[neighborIndex]

                    if state.nodeIndex[neighbor] == nil {
                        // Neighbor not visited, recurse
                        callStack.append((node, 2, neighborIndex))  // Resume after recursive call
                        callStack.append((neighbor, 0, 0))  // Start DFS on neighbor
                    } else if state.onStack.contains(neighbor) {
                        // Neighbor is on stack, update lowlink
                        state.nodeLowLink[node] = Swift.min(
                            state.nodeLowLink[node]!,
                            state.nodeIndex[neighbor]!
                        )
                        // Continue to next neighbor
                        callStack.append((node, 1, neighborIndex + 1))
                    } else {
                        // Continue to next neighbor
                        callStack.append((node, 1, neighborIndex + 1))
                    }
                } else {
                    // All neighbors processed, check if root of SCC
                    if state.nodeLowLink[node] == state.nodeIndex[node] {
                        // Start new SCC
                        var component: [String] = []
                        repeat {
                            let w = state.stack.removeLast()
                            state.onStack.remove(w)
                            component.append(w)
                            state.nodeToComponent[w] = state.components.count
                        } while component.last != node

                        state.components.append(component)
                    }
                }

            case 2:
                // Resume after recursive call
                let neighbors = state.neighborsCache[node] ?? []
                let neighbor = neighbors[neighborIndex]

                // Update lowlink from child
                state.nodeLowLink[node] = Swift.min(
                    state.nodeLowLink[node]!,
                    state.nodeLowLink[neighbor]!
                )

                // Continue to next neighbor
                callStack.append((node, 1, neighborIndex + 1))

            default:
                break
            }
        }
    }

    // MARK: - Helper Methods

    /// Collect all unique nodes from the graph
    private func collectAllNodes(edgeLabel: String? = nil) async throws -> Set<String> {
        let maxNodes = configuration.maxNodes

        return try await database.withTransaction(configuration: .default) { transaction in
            var nodes = Set<String>()

            // Scan all edges and collect both endpoints
            for try await edge in self.scanner.scanAllEdges(edgeLabel: edgeLabel, transaction: transaction) {
                if nodes.count >= maxNodes {
                    break
                }
                nodes.insert(edge.source)
                nodes.insert(edge.target)
            }

            return nodes
        }
    }

    /// Check if there's a path from source to target (BFS)
    private func hasPath(
        from source: String,
        to target: String,
        edgeLabel: String?
    ) async throws -> Bool {
        if source == target { return true }

        let maxNodes = configuration.maxNodes

        return try await database.withTransaction(configuration: .default) { transaction in
            var visited = Set<String>()
            var queue = [source]
            visited.insert(source)

            while !queue.isEmpty {
                let current = queue.removeFirst()

                let neighbors = try await self.scanner.scanAllOutgoing(
                    from: current,
                    edgeLabel: edgeLabel,
                    transaction: transaction
                )

                for neighbor in neighbors {
                    if neighbor.target == target {
                        return true
                    }

                    if !visited.contains(neighbor.target) {
                        visited.insert(neighbor.target)
                        queue.append(neighbor.target)

                        // Limit search to prevent infinite loops
                        if visited.count > maxNodes {
                            return false
                        }
                    }
                }
            }

            return false
        }
    }
}

// MARK: - FDBContext Extension

extension FDBContext {
    /// Create an SCC finder for the given edge type
    ///
    /// Uses the first graph index found on the type.
    ///
    /// **Usage**:
    /// ```swift
    /// let sccFinder = try await context.sccFinder(for: Edge.self)
    /// let result = try await sccFinder.findSCCs()
    ///
    /// print("Found \(result.componentCount) strongly connected components")
    /// print("Is DAG: \(result.isDAG)")
    /// ```
    ///
    /// - Parameters:
    ///   - type: The edge type with a graph index
    ///   - configuration: Optional configuration for SCC computation
    /// - Returns: SCC finder instance
    public func sccFinder<Edge: Persistable>(
        for type: Edge.Type,
        configuration: SCCConfiguration = .default
    ) async throws -> SCCFinder {
        // Find the graph index descriptor
        guard let descriptor = type.indexDescriptors.first(where: {
            $0.kindIdentifier == GraphIndexKind<Edge>.identifier
        }),
        let kind = descriptor.kind as? GraphIndexKind<Edge> else {
            throw SCCError.graphIndexNotFound
        }

        let typeSubspace = try await indexQueryContext.indexSubspace(for: type)
        let graphSubspace = typeSubspace.subspace(descriptor.name)
        let scanner = GraphEdgeScanner(indexSubspace: graphSubspace, strategy: kind.strategy)
        return SCCFinder(database: container.database, scanner: scanner, configuration: configuration)
    }
}

// MARK: - SCC Errors

/// Errors for SCC operations
public enum SCCError: Error, CustomStringConvertible, Sendable {
    case graphIndexNotFound

    public var description: String {
        switch self {
        case .graphIndexNotFound:
            return "No graph index found on the type. Add a GraphIndexKind to the type."
        }
    }
}
