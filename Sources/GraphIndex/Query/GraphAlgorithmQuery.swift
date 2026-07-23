// GraphAlgorithmQuery.swift
// GraphIndex - Unified entry point for graph algorithms
//
// Provides FDBContext extension for PageRank and Community Detection.

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseEngine
import StorageKit
import Graph

// MARK: - GraphAlgorithmEntryPoint

/// Entry point for graph algorithms (PageRank, Community Detection)
///
/// **Usage**:
/// ```swift
/// import GraphIndex
///
/// // PageRank
/// let pagerank = try await context.graphAlgorithm(Edge.self)
///     .index(\.source, \.label, \.target)
///     .pageRank()
///     .compute()
///
/// // Community Detection
/// let communities = try await context.graphAlgorithm(Edge.self)
///     .defaultIndex()
///     .communityDetection()
///     .detect()
/// ```
public struct GraphAlgorithmEntryPoint<T: Persistable>: Sendable {

    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// Specify the graph index fields
    ///
    /// - Parameters:
    ///   - from: KeyPath to the source/subject field
    ///   - edge: KeyPath to the edge/predicate field
    ///   - to: KeyPath to the target/object field
    /// - Returns: Graph algorithm builder
    public func index<V1, V2, V3>(
        _ from: KeyPath<T, V1>,
        _ edge: KeyPath<T, V2>,
        _ to: KeyPath<T, V3>
    ) throws -> GraphAlgorithmBuilder<T> {
        return GraphAlgorithmBuilder(
            queryContext: queryContext,
            index: try PropertyGraphIndexResolver.exact(
                signature: PropertyGraphIndexSignature(
                    sourceFieldName: T.fieldName(for: from),
                    labelFieldName: T.fieldName(for: edge),
                    targetFieldName: T.fieldName(for: to)
                ),
                for: T.self,
                in: queryContext
            )
        )
    }

    /// Select an entity-owned graph index by its exact declared name.
    public func index(named indexName: String) throws -> GraphAlgorithmBuilder<T> {
        GraphAlgorithmBuilder(
            queryContext: queryContext,
            index: try PropertyGraphIndexResolver.exact(
                named: indexName,
                for: T.self,
                in: queryContext
            )
        )
    }

    /// Use the default graph index (first GraphIndexKind found)
    ///
    /// - Returns: Graph algorithm builder configured with the default index
    public func defaultIndex() throws -> GraphAlgorithmBuilder<T> {
        return GraphAlgorithmBuilder(
            queryContext: queryContext,
            index: try PropertyGraphIndexResolver.unique(
                for: T.self,
                in: queryContext
            )
        )
    }
}

// MARK: - GraphAlgorithmBuilder

/// Builder for selecting and configuring graph algorithms
public struct GraphAlgorithmBuilder<T: Persistable>: Sendable {

    private let queryContext: IndexQueryContext
    private let index: DeclaredPropertyGraphIndex

    internal init(
        queryContext: IndexQueryContext,
        index: DeclaredPropertyGraphIndex
    ) {
        self.queryContext = queryContext
        self.index = index
    }

    /// Configure PageRank algorithm
    ///
    /// - Parameter configuration: Optional configuration (defaults to .default)
    /// - Returns: PageRank query builder
    public func pageRank(
        configuration: PageRankConfiguration = .default
    ) -> PageRankQueryBuilder<T> {
        PageRankQueryBuilder(
            queryContext: queryContext,
            index: index,
            configuration: configuration
        )
    }

    /// Configure Community Detection algorithm
    ///
    /// - Parameter configuration: Optional configuration (defaults to .default)
    /// - Returns: Community detection query builder
    public func communityDetection(
        configuration: CommunityDetectionConfiguration = .default
    ) -> CommunityDetectionQueryBuilder<T> {
        CommunityDetectionQueryBuilder(
            queryContext: queryContext,
            index: index,
            configuration: configuration
        )
    }
}

// MARK: - PageRankQueryBuilder

/// Query builder for PageRank computation
///
/// **Usage**:
/// ```swift
/// let result = try await context.graphAlgorithm(Edge.self)
///     .defaultIndex()
///     .pageRank(configuration: .default)
///     .edgeLabel("follows")
///     .compute()
///
/// for (nodeID, score) in result.topK(10) {
///     print("\(nodeID): \(score)")
/// }
/// ```
public struct PageRankQueryBuilder<T: Persistable>: Sendable {

    private let queryContext: IndexQueryContext
    private let index: DeclaredPropertyGraphIndex
    private var configuration: PageRankConfiguration
    private var edgeLabelFilter: String?

    internal init(
        queryContext: IndexQueryContext,
        index: DeclaredPropertyGraphIndex,
        configuration: PageRankConfiguration
    ) {
        self.queryContext = queryContext
        self.index = index
        self.configuration = configuration
    }

    /// Filter by edge label
    ///
    /// - Parameter label: Edge label to match
    /// - Returns: Updated builder
    public func edgeLabel(_ label: String) -> Self {
        var copy = self
        copy.edgeLabelFilter = label
        return copy
    }

    /// Set damping factor
    ///
    /// - Parameter factor: Damping factor (typically 0.85)
    /// - Returns: Updated builder
    public func dampingFactor(_ factor: Double) -> Self {
        var copy = self
        copy.configuration = PageRankConfiguration(
            dampingFactor: factor,
            maxIterations: configuration.maxIterations,
            convergenceThreshold: configuration.convergenceThreshold,
            batchSize: configuration.batchSize
        )
        return copy
    }

    /// Set maximum iterations
    ///
    /// - Parameter iterations: Maximum number of iterations
    /// - Returns: Updated builder
    public func maxIterations(_ iterations: Int) -> Self {
        var copy = self
        copy.configuration = PageRankConfiguration(
            dampingFactor: configuration.dampingFactor,
            maxIterations: iterations,
            convergenceThreshold: configuration.convergenceThreshold,
            batchSize: configuration.batchSize
        )
        return copy
    }

    /// Compute PageRank
    ///
    /// - Returns: PageRankResult with scores for all nodes
    public func compute() async throws -> PageRankResult {
        let resolvedIndex = try await PropertyGraphIndexResolver.resolve(
            index,
            for: T.self,
            in: queryContext
        )

        return try await queryContext.withTransaction { transaction in
            let computer = PageRankComputer(
                snapshot: GraphReadSnapshot(transaction: transaction),
                subspace: resolvedIndex.indexSubspace,
                strategy: resolvedIndex.metadata.strategy,
                configuration: configuration
            )
            return try await computer.compute(
                edgeLabel: edgeLabelFilter.map(GraphIdentity.identifier)
            )
        }
    }

    /// Compute personalized PageRank from a specific node
    ///
    /// - Parameter startNode: Starting node for personalized PageRank
    /// - Returns: PageRankResult with scores relative to startNode
    public func computePersonalized(from startNode: String) async throws -> PageRankResult {
        let resolvedIndex = try await PropertyGraphIndexResolver.resolve(
            index,
            for: T.self,
            in: queryContext
        )

        return try await queryContext.withTransaction { transaction in
            let computer = PageRankComputer(
                snapshot: GraphReadSnapshot(transaction: transaction),
                subspace: resolvedIndex.indexSubspace,
                strategy: resolvedIndex.metadata.strategy,
                configuration: configuration
            )
            return try await computer.computePersonalized(
                from: .identifier(startNode),
                edgeLabel: edgeLabelFilter.map(GraphIdentity.identifier)
            )
        }
    }
}

// MARK: - CommunityDetectionQueryBuilder

/// Query builder for Community Detection
///
/// **Usage**:
/// ```swift
/// let result = try await context.graphAlgorithm(Edge.self)
///     .defaultIndex()
///     .communityDetection()
///     .edgeLabel("friends")
///     .detect()
///
/// print("Found \(result.communityCount) communities")
/// ```
public struct CommunityDetectionQueryBuilder<T: Persistable>: Sendable {

    private let queryContext: IndexQueryContext
    private let index: DeclaredPropertyGraphIndex
    private var configuration: CommunityDetectionConfiguration
    private var edgeLabelFilter: String?

    internal init(
        queryContext: IndexQueryContext,
        index: DeclaredPropertyGraphIndex,
        configuration: CommunityDetectionConfiguration
    ) {
        self.queryContext = queryContext
        self.index = index
        self.configuration = configuration
    }

    /// Filter by edge label
    ///
    /// - Parameter label: Edge label to match
    /// - Returns: Updated builder
    public func edgeLabel(_ label: String) -> Self {
        var copy = self
        copy.edgeLabelFilter = label
        return copy
    }

    /// Set maximum iterations
    ///
    /// - Parameter iterations: Maximum number of iterations
    /// - Returns: Updated builder
    public func maxIterations(_ iterations: Int) -> Self {
        var copy = self
        copy.configuration = CommunityDetectionConfiguration(
            maxIterations: iterations,
            batchSize: configuration.batchSize,
            computeModularity: configuration.computeModularity,
            minCommunitySize: configuration.minCommunitySize
        )
        return copy
    }

    /// Enable modularity computation
    ///
    /// - Returns: Updated builder
    public func withModularity() -> Self {
        var copy = self
        copy.configuration = CommunityDetectionConfiguration(
            maxIterations: configuration.maxIterations,
            batchSize: configuration.batchSize,
            computeModularity: true,
            minCommunitySize: configuration.minCommunitySize
        )
        return copy
    }

    /// Set minimum community size
    ///
    /// - Parameter size: Minimum members per community
    /// - Returns: Updated builder
    public func minCommunitySize(_ size: Int) -> Self {
        var copy = self
        copy.configuration = CommunityDetectionConfiguration(
            maxIterations: configuration.maxIterations,
            batchSize: configuration.batchSize,
            computeModularity: configuration.computeModularity,
            minCommunitySize: size
        )
        return copy
    }

    /// Detect communities
    ///
    /// - Returns: CommunityResult with node assignments
    public func detect() async throws -> CommunityResult {
        let resolvedIndex = try await PropertyGraphIndexResolver.resolve(
            index,
            for: T.self,
            in: queryContext
        )

        return try await queryContext.withTransaction { transaction in
            let detector = CommunityDetector(
                snapshot: GraphReadSnapshot(transaction: transaction),
                subspace: resolvedIndex.indexSubspace,
                strategy: resolvedIndex.metadata.strategy,
                configuration: configuration
            )
            return try await detector.detect(
                edgeLabel: edgeLabelFilter.map(GraphIdentity.identifier)
            )
        }
    }

    /// Detect community for a specific node
    ///
    /// - Parameters:
    ///   - node: Node to find community for
    ///   - maxHops: Maximum hops from node to consider
    /// - Returns: Set of node IDs in the same community
    public func detectLocal(for node: String, maxHops: Int = 3) async throws -> Set<String> {
        let resolvedIndex = try await PropertyGraphIndexResolver.resolve(
            index,
            for: T.self,
            in: queryContext
        )

        let identities = try await queryContext.withTransaction { transaction in
            let detector = CommunityDetector(
                snapshot: GraphReadSnapshot(transaction: transaction),
                subspace: resolvedIndex.indexSubspace,
                strategy: resolvedIndex.metadata.strategy,
                configuration: configuration
            )
            return try await detector.detectLocalCommunity(
                for: .identifier(node),
                maxHops: maxHops,
                edgeLabel: edgeLabelFilter.map(GraphIdentity.identifier)
            )
        }
        return try Set(identities.map { try $0.requirePropertyGraphIdentifier() })
    }
}

// MARK: - FDBContext Extension

extension FDBContext {
    /// Start a graph algorithm query (PageRank, Community Detection)
    ///
    /// **Usage**:
    /// ```swift
    /// import GraphIndex
    ///
    /// // PageRank
    /// let pagerank = try await context.graphAlgorithm(Edge.self)
    ///     .index(\.source, \.label, \.target)
    ///     .pageRank()
    ///     .compute()
    ///
    /// for (nodeID, score) in pagerank.topK(10) {
    ///     print("\(nodeID): \(score)")
    /// }
    ///
    /// // Community Detection
    /// let communities = try await context.graphAlgorithm(Edge.self)
    ///     .defaultIndex()
    ///     .communityDetection()
    ///     .detect()
    ///
    /// print("Found \(communities.communityCount) communities")
    /// ```
    ///
    /// - Parameter type: The Persistable type representing graph edges
    /// - Returns: Entry point for configuring graph algorithms
    public func graphAlgorithm<T: Persistable>(_ type: T.Type) -> GraphAlgorithmEntryPoint<T> {
        GraphAlgorithmEntryPoint(queryContext: indexQueryContext)
    }
}

// MARK: - Errors

/// Errors for graph algorithm operations
public enum GraphAlgorithmError: Error, CustomStringConvertible {
    case indexNotConfigured
    case indexNotFound(String)

    public var description: String {
        switch self {
        case .indexNotConfigured:
            return "Graph index not configured. Use .index() to specify fields or .defaultIndex()."
        case .indexNotFound(let name):
            return "Graph index not found: \(name)"
        }
    }
}
