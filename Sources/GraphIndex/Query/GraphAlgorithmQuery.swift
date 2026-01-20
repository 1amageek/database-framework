// GraphAlgorithmQuery.swift
// GraphIndex - Unified entry point for graph algorithms
//
// Provides FDBContext extension for PageRank and Community Detection.

import Foundation
import Core
import DatabaseEngine
import FoundationDB
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
    ) -> GraphAlgorithmBuilder<T> {
        let fromField = T.fieldName(for: from)
        let edgeField = T.fieldName(for: edge)
        let toField = T.fieldName(for: to)
        return GraphAlgorithmBuilder(
            queryContext: queryContext,
            fromFieldName: fromField,
            edgeFieldName: edgeField,
            toFieldName: toField
        )
    }

    /// Use the default graph index (first GraphIndexKind found)
    ///
    /// - Returns: Graph algorithm builder configured with the default index
    public func defaultIndex() -> GraphAlgorithmBuilder<T> {
        let descriptor = T.indexDescriptors.first { desc in
            desc.kindIdentifier == GraphIndexKind<T>.identifier
        }

        guard let desc = descriptor,
              let kind = desc.kind as? GraphIndexKind<T> else {
            return GraphAlgorithmBuilder(
                queryContext: queryContext,
                fromFieldName: "",
                edgeFieldName: "",
                toFieldName: ""
            )
        }

        return GraphAlgorithmBuilder(
            queryContext: queryContext,
            fromFieldName: kind.fromField,
            edgeFieldName: kind.edgeField,
            toFieldName: kind.toField
        )
    }
}

// MARK: - GraphAlgorithmBuilder

/// Builder for selecting and configuring graph algorithms
public struct GraphAlgorithmBuilder<T: Persistable>: Sendable {

    private let queryContext: IndexQueryContext
    private let fromFieldName: String
    private let edgeFieldName: String
    private let toFieldName: String

    internal init(
        queryContext: IndexQueryContext,
        fromFieldName: String,
        edgeFieldName: String,
        toFieldName: String
    ) {
        self.queryContext = queryContext
        self.fromFieldName = fromFieldName
        self.edgeFieldName = edgeFieldName
        self.toFieldName = toFieldName
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
            fromFieldName: fromFieldName,
            edgeFieldName: edgeFieldName,
            toFieldName: toFieldName,
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
            fromFieldName: fromFieldName,
            edgeFieldName: edgeFieldName,
            toFieldName: toFieldName,
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
    private let fromFieldName: String
    private let edgeFieldName: String
    private let toFieldName: String
    private var configuration: PageRankConfiguration
    private var edgeLabelFilter: String?

    internal init(
        queryContext: IndexQueryContext,
        fromFieldName: String,
        edgeFieldName: String,
        toFieldName: String,
        configuration: PageRankConfiguration
    ) {
        self.queryContext = queryContext
        self.fromFieldName = fromFieldName
        self.edgeFieldName = edgeFieldName
        self.toFieldName = toFieldName
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
        guard !fromFieldName.isEmpty else {
            throw GraphAlgorithmError.indexNotConfigured
        }

        let indexSubspace = try await getIndexSubspace()

        let computer = PageRankComputer<T>(
            database: queryContext.context.container.database,
            subspace: indexSubspace,
            configuration: configuration
        )

        return try await computer.compute(edgeLabel: edgeLabelFilter)
    }

    /// Compute personalized PageRank from a specific node
    ///
    /// - Parameter startNode: Starting node for personalized PageRank
    /// - Returns: PageRankResult with scores relative to startNode
    public func computePersonalized(from startNode: String) async throws -> PageRankResult {
        guard !fromFieldName.isEmpty else {
            throw GraphAlgorithmError.indexNotConfigured
        }

        let indexSubspace = try await getIndexSubspace()

        let computer = PageRankComputer<T>(
            database: queryContext.context.container.database,
            subspace: indexSubspace,
            configuration: configuration
        )

        return try await computer.computePersonalized(
            from: startNode,
            edgeLabel: edgeLabelFilter
        )
    }

    private func getIndexSubspace() async throws -> Subspace {
        let indexName = "\(T.persistableType)_graph_\(fromFieldName)_\(edgeFieldName)_\(toFieldName)"

        guard let _ = queryContext.schema.indexDescriptor(named: indexName) else {
            throw GraphAlgorithmError.indexNotFound(indexName)
        }

        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        return typeSubspace.subspace(indexName)
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
    private let fromFieldName: String
    private let edgeFieldName: String
    private let toFieldName: String
    private var configuration: CommunityDetectionConfiguration
    private var edgeLabelFilter: String?

    internal init(
        queryContext: IndexQueryContext,
        fromFieldName: String,
        edgeFieldName: String,
        toFieldName: String,
        configuration: CommunityDetectionConfiguration
    ) {
        self.queryContext = queryContext
        self.fromFieldName = fromFieldName
        self.edgeFieldName = edgeFieldName
        self.toFieldName = toFieldName
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
        guard !fromFieldName.isEmpty else {
            throw GraphAlgorithmError.indexNotConfigured
        }

        let indexSubspace = try await getIndexSubspace()

        let detector = CommunityDetector<T>(
            database: queryContext.context.container.database,
            subspace: indexSubspace,
            configuration: configuration
        )

        return try await detector.detect(edgeLabel: edgeLabelFilter)
    }

    /// Detect community for a specific node
    ///
    /// - Parameters:
    ///   - node: Node to find community for
    ///   - maxHops: Maximum hops from node to consider
    /// - Returns: Set of node IDs in the same community
    public func detectLocal(for node: String, maxHops: Int = 3) async throws -> Set<String> {
        guard !fromFieldName.isEmpty else {
            throw GraphAlgorithmError.indexNotConfigured
        }

        let indexSubspace = try await getIndexSubspace()

        let detector = CommunityDetector<T>(
            database: queryContext.context.container.database,
            subspace: indexSubspace,
            configuration: configuration
        )

        return try await detector.detectLocalCommunity(
            for: node,
            maxHops: maxHops,
            edgeLabel: edgeLabelFilter
        )
    }

    private func getIndexSubspace() async throws -> Subspace {
        let indexName = "\(T.persistableType)_graph_\(fromFieldName)_\(edgeFieldName)_\(toFieldName)"

        guard let _ = queryContext.schema.indexDescriptor(named: indexName) else {
            throw GraphAlgorithmError.indexNotFound(indexName)
        }

        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        return typeSubspace.subspace(indexName)
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
