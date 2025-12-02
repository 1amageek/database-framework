// VectorQuery.swift
// VectorIndex - Query extension for vector similarity search

import Foundation
import DatabaseEngine
import Core

// MARK: - Vector Query Builder

/// Builder for vector similarity search queries
///
/// **Usage**:
/// ```swift
/// import VectorIndex
///
/// let similar = try await context.findSimilar(Product.self)
///     .vector(\.embedding, dimensions: 128)
///     .query(queryVector, k: 10)
///     .metric(.cosine)
///     .execute()
/// // Returns: [(item: Product, distance: Double)]
/// ```
public struct VectorQueryBuilder<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let fieldName: String
    private let dimensions: Int
    private var queryVector: [Float]?
    private var k: Int = 10
    private var distanceMetric: VectorDistanceMetric = .cosine

    internal init(queryContext: IndexQueryContext, fieldName: String, dimensions: Int) {
        self.queryContext = queryContext
        self.fieldName = fieldName
        self.dimensions = dimensions
    }

    /// Set the query vector and number of results
    ///
    /// - Parameters:
    ///   - vector: The query vector to search with
    ///   - k: Number of nearest neighbors to return
    /// - Returns: Updated query builder
    public func query(_ vector: [Float], k: Int) -> Self {
        var copy = self
        copy.queryVector = vector
        copy.k = k
        return copy
    }

    /// Set the distance metric
    ///
    /// - Parameter metric: Distance metric (.cosine, .euclidean, .dotProduct)
    /// - Returns: Updated query builder
    public func metric(_ metric: VectorDistanceMetric) -> Self {
        var copy = self
        copy.distanceMetric = metric
        return copy
    }

    /// Execute the vector similarity search
    ///
    /// - Returns: Array of (item, distance) tuples sorted by distance
    /// - Throws: Error if search fails or query vector not set
    public func execute() async throws -> [(item: T, distance: Double)] {
        guard let vector = queryVector else {
            throw VectorQueryError.noQueryVector
        }

        guard vector.count == dimensions else {
            throw VectorQueryError.dimensionMismatch(expected: dimensions, actual: vector.count)
        }

        let indexName = buildIndexName()

        return try await queryContext.executeVectorSearch(
            type: T.self,
            indexName: indexName,
            queryVector: vector,
            k: k,
            dimensions: dimensions,
            metric: distanceMetric
        )
    }

    /// Build the index name based on type and field
    private func buildIndexName() -> String {
        return "\(T.persistableType)_\(fieldName)_vector"
    }
}

// MARK: - Vector Entry Point

/// Entry point for vector queries
public struct VectorEntryPoint<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// Specify the vector field to search
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the [Float] field
    ///   - dimensions: Number of dimensions in the vectors
    /// - Returns: Vector query builder
    public func vector(_ keyPath: KeyPath<T, [Float]>, dimensions: Int) -> VectorQueryBuilder<T> {
        VectorQueryBuilder(
            queryContext: queryContext,
            fieldName: T.fieldName(for: keyPath),
            dimensions: dimensions
        )
    }

    /// Specify the optional vector field to search
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the optional [Float] field
    ///   - dimensions: Number of dimensions in the vectors
    /// - Returns: Vector query builder
    public func vector(_ keyPath: KeyPath<T, [Float]?>, dimensions: Int) -> VectorQueryBuilder<T> {
        VectorQueryBuilder(
            queryContext: queryContext,
            fieldName: T.fieldName(for: keyPath),
            dimensions: dimensions
        )
    }
}

// MARK: - FDBContext Extension

extension FDBContext {

    /// Start a vector similarity search query
    ///
    /// This method is available when you import `VectorIndex`.
    ///
    /// **Usage**:
    /// ```swift
    /// import VectorIndex
    ///
    /// let similar = try await context.findSimilar(Product.self)
    ///     .vector(\.embedding, dimensions: 128)
    ///     .query(queryVector, k: 10)
    ///     .metric(.cosine)
    ///     .execute()
    /// // Returns: [(item: Product, distance: Double)]
    /// ```
    ///
    /// - Parameter type: The Persistable type to search
    /// - Returns: Entry point for configuring the search
    public func findSimilar<T: Persistable>(_ type: T.Type) -> VectorEntryPoint<T> {
        VectorEntryPoint(queryContext: indexQueryContext)
    }
}

// MARK: - Vector Query Error

/// Errors for vector query operations
public enum VectorQueryError: Error, CustomStringConvertible {
    /// No query vector provided
    case noQueryVector

    /// Query vector dimension mismatch
    case dimensionMismatch(expected: Int, actual: Int)

    /// Index not found
    case indexNotFound(String)

    public var description: String {
        switch self {
        case .noQueryVector:
            return "No query vector provided for vector similarity search"
        case .dimensionMismatch(let expected, let actual):
            return "Vector dimension mismatch: expected \(expected), got \(actual)"
        case .indexNotFound(let name):
            return "Vector index not found: \(name)"
        }
    }
}
