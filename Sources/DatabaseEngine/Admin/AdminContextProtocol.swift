#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit

/// Administrative statistics, query analysis, and index operations.
///
/// **Usage**:
/// ```swift
/// let admin = container.newAdminContext()
///
/// // Read collection statistics
/// let stats = try await admin.collectionStatistics(User.self)
/// print("Document count: \(stats.documentCount)")
///
/// // Explain a query plan
/// let plan = try await admin.explain(
///     Query<User>().where(User.fields.age > 18)
/// )
/// print("Plan type: \(plan.planType)")
/// ```
public protocol AdminContextProtocol: Sendable {
    // MARK: - Collection Statistics

    /// Returns statistics for a persistable collection.
    ///
    /// - Parameter type: Persistable type to inspect.
    /// - Returns: Collection statistics.
    func collectionStatistics<T: Persistable>(
        _ type: T.Type
    ) async throws -> AdminCollectionStatistics

    // MARK: - Index Statistics

    /// Returns statistics for one index.
    ///
    /// - Parameter indexName: Index name.
    /// - Returns: Index statistics.
    func indexStatistics(
        _ indexName: String
    ) async throws -> AdminIndexStatistics

    /// Returns statistics for every index.
    ///
    /// - Returns: All index statistics.
    func allIndexStatistics() async throws -> [AdminIndexStatistics]

    // MARK: - Query Analysis

    /// Explains a query without executing it.
    ///
    /// This is the database-framework equivalent of PostgreSQL `EXPLAIN`.
    ///
    /// - Parameter query: Query to analyze.
    /// - Returns: Planned execution path.
    func explain<T: Persistable>(
        _ query: Query<T>
    ) async throws -> AdminQueryPlan

    /// Executes a query and returns measured execution statistics.
    ///
    /// This is the database-framework equivalent of PostgreSQL
    /// `EXPLAIN ANALYZE`.
    ///
    /// - Parameter query: Query to execute and analyze.
    /// - Returns: Plan and measured execution statistics.
    func explainAnalyze<T: Persistable>(
        _ query: Query<T>
    ) async throws -> AdminQueryExecutionStatistics

    // MARK: - Index Management

    /// Rebuilds an index.
    ///
    /// - Parameters:
    ///   - indexName: Index to rebuild.
    ///   - progress: Progress callback receiving values from 0.0 through 1.0.
    func rebuildIndex(_ indexName: String, progress: (@Sendable (Double) -> Void)?) async throws

    /// Refreshes all collected statistics.
    ///
    /// This samples stored entities and is analogous to PostgreSQL `ANALYZE`.
    func updateStatistics() async throws

    /// Refreshes statistics for one persistable type.
    ///
    /// - Parameter type: Persistable type to analyze.
    func updateStatistics<T: Persistable>(for type: T.Type) async throws

    // MARK: - FDB-Specific Features

    /// Returns the current read version.
    ///
    /// FoundationDB supplies this global transaction-ordering version.
    ///
    /// - Returns: Current read version.
    func currentReadVersion() async throws -> UInt64

    /// Estimates the byte size of a key range.
    ///
    /// FoundationDB performs this estimate on the server.
    ///
    /// - Parameters:
    ///   - type: Persistable type to estimate.
    /// - Returns: Estimated byte count.
    func estimatedStorageSize<T: Persistable>(for type: T.Type) async throws -> Int64
}

// MARK: - Default Implementations

extension AdminContextProtocol {
    /// Rebuilds an index without a progress callback.
    public func rebuildIndex(_ indexName: String) async throws {
        try await rebuildIndex(indexName, progress: nil)
    }
}
