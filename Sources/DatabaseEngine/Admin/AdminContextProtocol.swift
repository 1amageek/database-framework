#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core

// Re-export public types for protocol
public typealias QueryPlanPublic = Core.QueryPlan
public typealias QueryExecutionStatsPublic = Core.QueryExecutionStats
public typealias CollectionStatisticsPublic = Core.CollectionStatistics
public typealias IndexStatisticsPublic = Core.IndexStatistics

/// Administrative statistics, query analysis, and index operations.
///
/// **Usage**:
/// ```swift
/// let admin = container.newAdminContext()
///
/// // 統計情報の取得
/// let stats = try await admin.collectionStatistics(User.self)
/// print("Document count: \(stats.documentCount)")
///
/// // クエリ実行計画の取得
/// let plan = try await admin.explain(Query<User>().where(\.age > 18))
/// print("Plan type: \(plan.planType)")
/// ```
public protocol AdminContextProtocol: Sendable {
    // MARK: - Collection Statistics

    /// コレクション（Type）の統計情報を取得
    ///
    /// - Parameter type: 対象の型
    /// - Returns: コレクションの統計情報
    func collectionStatistics<T: Persistable>(_ type: T.Type) async throws -> CollectionStatisticsPublic

    // MARK: - Index Statistics

    /// インデックスの統計情報を取得
    ///
    /// - Parameter indexName: インデックス名
    /// - Returns: インデックスの統計情報
    func indexStatistics(_ indexName: String) async throws -> IndexStatisticsPublic

    /// 全インデックスの統計情報を取得
    ///
    /// - Returns: 全インデックスの統計情報配列
    func allIndexStatistics() async throws -> [IndexStatisticsPublic]

    // MARK: - Query Analysis

    /// クエリの実行計画を取得（実行しない）
    ///
    /// PostgreSQLの`EXPLAIN`に相当する機能。
    /// クエリを実行せずに、どのようなプランで実行されるかを分析する。
    ///
    /// - Parameter query: 分析対象のクエリ
    /// - Returns: クエリ実行計画
    func explain<T: Persistable>(_ query: Query<T>) async throws -> QueryPlanPublic

    /// クエリを実行し、実行統計を取得
    ///
    /// PostgreSQLの`EXPLAIN ANALYZE`に相当する機能。
    /// 実際にクエリを実行し、実際の統計情報を収集する。
    ///
    /// - Parameter query: 分析対象のクエリ
    /// - Returns: クエリ実行統計（計画 + 実測値）
    func explainAnalyze<T: Persistable>(_ query: Query<T>) async throws -> QueryExecutionStatsPublic

    // MARK: - Index Management

    /// インデックスを再構築
    ///
    /// - Parameters:
    ///   - indexName: 再構築するインデックス名
    ///   - progress: 進捗コールバック（0.0〜1.0）
    func rebuildIndex(_ indexName: String, progress: (@Sendable (Double) -> Void)?) async throws

    /// 統計情報を更新（収集）
    ///
    /// PostgreSQLの`ANALYZE`に相当する機能。
    /// サンプリングを行い、統計情報を更新する。
    func updateStatistics() async throws

    /// 特定の型の統計情報を更新
    ///
    /// - Parameter type: 対象の型
    func updateStatistics<T: Persistable>(for type: T.Type) async throws

    // MARK: - FDB-Specific Features

    /// 現在のReadVersionを取得
    ///
    /// FoundationDB固有の機能。グローバルなトランザクション順序を表すバージョンを取得。
    ///
    /// - Returns: 現在のReadVersion
    func currentReadVersion() async throws -> UInt64

    /// キー範囲の推定サイズを取得
    ///
    /// FoundationDB固有の機能。サーバーサイドでキー範囲のサイズを推定。
    ///
    /// - Parameters:
    ///   - type: 対象の型
    /// - Returns: 推定サイズ（バイト）
    func estimatedStorageSize<T: Persistable>(for type: T.Type) async throws -> Int64
}

// MARK: - Default Implementations

extension AdminContextProtocol {
    /// インデックス再構築（進捗コールバックなし）
    public func rebuildIndex(_ indexName: String) async throws {
        try await rebuildIndex(indexName, progress: nil)
    }
}
