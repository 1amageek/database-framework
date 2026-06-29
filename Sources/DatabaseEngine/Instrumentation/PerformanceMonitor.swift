// PerformanceMonitor.swift
// DatabaseEngine - Performance monitoring and slow query logging
//
// Provides comprehensive performance monitoring including:
// - Slow query logging with configurable threshold
// - Latency percentile tracking (P50, P99)
// - QPS (queries per second) calculation
// - Transaction statistics

import Foundation
import Core
import Synchronization

// MARK: - SlowQueryEntry

/// スロークエリログエントリ
///
/// 閾値を超えた遅いトランザクションの情報を記録する。
///
/// **Usage**:
/// ```swift
/// let slowQueries = await monitor.getSlowQueries(limit: 10)
/// for query in slowQueries {
///     print("Slow: \(query.queryDescription) - \(query.executionTime)s")
/// }
/// ```
public struct SlowQueryEntry: Sendable {
    /// 記録された時刻
    public let timestamp: Date

    /// クエリ/トランザクションの説明
    public let queryDescription: String

    /// 対象の型名（わかる場合）
    public let typeName: String?

    /// 実行時間（秒）
    public let executionTime: TimeInterval

    /// トランザクションID（あれば）
    public let transactionID: String?

    public init(
        timestamp: Date = Date(),
        queryDescription: String,
        typeName: String? = nil,
        executionTime: TimeInterval,
        transactionID: String? = nil
    ) {
        self.timestamp = timestamp
        self.queryDescription = queryDescription
        self.typeName = typeName
        self.executionTime = executionTime
        self.transactionID = transactionID
    }
}

extension SlowQueryEntry: CustomStringConvertible {
    public var description: String {
        let typeStr = typeName.map { " [\($0)]" } ?? ""
        let idStr = transactionID.map { " (tx: \($0))" } ?? ""
        return "[\(timestamp)]\(typeStr) \(queryDescription) - \(String(format: "%.3f", executionTime * 1000))ms\(idStr)"
    }
}

// MARK: - DatabaseMetrics

/// データベースメトリクス
///
/// 現在のパフォーマンス状況を表すメトリクス。
///
/// **Usage**:
/// ```swift
/// let metrics = await monitor.currentMetrics()
/// print("P50 latency: \(metrics.latencyP50Ms)ms")
/// print("QPS: \(metrics.queriesPerSecond)")
/// ```
public struct DatabaseMetrics: Sendable {
    /// メトリクス取得時刻
    public let timestamp: Date

    /// アクティブなトランザクション数
    public let activeTransactions: Int

    /// レイテンシP50（秒）
    public let latencyP50: TimeInterval

    /// レイテンシP99（秒）
    public let latencyP99: TimeInterval

    /// 秒間クエリ数
    public let queriesPerSecond: Double

    /// 総トランザクション数
    public let totalTransactions: Int64

    /// 成功したトランザクション数
    public let successfulTransactions: Int64

    /// 成功率 (0.0-1.0)
    public var successRate: Double {
        guard totalTransactions > 0 else { return 0 }
        return Double(successfulTransactions) / Double(totalTransactions)
    }

    /// レイテンシP50（ミリ秒）
    public var latencyP50Ms: Double {
        latencyP50 * 1000
    }

    /// レイテンシP99（ミリ秒）
    public var latencyP99Ms: Double {
        latencyP99 * 1000
    }

    public init(
        timestamp: Date = Date(),
        activeTransactions: Int,
        latencyP50: TimeInterval,
        latencyP99: TimeInterval,
        queriesPerSecond: Double,
        totalTransactions: Int64,
        successfulTransactions: Int64
    ) {
        self.timestamp = timestamp
        self.activeTransactions = activeTransactions
        self.latencyP50 = latencyP50
        self.latencyP99 = latencyP99
        self.queriesPerSecond = queriesPerSecond
        self.totalTransactions = totalTransactions
        self.successfulTransactions = successfulTransactions
    }
}

extension DatabaseMetrics: CustomStringConvertible {
    public var description: String {
        """
        DatabaseMetrics (\(timestamp)):
          Active transactions: \(activeTransactions)
          Latency: P50=\(String(format: "%.2f", latencyP50Ms))ms, P99=\(String(format: "%.2f", latencyP99Ms))ms
          QPS: \(String(format: "%.2f", queriesPerSecond))
          Transactions: \(totalTransactions) (success rate: \(String(format: "%.1f%%", successRate * 100)))
        """
    }
}

// MARK: - PerformanceMonitorProtocol

/// パフォーマンス監視プロトコル
///
/// データベースのパフォーマンス監視機能を提供する。
public protocol PerformanceMonitorProtocol: Sendable {
    /// スロークエリログを有効化
    ///
    /// - Parameter threshold: スロークエリとみなす閾値（秒）
    func enableSlowQueryLog(threshold: TimeInterval)

    /// スロークエリログを無効化
    func disableSlowQueryLog()

    /// スロークエリを取得
    ///
    /// - Parameter limit: 取得する最大件数
    /// - Returns: スロークエリエントリの配列（新しい順）
    func getSlowQueries(limit: Int) -> [SlowQueryEntry]

    /// スロークエリログをクリア
    func clearSlowQueries()

    /// 現在のメトリクスを取得
    func currentMetrics() -> DatabaseMetrics

    /// 全てのメトリクスをリセット
    func reset()
}

// MARK: - PerformanceMonitor

/// パフォーマンス監視クラス
///
/// `TransactionListener`を実装し、トランザクションイベントからメトリクスを収集する。
///
/// **Usage**:
/// ```swift
/// let monitor = PerformanceMonitor()
/// container.addTransactionListener(monitor)
///
/// // スロークエリログを有効化 (100ms以上)
/// monitor.enableSlowQueryLog(threshold: 0.1)
///
/// // ... トランザクション実行 ...
///
/// let metrics = monitor.currentMetrics()
/// print("P50 latency: \(metrics.latencyP50Ms)ms")
///
/// let slowQueries = monitor.getSlowQueries(limit: 10)
/// for query in slowQueries {
///     print(query)
/// }
/// ```
public final class PerformanceMonitor: PerformanceMonitorProtocol, TransactionListener {

    // MARK: - Configuration

    /// スロークエリ閾値（nil = 無効）
    private let slowQueryThreshold: Mutex<TimeInterval?>

    /// スロークエリログの最大件数
    private let maxSlowQueries: Int

    /// レイテンシサンプルの最大数
    private let maxLatencySamples: Int

    /// QPS計算のウィンドウ（秒）
    private let qpsWindow: TimeInterval

    // MARK: - State

    /// スロークエリログ（循環バッファ）
    private let slowQueries: Mutex<[SlowQueryEntry]>

    /// レイテンシサンプル（Reservoir Sampling用）
    private let latencySamples: Mutex<LatencySampleState>

    /// トランザクションカウンタ
    private let counters: Mutex<TransactionCounters>

    /// クエリタイムスタンプ（QPS計算用）
    private let queryTimestamps: Mutex<[Date]>

    // MARK: - Internal Types

    private struct LatencySampleState: Sendable {
        var samples: [TimeInterval] = []
        var totalCount: Int = 0
    }

    private struct TransactionCounters: Sendable {
        var total: Int64 = 0
        var successful: Int64 = 0
        var active: Int = 0
    }

    // MARK: - Initialization

    /// パフォーマンスモニターを作成
    ///
    /// - Parameters:
    ///   - maxSlowQueries: スロークエリログの最大件数（デフォルト: 100）
    ///   - maxLatencySamples: レイテンシサンプルの最大数（デフォルト: 1000）
    ///   - qpsWindow: QPS計算のウィンドウ秒数（デフォルト: 60）
    public init(
        maxSlowQueries: Int = 100,
        maxLatencySamples: Int = 1000,
        qpsWindow: TimeInterval = 60
    ) {
        self.maxSlowQueries = maxSlowQueries
        self.maxLatencySamples = maxLatencySamples
        self.qpsWindow = qpsWindow

        self.slowQueryThreshold = Mutex(nil)
        self.slowQueries = Mutex([])
        self.latencySamples = Mutex(LatencySampleState())
        self.counters = Mutex(TransactionCounters())
        self.queryTimestamps = Mutex([])
    }

    // MARK: - TransactionListener

    public func onEvent(_ event: TransactionEvent) {
        switch event {
        case .created:
            counters.withLock { $0.active += 1 }

        case .committed(let id, _, let duration, _):
            counters.withLock { counters in
                counters.active -= 1
                counters.total += 1
                counters.successful += 1
            }
            recordLatency(duration)
            recordQueryTimestamp()
            checkSlowQuery(duration: duration, transactionID: id)

        case .failed(let id, _, let duration, _):
            counters.withLock { counters in
                counters.active -= 1
                counters.total += 1
            }
            recordLatency(duration)
            recordQueryTimestamp()
            checkSlowQuery(duration: duration, transactionID: id, failed: true)

        case .cancelled:
            counters.withLock { counters in
                counters.active -= 1
                counters.total += 1
            }

        case .committing, .closed:
            break
        }
    }

    // MARK: - PerformanceMonitorProtocol

    public func enableSlowQueryLog(threshold: TimeInterval) {
        slowQueryThreshold.withLock { $0 = threshold }
    }

    public func disableSlowQueryLog() {
        slowQueryThreshold.withLock { $0 = nil }
    }

    public func getSlowQueries(limit: Int) -> [SlowQueryEntry] {
        slowQueries.withLock { queries in
            Array(queries.suffix(limit).reversed())
        }
    }

    public func clearSlowQueries() {
        slowQueries.withLock { $0.removeAll() }
    }

    public func currentMetrics() -> DatabaseMetrics {
        let currentCounters = counters.withLock { $0 }
        let (p50, p99) = calculatePercentiles()
        let qps = calculateQPS()

        return DatabaseMetrics(
            activeTransactions: currentCounters.active,
            latencyP50: p50,
            latencyP99: p99,
            queriesPerSecond: qps,
            totalTransactions: currentCounters.total,
            successfulTransactions: currentCounters.successful
        )
    }

    public func reset() {
        slowQueries.withLock { $0.removeAll() }
        latencySamples.withLock { $0 = LatencySampleState() }
        counters.withLock { $0 = TransactionCounters() }
        queryTimestamps.withLock { $0.removeAll() }
    }

    // MARK: - Private Methods

    /// レイテンシを記録（Reservoir Sampling）
    private func recordLatency(_ duration: TimeInterval) {
        latencySamples.withLock { state in
            state.totalCount += 1

            if state.samples.count < maxLatencySamples {
                // バッファに空きがあれば追加
                state.samples.append(duration)
            } else {
                // Reservoir Sampling: 確率的に置換
                let index = Int.random(in: 0..<state.totalCount)
                if index < maxLatencySamples {
                    state.samples[index] = duration
                }
            }
        }
    }

    /// クエリタイムスタンプを記録（QPS計算用）
    private func recordQueryTimestamp() {
        let now = Date()
        let cutoff = now.addingTimeInterval(-qpsWindow)

        queryTimestamps.withLock { timestamps in
            // 古いタイムスタンプを削除
            timestamps.removeAll { $0 < cutoff }
            timestamps.append(now)
        }
    }

    /// スロークエリをチェックして記録
    private func checkSlowQuery(duration: TimeInterval, transactionID: String?, failed: Bool = false) {
        guard let threshold = slowQueryThreshold.withLock({ $0 }) else { return }
        guard duration >= threshold else { return }

        let entry = SlowQueryEntry(
            queryDescription: failed ? "Failed transaction" : "Transaction",
            executionTime: duration,
            transactionID: transactionID
        )

        slowQueries.withLock { queries in
            queries.append(entry)
            // 最大件数を超えたら古いものを削除
            if queries.count > maxSlowQueries {
                queries.removeFirst(queries.count - maxSlowQueries)
            }
        }
    }

    /// パーセンタイルを計算
    private func calculatePercentiles() -> (p50: TimeInterval, p99: TimeInterval) {
        let samples = latencySamples.withLock { $0.samples }

        guard !samples.isEmpty else {
            return (0, 0)
        }

        let sorted = samples.sorted()
        let count = sorted.count

        let p50Index = Int(Double(count) * 0.50)
        let p99Index = min(Int(Double(count) * 0.99), count - 1)

        return (sorted[p50Index], sorted[p99Index])
    }

    /// QPSを計算
    private func calculateQPS() -> Double {
        let timestamps = queryTimestamps.withLock { $0 }
        guard !timestamps.isEmpty else { return 0 }

        let now = Date()
        let cutoff = now.addingTimeInterval(-qpsWindow)
        let recentCount = timestamps.filter { $0 >= cutoff }.count

        return Double(recentCount) / qpsWindow
    }
}

// MARK: - PerformanceMonitor Extensions

extension PerformanceMonitor {
    /// 手動でスロークエリを記録
    ///
    /// クエリ実行時に直接呼び出して記録する場合に使用。
    ///
    /// - Parameters:
    ///   - description: クエリの説明
    ///   - typeName: 対象の型名
    ///   - executionTime: 実行時間（秒）
    public func recordSlowQuery(
        description: String,
        typeName: String? = nil,
        executionTime: TimeInterval
    ) {
        guard let threshold = slowQueryThreshold.withLock({ $0 }) else { return }
        guard executionTime >= threshold else { return }

        let entry = SlowQueryEntry(
            queryDescription: description,
            typeName: typeName,
            executionTime: executionTime
        )

        slowQueries.withLock { queries in
            queries.append(entry)
            if queries.count > maxSlowQueries {
                queries.removeFirst(queries.count - maxSlowQueries)
            }
        }
    }

    /// 現在のスロークエリ閾値を取得
    public var currentThreshold: TimeInterval? {
        slowQueryThreshold.withLock { $0 }
    }

    /// スロークエリログが有効かどうか
    public var isSlowQueryLogEnabled: Bool {
        currentThreshold != nil
    }

    /// 記録されているスロークエリ数
    public var slowQueryCount: Int {
        slowQueries.withLock { $0.count }
    }

    /// サンプル数
    public var sampleCount: Int {
        latencySamples.withLock { $0.samples.count }
    }
}
