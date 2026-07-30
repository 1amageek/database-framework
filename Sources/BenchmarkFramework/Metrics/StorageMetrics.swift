import Foundation
import DatabaseEngine
import StorageKit

public struct StorageMetrics: Codable, Sendable, Hashable {
    public let bytes: Int
    public let megabytes: Double

    public init(bytes: Int) {
        self.bytes = bytes
        self.megabytes = Double(bytes) / (1024.0 * 1024.0)
    }

    /// Measure storage size of a subspace in FoundationDB
    /// - Parameters:
    ///   - database: The database connection
    ///   - subspace: The subspace to measure
    ///   - clock: The monotonic clock used for transaction deadlines and retry delays
    /// - Returns: Storage metrics
    public static func measure(
        database: any StorageEngine,
        subspace: Subspace,
        clock: any StorageMonotonicClock
    ) async throws -> StorageMetrics {
        let range = subspace.range()
        let transactionExecutor = StorageTransactionExecutor(engine: database)

        let totalBytes = try await transactionExecutor.withTransaction(
            configuration: .default,
            clock: clock
        ) { transaction in
            var bytes = 0

            let kvs = try await TransactionRangeCollection.collect(using: transaction, from: .firstGreaterOrEqual(range.begin), to: .firstGreaterOrEqual(range.end))
            for (key, value) in kvs {
                bytes += key.count + value.count
            }

            return bytes
        }

        return StorageMetrics(bytes: totalBytes)
    }

    /// Calculate improvement percentage (positive = less storage)
    /// - Parameters:
    ///   - baseline: Baseline storage bytes
    ///   - optimized: Optimized storage bytes
    /// - Returns: Improvement percentage (positive = improvement)
    public static func improvement(baseline: Int, optimized: Int) -> Double {
        guard baseline > 0 else { return 0 }
        return (Double(baseline - optimized) / Double(baseline)) * 100.0
    }
}
