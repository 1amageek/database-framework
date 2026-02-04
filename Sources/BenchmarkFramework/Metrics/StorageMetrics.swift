import Foundation
import FoundationDB

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
    /// - Returns: Storage metrics
    public static func measure(
        database: any DatabaseProtocol,
        subspace: Subspace
    ) async throws -> StorageMetrics {
        let range = subspace.range()

        let totalBytes = try await database.withTransaction { transaction in
            var bytes = 0

            let kvs = try await transaction.getRange(begin: range.begin, end: range.end)
            for try await (key, value) in kvs {
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
