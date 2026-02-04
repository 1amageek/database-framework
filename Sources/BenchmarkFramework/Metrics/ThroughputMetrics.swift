import Foundation

public struct ThroughputMetrics: Codable, Sendable, Hashable {
    public let opsPerSecond: Double
    public let totalOperations: Int
    public let durationSeconds: Double

    public init(
        opsPerSecond: Double,
        totalOperations: Int,
        durationSeconds: Double
    ) {
        self.opsPerSecond = opsPerSecond
        self.totalOperations = totalOperations
        self.durationSeconds = durationSeconds
    }

    /// Measure throughput by counting operations in a fixed time window
    /// - Parameters:
    ///   - duration: Duration to measure (default: 10 seconds)
    ///   - operation: The operation to measure
    /// - Returns: Throughput metrics
    public static func measure(
        duration: TimeInterval = 10.0,
        operation: @Sendable () async throws -> Void
    ) async throws -> ThroughputMetrics {
        let startTime = Date()
        var count = 0

        while Date().timeIntervalSince(startTime) < duration {
            try await operation()
            count += 1
        }

        let actualDuration = Date().timeIntervalSince(startTime)
        let opsPerSecond = Double(count) / actualDuration

        return ThroughputMetrics(
            opsPerSecond: opsPerSecond,
            totalOperations: count,
            durationSeconds: actualDuration
        )
    }

    /// Calculate improvement percentage (positive = faster)
    /// - Parameters:
    ///   - baseline: Baseline ops/sec
    ///   - optimized: Optimized ops/sec
    /// - Returns: Improvement percentage (positive = improvement)
    public static func improvement(baseline: Double, optimized: Double) -> Double {
        guard baseline > 0 else { return 0 }
        return ((optimized - baseline) / baseline) * 100.0
    }
}
