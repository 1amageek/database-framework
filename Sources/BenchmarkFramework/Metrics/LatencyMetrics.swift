import Foundation

public struct LatencyMetrics: Codable, Sendable, Hashable {
    public let p50: Double
    public let p95: Double
    public let p99: Double
    public let avg: Double
    public let min: Double
    public let max: Double
    public let samples: Int

    public init(
        p50: Double,
        p95: Double,
        p99: Double,
        avg: Double,
        min: Double,
        max: Double,
        samples: Int
    ) {
        self.p50 = p50
        self.p95 = p95
        self.p99 = p99
        self.avg = avg
        self.min = min
        self.max = max
        self.samples = samples
    }

    /// Measure latency of multiple operations
    /// - Parameters:
    ///   - warmupIterations: Number of warmup iterations (default: 10)
    ///   - iterations: Number of measurement iterations (default: 100)
    ///   - operation: The operation to measure
    /// - Returns: Latency metrics in milliseconds
    public static func measure(
        warmupIterations: Int = 10,
        iterations: Int = 100,
        operation: @Sendable () async throws -> Void
    ) async throws -> LatencyMetrics {
        var samples: [Double] = []
        samples.reserveCapacity(iterations)

        // Warmup phase
        for _ in 0..<warmupIterations {
            try await operation()
        }

        // Measurement phase
        for _ in 0..<iterations {
            let start = DispatchTime.now()
            try await operation()
            let end = DispatchTime.now()

            let nanos = Double(end.uptimeNanoseconds - start.uptimeNanoseconds)
            let millis = nanos / 1_000_000.0
            samples.append(millis)
        }

        return calculate(from: samples)
    }

    /// Calculate percentiles from samples
    public static func calculate(from samples: [Double]) -> LatencyMetrics {
        guard !samples.isEmpty else {
            return LatencyMetrics(
                p50: 0, p95: 0, p99: 0,
                avg: 0, min: 0, max: 0,
                samples: 0
            )
        }

        let sorted = samples.sorted()
        let count = sorted.count

        let p50 = percentile(sorted, 0.50)
        let p95 = percentile(sorted, 0.95)
        let p99 = percentile(sorted, 0.99)
        let avg = samples.reduce(0.0, +) / Double(count)
        let min = sorted.first!
        let max = sorted.last!

        return LatencyMetrics(
            p50: p50,
            p95: p95,
            p99: p99,
            avg: avg,
            min: min,
            max: max,
            samples: count
        )
    }

    /// Calculate percentile value
    private static func percentile(_ sorted: [Double], _ p: Double) -> Double {
        guard !sorted.isEmpty else { return 0 }

        let index = p * Double(sorted.count - 1)
        let lower = Int(index)
        let upper = Swift.min(lower + 1, sorted.count - 1)
        let weight = index - Double(lower)

        return sorted[lower] * (1.0 - weight) + sorted[upper] * weight
    }

    /// Calculate improvement percentage (positive = faster)
    /// - Parameters:
    ///   - baseline: Baseline latency
    ///   - optimized: Optimized latency
    /// - Returns: Improvement percentage (positive = improvement)
    public static func improvement(baseline: Double, optimized: Double) -> Double {
        guard baseline > 0 else { return 0 }
        return ((baseline - optimized) / baseline) * 100.0
    }
}
