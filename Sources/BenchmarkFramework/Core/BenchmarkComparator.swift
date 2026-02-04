import Foundation

public struct BenchmarkComparator: Sendable {
    public init() {}

    /// Compare two scenario results and calculate improvement metrics
    public func compare(
        baseline: ScenarioResult,
        optimized: ScenarioResult
    ) -> ImprovementMetrics {
        let latencyImprovement = LatencyMetrics.improvement(
            baseline: baseline.metrics.latency.p95,
            optimized: optimized.metrics.latency.p95
        )

        let throughputImprovement: Double?
        if let baselineThroughput = baseline.metrics.throughput?.opsPerSecond,
           let optimizedThroughput = optimized.metrics.throughput?.opsPerSecond {
            throughputImprovement = ThroughputMetrics.improvement(
                baseline: baselineThroughput,
                optimized: optimizedThroughput
            )
        } else {
            throughputImprovement = nil
        }

        let memoryImprovement: Double?
        if let baselineMemory = baseline.metrics.memory?.peakMB,
           let optimizedMemory = optimized.metrics.memory?.peakMB {
            memoryImprovement = MemoryMetrics.improvement(
                baseline: baselineMemory,
                optimized: optimizedMemory
            )
        } else {
            memoryImprovement = nil
        }

        let storageImprovement: Double?
        if let baselineStorage = baseline.metrics.storage?.bytes,
           let optimizedStorage = optimized.metrics.storage?.bytes {
            storageImprovement = StorageMetrics.improvement(
                baseline: baselineStorage,
                optimized: optimizedStorage
            )
        } else {
            storageImprovement = nil
        }

        return ImprovementMetrics(
            latencyP95: latencyImprovement,
            throughput: throughputImprovement,
            memoryPeak: memoryImprovement,
            storage: storageImprovement
        )
    }

    /// Find the best performing strategy based on latency p95
    public func findBest(in strategies: [ScenarioResult]) -> ScenarioResult? {
        strategies.min(by: { $0.metrics.latency.p95 < $1.metrics.latency.p95 })
    }

    /// Rank strategies by performance (best to worst)
    public func rank(strategies: [ScenarioResult]) -> [ScenarioResult] {
        strategies.sorted(by: { $0.metrics.latency.p95 < $1.metrics.latency.p95 })
    }
}
