import Foundation

public struct RegressionDetector: Sendable {
    public let config: ThresholdConfig

    public init(config: ThresholdConfig = .default) {
        self.config = config
    }

    public func check(
        current: ComparisonResult,
        baseline: ComparisonResult
    ) -> RegressionCheckResult {
        var regressions: [Regression] = []

        // Check latency regression (higher is worse)
        let latencyChange = calculateChange(
            current: current.optimized.metrics.latency.p95,
            baseline: baseline.optimized.metrics.latency.p95
        )
        if latencyChange > config.latencyThreshold {
            regressions.append(
                Regression(
                    metric: .latencyP95,
                    change: latencyChange,
                    threshold: config.latencyThreshold,
                    current: current.optimized.metrics.latency.p95,
                    baseline: baseline.optimized.metrics.latency.p95
                )
            )
        }

        // Check throughput regression (lower is worse)
        if let currentThroughput = current.optimized.metrics.throughput?.opsPerSecond,
           let baselineThroughput = baseline.optimized.metrics.throughput?.opsPerSecond {
            let throughputChange = calculateChange(
                current: baselineThroughput,
                baseline: currentThroughput
            )
            if throughputChange > config.throughputThreshold {
                regressions.append(
                    Regression(
                        metric: .throughput,
                        change: throughputChange,
                        threshold: config.throughputThreshold,
                        current: currentThroughput,
                        baseline: baselineThroughput
                    )
                )
            }
        }

        // Check memory regression (higher is worse)
        if let currentMemory = current.optimized.metrics.memory?.peakMB,
           let baselineMemory = baseline.optimized.metrics.memory?.peakMB {
            let memoryChange = calculateChange(
                current: currentMemory,
                baseline: baselineMemory
            )
            if memoryChange > config.memoryThreshold {
                regressions.append(
                    Regression(
                        metric: .memoryPeak,
                        change: memoryChange,
                        threshold: config.memoryThreshold,
                        current: currentMemory,
                        baseline: baselineMemory
                    )
                )
            }
        }

        // Check storage regression (higher is worse)
        if let currentStorage = current.optimized.metrics.storage?.bytes,
           let baselineStorage = baseline.optimized.metrics.storage?.bytes {
            let storageChange = calculateChange(
                current: Double(currentStorage),
                baseline: Double(baselineStorage)
            )
            if storageChange > config.storageThreshold {
                regressions.append(
                    Regression(
                        metric: .storage,
                        change: storageChange,
                        threshold: config.storageThreshold,
                        current: Double(currentStorage),
                        baseline: Double(baselineStorage)
                    )
                )
            }
        }

        return regressions.isEmpty
            ? .passed
            : .regressed(regressions)
    }

    private func calculateChange(current: Double, baseline: Double) -> Double {
        guard baseline > 0 else { return 0 }
        return abs((current - baseline) / baseline)
    }
}

public enum RegressionCheckResult: Sendable {
    case passed
    case regressed([Regression])

    public var isRegressed: Bool {
        if case .regressed = self {
            return true
        }
        return false
    }

    public var regressions: [Regression] {
        if case .regressed(let list) = self {
            return list
        }
        return []
    }
}

public struct Regression: Sendable, Codable {
    public let metric: MetricType
    public let change: Double
    public let threshold: Double
    public let current: Double
    public let baseline: Double

    public init(
        metric: MetricType,
        change: Double,
        threshold: Double,
        current: Double,
        baseline: Double
    ) {
        self.metric = metric
        self.change = change
        self.threshold = threshold
        self.current = current
        self.baseline = baseline
    }

    public enum MetricType: String, Sendable, Codable {
        case latencyP95
        case throughput
        case memoryPeak
        case storage
    }
}
