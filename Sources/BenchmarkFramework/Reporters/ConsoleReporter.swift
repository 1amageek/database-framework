import Foundation

public struct ConsoleReporter: Sendable {
    public init() {}

    /// Print comparison result to console
    public static func print(_ result: ComparisonResult) {
        Swift.print("")
        Swift.print(String(repeating: "=", count: 70))
        Swift.print("📊 \(result.name)")
        Swift.print(String(repeating: "=", count: 70))

        Swift.print("\nBaseline (\(result.baseline.name)):")
        printMetrics(result.baseline.metrics)

        Swift.print("\nOptimized (\(result.optimized.name)):")
        printMetrics(result.optimized.metrics)

        Swift.print("\n💡 Improvement:")
        printImprovement(result.improvement)

        Swift.print(String(repeating: "=", count: 70))
        Swift.print("")
    }

    /// Print strategy comparison result to console
    public static func print(_ result: StrategyComparisonResult) {
        Swift.print("")
        Swift.print(String(repeating: "=", count: 70))
        Swift.print("📊 \(result.name)")
        Swift.print(String(repeating: "=", count: 70))

        for (index, strategy) in result.strategies.enumerated() {
            Swift.print("\n[\(index + 1)] \(strategy.name):")
            printMetrics(strategy.metrics)
        }

        Swift.print(String(repeating: "=", count: 70))
        Swift.print("")
    }

    /// Print scalability result to console
    public static func print(_ result: ScalabilityResult) {
        Swift.print("")
        Swift.print(String(repeating: "=", count: 70))
        Swift.print("📊 \(result.name)")
        Swift.print(String(repeating: "=", count: 70))

        for point in result.dataPoints {
            Swift.print("\nData Size: \(point.dataSize)")
            printMetrics(point.metrics, indent: "  ")
        }

        Swift.print(String(repeating: "=", count: 70))
        Swift.print("")
    }

    private static func printMetrics(_ metrics: MetricsSnapshot, indent: String = "   ") {
        Swift.print("\(indent)Latency p50: \(format(metrics.latency.p50))ms")
        Swift.print("\(indent)Latency p95: \(format(metrics.latency.p95))ms")
        Swift.print("\(indent)Latency p99: \(format(metrics.latency.p99))ms")

        if let throughput = metrics.throughput {
            Swift.print("\(indent)Throughput: \(Int(throughput.opsPerSecond)) ops/s")
        }

        if let memory = metrics.memory {
            Swift.print("\(indent)Memory (peak): \(format(memory.peakMB))MB")
            Swift.print("\(indent)Memory (avg): \(format(memory.avgMB))MB")
        }

        if let storage = metrics.storage {
            Swift.print("\(indent)Storage: \(format(storage.megabytes))MB")
        }

        if let accuracy = metrics.accuracy {
            if let recall = accuracy.recall {
                Swift.print("\(indent)Recall: \(format(recall * 100))%")
            }
            if let precision = accuracy.precision {
                Swift.print("\(indent)Precision: \(format(precision * 100))%")
            }
        }
    }

    private static func printImprovement(_ improvement: ImprovementMetrics) {
        Swift.print("   Latency (p95): \(formatImprovement(improvement.latencyP95))")

        if let throughput = improvement.throughput {
            Swift.print("   Throughput: \(formatImprovement(throughput))")
        }

        if let memory = improvement.memoryPeak {
            Swift.print("   Memory (peak): \(formatImprovement(memory))")
        }

        if let storage = improvement.storage {
            Swift.print("   Storage: \(formatImprovement(storage))")
        }
    }

    private static func format(_ value: Double) -> String {
        String(format: "%.2f", value)
    }

    private static func formatImprovement(_ value: Double) -> String {
        let sign = value > 0 ? "+" : ""
        let emoji = value > 0 ? "✅" : "⚠️"
        return "\(sign)\(format(value))% \(emoji)"
    }
}
