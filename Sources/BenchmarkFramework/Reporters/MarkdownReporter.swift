import Foundation

public struct MarkdownReporter: Sendable {
    public init() {}

    /// Generate markdown report for comparison result
    public static func generate(
        _ result: ComparisonResult,
        regressionCheck: RegressionCheckResult? = nil
    ) -> String {
        var lines: [String] = []

        lines.append("## 🚀 \(result.name)")
        lines.append("")

        if let check = regressionCheck, check.isRegressed {
            lines.append("⚠️ **REGRESSION DETECTED**")
            lines.append("")
            for regression in check.regressions {
                lines.append(formatRegression(regression))
            }
            lines.append("")
        }

        lines.append("### Performance Metrics")
        lines.append("")
        lines.append("| Metric | Baseline | Optimized | Change |")
        lines.append("|--------|----------|-----------|--------|")

        let latencyRow = formatMetricRow(
            "Latency (p95)",
            baseline: "\(format(result.baseline.metrics.latency.p95))ms",
            optimized: "\(format(result.optimized.metrics.latency.p95))ms",
            change: result.improvement.latencyP95
        )
        lines.append(latencyRow)

        if let throughputImprovement = result.improvement.throughput,
           let baselineThroughput = result.baseline.metrics.throughput?.opsPerSecond,
           let optimizedThroughput = result.optimized.metrics.throughput?.opsPerSecond {
            let throughputRow = formatMetricRow(
                "Throughput",
                baseline: "\(Int(baselineThroughput)) ops/s",
                optimized: "\(Int(optimizedThroughput)) ops/s",
                change: throughputImprovement
            )
            lines.append(throughputRow)
        }

        if let memoryImprovement = result.improvement.memoryPeak,
           let baselineMemory = result.baseline.metrics.memory?.peakMB,
           let optimizedMemory = result.optimized.metrics.memory?.peakMB {
            let memoryRow = formatMetricRow(
                "Memory (peak)",
                baseline: "\(format(baselineMemory))MB",
                optimized: "\(format(optimizedMemory))MB",
                change: memoryImprovement
            )
            lines.append(memoryRow)
        }

        if let storageImprovement = result.improvement.storage,
           let baselineStorage = result.baseline.metrics.storage?.megabytes,
           let optimizedStorage = result.optimized.metrics.storage?.megabytes {
            let storageRow = formatMetricRow(
                "Storage",
                baseline: "\(format(baselineStorage))MB",
                optimized: "\(format(optimizedStorage))MB",
                change: storageImprovement
            )
            lines.append(storageRow)
        }

        lines.append("")
        return lines.joined(separator: "\n")
    }

    /// Generate markdown report for multiple comparisons
    public static func generate(
        _ results: [ComparisonResult],
        regressionChecks: [RegressionCheckResult] = []
    ) -> String {
        var lines: [String] = []

        lines.append("# 🚀 Performance Benchmark Results")
        lines.append("")

        let hasRegressions = regressionChecks.contains { $0.isRegressed }
        if hasRegressions {
            lines.append("## ⚠️ Regressions Detected")
            lines.append("")
            for (index, check) in regressionChecks.enumerated() where check.isRegressed {
                if index < results.count {
                    lines.append("### \(results[index].name)")
                    for regression in check.regressions {
                        lines.append(formatRegression(regression))
                    }
                    lines.append("")
                }
            }
        }

        lines.append("## Summary")
        lines.append("")

        for (index, result) in results.enumerated() {
            let status = (index < regressionChecks.count && regressionChecks[index].isRegressed)
                ? "⚠️"
                : "✅"
            lines.append("### \(result.name) \(status)")
            lines.append("")
            lines.append(generate(result, regressionCheck: index < regressionChecks.count ? regressionChecks[index] : nil))
        }

        return lines.joined(separator: "\n")
    }

    private static func formatMetricRow(
        _ name: String,
        baseline: String,
        optimized: String,
        change: Double
    ) -> String {
        let changeStr = formatChange(change)
        return "| \(name) | \(baseline) | \(optimized) | \(changeStr) |"
    }

    private static func formatChange(_ value: Double) -> String {
        let sign = value > 0 ? "+" : ""
        let emoji = value > 0 ? "✅" : value < -10 ? "⚠️" : "➖"
        return "\(sign)\(format(value))% \(emoji)"
    }

    private static func formatRegression(_ regression: Regression) -> String {
        let metricName: String
        switch regression.metric {
        case .latencyP95:
            metricName = "Latency (p95)"
        case .throughput:
            metricName = "Throughput"
        case .memoryPeak:
            metricName = "Memory (peak)"
        case .storage:
            metricName = "Storage"
        }

        return "- **\(metricName)**: Regressed by \(format(regression.change * 100))% " +
               "(threshold: \(format(regression.threshold * 100))%)"
    }

    private static func format(_ value: Double) -> String {
        String(format: "%.2f", value)
    }
}
