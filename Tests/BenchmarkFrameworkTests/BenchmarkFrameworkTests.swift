import Testing
import Foundation
@testable import BenchmarkFramework

@Suite("BenchmarkFramework Tests")
struct BenchmarkFrameworkTests {
    // MARK: - LatencyMetrics Tests

    @Test("LatencyMetrics: パーセンタイル計算")
    func latencyPercentileCalculation() throws {
        let samples = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        let metrics = LatencyMetrics.calculate(from: samples)

        #expect(metrics.min == 1.0)
        #expect(metrics.max == 10.0)
        #expect(metrics.avg == 5.5)
        #expect(metrics.samples == 10)

        // p50 should be around 5.5
        #expect(abs(metrics.p50 - 5.5) < 0.5)

        // p95 should be around 9.55
        #expect(abs(metrics.p95 - 9.55) < 0.5)

        // p99 should be around 9.91
        #expect(abs(metrics.p99 - 9.91) < 0.5)
    }

    @Test("LatencyMetrics: 空サンプル処理")
    func latencyEmptySamples() throws {
        let metrics = LatencyMetrics.calculate(from: [])

        #expect(metrics.min == 0)
        #expect(metrics.max == 0)
        #expect(metrics.avg == 0)
        #expect(metrics.samples == 0)
    }

    @Test("LatencyMetrics: 改善率計算")
    func latencyImprovement() throws {
        let baseline = 100.0
        let optimized = 50.0

        let improvement = LatencyMetrics.improvement(baseline: baseline, optimized: optimized)

        #expect(improvement == 50.0)
    }

    @Test("LatencyMetrics: measure() 実行")
    func latencyMeasure() async throws {
        let metrics = try await LatencyMetrics.measure(
            warmupIterations: 2,
            iterations: 10
        ) {
            try? await Task.sleep(nanoseconds: 1_000_000) // 1ms
        }

        #expect(metrics.samples == 10)
        #expect(metrics.avg > 0)
        #expect(metrics.min > 0)
        #expect(metrics.max > 0)
        #expect(metrics.p50 > 0)
    }

    // MARK: - ThroughputMetrics Tests

    @Test("ThroughputMetrics: 改善率計算")
    func throughputImprovement() throws {
        let baseline = 100.0
        let optimized = 200.0

        let improvement = ThroughputMetrics.improvement(baseline: baseline, optimized: optimized)

        #expect(improvement == 100.0)
    }

    @Test("ThroughputMetrics: measure() 実行")
    func throughputMeasure() async throws {
        let metrics = try await ThroughputMetrics.measure(duration: 1.0) {
            // Fast operation
        }

        #expect(metrics.totalOperations > 0)
        #expect(metrics.opsPerSecond > 0)
        #expect(metrics.durationSeconds >= 1.0)
        #expect(metrics.durationSeconds < 1.5)
    }

    // MARK: - MemoryMetrics Tests

    @Test("MemoryMetrics: 改善率計算")
    func memoryImprovement() throws {
        let baseline = 100.0
        let optimized = 50.0

        let improvement = MemoryMetrics.improvement(baseline: baseline, optimized: optimized)

        #expect(improvement == 50.0)
    }

    // MARK: - StorageMetrics Tests

    @Test("StorageMetrics: 改善率計算")
    func storageImprovement() throws {
        let baseline = 1000
        let optimized = 500

        let improvement = StorageMetrics.improvement(baseline: baseline, optimized: optimized)

        #expect(improvement == 50.0)
    }

    @Test("StorageMetrics: バイトからメガバイトへの変換")
    func storageBytesToMB() throws {
        let metrics = StorageMetrics(bytes: 1_048_576) // 1MB

        #expect(metrics.bytes == 1_048_576)
        #expect(abs(metrics.megabytes - 1.0) < 0.001)
    }

    // MARK: - BenchmarkComparator Tests

    @Test("BenchmarkComparator: 比較計算")
    func comparatorComparison() throws {
        let baseline = ScenarioResult(
            name: "Baseline",
            metrics: MetricsSnapshot(
                latency: LatencyMetrics(p50: 10, p95: 20, p99: 30, avg: 15, min: 5, max: 35, samples: 100),
                throughput: ThroughputMetrics(opsPerSecond: 100, totalOperations: 1000, durationSeconds: 10),
                memory: MemoryMetrics(peakMB: 100, avgMB: 80, samples: 50)
            )
        )

        let optimized = ScenarioResult(
            name: "Optimized",
            metrics: MetricsSnapshot(
                latency: LatencyMetrics(p50: 5, p95: 10, p99: 15, avg: 7.5, min: 2, max: 18, samples: 100),
                throughput: ThroughputMetrics(opsPerSecond: 200, totalOperations: 2000, durationSeconds: 10),
                memory: MemoryMetrics(peakMB: 50, avgMB: 40, samples: 50)
            )
        )

        let comparator = BenchmarkComparator()
        let improvement = comparator.compare(baseline: baseline, optimized: optimized)

        #expect(improvement.latencyP95 == 50.0)
        #expect(improvement.throughput == 100.0)
        #expect(improvement.memoryPeak == 50.0)
    }

    @Test("BenchmarkComparator: 最良ストラテジーの検出")
    func comparatorFindBest() throws {
        let strategies = [
            ScenarioResult(
                name: "Slow",
                metrics: MetricsSnapshot(
                    latency: LatencyMetrics(p50: 100, p95: 200, p99: 300, avg: 150, min: 50, max: 350, samples: 100)
                )
            ),
            ScenarioResult(
                name: "Fast",
                metrics: MetricsSnapshot(
                    latency: LatencyMetrics(p50: 10, p95: 20, p99: 30, avg: 15, min: 5, max: 35, samples: 100)
                )
            ),
            ScenarioResult(
                name: "Medium",
                metrics: MetricsSnapshot(
                    latency: LatencyMetrics(p50: 50, p95: 100, p99: 150, avg: 75, min: 25, max: 175, samples: 100)
                )
            ),
        ]

        let comparator = BenchmarkComparator()
        let best = comparator.findBest(in: strategies)

        #expect(best?.name == "Fast")
    }

    // MARK: - BenchmarkRunner Tests

    @Test("BenchmarkRunner: compare() 実行")
    func runnerCompare() async throws {
        let runner = BenchmarkRunner(config: .quick)

        let result = try await runner.compare(
            name: "Simple Comparison",
            baseline: { () async throws -> Int in
                try? await Task.sleep(nanoseconds: 2_000_000) // 2ms
                return 42
            },
            optimized: { () async throws -> Int in
                try? await Task.sleep(nanoseconds: 1_000_000) // 1ms
                return 42
            },
            verify: { (baseline: Int, optimized: Int) in
                #expect(baseline == optimized)
            }
        )

        #expect(result.name == "Simple Comparison")
        #expect(result.baseline.name == "Baseline")
        #expect(result.optimized.name == "Optimized")
        #expect(result.improvement.latencyP95 > 0)
    }

    @Test("BenchmarkRunner: compareStrategies() 実行")
    func runnerCompareStrategies() async throws {
        let runner = BenchmarkRunner(config: .quick)

        let result = try await runner.compareStrategies(
            name: "Multiple Strategies",
            strategies: [
                ("Fast", { @Sendable () async throws -> Int in return 1 }),
                ("Medium", { @Sendable () async throws -> Int in
                    try? await Task.sleep(nanoseconds: 1_000_000)
                    return 2
                }),
                ("Slow", { @Sendable () async throws -> Int in
                    try? await Task.sleep(nanoseconds: 2_000_000)
                    return 3
                }),
            ]
        )

        #expect(result.name == "Multiple Strategies")
        #expect(result.strategies.count == 3)
    }

    @Test("BenchmarkRunner: scale() 実行")
    func runnerScale() async throws {
        let runner = BenchmarkRunner(config: .quick)

        let result = try await runner.scale(
            name: "Scalability Test",
            dataSizes: [10, 100, 1000]
        ) { size in
            // Simulate work proportional to size
            for _ in 0..<size {
                _ = size * 2
            }
            return size
        }

        #expect(result.name == "Scalability Test")
        #expect(result.dataPoints.count == 3)
        #expect(result.dataPoints[0].dataSize == 10)
        #expect(result.dataPoints[1].dataSize == 100)
        #expect(result.dataPoints[2].dataSize == 1000)

        // Latency should increase with data size
        #expect(result.dataPoints[1].metrics.latency.avg > result.dataPoints[0].metrics.latency.avg)
    }

    // MARK: - JSON Round-trip Tests

    @Test("JSONReporter: ComparisonResult round-trip")
    func jsonRoundTripComparison() throws {
        let original = ComparisonResult(
            name: "Test Comparison",
            baseline: ScenarioResult(
                name: "Baseline",
                metrics: MetricsSnapshot(
                    latency: LatencyMetrics(p50: 10, p95: 20, p99: 30, avg: 15, min: 5, max: 35, samples: 100)
                )
            ),
            optimized: ScenarioResult(
                name: "Optimized",
                metrics: MetricsSnapshot(
                    latency: LatencyMetrics(p50: 5, p95: 10, p99: 15, avg: 7.5, min: 2, max: 18, samples: 100)
                )
            ),
            improvement: ImprovementMetrics(latencyP95: 50.0)
        )

        let tempFile = NSTemporaryDirectory() + "test_comparison.json"
        defer { try? FileManager.default.removeItem(atPath: tempFile) }

        try JSONReporter.write(original, to: tempFile)
        let loaded = try JSONReporter.readComparison(from: tempFile)

        #expect(loaded.name == original.name)
        #expect(loaded.baseline.name == original.baseline.name)
        #expect(loaded.optimized.name == original.optimized.name)
        #expect(loaded.improvement.latencyP95 == original.improvement.latencyP95)
    }

    @Test("JSONReporter: BenchmarkResult round-trip")
    func jsonRoundTripBenchmark() throws {
        let original = BenchmarkResult(
            name: "Test Benchmark",
            timestamp: Date(),
            environment: EnvironmentInfo.current(),
            scenarios: [
                ScenarioResult(
                    name: "Scenario 1",
                    metrics: MetricsSnapshot(
                        latency: LatencyMetrics(p50: 10, p95: 20, p99: 30, avg: 15, min: 5, max: 35, samples: 100)
                    )
                )
            ]
        )

        let tempFile = NSTemporaryDirectory() + "test_benchmark.json"
        defer { try? FileManager.default.removeItem(atPath: tempFile) }

        try JSONReporter.write(original, to: tempFile)
        let loaded = try JSONReporter.read(from: tempFile)

        #expect(loaded.name == original.name)
        #expect(loaded.scenarios.count == original.scenarios.count)
        #expect(loaded.scenarios[0].name == original.scenarios[0].name)
    }
}
