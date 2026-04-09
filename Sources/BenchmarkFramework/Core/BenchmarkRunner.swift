import Foundation

public final class BenchmarkRunner: Sendable {
    public struct Config: Sendable {
        public var warmupIterations: Int
        public var measurementIterations: Int
        public var throughputDuration: TimeInterval
        public var comparisonRounds: Int
        public var measureMemory: Bool
        public var memorySampleInterval: TimeInterval

        public init(
            warmupIterations: Int = 10,
            measurementIterations: Int = 100,
            throughputDuration: TimeInterval = 10.0,
            comparisonRounds: Int = 3,
            measureMemory: Bool = false,
            memorySampleInterval: TimeInterval = 0.1
        ) {
            self.warmupIterations = warmupIterations
            self.measurementIterations = measurementIterations
            self.throughputDuration = throughputDuration
            self.comparisonRounds = max(1, comparisonRounds)
            self.measureMemory = measureMemory
            self.memorySampleInterval = memorySampleInterval
        }

        public static let `default` = Config()

        public static let quick = Config(
            warmupIterations: 5,
            measurementIterations: 20,
            throughputDuration: 3.0,
            comparisonRounds: 3,
            measureMemory: false
        )
    }

    private let config: Config
    private let comparator: BenchmarkComparator

    public init(config: Config = .default) {
        self.config = config
        self.comparator = BenchmarkComparator()
    }

    /// Compare baseline vs optimized implementation
    public func compare<T>(
        name: String,
        baseline: @Sendable () async throws -> T,
        optimized: @Sendable () async throws -> T,
        verify: @Sendable (T, T) throws -> Void = { _, _ in }
    ) async throws -> ComparisonResult {
        let baselineResult = try await measureScenario(
            name: "Baseline",
            operation: baseline
        )

        let optimizedResult = try await measureScenario(
            name: "Optimized",
            operation: optimized
        )

        // Verify results match
        let baselineValue = try await baseline()
        let optimizedValue = try await optimized()
        try verify(baselineValue, optimizedValue)

        let improvement = comparator.compare(
            baseline: baselineResult,
            optimized: optimizedResult
        )

        return ComparisonResult(
            name: name,
            baseline: baselineResult,
            optimized: optimizedResult,
            improvement: improvement
        )
    }

    /// Compare multiple strategies
    public func compareStrategies<T>(
        name: String,
        strategies: [(String, @Sendable () async throws -> T)]
    ) async throws -> StrategyComparisonResult {
        guard !strategies.isEmpty else {
            return StrategyComparisonResult(name: name, strategies: [])
        }

        let roundCount = config.comparisonRounds
        var perStrategyResults = Array(repeating: [ScenarioResult](), count: strategies.count)

        for round in 0..<roundCount {
            for offset in 0..<strategies.count {
                let strategyIndex = (round + offset) % strategies.count
                let (strategyName, operation) = strategies[strategyIndex]
                let result = try await measureScenario(
                    name: strategyName,
                    operation: operation
                )
                perStrategyResults[strategyIndex].append(result)
            }
        }

        let aggregated = strategies.enumerated().map { index, strategy in
            aggregateScenarioResults(
                name: strategy.0,
                results: perStrategyResults[index]
            )
        }

        return StrategyComparisonResult(name: name, strategies: aggregated)
    }

    /// Measure scalability across different data sizes
    public func scale<T>(
        name: String,
        dataSizes: [Int],
        operation: @Sendable (Int) async throws -> T
    ) async throws -> ScalabilityResult {
        var dataPoints: [ScalabilityResult.DataPoint] = []

        for size in dataSizes {
            let scenario = try await measureScenario(
                name: "Size \(size)",
                operation: { try await operation(size) }
            )

            dataPoints.append(
                ScalabilityResult.DataPoint(
                    dataSize: size,
                    metrics: scenario.metrics
                )
            )
        }

        return ScalabilityResult(name: name, dataPoints: dataPoints)
    }

    /// Measure a single scenario
    private func measureScenario<T>(
        name: String,
        operation: @Sendable () async throws -> T
    ) async throws -> ScenarioResult {
        let latency = try await LatencyMetrics.measure(
            warmupIterations: config.warmupIterations,
            iterations: config.measurementIterations,
            operation: { _ = try await operation() }
        )

        let throughput = try await ThroughputMetrics.measure(
            duration: config.throughputDuration,
            operation: { _ = try await operation() }
        )

        let memory: MemoryMetrics?
        if config.measureMemory {
            memory = try await MemoryMetrics.measure(
                sampleInterval: config.memorySampleInterval,
                operation: { _ = try await operation() }
            )
        } else {
            memory = nil
        }

        let metrics = MetricsSnapshot(
            latency: latency,
            throughput: throughput,
            memory: memory,
            storage: nil,
            accuracy: nil
        )

        return ScenarioResult(name: name, metrics: metrics)
    }

    private func aggregateScenarioResults(
        name: String,
        results: [ScenarioResult]
    ) -> ScenarioResult {
        guard results.count > 1 else {
            return results[0]
        }

        let latency = aggregateLatency(results.map(\.metrics.latency))
        let throughput = aggregateThroughput(results.compactMap(\.metrics.throughput))
        let memory = aggregateMemory(results.compactMap(\.metrics.memory))
        let storage = aggregateStorage(results.compactMap(\.metrics.storage))
        let accuracy = aggregateAccuracy(results.compactMap(\.metrics.accuracy))

        return ScenarioResult(
            name: name,
            metrics: MetricsSnapshot(
                latency: latency,
                throughput: throughput,
                memory: memory,
                storage: storage,
                accuracy: accuracy
            )
        )
    }

    private func aggregateLatency(_ metrics: [LatencyMetrics]) -> LatencyMetrics {
        LatencyMetrics(
            p50: median(metrics.map(\.p50)),
            p95: median(metrics.map(\.p95)),
            p99: median(metrics.map(\.p99)),
            avg: median(metrics.map(\.avg)),
            min: metrics.map(\.min).min() ?? 0,
            max: metrics.map(\.max).max() ?? 0,
            samples: metrics.reduce(0) { $0 + $1.samples }
        )
    }

    private func aggregateThroughput(_ metrics: [ThroughputMetrics]) -> ThroughputMetrics? {
        guard !metrics.isEmpty else { return nil }

        let totalOperations = Int(median(metrics.map { Double($0.totalOperations) }).rounded())
        let totalDuration = median(metrics.map(\.durationSeconds))
        let opsPerSecond = median(metrics.map(\.opsPerSecond))

        return ThroughputMetrics(
            opsPerSecond: opsPerSecond,
            totalOperations: totalOperations,
            durationSeconds: totalDuration
        )
    }

    private func aggregateMemory(_ metrics: [MemoryMetrics]) -> MemoryMetrics? {
        guard !metrics.isEmpty else { return nil }

        return MemoryMetrics(
            peakMB: metrics.map(\.peakMB).max() ?? 0,
            avgMB: average(metrics.map(\.avgMB)),
            samples: metrics.reduce(0) { $0 + $1.samples }
        )
    }

    private func aggregateStorage(_ metrics: [StorageMetrics]) -> StorageMetrics? {
        guard !metrics.isEmpty else { return nil }

        let avgBytes = Int(average(metrics.map { Double($0.bytes) }).rounded())
        return StorageMetrics(bytes: avgBytes)
    }

    private func aggregateAccuracy(_ metrics: [AccuracyMetrics]) -> AccuracyMetrics? {
        guard !metrics.isEmpty else { return nil }

        let recalls = metrics.compactMap(\.recall)
        let precisions = metrics.compactMap(\.precision)
        let f1Scores = metrics.compactMap(\.f1Score)

        return AccuracyMetrics(
            recall: recalls.isEmpty ? nil : average(recalls),
            precision: precisions.isEmpty ? nil : average(precisions),
            f1Score: f1Scores.isEmpty ? nil : average(f1Scores)
        )
    }

    private func average(_ values: [Double]) -> Double {
        guard !values.isEmpty else { return 0 }
        return values.reduce(0.0, +) / Double(values.count)
    }

    private func median(_ values: [Double]) -> Double {
        guard !values.isEmpty else { return 0 }
        let sorted = values.sorted()
        let middle = sorted.count / 2
        if sorted.count.isMultiple(of: 2) {
            return (sorted[middle - 1] + sorted[middle]) / 2
        }
        return sorted[middle]
    }
}
