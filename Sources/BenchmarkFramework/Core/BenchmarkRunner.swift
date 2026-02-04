import Foundation

public final class BenchmarkRunner: Sendable {
    public struct Config: Sendable {
        public var warmupIterations: Int
        public var measurementIterations: Int
        public var throughputDuration: TimeInterval
        public var measureMemory: Bool
        public var memorySampleInterval: TimeInterval

        public init(
            warmupIterations: Int = 10,
            measurementIterations: Int = 100,
            throughputDuration: TimeInterval = 10.0,
            measureMemory: Bool = false,
            memorySampleInterval: TimeInterval = 0.1
        ) {
            self.warmupIterations = warmupIterations
            self.measurementIterations = measurementIterations
            self.throughputDuration = throughputDuration
            self.measureMemory = measureMemory
            self.memorySampleInterval = memorySampleInterval
        }

        public static let `default` = Config()

        public static let quick = Config(
            warmupIterations: 5,
            measurementIterations: 20,
            throughputDuration: 3.0,
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
        var results: [ScenarioResult] = []

        for (strategyName, operation) in strategies {
            let result = try await measureScenario(
                name: strategyName,
                operation: operation
            )
            results.append(result)
        }

        return StrategyComparisonResult(name: name, strategies: results)
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
}
