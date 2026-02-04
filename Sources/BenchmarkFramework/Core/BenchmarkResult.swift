import Foundation

public struct BenchmarkResult: Codable, Sendable {
    public let name: String
    public let timestamp: Date
    public let environment: EnvironmentInfo
    public let scenarios: [ScenarioResult]

    public init(
        name: String,
        timestamp: Date = Date(),
        environment: EnvironmentInfo,
        scenarios: [ScenarioResult]
    ) {
        self.name = name
        self.timestamp = timestamp
        self.environment = environment
        self.scenarios = scenarios
    }
}

public struct EnvironmentInfo: Codable, Sendable {
    public let platform: String
    public let osVersion: String
    public let swiftVersion: String
    public let commitHash: String?

    public init(
        platform: String,
        osVersion: String,
        swiftVersion: String,
        commitHash: String? = nil
    ) {
        self.platform = platform
        self.osVersion = osVersion
        self.swiftVersion = swiftVersion
        self.commitHash = commitHash
    }

    public static func current(commitHash: String? = nil) -> EnvironmentInfo {
        let platform: String
        #if os(macOS)
        platform = "macOS"
        #elseif os(Linux)
        platform = "Linux"
        #elseif os(Windows)
        platform = "Windows"
        #else
        platform = "Unknown"
        #endif

        let osVersion = ProcessInfo.processInfo.operatingSystemVersionString

        let swiftVersion: String
        #if swift(>=6.2)
        swiftVersion = "6.2+"
        #elseif swift(>=6.0)
        swiftVersion = "6.0+"
        #else
        swiftVersion = "5.x"
        #endif

        return EnvironmentInfo(
            platform: platform,
            osVersion: osVersion,
            swiftVersion: swiftVersion,
            commitHash: commitHash
        )
    }
}

public struct ScenarioResult: Codable, Sendable {
    public let name: String
    public let metrics: MetricsSnapshot

    public init(name: String, metrics: MetricsSnapshot) {
        self.name = name
        self.metrics = metrics
    }
}

public struct MetricsSnapshot: Codable, Sendable {
    public let latency: LatencyMetrics
    public let throughput: ThroughputMetrics?
    public let memory: MemoryMetrics?
    public let storage: StorageMetrics?
    public let accuracy: AccuracyMetrics?

    public init(
        latency: LatencyMetrics,
        throughput: ThroughputMetrics? = nil,
        memory: MemoryMetrics? = nil,
        storage: StorageMetrics? = nil,
        accuracy: AccuracyMetrics? = nil
    ) {
        self.latency = latency
        self.throughput = throughput
        self.memory = memory
        self.storage = storage
        self.accuracy = accuracy
    }
}

public struct AccuracyMetrics: Codable, Sendable {
    public let recall: Double?
    public let precision: Double?
    public let f1Score: Double?

    public init(recall: Double? = nil, precision: Double? = nil, f1Score: Double? = nil) {
        self.recall = recall
        self.precision = precision
        self.f1Score = f1Score
    }
}

public struct ComparisonResult: Codable, Sendable {
    public let name: String
    public let baseline: ScenarioResult
    public let optimized: ScenarioResult
    public let improvement: ImprovementMetrics

    public init(
        name: String,
        baseline: ScenarioResult,
        optimized: ScenarioResult,
        improvement: ImprovementMetrics
    ) {
        self.name = name
        self.baseline = baseline
        self.optimized = optimized
        self.improvement = improvement
    }
}

public struct ImprovementMetrics: Codable, Sendable {
    public let latencyP95: Double
    public let throughput: Double?
    public let memoryPeak: Double?
    public let storage: Double?

    public init(
        latencyP95: Double,
        throughput: Double? = nil,
        memoryPeak: Double? = nil,
        storage: Double? = nil
    ) {
        self.latencyP95 = latencyP95
        self.throughput = throughput
        self.memoryPeak = memoryPeak
        self.storage = storage
    }
}

public struct StrategyComparisonResult: Codable, Sendable {
    public let name: String
    public let strategies: [ScenarioResult]

    public init(name: String, strategies: [ScenarioResult]) {
        self.name = name
        self.strategies = strategies
    }
}

public struct ScalabilityResult: Codable, Sendable {
    public let name: String
    public let dataPoints: [DataPoint]

    public init(name: String, dataPoints: [DataPoint]) {
        self.name = name
        self.dataPoints = dataPoints
    }

    public struct DataPoint: Codable, Sendable {
        public let dataSize: Int
        public let metrics: MetricsSnapshot

        public init(dataSize: Int, metrics: MetricsSnapshot) {
            self.dataSize = dataSize
            self.metrics = metrics
        }
    }
}
