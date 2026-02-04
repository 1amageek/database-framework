import Foundation

public struct ThresholdConfig: Codable, Sendable {
    public var latencyThreshold: Double
    public var throughputThreshold: Double
    public var memoryThreshold: Double
    public var storageThreshold: Double

    public init(
        latencyThreshold: Double = 0.10,
        throughputThreshold: Double = 0.10,
        memoryThreshold: Double = 0.20,
        storageThreshold: Double = 0.20
    ) {
        self.latencyThreshold = latencyThreshold
        self.throughputThreshold = throughputThreshold
        self.memoryThreshold = memoryThreshold
        self.storageThreshold = storageThreshold
    }

    public static let `default` = ThresholdConfig()

    public static let strict = ThresholdConfig(
        latencyThreshold: 0.05,
        throughputThreshold: 0.05,
        memoryThreshold: 0.10,
        storageThreshold: 0.10
    )

    public static let relaxed = ThresholdConfig(
        latencyThreshold: 0.20,
        throughputThreshold: 0.20,
        memoryThreshold: 0.30,
        storageThreshold: 0.30
    )
}
