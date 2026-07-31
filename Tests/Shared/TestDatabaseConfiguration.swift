import DatabaseEngine
import DatabaseTypes
import StorageKit

/// Process-local monotonic time used by tests that do not exercise time.
public struct TestProcessMonotonicClock: StorageMonotonicClock {
    private static let clock = ContinuousClock()
    private static let origin = clock.now

    public init() {}

    public var now: StorageInstant {
        StorageInstant(
            durationSinceReference: Self.origin.duration(to: Self.clock.now)
        )
    }

    public func sleep(
        until deadline: StorageInstant
    ) async throws(StorageClockError) {
        let remaining = now.duration(to: deadline)
        guard remaining > .zero else { return }
        do {
            try await Self.clock.sleep(for: remaining)
        } catch {
            throw .cancelled
        }
    }
}

/// Stable absolute time used by tests that do not exercise wall-clock behavior.
public struct FixedTestWallClock: WallClock {
    public let now: Timestamp

    public init() {
        self.now = Timestamp(secondsSinceUnixEpoch: 0)
    }

    public init(now: Timestamp) {
        self.now = now
    }
}

public extension DBConfiguration {
    /// Creates an explicitly clocked configuration for tests.
    static func testing(
        name: String? = nil,
        storageEngine: any StorageEngine,
        indexConfigurations: [any IndexRuntimeConfiguration] = [],
        itemStorage: ItemStorageConfiguration = .v1,
        logging: DatabaseLoggingConfiguration = .disabled,
        metrics: DatabaseMetricsConfiguration = .disabled
    ) throws -> DBConfiguration {
        DBConfiguration(
            name: name,
            storageEngine: storageEngine,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            indexConfigurations: indexConfigurations,
            itemStorage: itemStorage,
            logging: logging,
            metrics: metrics
        )
    }
}
