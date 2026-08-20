import DatabaseEngine
import DatabaseTypes
import StorageKit

struct BenchmarkProcessMonotonicClock: StorageMonotonicClock {
    private static let clock = ContinuousClock()
    private static let origin = clock.now

    var now: StorageInstant {
        StorageInstant(
            durationSinceReference: Self.origin.duration(to: Self.clock.now)
        )
    }

    func sleep(
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

struct FixedBenchmarkWallClock: WallClock {
    let now: Timestamp

    init(now: Timestamp = Timestamp(secondsSinceUnixEpoch: 0)) {
        self.now = now
    }
}
