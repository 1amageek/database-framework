#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

internal struct MonotonicTimestamp: Sendable {
    fileprivate let instant: ContinuousClock.Instant

    var uptimeNanoseconds: UInt64 {
        MonotonicClock.nanoseconds(from: MonotonicClock.epoch, to: instant)
    }
}

internal enum MonotonicClock {
    fileprivate static let clock = ContinuousClock()
    fileprivate static let epoch = clock.now

    static func now() -> MonotonicTimestamp {
        _ = epoch
        return MonotonicTimestamp(instant: clock.now)
    }

    fileprivate static func nanoseconds(
        from start: ContinuousClock.Instant,
        to end: ContinuousClock.Instant
    ) -> UInt64 {
        let components = start.duration(to: end).components
        let secondsNanoseconds = components.seconds * 1_000_000_000
        let attosecondsNanoseconds = components.attoseconds / 1_000_000_000
        return UInt64(max(0, secondsNanoseconds + attosecondsNanoseconds))
    }
}
