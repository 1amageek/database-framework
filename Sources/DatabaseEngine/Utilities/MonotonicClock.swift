import StorageKit

/// Converts injected monotonic instants into bounded measurement values.
///
/// The storage clock owns the platform-specific time source. DatabaseEngine
/// only computes elapsed durations and therefore has no dependency on
/// `ContinuousClock` or another runtime-specific clock implementation.
package enum DatabaseMonotonicMeasurement {
    package static func nanoseconds(
        from start: StorageInstant,
        to end: StorageInstant
    ) -> UInt64 {
        nanoseconds(start.duration(to: end))
    }

    package static func nanoseconds(_ duration: Duration) -> UInt64 {
        guard duration > .zero else {
            return 0
        }

        let components = duration.components
        let (wholeNanoseconds, secondsOverflow) =
            components.seconds.multipliedReportingOverflow(by: 1_000_000_000)
        guard !secondsOverflow else {
            return UInt64.max
        }

        let fractionalNanoseconds = components.attoseconds / 1_000_000_000
        let (nanoseconds, additionOverflow) =
            wholeNanoseconds.addingReportingOverflow(fractionalNanoseconds)
        guard !additionOverflow, nanoseconds >= 0 else {
            return UInt64.max
        }
        return UInt64(nanoseconds)
    }
}
