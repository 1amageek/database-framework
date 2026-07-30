import StorageKit

internal struct MonotonicTimestamp: Sendable {
    fileprivate let instant: StorageInstant

    var uptimeNanoseconds: UInt64 {
        let duration = StorageInstant(
            durationSinceReference: .zero
        ).duration(to: instant)
        let components = duration.components
        guard components.seconds >= 0 else { return 0 }
        let seconds = UInt64(components.seconds)
        let secondNanoseconds = seconds.multipliedReportingOverflow(
            by: 1_000_000_000
        )
        guard !secondNanoseconds.overflow else { return UInt64.max }
        let fractionalNanoseconds = UInt64(
            max(0, components.attoseconds / 1_000_000_000)
        )
        let total = secondNanoseconds.partialValue.addingReportingOverflow(
            fractionalNanoseconds
        )
        return total.overflow ? UInt64.max : total.partialValue
    }
}

internal struct MonotonicClock: Sendable {
    private let source: any StorageMonotonicClock

    init(source: any StorageMonotonicClock) {
        self.source = source
    }

    func now() -> MonotonicTimestamp {
        MonotonicTimestamp(instant: source.now)
    }
}
