import DatabaseEngine
import DatabaseValue

/// Canonical solution occurrence order without copying binding rows.
struct SPARQLSolutionFingerprintOrder: ~Copyable {
    private struct Entry: Sendable {
        let sourceIndex: Int
        let fingerprint: DatabaseBytes
        var occurrence: UInt64
    }

    private static let arrayOverheadByteCount: UInt64 = 64
    private static let entryCapacitySlotByteCount: UInt64 = 64
    private static let fingerprintPayloadByteCount: UInt64 = 32

    private let entries: [Entry]
    private let reservation: DatabaseIntermediateReservation

    private init(
        entries: consuming [Entry],
        reservation: DatabaseIntermediateReservation
    ) {
        self.entries = entries
        self.reservation = reservation
    }

    static func build(
        bindings: borrowing SPARQLRetainedBindings,
        workMeter: DatabaseWorkMeter
    ) throws -> SPARQLSolutionFingerprintOrder {
        let rowCount = UInt64(bindings.count)
        try workMeter.consume(rowCount, at: .sortInput)
        let entryBytes = try checkedMultiply(
            rowCount,
            Self.entryCapacitySlotByteCount,
            workMeter: workMeter
        )
        let fingerprintBytes = try checkedMultiply(
            rowCount,
            Self.fingerprintPayloadByteCount,
            workMeter: workMeter
        )
        let reservation = try workMeter.reserveIntermediate(
            rows: rowCount,
            bytes: try checkedAdd(
                Self.arrayOverheadByteCount,
                try checkedAdd(
                    entryBytes,
                    fingerprintBytes,
                    workMeter: workMeter
                ),
                workMeter: workMeter
            ),
            at: .sortInput
        )

        var entries: [Entry] = []
        entries.reserveCapacity(bindings.count)
        for index in 0..<bindings.count {
            entries.append(
                try bindings.withElement(at: index) { binding in
                    Entry(
                        sourceIndex: index,
                        fingerprint: try binding.canonicalFingerprint(
                            workMeter: workMeter
                        ),
                        occurrence: 0
                    )
                }
            )
        }
        try entries.sort { lhs, rhs in
            try workMeter.consume(2, at: .sortComparison)
            if lhs.fingerprint != rhs.fingerprint {
                return lhs.fingerprint.lexicographicallyPrecedes(
                    rhs.fingerprint
                )
            }
            return lhs.sourceIndex < rhs.sourceIndex
        }

        var previousFingerprint: DatabaseBytes?
        var nextOccurrence: UInt64 = 0
        for index in entries.indices {
            if entries[index].fingerprint == previousFingerprint {
                entries[index].occurrence = nextOccurrence
                let (incremented, overflow) = nextOccurrence
                    .addingReportingOverflow(1)
                guard !overflow else {
                    throw intermediateOverflow(workMeter: workMeter)
                }
                nextOccurrence = incremented
            } else {
                previousFingerprint = entries[index].fingerprint
                entries[index].occurrence = 0
                nextOccurrence = 1
            }
        }
        return SPARQLSolutionFingerprintOrder(
            entries: consume entries,
            reservation: reservation
        )
    }

    borrowing func forEach<Failure: Error>(
        _ body: (
            _ sourceIndex: Int,
            _ fingerprint: borrowing DatabaseBytes,
            _ occurrence: UInt64
        ) throws(Failure) -> Void
    ) throws(Failure) {
        for entry in entries {
            try body(
                entry.sourceIndex,
                entry.fingerprint,
                entry.occurrence
            )
        }
    }

    private static func checkedMultiply(
        _ left: UInt64,
        _ right: UInt64,
        workMeter: DatabaseWorkMeter
    ) throws -> UInt64 {
        let (result, overflow) = left.multipliedReportingOverflow(by: right)
        guard !overflow else {
            throw intermediateOverflow(workMeter: workMeter)
        }
        return result
    }

    private static func checkedAdd(
        _ left: UInt64,
        _ right: UInt64,
        workMeter: DatabaseWorkMeter
    ) throws -> UInt64 {
        let (result, overflow) = left.addingReportingOverflow(right)
        guard !overflow else {
            throw intermediateOverflow(workMeter: workMeter)
        }
        return result
    }

    private static func intermediateOverflow(
        workMeter: DatabaseWorkMeter
    ) -> DatabaseWorkLimitError {
        DatabaseWorkLimitError.maximumIntermediateBytes(
            stage: .sortInput,
            consumed: workMeter.retainedIntermediateBytes,
            requested: UInt64.max,
            maximum: workMeter.budget.maximumIntermediateBytes
        )
    }
}
