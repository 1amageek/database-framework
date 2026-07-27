import DatabaseTypes
import DatabaseKit
import DatabaseEngine

/// Request-accounted distinctness sidecar for aggregate values.
final class SPARQLRetainedFieldValueSet {
    private static let containerByteCount: UInt64 = 64
    private static let capacitySlotByteCount: UInt64 = 96

    private var values: Set<FieldValue>
    private let reservation: DatabaseIntermediateReservation
    private let footprintMeter: SPARQLBindingFootprintMeter
    private let stage: DatabaseWorkStage
    private var accountedCapacity: Int

    private init(
        reservation: DatabaseIntermediateReservation,
        footprintMeter: SPARQLBindingFootprintMeter,
        stage: DatabaseWorkStage
    ) {
        self.values = []
        self.reservation = reservation
        self.footprintMeter = footprintMeter
        self.stage = stage
        self.accountedCapacity = 0
    }

    static func make(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .deduplication
    ) throws -> SPARQLRetainedFieldValueSet {
        let footprintMeter = try SPARQLBindingFootprintMeter.make(
            workMeter: workMeter,
            stage: stage
        )
        do {
            return SPARQLRetainedFieldValueSet(
                reservation: try workMeter.reserveIntermediate(
                    bytes: containerByteCount,
                    at: stage
                ),
                footprintMeter: footprintMeter,
                stage: stage
            )
        } catch {
            footprintMeter.shutdown()
            throw error
        }
    }

    func insert(_ value: borrowing FieldValue) throws -> Bool {
        guard !values.contains(value) else { return false }
        let footprint = try footprintMeter.footprint(of: value)
        let requiredCount = try Self.checkedIncrement(values.count)
        let targetCapacity = try Self.targetCapacity(
            current: accountedCapacity,
            requiredCount: requiredCount
        )
        let capacityBytes = try Self.checkedMultiply(
            UInt64(targetCapacity - accountedCapacity),
            Self.capacitySlotByteCount
        )
        try reservation.reserveAdditional(
            rows: footprint.rows,
            bytes: try Self.checkedAdd(footprint.bytes, capacityBytes),
            at: stage
        )
        if targetCapacity != accountedCapacity {
            values.reserveCapacity(targetCapacity)
            accountedCapacity = targetCapacity
        }
        let insertion = values.insert(copy value)
        precondition(
            insertion.inserted,
            "Aggregate value membership changed during admitted insertion"
        )
        return true
    }

    private static func targetCapacity(
        current: Int,
        requiredCount: Int
    ) throws -> Int {
        guard requiredCount > current else { return current }
        var capacity = max(1, current)
        while capacity < requiredCount {
            let (next, overflow) = capacity.multipliedReportingOverflow(by: 2)
            guard !overflow else {
                throw SPARQLGroupStorageError.capacityOverflow
            }
            capacity = next
        }
        return capacity
    }

    private static func checkedIncrement(_ value: Int) throws -> Int {
        let (result, overflow) = value.addingReportingOverflow(1)
        guard !overflow else {
            throw SPARQLGroupStorageError.capacityOverflow
        }
        return result
    }

    private static func checkedMultiply(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = left.multipliedReportingOverflow(by: right)
        guard !overflow else {
            throw SPARQLGroupStorageError.capacityOverflow
        }
        return result
    }

    private static func checkedAdd(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = left.addingReportingOverflow(right)
        guard !overflow else {
            throw SPARQLGroupStorageError.capacityOverflow
        }
        return result
    }
}
