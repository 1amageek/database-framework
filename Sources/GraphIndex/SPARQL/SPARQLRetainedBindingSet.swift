import DatabaseEngine

enum SPARQLRetainedBindingSetError: Error, Sendable, Equatable {
    case capacityOverflow
}

/// Request-accounted hash sidecar for order-preserving SPARQL deduplication.
/// Binding payload storage remains shared with the source relation; the fixed
/// slot charge covers only Set buckets and copied value-owner headers.
struct SPARQLRetainedBindingSet: ~Copyable {
    private static let containerByteCount: UInt64 = 64
    private static let capacitySlotByteCount: UInt64 = 192

    private var values: Set<VariableBinding>
    private let reservation: DatabaseIntermediateReservation
    private var accountedCapacity: Int
    private let stage: DatabaseWorkStage

    private init(
        reservation: DatabaseIntermediateReservation,
        capacity: Int,
        stage: DatabaseWorkStage
    ) {
        var values: Set<VariableBinding> = []
        values.reserveCapacity(capacity)
        self.values = values
        self.reservation = reservation
        self.accountedCapacity = capacity
        self.stage = stage
    }

    static func make(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        expectedCount: Int = 0
    ) throws -> SPARQLRetainedBindingSet {
        let capacity = try targetCapacity(current: 0, requiredCount: expectedCount)
        let slotBytes = try checkedMultiply(
            UInt64(capacity),
            capacitySlotByteCount
        )
        let reservation = try workMeter.reserveIntermediate(
            bytes: try checkedAdd(containerByteCount, slotBytes),
            at: stage
        )
        return SPARQLRetainedBindingSet(
            reservation: reservation,
            capacity: capacity,
            stage: stage
        )
    }

    /// Returns true only for a newly retained binding. Membership is checked
    /// before growth admission, so duplicates do not inflate capacity claims.
    mutating func insert(
        _ binding: borrowing VariableBinding
    ) throws -> Bool {
        guard !values.contains(binding) else { return false }
        let requiredCount = try Self.checkedIncrement(values.count)
        let targetCapacity = try Self.targetCapacity(
            current: accountedCapacity,
            requiredCount: requiredCount
        )
        if targetCapacity != accountedCapacity {
            let additionalCapacity = targetCapacity - accountedCapacity
            let additionalBytes = try Self.checkedMultiply(
                UInt64(additionalCapacity),
                Self.capacitySlotByteCount
            )
            try reservation.reserveAdditional(
                bytes: additionalBytes,
                at: stage
            )
            values.reserveCapacity(targetCapacity)
            accountedCapacity = targetCapacity
        }

        let inserted = values.insert(copy binding).inserted
        precondition(inserted, "Binding membership changed during insertion")
        return true
    }

    private static func targetCapacity(
        current: Int,
        requiredCount: Int
    ) throws -> Int {
        guard requiredCount > current else { return current }
        var capacity = max(current, 1)
        while capacity < requiredCount {
            let (next, overflow) = capacity.multipliedReportingOverflow(by: 2)
            guard !overflow else {
                throw SPARQLRetainedBindingSetError.capacityOverflow
            }
            capacity = next
        }
        return capacity
    }

    private static func checkedIncrement(_ value: Int) throws -> Int {
        let (result, overflow) = value.addingReportingOverflow(1)
        guard !overflow else {
            throw SPARQLRetainedBindingSetError.capacityOverflow
        }
        return result
    }

    private static func checkedMultiply(
        _ lhs: UInt64,
        _ rhs: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = lhs.multipliedReportingOverflow(by: rhs)
        guard !overflow else {
            throw SPARQLRetainedBindingSetError.capacityOverflow
        }
        return result
    }

    private static func checkedAdd(
        _ lhs: UInt64,
        _ rhs: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = lhs.addingReportingOverflow(rhs)
        guard !overflow else {
            throw SPARQLRetainedBindingSetError.capacityOverflow
        }
        return result
    }
}
