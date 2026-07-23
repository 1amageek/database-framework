import DatabaseEngine

/// Request-accounted hash index used only while constructing a group
/// partition. Key value payloads are owned by the flat retained key buffer;
/// this sidecar accounts for hash buckets and key-array owner storage.
struct SPARQLGroupLookup: ~Copyable {
    private static let containerByteCount: UInt64 = 64
    private static let capacitySlotByteCount: UInt64 = 128
    private static let keyArrayOwnerByteCount: UInt64 = 64
    private static let keyValueSlotByteCount: UInt64 = 64

    private var identifiers: [[GroupValue]: Int]
    private let reservation: DatabaseIntermediateReservation
    private var accountedCapacity: Int

    private init(reservation: DatabaseIntermediateReservation) {
        self.identifiers = [:]
        self.reservation = reservation
        self.accountedCapacity = 0
    }

    static func make(
        workMeter: DatabaseWorkMeter
    ) throws -> SPARQLGroupLookup {
        SPARQLGroupLookup(
            reservation: try workMeter.reserveIntermediate(
                bytes: containerByteCount,
                at: .aggregateInput
            )
        )
    }

    /// Returns an existing identifier or retains the new key only after its
    /// complete hash/key-owner footprint has been admitted.
    mutating func identifier(
        for key: borrowing [GroupValue],
        inserting newIdentifier: Int
    ) throws -> (identifier: Int, inserted: Bool) {
        let retainedKey = copy key
        if let identifier = identifiers[retainedKey] {
            return (identifier, false)
        }
        guard newIdentifier >= 0 else {
            throw SPARQLGroupStorageError.invalidGroupIdentifier(
                newIdentifier
            )
        }

        let requiredCount = try Self.checkedIncrement(identifiers.count)
        let targetCapacity = try Self.targetCapacity(
            current: accountedCapacity,
            requiredCount: requiredCount
        )
        let capacityBytes = try Self.checkedMultiply(
            UInt64(targetCapacity - accountedCapacity),
            Self.capacitySlotByteCount
        )
        let keySlotBytes = try Self.checkedMultiply(
            UInt64(key.count),
            Self.keyValueSlotByteCount
        )
        let retainedBytes = try Self.checkedAdd(
            capacityBytes,
            try Self.checkedAdd(Self.keyArrayOwnerByteCount, keySlotBytes)
        )
        try reservation.reserveAdditional(
            bytes: retainedBytes,
            at: .aggregateInput
        )

        if targetCapacity != accountedCapacity {
            identifiers.reserveCapacity(targetCapacity)
            accountedCapacity = targetCapacity
        }
        let insertion = identifiers.updateValue(
            newIdentifier,
            forKey: retainedKey
        )
        precondition(
            insertion == nil,
            "Group membership changed during admitted insertion"
        )
        return (newIdentifier, true)
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
