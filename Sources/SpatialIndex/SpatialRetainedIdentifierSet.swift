import DatabaseEngine
import DatabaseTypes

/// Request-accounted identity set used across adaptive spatial scan rounds.
///
/// The set owns identifiers after each per-radius model batch is released, so
/// both hash capacity and identifier payloads are admitted before insertion.
struct SpatialRetainedIdentifierSet: ~Copyable {
    private static let containerByteCount: UInt64 = 64
    private static let capacitySlotByteCount: UInt64 = 96

    private var values: Set<ReferenceIdentifier> = []
    private let reservation: DatabaseIntermediateReservation
    private var accountedCapacity = 0

    init(workMeter: DatabaseWorkMeter) throws {
        self.reservation = try workMeter.reserveIntermediate(
            bytes: Self.containerByteCount,
            at: .projection
        )
    }

    mutating func insert(
        _ identifier: borrowing ReferenceIdentifier
    ) throws -> Bool {
        guard !values.contains(identifier) else { return false }
        let (requiredCount, countOverflow) = values.count
            .addingReportingOverflow(1)
        guard !countOverflow else {
            throw DatabaseRetainedHashTableLayoutError.capacityOverflow(
                currentCapacity: accountedCapacity
            )
        }
        let layout = try DatabaseRetainedHashTableLayout.validated(
            containerByteCount: Self.containerByteCount,
            elementCapacitySlotByteCount: Self.capacitySlotByteCount
        )
        let growth = try layout.growth(
            from: accountedCapacity,
            toFit: requiredCount
        )
        let payloadBytes = try Self.retainedPayloadByteCount(identifier)
        try reservation.reserveAdditional(
            bytes: try DatabaseIntermediateFootprint(
                bytes: growth.additionalByteCount
            ).adding(DatabaseIntermediateFootprint(bytes: payloadBytes)).bytes,
            at: .projection
        )
        if growth.capacity != accountedCapacity {
            values.reserveCapacity(growth.capacity)
            accountedCapacity = growth.capacity
        }
        let inserted = values.insert(copy identifier).inserted
        precondition(inserted, "Identifier membership changed during insertion")
        return true
    }

    private static func retainedPayloadByteCount(
        _ identifier: borrowing ReferenceIdentifier
    ) throws -> UInt64 {
        switch identifier {
        case .bool, .int8, .int16, .int32, .int64,
             .uint8, .uint16, .uint32, .uint64, .uuid:
            return 16
        case .string(let value):
            return try DatabaseIntermediateFootprint(bytes: 32).adding(
                DatabaseIntermediateFootprint(bytes: UInt64(value.utf8.count))
            ).bytes
        case .bytes(let value):
            guard let retainedByteCount = value.retainedByteCount else {
                throw DatabaseIntermediateFootprintError
                    .canonicalValueByteCountUnavailable
            }
            return try DatabaseIntermediateFootprint(bytes: 32).adding(
                DatabaseIntermediateFootprint(
                    bytes: UInt64(retainedByteCount)
                )
            ).bytes
        case .composite(let values):
            var footprint = try DatabaseIntermediateFootprint(bytes: 32)
                .adding(
                    try DatabaseIntermediateFootprint(
                        bytes: UInt64(MemoryLayout<ReferenceIdentifier>.stride)
                    ).multiplied(by: UInt64(values.count))
                )
            for value in values {
                footprint = try footprint.adding(
                    DatabaseIntermediateFootprint(
                        bytes: try retainedPayloadByteCount(value)
                    )
                )
            }
            return footprint.bytes
        }
    }
}
