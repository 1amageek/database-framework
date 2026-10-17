#if DATABASE_MULTI_BASE
import DatabaseKit
import DatabaseTypes

/// One selected immutable extremum; no member page or executable context escapes.
struct CompositionAggregateExtremum: Sendable {
    let value: QueryValue
    let type: QueryColumnType
    private let reservation: DatabaseIntermediateReservation

    static func retaining(_ value: borrowing QueryValue, type: QueryColumnType,
        workMeter: DatabaseWorkMeter) throws -> Self {
        var footprint = try CanonicalRelationalFootprintMeter.valueFootprint(
            of: value, workMeter: workMeter, stage: .aggregateInput)
        let descriptor: QueryCompositeType?
        switch type {
        case .sql(.composite(let composite)), .sql(.array(.composite(let composite))):
            descriptor = composite
        default: descriptor = nil
        }
        if let descriptor {
            let measured = try descriptor.measure()
            try workMeter.consume(measured.nodeCount, at: .aggregateInput)
            footprint = try footprint.adding(DatabaseIntermediateFootprint(bytes: measured.retainedByteCount))
        }
        let reservation = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .aggregateInput)
        return Self(value: copy value, type: type, reservation: reservation)
    }
}
#endif
