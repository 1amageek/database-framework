import DatabaseKit

/// Invocation-confined allocation admission; no mutable owner escapes evaluation.
final class SQLAggregateWorkspace {
    let meter: DatabaseWorkMeter
    let reservation: DatabaseIntermediateReservation

    init(_ meter: DatabaseWorkMeter) throws {
        self.meter = meter
        reservation = try meter.reserveIntermediate(at: .aggregateInput)
    }

    deinit { reservation.release() }

    func prepare<Element>(_ array: inout [Element], adding count: Int = 1) throws {
        let next = array.count.addingReportingOverflow(count)
        guard count >= 0, !next.overflow else { throw QueryValueError.arithmeticOverflow }
        guard next.partialValue > array.capacity else { return }
        let layout = try DatabaseRetainedArrayLayout.forElement(Element.self)
        let growth = try layout.growth(from: array.capacity, toFit: next.partialValue)
        let header = array.capacity == 0 ? layout.containerByteCount : 0
        try reservation.reserveAdditional(bytes: try DatabaseIntermediateFootprint(bytes: header)
            .adding(.init(bytes: growth.additionalByteCount)).bytes, at: .aggregateInput)
        array.reserveCapacity(growth.capacity)
        if array.capacity > growth.capacity {
            try reservation.reserveAdditional(bytes: try DatabaseIntermediateFootprint(bytes: layout.elementCapacitySlotByteCount)
                .multiplied(by: UInt64(array.capacity - growth.capacity)).bytes, at: .aggregateInput)
        }
    }

    func append<Element>(_ value: Element, to array: inout [Element]) throws {
        try meter.consume(at: .aggregateInput)
        try prepare(&array)
        array.append(value)
    }

    func retain(_ cell: DatabaseContext.ProjectedQueryValue) throws -> DatabaseContext.ProjectedQueryValue {
        for lease in cell.leases {
            try meter.consume(at: .aggregateInput)
            guard lease.workMeter === meter else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "aggregate foreign payload meter")
            }
        }
        if cell.payloadIsRetained { return cell }
        let footprint = try CanonicalRelationalFootprintMeter.valueFootprint(of: cell.value,
            workMeter: meter, stage: .aggregateInput)
        let claim = try meter.reserveIntermediate(bytes: footprint.bytes, at: .aggregateInput)
        var leases: [DatabaseQueryPayloadLease] = []
        try prepare(&leases, adding: cell.leases.count + 1)
        for lease in cell.leases { try append(lease, to: &leases) }
        try append(DatabaseQueryPayloadLease(retaining: claim), to: &leases)
        return .init(value: cell.value, leases: leases, payloadIsRetained: true)
    }
}
