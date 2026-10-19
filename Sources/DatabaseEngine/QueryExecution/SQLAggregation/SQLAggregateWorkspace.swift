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

    func prepare<Element>(_ array: inout AdmittedArray<Element>, adding count: Int = 1) throws {
        try array.admit(adding: count, layout: DatabaseRetainedArrayLayout.forElement(Element.self)) {
            try reservation.reserveAdditional(bytes: $0, at: .aggregateInput)
        }
    }

    func append<Element>(_ value: Element, to array: inout AdmittedArray<Element>) throws {
        try meter.consume(at: .aggregateInput)
        try prepare(&array)
        array.wrappedValue.append(value)
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
        @AdmittedArray var leases: [DatabaseQueryPayloadLease] = []
        try prepare(&$leases, adding: cell.leases.count + 1)
        for lease in cell.leases { try append(lease, to: &$leases) }
        try append(DatabaseQueryPayloadLease(retaining: claim), to: &$leases)
        return .init(value: cell.value, leases: leases, payloadIsRetained: true)
    }
}
