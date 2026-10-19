import DatabaseKit

/// A unique, request-admitted destination. Actual capacity is reconciled before use.
struct SQLArrayBuffer<Element: Sendable>: ~Copyable {
    private(set) var values: [Element]
    let reservation: DatabaseIntermediateReservation
    private let layout: DatabaseRetainedArrayLayout
    private var capacity: Int

    init(count: Int = 0, extraBytes: UInt64 = 0, meter: DatabaseWorkMeter) throws {
        let layout = try DatabaseRetainedArrayLayout.forElement(Element.self)
        let reservation = try meter.reserveIntermediate(bytes: try DatabaseIntermediateFootprint(
            bytes: layout.containerByteCount).adding(.init(bytes: extraBytes)).bytes,
            at: .expressionEvaluation)
        var values: [Element] = []
        let growth = try layout.growth(from: 0, toFit: count)
        try reservation.reserveAdditional(bytes: growth.additionalByteCount, at: .expressionEvaluation)
        values.reserveCapacity(growth.capacity)
        let capacity = max(growth.capacity, values.capacity)
        if capacity > growth.capacity {
            let extra = try DatabaseIntermediateFootprint(bytes: layout.elementCapacitySlotByteCount)
                .multiplied(by: UInt64(capacity - growth.capacity)).bytes
            try reservation.reserveAdditional(bytes: extra, at: .expressionEvaluation)
        }
        // Install self only after fallible admission, growth and reconciliation.
        self.values = values
        self.reservation = reservation
        self.layout = layout
        self.capacity = capacity
    }

    mutating func reserve(_ count: Int) throws {
        let growth = try layout.growth(from: capacity, toFit: count)
        guard growth.capacity > capacity else { return }
        try reservation.reserveAdditional(bytes: growth.additionalByteCount, at: .expressionEvaluation)
        values.reserveCapacity(growth.capacity)
        capacity = growth.capacity
    }

    mutating func append(_ value: Element) throws {
        let next = values.count.addingReportingOverflow(1)
        guard !next.overflow else { throw SQLArrayStorage.failure(.arraySubscriptOutOfRange) }
        try reserve(next.partialValue)
        try reservation.workMeter.consume(at: .expressionEvaluation)
        values.append(value)
    }

    consuming func finish() -> (values: [Element], reservation: DatabaseIntermediateReservation) {
        (values, reservation)
    }
}
