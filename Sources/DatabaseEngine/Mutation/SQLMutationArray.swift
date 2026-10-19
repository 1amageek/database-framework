/// Mutable statement-local membership and candidate storage with admitted growth.
struct SQLMutationArray<Element: Sendable>: Sendable {
    private(set) var elements: [Element] = []
    let reservation: DatabaseIntermediateReservation
    private let layout: DatabaseRetainedArrayLayout
    private var capacity = 0

    init(workMeter: DatabaseWorkMeter) throws {
        layout = try DatabaseRetainedArrayLayout.forElement(Element.self)
        reservation = try workMeter.reserveIntermediate(bytes: layout.containerByteCount, at: .mutationPlanning)
    }

    mutating func append(_ element: Element, footprint: DatabaseIntermediateFootprint = DatabaseIntermediateFootprint()) throws {
        let next = elements.count.addingReportingOverflow(1)
        guard !next.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
        let growth = try layout.growth(from: capacity, toFit: next.partialValue)
        try reservation.reserveAdditional(rows: footprint.rows,
            bytes: try footprint.adding(DatabaseIntermediateFootprint(bytes: growth.additionalByteCount)).bytes,
            at: .mutationPlanning)
        if growth.capacity != capacity {
            elements.reserveCapacity(growth.capacity)
            capacity = growth.capacity
        }
        elements.append(element)
    }
}
