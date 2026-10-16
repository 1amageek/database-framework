import DatabaseKit

/// Task-local Array capacity admitted through the canonical request layout.
struct SQLPatternStorage<Element: Sendable>: ~Copyable {
    private(set) var values: [Element]
    private let reservation: DatabaseIntermediateReservation
    private let layout: DatabaseRetainedArrayLayout
    private var capacity: Int
    let meter: DatabaseWorkMeter

    init(meter: DatabaseWorkMeter, count: Int = 0) throws {
        let preparedLayout = try DatabaseRetainedArrayLayout.forElement(Element.self)
        let growth = try preparedLayout.growth(from: 0, toFit: count)
        let preparedReservation = try meter.reserveIntermediate(bytes: preparedLayout.containerByteCount + growth.additionalByteCount,
                                                               at: .expressionEvaluation)
        var preparedValues: [Element] = []
        preparedValues.reserveCapacity(growth.capacity)
        self.meter = meter
        layout = preparedLayout
        reservation = preparedReservation
        capacity = growth.capacity
        values = preparedValues
    }

    var count: Int { values.count }
    var isEmpty: Bool { values.isEmpty }
    subscript(_ index: Int) -> Element {
        get { values[index] }
        set { values[index] = newValue }
    }
    mutating func append(_ value: Element) throws {
        try meter.consume(at: .expressionEvaluation)
        let growth = try layout.growth(from: capacity, toFit: values.count + 1)
        if growth.capacity > capacity {
            try reservation.reserveAdditional(bytes: growth.additionalByteCount, at: .expressionEvaluation)
            values.reserveCapacity(growth.capacity)
            capacity = growth.capacity
        }
        values.append(value)
    }
    mutating func popLast() -> Element? { values.popLast() }
    mutating func clear() { values.removeAll(keepingCapacity: true) }
    borrowing func finish() throws -> DatabaseSharedRetainedArray<Element> {
        let owner = try meter.reserveIntermediate(bytes: layout.sharedOwnerByteCount, at: .expressionEvaluation)
        return DatabaseSharedRetainedArray(elements: values, elementReservation: reservation, ownerReservation: owner)
    }
}
