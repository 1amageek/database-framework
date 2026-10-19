import DatabaseKit
import DatabaseTypes

/// Retry-stable physical identifiers owned by one SQL request.
package actor SQLMutationRequest {
    private struct Event: Sendable {
        let relation: String
        let column: FieldSchema
        let statement: Int
        let value: FieldValue
        let reservation: DatabaseIntermediateReservation
    }

    private let meter: DatabaseWorkMeter
    private var events: [Event] = []
    private var cursor = 0
    private var capacity = 0

    package init(workMeter: DatabaseWorkMeter) { self.meter = workMeter }

    package func beginAttempt() { cursor = 0 }

    package func allocateHiddenIdentity(relationID: String, column: FieldSchema, statement: Int) throws -> FieldValue {
        try meter.consume(at: .mutationPlanning)
        guard !column.isArray, column.queryType == nil else {
            throw SQLMutationError.unsupportedHiddenIdentifier(column.name)
        }
        if cursor < events.count {
            let event = events[cursor]
            guard event.relation.utf8.elementsEqual(relationID.utf8), event.column == column,
                  event.statement == statement else { throw SQLMutationError.identityReplayDiverged }
            cursor += 1
            return event.value
        }
        let layout = try DatabaseRetainedArrayLayout.forElement(Event.self)
        let growth = try layout.growth(from: capacity, toFit: events.count + 1)
        let reservation = try meter.reserveIntermediate(bytes: growth.additionalByteCount
            + (capacity == 0 ? layout.containerByteCount : 0)
            + UInt64(relationID.utf8.count + column.name.utf8.count) + 128, at: .mutationPlanning)
        var random = SystemRandomNumberGenerator()
        let identifier = DatabaseTypes.UUID(high: UInt64.random(in: .min ... .max, using: &random),
            low: UInt64.random(in: .min ... .max, using: &random))
        let value: FieldValue
        switch column.type {
        case .uuid: value = .uuid(identifier)
        case .string: value = .string(identifier.description)
        case .bytes: value = .bytes(ByteString(identifier.bytes))
        default: throw SQLMutationError.unsupportedHiddenIdentifier(column.name)
        }
        if growth.capacity != capacity {
            events.reserveCapacity(growth.capacity)
            capacity = growth.capacity
        }
        events.append(Event(relation: relationID, column: column, statement: statement,
            value: value, reservation: reservation))
        cursor += 1
        return value
    }
}
