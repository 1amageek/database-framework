import DatabaseKit
import DatabaseTypes

/// Task-confined admission owner; it never crosses a task or Sendable boundary.
final class SQLJSONMemory {
    let meter: DatabaseWorkMeter
    let reservation: DatabaseIntermediateReservation

    init(_ meter: DatabaseWorkMeter) throws {
        self.meter = meter
        reservation = try meter.reserveIntermediate(at: .expressionEvaluation)
    }

    deinit { reservation.release() }

    func work(_ count: UInt64 = 1) throws { try meter.consume(count, at: .expressionEvaluation) }

    func bytes(_ count: UInt64) throws {
        try reservation.reserveAdditional(bytes: count, at: .expressionEvaluation)
    }

    func prepare<T>(_ array: inout [T], adding count: Int = 1) throws {
        let next = array.count.addingReportingOverflow(count)
        guard count >= 0, !next.overflow else { throw QueryValueError.arithmeticOverflow }
        guard next.partialValue > array.capacity else { return }
        let layout = try DatabaseRetainedArrayLayout.forElement(T.self)
        let growth = try layout.growth(from: array.capacity, toFit: next.partialValue)
        let previous = array.capacity
        let prospective = previous == 0 ? try Self.add(growth.additionalByteCount, layout.containerByteCount) : growth.additionalByteCount
        try bytes(prospective)
        array.reserveCapacity(growth.capacity)
        if array.capacity > growth.capacity {
            try bytes(try Self.product(array.capacity - growth.capacity, max(1, MemoryLayout<T>.stride)))
        }
    }

    func append<T>(_ value: T, to array: inout [T]) throws {
        try prepare(&array)
        array.append(value)
    }

    static func product(_ count: Int, _ stride: Int) throws -> UInt64 {
        guard count >= 0, stride >= 0 else { throw QueryValueError.arithmeticOverflow }
        let n = UInt64(count).multipliedReportingOverflow(by: UInt64(stride))
        guard !n.overflow else { throw QueryValueError.arithmeticOverflow }
        return n.partialValue
    }

    static func add(_ left: UInt64, _ right: UInt64) throws -> UInt64 {
        let sum = left.addingReportingOverflow(right)
        guard !sum.overflow else { throw QueryValueError.arithmeticOverflow }
        return sum.partialValue
    }

    static func validate(_ value: QueryValue, type: QueryColumnType, meter: DatabaseWorkMeter) throws {
        let cells: Int
        switch value {
        case .array(let array): cells = array.elements.count
        case .primitive(.array(let array)): cells = array.count
        default: cells = 0
        }
        try meter.consume(try add(UInt64(cells), 1), at: .expressionEvaluation)
        guard QueryValueValidator.accepts(value, type: type) else { throw failure("selected JSON input") }
    }

    static func retain(_ value: QueryValue, meter: DatabaseWorkMeter) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        if value.isNull { return (value, nil) }
        let footprint = try CanonicalRelationalFootprintMeter.valueFootprint(of: value,
            workMeter: meter, stage: .expressionEvaluation)
        let claim = try meter.reserveIntermediate(bytes: footprint.bytes, at: .expressionEvaluation)
        return (value, DatabaseQueryPayloadLease(retaining: claim))
    }

    static func failure(_ reason: String) -> DatabaseExpressionEvaluationError {
        .typeMismatch(operation: reason)
    }
}
