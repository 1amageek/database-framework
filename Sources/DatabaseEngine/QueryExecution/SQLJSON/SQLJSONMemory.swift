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

    func prepare<T>(_ array: inout AdmittedArray<T>, adding count: Int = 1) throws {
        try array.admit(adding: count, layout: DatabaseRetainedArrayLayout.forElement(T.self)) { try bytes($0) }
    }

    func append<T>(_ value: T, to array: inout AdmittedArray<T>) throws {
        try prepare(&array)
        array.wrappedValue.append(value)
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
        case .composite(let record):
            cells = record.fields.count
            let descriptor = record.type
            let footprint = try descriptor.measure()
            let work = try add(try add(footprint.nodeCount, footprint.collectionElementCount), descriptor.storageFootprint.utf8ByteCount)
            try meter.consume(work, at: .expressionEvaluation)
        case .array(let array):
            cells = array.elements.count
            if case .composite(let descriptor) = array.elementType {
                let footprint = try descriptor.measure()
                let work = try add(try add(footprint.nodeCount, footprint.collectionElementCount), descriptor.storageFootprint.utf8ByteCount)
                try meter.consume(work, at: .expressionEvaluation)
            }
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
