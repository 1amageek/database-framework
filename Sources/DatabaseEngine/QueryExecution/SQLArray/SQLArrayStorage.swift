import DatabaseKit
import DatabaseTypes

/// Shape and destination ownership only; scalar policy remains in QueryExecution.
enum SQLArrayStorage {
    static let null = QueryValue.primitive(.null)
    static func failure(_ error: SQLCollectionError) -> DatabaseExpressionEvaluationError { .collection(error) }
    static func mismatch(_ operation: String) -> DatabaseExpressionEvaluationError { .typeMismatch(operation: operation) }

    static func elementType(_ type: QueryColumnType) throws -> DataType {
        guard case .sql(.array(let element)) = type,
              QueryValueValidator.accepts(null, type: .sql(element)) else { throw mismatch("selected SQL array declaration") }
        if case .array = element { throw mismatch("nested SQL array element declaration") }
        return element
    }

    static func validate(_ value: QueryValue, type: QueryColumnType, meter: DatabaseWorkMeter) throws {
        let count: Int
        switch value {
        case .array(let array): count = array.elements.count
        case .primitive(.array(let array)): count = array.count
        default: count = 0
        }
        try meter.consume(UInt64(count) + 1, at: .expressionEvaluation)
        guard QueryValueValidator.accepts(value, type: type) else { throw mismatch("selected SQL array operand") }
    }

    static func array(_ value: QueryValue, type: QueryColumnType) throws -> QueryArrayValue? {
        _ = try elementType(type)
        if value.isNull { return nil }
        guard case .array(let array) = value else { throw mismatch("normalized SQL array operand") }
        return array
    }

    static func integer(_ value: QueryValue) throws -> Int32 {
        guard case .primitive(.int32(let number)) = value else { throw mismatch("selected array integer") }
        return number
    }

    static func length(_ count: Int) throws -> Int32 {
        guard let count = Int32(exactly: count) else { throw failure(.arraySubscriptOutOfRange) }
        return count
    }

    static func dimension(lower: Int32 = 1, length: Int) throws -> QueryArrayDimension {
        let count = try self.length(length)
        guard count > 0, !lower.addingReportingOverflow(count - 1).overflow else {
            throw failure(.arraySubscriptOutOfRange)
        }
        return try QueryArrayDimension(lowerBound: lower, length: count)
    }

    static func checkedAdd(_ left: Int, _ right: Int) throws -> Int {
        let result = left.addingReportingOverflow(right)
        guard !result.overflow else { throw failure(.arraySubscriptOutOfRange) }
        return result.partialValue
    }

    static func checkedMultiply(_ left: Int, _ right: Int) throws -> Int {
        let result = left.multipliedReportingOverflow(by: right)
        guard !result.overflow else { throw failure(.arraySubscriptOutOfRange) }
        return result.partialValue
    }

    static func makeArray(element: DataType, rank: Int, count: Int, meter: DatabaseWorkMeter,
        limits: QueryValueLimits, dimension: (Int) throws -> QueryArrayDimension,
        cell: (Int, DatabaseIntermediateReservation) throws -> QueryValue) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        guard count >= 0, rank >= 0, rank <= 6, (count == 0) == (rank == 0) else { throw failure(.invalidArrayShape) }
        guard UInt64(count) <= limits.maximumCollectionElements else {
            throw QueryValueError.resourceLimit(resource: "collection elements", actual: UInt64(count), maximum: limits.maximumCollectionElements)
        }
        var dimensions = try SQLArrayBuffer<QueryArrayDimension>(count: rank, meter: meter)
        var values = try SQLArrayBuffer<QueryValue>(count: count, extraBytes: UInt64(MemoryLayout<QueryArrayValue>.stride), meter: meter)
        var product = rank == 0 ? 0 : 1
        for index in 0..<rank {
            let value = try dimension(index)
            product = try checkedMultiply(product, Int(value.length))
            try dimensions.append(value)
        }
        guard product == count else { throw failure(.invalidArrayShape) }
        for index in 0..<count { try values.append(cell(index, values.reservation)) }
        // Intrinsic validation scans cells and uses a bounded traversal stack.
        try meter.consume(UInt64(count) + UInt64(rank) + 1, at: .expressionEvaluation)
        let finalDimensions = dimensions.finish(), finalValues = values.finish()
        let result = try QueryArrayValue(elementType: element, dimensions: finalDimensions.values,
            elements: finalValues.values, limits: limits)
        return (.array(result), DatabaseQueryPayloadLease(retaining: finalDimensions.reservation,
            and: finalValues.reservation))
    }

    static func empty(element: DataType, meter: DatabaseWorkMeter, limits: QueryValueLimits) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        try makeArray(element: element, rank: 0, count: 0, meter: meter, limits: limits,
            dimension: { _ in throw failure(.invalidArrayShape) }, cell: { _, _ in null })
    }

    static func scalarCast(_ value: QueryValue, from source: QueryColumnType, to target: QueryColumnType,
        context: CanonicalQueryTypeBinder.CastContext, environment: SQLTemporalEnvironment?,
        meter: DatabaseWorkMeter, limits: QueryValueLimits, destination: DatabaseIntermediateReservation) throws -> QueryValue {
        if CanonicalQueryTypeBinder.isJSON(source) || CanonicalQueryTypeBinder.isJSON(target) {
            if source == target { return value }
            let result = try CanonicalSQLScalarCast.evaluateCollection(value, from: source, to: target,
                context: context, temporalEnvironment: environment, workMeter: meter, limits: limits)
            // The producer's immutable lease cannot be resized. Admit destination
            // ownership while it is still alive, then let its claim leave scope.
            return try withExtendedLifetime(result) {
                let footprint = try CanonicalRelationalFootprintMeter.valueFootprint(of: result.value,
                    workMeter: meter, stage: .expressionEvaluation)
                try destination.reserveAdditional(bytes: footprint.bytes, at: .expressionEvaluation)
                return result.value
            }
        }
        if CanonicalSQLScalarCast.usesPayload(target) {
            let result = try CanonicalSQLScalarCast.evaluatePayload(value, from: source, to: target,
                context: context, temporalEnvironment: environment, workMeter: meter)
            if let claim = result.retainedReservation { try destination.absorbAll(from: claim) }
            return result.withValue { .primitive(copy $0) }
        }
        return try CanonicalSQLScalarCast.evaluateValue(value, from: source, to: target,
            context: context, temporalEnvironment: environment, workMeter: meter)
    }

    static func equal(_ left: QueryValue, _ right: QueryValue, type: DataType,
        nullsMatch: Bool, environment: SQLTemporalEnvironment?, meter: DatabaseWorkMeter) throws -> Bool {
        try meter.consume(at: .expressionEvaluation)
        if left.isNull || right.isNull { return nullsMatch && left.isNull && right.isNull }
        return try CanonicalSQLValueSemantics.equalValues(left, right, leftType: .sql(type), rightType: .sql(type),
            environment: environment, workMeter: meter)
    }
}
