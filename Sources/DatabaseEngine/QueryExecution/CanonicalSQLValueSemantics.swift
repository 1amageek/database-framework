import DatabaseKit
import DatabaseTypes

/// SQL character semantics are selected from declarations, never value shape.
/// Primitive/RDF callers continue to use their own exact identity contract.
enum CanonicalSQLValueSemantics {
    static func isTemporal(_ type: QueryColumnType?) -> Bool {
        guard let type, case .sql = type else { return false }
        return CanonicalQueryTypeBinder.temporalType(type) != nil
    }

    static func requireComparable(_ type: QueryColumnType, operation: String,
        workMeter: DatabaseWorkMeter?, stage: DatabaseWorkStage = .expressionEvaluation) throws {
        let layout = try DatabaseRetainedArrayLayout.forElement(QueryColumnType.self)
        let reservation = try workMeter?.reserveIntermediate(bytes: layout.containerByteCount, at: stage)
        defer { reservation?.release() }
        var pending: [QueryColumnType] = []
        func push(_ type: QueryColumnType) throws {
            let growth = try layout.growth(from: pending.capacity, toFit: pending.count + 1)
            if growth.capacity > pending.capacity {
                try reservation?.reserveAdditional(bytes: growth.additionalByteCount, at: stage)
                pending.reserveCapacity(growth.capacity)
                if pending.capacity > growth.capacity {
                    try reservation?.reserveAdditional(bytes: try DatabaseIntermediateFootprint(bytes: layout.elementCapacitySlotByteCount)
                        .multiplied(by: UInt64(pending.capacity - growth.capacity)).bytes, at: stage)
                }
            }
            pending.append(type)
        }
        try push(type)
        while let type = pending.popLast() {
            try workMeter?.consume(at: stage)
            switch type {
            case .sql(.json):
                throw DatabaseExpressionEvaluationError.unsupportedExpression("\(operation) of JSON has no SQL comparison")
            case .sql(.array(let element)): try push(.sql(element))
            case .sql(.composite(let descriptor)):
                for column in descriptor.columns { try push(column.type) }
            default: break
            }
        }
    }

    static func compareValues(_ left: QueryValue, _ right: QueryValue,
        leftType: QueryColumnType, rightType: QueryColumnType,
        environment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter) throws -> QueryComparison {
        switch (leftType, rightType) {
        case (.sql(.composite), .sql(.composite)):
            return try CanonicalSQLCompositeEvaluator.compare(left, right, leftType: leftType, rightType: rightType,
                environment: environment, workMeter: workMeter)
        case (.sql(.array), .sql(.array)):
            return try CanonicalSQLArrayEvaluator.compare(left, right, leftType: leftType,
                rightType: rightType, temporalEnvironment: environment, workMeter: workMeter)
        case (.sql(.jsonb), .sql(.jsonb)):
            guard case .jsonb(let left) = left, case .jsonb(let right) = right else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected JSONB comparison")
            }
            return try CanonicalSQLJSONEvaluator.compare(left, right, workMeter: workMeter)
        case (.sql(.json), _), (_, .sql(.json)):
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "SQL JSON comparison")
        default: break
        }
        if isTemporal(leftType) || isTemporal(rightType) {
            guard let environment else { throw DatabaseExpressionEvaluationError.missingTemporalEnvironment }
            return try CanonicalSQLTemporalEvaluator.compare(left, right, leftType: leftType,
                rightType: rightType, environment: environment, workMeter: workMeter)
        }
        func scalar(_ value: QueryValue) throws -> FieldValue {
            switch value {
            case .primitive(let value): return value
            case .numeric(let value): return .decimal(value.value)
            default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected comparison")
            }
        }
        let left = try scalar(left), right = try scalar(right)
        try chargeComparison(left, workMeter: workMeter, stage: .expressionEvaluation)
        try chargeComparison(right, workMeter: workMeter, stage: .expressionEvaluation)
        return try compare(left, right, leftType: leftType, rightType: rightType)
    }

    static func equalValues(_ left: QueryValue, _ right: QueryValue,
        leftType: QueryColumnType, rightType: QueryColumnType,
        environment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter) throws -> Bool {
        switch (leftType, rightType) {
        case (.sql(.composite), _), (_, .sql(.composite)), (.sql(.array), _), (_, .sql(.array)), (.sql(.jsonb), _), (_, .sql(.jsonb)), (.sql(.json), _), (_, .sql(.json)):
            return try compareValues(left, right, leftType: leftType, rightType: rightType,
                environment: environment, workMeter: workMeter) == .equal
        default: break
        }
        if isTemporal(leftType) || isTemporal(rightType) {
            return try compareValues(left, right, leftType: leftType, rightType: rightType,
                environment: environment, workMeter: workMeter) == .equal
        }
        func scalar(_ value: QueryValue) throws -> FieldValue {
            switch value {
            case .primitive(let value): return value
            case .numeric(let value): return .decimal(value.value)
            default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected equality")
            }
        }
        let left = try scalar(left), right = try scalar(right)
        try chargeComparison(left, workMeter: workMeter, stage: .expressionEvaluation)
        try chargeComparison(right, workMeter: workMeter, stage: .expressionEvaluation)
        return try equal(left, right, leftType: leftType, rightType: rightType)
    }

    static func temporalIdentity(_ value: QueryValue, type: QueryColumnType?,
        environment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter) throws -> SQLTemporalIdentity? {
        guard let type, isTemporal(type), !value.isNull else { return nil }
        guard let environment else { throw DatabaseExpressionEvaluationError.missingTemporalEnvironment }
        return try CanonicalSQLTemporalEvaluator.identity(value, type: type,
            environment: environment, workMeter: workMeter)
    }

    static func compareValues(_ left: QueryValue, _ right: QueryValue, type: QueryColumnType,
        using key: SortKey, environment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter) throws -> QueryComparison {
        if left.isNull || right.isNull {
            let selected: SortKey
            if case .sql = type, key.nulls == nil {
                selected = SortKey(key.expression, direction: key.direction,
                    nulls: key.direction == .ascending ? .last : .first)
            } else { selected = key }
            return try FieldValueComparator.compare(left.isNull ? .null : .bool(true),
                right.isNull ? .null : .bool(true), using: selected)
        }
        let result = try compareValues(left, right, leftType: type, rightType: type,
            environment: environment, workMeter: workMeter)
        guard key.direction == .descending else { return result }
        switch result {
        case .lessThan: return .greaterThan
        case .equal: return .equal
        case .greaterThan: return .lessThan
        }
    }

    static func isCharacter(_ type: QueryColumnType?) -> Bool {
        switch type {
        case .sql(.text)?, .sql(.char)?, .sql(.varchar)?: return true
        default: return false
        }
    }

    static func permitsStoredKey(_ expression: CanonicalBoundExpression) -> Bool {
        if isTemporal(expression.type) { return false }
        switch expression.type {
        case .sql(.array), .sql(.composite), .sql(.json), .sql(.jsonb): return false
        default: break
        }
        if case .sql(.char) = expression.type { return false }
        return expression.children.allSatisfy(permitsStoredKey)
    }

    private static func bytes(_ text: String, type: QueryColumnType?) -> String.UTF8View.SubSequence {
        var bytes = text.utf8[...]
        if case .sql(.char)? = type {
            while bytes.last == 32 { bytes = bytes.dropLast() }
        }
        return bytes
    }

    static func chargeComparison(_ value: FieldValue, workMeter: DatabaseWorkMeter, stage: DatabaseWorkStage) throws {
        let bytes: UInt64
        switch value {
        case .string(let text): bytes = UInt64(text.utf8.count)
        case .bytes(let data): bytes = UInt64(data.count)
        default: bytes = 0
        }
        try DatabaseByteProcessingMeter.consume(byteCount: bytes, workMeter: workMeter, stage: stage)
    }

    static func equal(
        _ left: FieldValue, _ right: FieldValue,
        leftType: QueryColumnType?, rightType: QueryColumnType?
    ) throws(FieldValueComparisonError) -> Bool {
        if isCharacter(leftType), isCharacter(rightType) {
            return try compare(left, right, leftType: leftType, rightType: rightType) == .equal
        }
        return try FieldValueComparator.equal(left, right)
    }

    static func compare(
        _ left: FieldValue, _ right: FieldValue,
        leftType: QueryColumnType?, rightType: QueryColumnType?
    ) throws(FieldValueComparisonError) -> QueryComparison {
        if isCharacter(leftType), isCharacter(rightType),
           case .string(let left) = left, case .string(let right) = right {
            let left = bytes(left, type: leftType), right = bytes(right, type: rightType)
            if left.elementsEqual(right) { return .equal }
            return left.lexicographicallyPrecedes(right) ? .lessThan : .greaterThan
        }
        return try FieldValueComparator.compare(left, right)
    }

    static func compare(
        _ left: FieldValue, _ right: FieldValue,
        type: QueryColumnType?, using key: SortKey
    ) throws(FieldValueComparisonError) -> QueryComparison {
        if left.isNull || right.isNull { return try FieldValueComparator.compare(left, right, using: key) }
        let comparison = try compare(left, right, leftType: type, rightType: type)
        guard key.direction == .descending else { return comparison }
        switch comparison {
        case .lessThan: return .greaterThan
        case .equal: return .equal
        case .greaterThan: return .lessThan
        }
    }

    /// The caller admits the destination before CHAR normalization. Existing
    /// FieldValue text equality/hash already use exact UTF-8, so text/varchar
    /// retain their original owner and only CHAR requires a trimmed key copy.
    static func identity(
        _ value: FieldValue, type: QueryColumnType?, workMeter: DatabaseWorkMeter
    ) throws -> FieldValue {
        guard case .sql(.char)? = type, case .string(let text) = value else { return value }
        try DatabaseByteProcessingMeter.consume(byteCount: UInt64(text.utf8.count),
            workMeter: workMeter, stage: .expressionEvaluation)
        let selected = bytes(text, type: type)
        if selected.count == text.utf8.count { return value }
        return .string(String(decoding: selected, as: UTF8.self))
    }
}
