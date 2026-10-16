import DatabaseKit
import DatabaseTypes

/// SQL character semantics are selected from declarations, never value shape.
/// Primitive/RDF callers continue to use their own exact identity contract.
enum CanonicalSQLValueSemantics {
    static func isCharacter(_ type: QueryColumnType?) -> Bool {
        switch type {
        case .sql(.text)?, .sql(.char)?, .sql(.varchar)?: return true
        default: return false
        }
    }

    static func permitsStoredKey(_ expression: CanonicalBoundExpression) -> Bool {
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
