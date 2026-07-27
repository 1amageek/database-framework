// ComparisonConversion.swift
// DatabaseEngine - Conversion between ComparisonOperator and Expression

import DatabaseTypes
import DatabaseKit

// MARK: - ComparisonOperator → Expression

extension ComparisonOperator {
    /// Build a QueryIR Expression from this comparison operator, a column name, and a FieldValue.
    ///
    /// Maps all 12 ComparisonOperator cases to their QueryIR equivalents:
    /// - Simple comparisons (==, !=, <, <=, >, >=) → binary Expression
    /// - String operations (contains, hasPrefix, hasSuffix) → function calls
    /// - Set membership (in, notIn) → inList / notInList
    /// - Null checks (isNil, isNotNil) → isNull / isNotNull
    public func toExpression(
        column: String,
        value: FieldValue
    ) throws(LiteralConversionError) -> DatabaseKit.Expression {
        let col = DatabaseKit.Expression.column(ColumnRef(column: column))
        let lit = DatabaseKit.Expression.literal(try value.toLiteral())

        switch self {
        case .equal:
            return .equal(col, lit)
        case .notEqual:
            return .notEqual(col, lit)
        case .lessThan:
            return .lessThan(col, lit)
        case .lessThanOrEqual:
            return .lessThanOrEqual(col, lit)
        case .greaterThan:
            return .greaterThan(col, lit)
        case .greaterThanOrEqual:
            return .greaterThanOrEqual(col, lit)
        case .contains:
            return .function(FunctionCall(
                name: "CONTAINS",
                arguments: [col, lit]
            ))
        case .hasPrefix:
            return .function(FunctionCall(
                name: "STRSTARTS",
                arguments: [col, lit]
            ))
        case .hasSuffix:
            return .function(FunctionCall(
                name: "STRENDS",
                arguments: [col, lit]
            ))
        case .in:
            if case .array(let elements) = value {
                var values: [DatabaseKit.Expression] = []
                values.reserveCapacity(elements.count)
                for element in elements {
                    values.append(.literal(try element.toLiteral()))
                }
                return .inList(col, values: values)
            }
            return .inList(col, values: [lit])
        case .notIn:
            if case .array(let elements) = value {
                var values: [DatabaseKit.Expression] = []
                values.reserveCapacity(elements.count)
                for element in elements {
                    values.append(.literal(try element.toLiteral()))
                }
                return .notInList(
                    col,
                    values: values
                )
            }
            return .notInList(col, values: [lit])
        case .isNil:
            return .isNull(col)
        case .isNotNil:
            return .isNotNull(col)
        }
    }
}
