// ComparisonBridge.swift
// DatabaseEngine - Bridge between ComparisonOperator and QueryIR.Expression

import Foundation
import Core
import QueryIR

// MARK: - ComparisonOperator → QueryIR.Expression

extension ComparisonOperator {
    /// Build a QueryIR Expression from this comparison operator, a column name, and a FieldValue.
    ///
    /// Maps all 12 ComparisonOperator cases to their QueryIR equivalents:
    /// - Simple comparisons (==, !=, <, <=, >, >=) → binary Expression
    /// - String operations (contains, hasPrefix, hasSuffix) → function calls
    /// - Set membership (in) → inList
    /// - Null checks (isNil, isNotNil) → isNull / isNotNull
    public func toExpression(column: String, value: FieldValue) -> QueryIR.Expression {
        let col = QueryIR.Expression.column(QueryIR.ColumnRef(column: column))
        let lit = QueryIR.Expression.literal(value.toLiteral())

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
            return .function(QueryIR.FunctionCall(
                name: "CONTAINS",
                arguments: [col, lit]
            ))
        case .hasPrefix:
            return .function(QueryIR.FunctionCall(
                name: "STRSTARTS",
                arguments: [col, lit]
            ))
        case .hasSuffix:
            return .function(QueryIR.FunctionCall(
                name: "STRENDS",
                arguments: [col, lit]
            ))
        case .in:
            if case .array(let elements) = value {
                return .inList(col, values: elements.map { .literal($0.toLiteral()) })
            }
            return .inList(col, values: [lit])
        case .isNil:
            return .isNull(col)
        case .isNotNil:
            return .isNotNull(col)
        }
    }
}
