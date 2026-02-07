import Foundation
import Core
import QueryIR
import DatabaseClientProtocol

/// Evaluates QueryIR.Expression against [String: FieldValue] dictionaries
///
/// Used server-side to filter records after fetching from FDB.
/// Operates entirely on FieldValue for type-safe comparisons.
enum PredicateEvaluator {

    /// Evaluate an expression as a boolean against a field dictionary
    static func evaluate(_ expr: QueryIR.Expression, on record: [String: FieldValue]) -> Bool {
        switch expr {
        case .and(let lhs, let rhs):
            return evaluate(lhs, on: record) && evaluate(rhs, on: record)
        case .or(let lhs, let rhs):
            return evaluate(lhs, on: record) || evaluate(rhs, on: record)
        case .not(let inner):
            return !evaluate(inner, on: record)
        case .equal(let lhs, let rhs):
            let l = resolveValue(lhs, from: record)
            let r = resolveValue(rhs, from: record)
            return l == r
        case .notEqual(let lhs, let rhs):
            let l = resolveValue(lhs, from: record)
            let r = resolveValue(rhs, from: record)
            return l != r
        case .lessThan(let lhs, let rhs):
            guard let l = resolveValue(lhs, from: record),
                  let r = resolveValue(rhs, from: record),
                  let result = l.compare(to: r) else { return false }
            return result == .orderedAscending
        case .lessThanOrEqual(let lhs, let rhs):
            guard let l = resolveValue(lhs, from: record),
                  let r = resolveValue(rhs, from: record),
                  let result = l.compare(to: r) else { return false }
            return result != .orderedDescending
        case .greaterThan(let lhs, let rhs):
            guard let l = resolveValue(lhs, from: record),
                  let r = resolveValue(rhs, from: record),
                  let result = l.compare(to: r) else { return false }
            return result == .orderedDescending
        case .greaterThanOrEqual(let lhs, let rhs):
            guard let l = resolveValue(lhs, from: record),
                  let r = resolveValue(rhs, from: record),
                  let result = l.compare(to: r) else { return false }
            return result != .orderedAscending
        case .isNull(let inner):
            let v = resolveValue(inner, from: record)
            return v == nil || v == FieldValue.null
        case .isNotNull(let inner):
            let v = resolveValue(inner, from: record)
            return v != nil && v != FieldValue.null
        case .like(let inner, let pattern):
            guard let v = resolveValue(inner, from: record),
                  case .string(let str) = v else { return false }
            return matchLikePattern(str, pattern: pattern)
        case .inList(let expr, let list):
            guard let v = resolveValue(expr, from: record) else { return false }
            return list.contains { resolveValue($0, from: record) == v }
        case .literal(let lit):
            if case .bool(let b) = lit { return b }
            return false
        default:
            return false
        }
    }

    /// Resolve an expression to a FieldValue
    private static func resolveValue(_ expr: QueryIR.Expression, from record: [String: FieldValue]) -> FieldValue? {
        switch expr {
        case .column(let col):
            return resolveFieldValue(fieldName: col.column, in: record)
        case .literal(let lit):
            return literalToFieldValue(lit)
        case .variable(let v):
            return resolveFieldValue(fieldName: v.name, in: record)
        default:
            return nil
        }
    }

    /// Convert a Literal to FieldValue
    private static func literalToFieldValue(_ lit: Literal) -> FieldValue {
        switch lit {
        case .null:
            return .null
        case .bool(let b):
            return .bool(b)
        case .int(let i):
            return .int64(i)
        case .double(let d):
            return .double(d)
        case .string(let s):
            return .string(s)
        case .date(let d):
            return .double(d.timeIntervalSince1970)
        default:
            return .null
        }
    }

    /// Resolve a field value, supporting nested paths (e.g., "address.city")
    private static func resolveFieldValue(fieldName: String, in record: [String: FieldValue]) -> FieldValue? {
        if let value = record[fieldName] {
            return value
        }
        let parts = fieldName.split(separator: ".")
        guard parts.count > 1 else { return nil }
        return nil
    }

    /// Simple SQL LIKE pattern matching (% = any, _ = single char)
    private static func matchLikePattern(_ str: String, pattern: String) -> Bool {
        let regexPattern = "^" + NSRegularExpression.escapedPattern(for: pattern)
            .replacingOccurrences(of: "%", with: ".*")
            .replacingOccurrences(of: "_", with: ".") + "$"
        return str.range(of: regexPattern, options: .regularExpression) != nil
    }

    /// Sort records by SortKey descriptors
    static func sort(
        _ records: [[String: FieldValue]],
        by sortKeys: [SortKey]
    ) -> [[String: FieldValue]] {
        guard !sortKeys.isEmpty else { return records }

        return records.sorted { lhs, rhs in
            for sortKey in sortKeys {
                guard let fieldName = extractFieldName(from: sortKey.expression) else { continue }
                let ascending = sortKey.direction == .ascending

                let lhsValue = lhs[fieldName] ?? FieldValue.null
                let rhsValue = rhs[fieldName] ?? FieldValue.null

                guard let result = lhsValue.compare(to: rhsValue) else { continue }
                switch result {
                case .orderedAscending:
                    return ascending
                case .orderedDescending:
                    return !ascending
                case .orderedSame:
                    continue
                }
            }
            return false
        }
    }

    /// Extract field name from a sort expression
    private static func extractFieldName(from expr: QueryIR.Expression) -> String? {
        switch expr {
        case .column(let col):
            return col.column
        case .variable(let v):
            return v.name
        default:
            return nil
        }
    }
}
