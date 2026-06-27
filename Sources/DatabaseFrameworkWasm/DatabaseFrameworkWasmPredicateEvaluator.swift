import DatabaseKitWasmCore

/// Evaluates WASM query predicates against decoded records.
public enum DatabaseFrameworkWasmPredicateEvaluator {
    public static func matches(
        _ record: DatabaseKitWasmRecord,
        predicate: DatabaseKitWasmPredicate?
    ) throws(DatabaseFrameworkWasmError) -> Bool {
        guard let predicate else {
            return true
        }
        return try matches(record, predicate: predicate)
    }

    private static func matches(
        _ record: DatabaseKitWasmRecord,
        predicate: DatabaseKitWasmPredicate
    ) throws(DatabaseFrameworkWasmError) -> Bool {
        switch predicate {
        case .comparison(let field, let op, let value):
            guard let actual = fieldValue(named: field, in: record) else {
                return false
            }
            return try compare(actual, op: op, expected: value)
        case .and(let predicates):
            for predicate in predicates {
                guard try matches(record, predicate: predicate) else {
                    return false
                }
            }
            return true
        case .or(let predicates):
            for predicate in predicates {
                if try matches(record, predicate: predicate) {
                    return true
                }
            }
            return false
        case .not(let predicate):
            return try !matches(record, predicate: predicate)
        }
    }

    private static func fieldValue(
        named name: String,
        in record: DatabaseKitWasmRecord
    ) -> DatabaseKitWasmFieldValue? {
        for field in record.fields where field.name == name {
            return field.value
        }
        return nil
    }

    private static func compare(
        _ actual: DatabaseKitWasmFieldValue,
        op: DatabaseKitWasmComparisonOperator,
        expected: DatabaseKitWasmFieldValue
    ) throws(DatabaseFrameworkWasmError) -> Bool {
        switch op {
        case .equal:
            return actual == expected
        case .notEqual:
            return actual != expected
        case .lessThan:
            return try orderedCompare(actual, expected) < 0
        case .lessThanOrEqual:
            return try orderedCompare(actual, expected) <= 0
        case .greaterThan:
            return try orderedCompare(actual, expected) > 0
        case .greaterThanOrEqual:
            return try orderedCompare(actual, expected) >= 0
        case .contains:
            return contains(actual, expected)
        }
    }

    private static func orderedCompare(
        _ lhs: DatabaseKitWasmFieldValue,
        _ rhs: DatabaseKitWasmFieldValue
    ) throws(DatabaseFrameworkWasmError) -> Int {
        switch (lhs, rhs) {
        case (.bool(let lhs), .bool(let rhs)):
            return compare(lhs ? 1 : 0, rhs ? 1 : 0)
        case (.int64(let lhs), .int64(let rhs)):
            return compare(lhs, rhs)
        case (.double(let lhs), .double(let rhs)):
            if lhs == rhs {
                return 0
            }
            return lhs < rhs ? -1 : 1
        case (.string(let lhs), .string(let rhs)):
            if lhs == rhs {
                return 0
            }
            return lhs < rhs ? -1 : 1
        case (.bytes(let lhs), .bytes(let rhs)):
            return lexicographicCompare(lhs, rhs)
        default:
            throw DatabaseFrameworkWasmError.unsupportedPredicateComparison
        }
    }

    private static func contains(
        _ actual: DatabaseKitWasmFieldValue,
        _ expected: DatabaseKitWasmFieldValue
    ) -> Bool {
        switch (actual, expected) {
        case (.string(let actual), .string(let expected)):
            return containsSubsequence(Array(expected.utf8), in: Array(actual.utf8))
        case (.bytes(let actual), .bytes(let expected)):
            return containsSubsequence(expected, in: actual)
        case (.array(let actual), _):
            return actual.contains(expected)
        default:
            return false
        }
    }

    private static func compare<T: Comparable>(_ lhs: T, _ rhs: T) -> Int {
        if lhs == rhs {
            return 0
        }
        return lhs < rhs ? -1 : 1
    }

    private static func containsSubsequence(_ needle: [UInt8], in haystack: [UInt8]) -> Bool {
        guard !needle.isEmpty else {
            return true
        }
        guard needle.count <= haystack.count else {
            return false
        }
        let lastStart = haystack.count - needle.count
        for start in 0...lastStart {
            var matched = true
            for offset in 0..<needle.count where haystack[start + offset] != needle[offset] {
                matched = false
                break
            }
            if matched {
                return true
            }
        }
        return false
    }

    private static func lexicographicCompare(_ lhs: [UInt8], _ rhs: [UInt8]) -> Int {
        let count = min(lhs.count, rhs.count)
        for index in 0..<count {
            if lhs[index] != rhs[index] {
                return lhs[index] < rhs[index] ? -1 : 1
            }
        }
        return compare(lhs.count, rhs.count)
    }
}
