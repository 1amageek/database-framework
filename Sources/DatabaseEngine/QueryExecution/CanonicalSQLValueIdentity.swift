import DatabaseKit

/// A relational identity preserves SQL temporal equality without changing value representation.
enum CanonicalSQLValueIdentity: Sendable, Hashable {
    case value(QueryValue)
    case temporal(SQLTemporalIdentity)

    func payloadFootprint(workMeter: DatabaseWorkMeter, stage: DatabaseWorkStage) throws -> DatabaseIntermediateFootprint {
        switch self {
        case .value(let value): return try CanonicalRelationalFootprintMeter.valueFootprint(of: value, workMeter: workMeter, stage: stage)
        case .temporal: return DatabaseIntermediateFootprint()
        }
    }

    static func precedes(_ left: Self, _ right: Self) throws -> Bool {
        switch (left, right) {
        case (.temporal(let left), .temporal(let right)): return left < right
        case (.value(.primitive(let left)), .value(.primitive(let right))):
            return try FieldValueComparator.compare(left, right) == .lessThan
        default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "group identity ordering")
        }
    }
}
