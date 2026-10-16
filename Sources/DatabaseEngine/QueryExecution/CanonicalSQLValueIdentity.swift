import DatabaseKit

/// Selected relational identity; Kit representation identity remains unchanged.
enum CanonicalSQLValueIdentity: Sendable, Hashable {
    case value(QueryValue)
    case temporal(SQLTemporalIdentity)
    case collection(CanonicalSQLCollectionIdentity)

    static func prepareCollection(_ value: QueryValue, type: QueryColumnType?,
        environment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter) throws -> Self? {
        guard let type else { return nil }
        switch type {
        case .sql(.json), .sql(.array(.json)):
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "SQL JSON identity")
        case .sql(.array), .sql(.jsonb):
            if value.isNull { return nil }
            return .collection(try CanonicalSQLCollectionIdentity.prepare(value, type: type,
                environment: environment, workMeter: workMeter))
        default: return nil
        }
    }

    func payloadFootprint(workMeter: DatabaseWorkMeter, stage: DatabaseWorkStage) throws -> DatabaseIntermediateFootprint {
        switch self {
        case .value(let value): return try CanonicalRelationalFootprintMeter.valueFootprint(of: value, workMeter: workMeter, stage: stage)
        case .temporal, .collection: return DatabaseIntermediateFootprint()
        }
    }

    static func hash(_ values: [Self], workMeter: DatabaseWorkMeter, stage: DatabaseWorkStage) throws -> Int {
        var hasher = Hasher()
        try workMeter.consume(at: stage)
        hasher.combine(values.count)
        for value in values { try hash(value, into: &hasher, workMeter: workMeter, stage: stage) }
        return hasher.finalize()
    }

    static func hash(_ value: Self, workMeter: DatabaseWorkMeter, stage: DatabaseWorkStage) throws -> Int {
        var hasher = Hasher()
        try hash(value, into: &hasher, workMeter: workMeter, stage: stage)
        return hasher.finalize()
    }

    private static func hash(_ value: Self, into hasher: inout Hasher,
        workMeter: DatabaseWorkMeter, stage: DatabaseWorkStage) throws {
        try workMeter.consume(at: stage)
        switch value {
        case .collection(let key): hasher.combine(2); hasher.combine(key.digest)
        case .temporal(let key): hasher.combine(1); hasher.combine(key)
        case .value(let key):
            _ = try value.payloadFootprint(workMeter: workMeter, stage: stage)
            hasher.combine(0); hasher.combine(key)
        }
    }

    static func equal(_ left: [Self], _ right: [Self], workMeter: DatabaseWorkMeter, stage: DatabaseWorkStage) throws -> Bool {
        try workMeter.consume(at: stage)
        guard left.count == right.count else { return false }
        for index in left.indices {
            if try !equal(left[index], right[index], workMeter: workMeter, stage: stage) { return false }
        }
        return true
    }

    static func equal(_ left: Self, _ right: Self, workMeter: DatabaseWorkMeter, stage: DatabaseWorkStage) throws -> Bool {
        try workMeter.consume(at: stage)
        switch (left, right) {
        case (.collection(let left), .collection(let right)):
            return try left.equal(to: right, workMeter: workMeter, stage: stage)
        case (.temporal(let left), .temporal(let right)): return left == right
        case (.value(let a), .value(let b)):
            _ = try left.payloadFootprint(workMeter: workMeter, stage: stage)
            _ = try right.payloadFootprint(workMeter: workMeter, stage: stage)
            return a == b
        default: return false
        }
    }

    static func precedes(_ left: Self, _ right: Self,
        environment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter) throws -> Bool {
        try workMeter.consume(at: .sortComparison)
        switch (left, right) {
        case (.collection(let left), .collection(let right)):
            return try CanonicalSQLValueSemantics.compareValues(left.source, right.source,
                leftType: left.type, rightType: right.type, environment: environment,
                workMeter: workMeter) == .lessThan
        case (.temporal(let left), .temporal(let right)): return left < right
        case (.value(.primitive(let left)), .value(.primitive(let right))):
            try CanonicalSQLValueSemantics.chargeComparison(left, workMeter: workMeter, stage: .sortComparison)
            try CanonicalSQLValueSemantics.chargeComparison(right, workMeter: workMeter, stage: .sortComparison)
            return try FieldValueComparator.compare(left, right) == .lessThan
        default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "group identity ordering")
        }
    }
}
