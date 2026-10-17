import DatabaseKit
import DatabaseTypes

/// Selected SQL frame coordinates and non-wrapping RANGE predicates.
enum SQLWindowFrameEvaluator {
    struct Offsets: Sendable {
        let start: DatabaseContext.ProjectedQueryValue?
        let end: DatabaseContext.ProjectedQueryValue?
    }

    private enum Threshold {
        case integer(Int128)
        case floating(Double)
        case decimal(CanonicalSQLDecimalWorkspaceValue)
        case temporal(QueryValue, QueryColumnType)
        case temporalScalar(Int128)
        case infinity(Int8)
        case all
    }

    static func offsets(_ frame: CanonicalBoundWindowFrame?,
        direct: (CanonicalBoundExpression) async throws -> DatabaseContext.ProjectedQueryValue,
        workMeter: DatabaseWorkMeter) async throws -> Offsets {
        func selected(_ expression: CanonicalBoundExpression?) async throws -> DatabaseContext.ProjectedQueryValue? {
            guard let expression else { return nil }
            let cell = try await direct(expression)
            for lease in cell.leases {
                guard lease.workMeter === workMeter else { throw mismatch("window offset work meter") }
            }
            guard !cell.value.isNull else { throw invalid("frame offset must not be NULL") }
            if frame?.mode != .range {
                guard let value = try SQLWindowMemory.integer(cell.value, function: "window frame"), value >= 0 else {
                    throw invalid("frame offset must not be negative")
                }
            }
            return cell
        }
        let start = try await selected(frame?.startOffset)
        let end = try await selected(frame?.endOffset)
        return Offsets(start: start, end: end)
    }

    static func includes(_ candidate: Int, current: Int, peerStart: Int, peerEnd: Int,
        exclusion: WindowFrameExclusion) -> Bool {
        switch exclusion {
        case .noOthers: return true
        case .currentRow: return candidate != current
        case .group: return candidate < peerStart || candidate >= peerEnd
        case .ties: return candidate == current || candidate < peerStart || candidate >= peerEnd
        }
    }

    static func bounds(plan: CanonicalBoundWindow, position: Int,
        partition: Range<Int>, indices: [Int], peerStarts: [Int], peerEnds: [Int],
        groups: [Int], keys: [DatabaseContext.ProjectedQueryValue], keyCount: Int,
        offsets: Offsets, environment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter) throws -> Range<Int> {
        let mode = plan.frame?.mode ?? .range
        func boundary(_ bound: WindowFrameBound, offset: DatabaseContext.ProjectedQueryValue?, start: Bool) throws -> Int {
            try workMeter.consume(at: .expressionEvaluation)
            switch bound {
            case .unboundedPreceding: return partition.lowerBound
            case .unboundedFollowing: return partition.upperBound
            case .currentRow:
                if mode == .rows { return position + (start ? 0 : 1) }
                return start ? peerStarts[position] : peerEnds[position]
            case .preceding, .following:
                guard let offset else { throw mismatch("selected frame offset") }
                let preceding: Bool
                if case .preceding = bound { preceding = true } else { preceding = false }
                if mode == .rows || mode == .groups {
                    guard let count = try SQLWindowMemory.integer(offset.value, function: "window frame"), count >= 0 else {
                        throw invalid("frame offset must not be negative")
                    }
                    let base = mode == .rows ? position : groups[position]
                    let target = Int128(base) + (preceding ? -Int128(count) : Int128(count))
                    if mode == .rows {
                        return Int(max(Int128(partition.lowerBound), min(Int128(partition.upperBound), target + (start ? 0 : 1))))
                    }
                    if target < 0 { return partition.lowerBound }
                    if target > Int128(groups[partition.upperBound - 1]) { return partition.upperBound }
                    var candidate = partition.lowerBound
                    while candidate < partition.upperBound {
                        try workMeter.consume(at: .expressionEvaluation)
                        if Int128(groups[candidate]) == target { return start ? candidate : peerEnds[candidate] }
                        candidate = peerEnds[candidate]
                    }
                    throw mismatch("selected window group coordinate")
                }
                guard plan.orderBy.count == 1, let order = plan.orderBy.first,
                      let sort = order.sortKey else { throw mismatch("offset RANGE requires one ordering key") }
                let slot = plan.partitionBy.count
                let current = keys[indices[position] * keyCount + slot].value
                if current.isNull { return start ? peerStarts[position] : peerEnds[position] }
                let subtract = preceding != (sort.direction == .descending)
                let threshold = try Self.threshold(current: current, type: order.expression.type,
                    offset: offset.value, subtract: subtract, environment: environment, workMeter: workMeter)
                var candidate = partition.lowerBound
                while candidate < partition.upperBound {
                    try workMeter.consume(at: .expressionEvaluation)
                    let value = keys[indices[candidate] * keyCount + slot].value
                    let comparison: QueryComparison
                    if value.isNull {
                        comparison = try CanonicalSQLValueSemantics.compareValues(value, current,
                            type: order.expression.type, using: sort, environment: environment, workMeter: workMeter)
                    } else {
                        let natural = try compare(value, type: order.expression.type, threshold: threshold,
                            environment: environment, workMeter: workMeter)
                        if sort.direction == .descending {
                            comparison = natural == .equal ? .equal : natural == .lessThan ? .greaterThan : .lessThan
                        } else { comparison = natural }
                    }
                    if start ? comparison != .lessThan : comparison == .greaterThan { return candidate }
                    candidate += 1
                }
                return partition.upperBound
            }
        }
        let lower = try boundary(plan.frame?.start ?? .unboundedPreceding, offset: offsets.start, start: true)
        let upper = try boundary(plan.frame?.end ?? .currentRow, offset: offsets.end, start: false)
        return lower..<max(lower, upper)
    }

    private static func threshold(current: QueryValue, type: QueryColumnType, offset: QueryValue,
        subtract: Bool, environment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter) throws -> Threshold {
        try workMeter.consume(at: .expressionEvaluation)
        guard case .sql(let selected) = type else { throw mismatch("selected RANGE ordering type") }
        switch selected {
        case .smallint, .integer, .bigint:
            let value = try CanonicalSQLNumericEvaluator.integer(current)
            let distance = try CanonicalSQLNumericEvaluator.integer(offset)
            guard distance >= 0 else { throw invalid("RANGE offset must not be negative") }
            return .integer(Int128(value) + (subtract ? -Int128(distance) : Int128(distance)))
        case .real, .doublePrecision:
            func floating(_ value: QueryValue) throws -> Double {
                switch value {
                case .primitive(.float32(let value)): return Double(value)
                case .primitive(.float64(let value)): return value
                default: throw mismatch("selected floating RANGE")
                }
            }
            let value = try floating(current), distance = try floating(offset)
            guard distance >= 0, distance.isFinite else { throw invalid("RANGE offset must be finite and nonnegative") }
            return .floating(value + (subtract ? -distance : distance))
        case .decimal:
            let value = try CanonicalSQLDecimalWorkspaceValue(CanonicalSQLNumericEvaluator.numeric(current).value, workMeter: workMeter)
            let distance = try CanonicalSQLDecimalWorkspaceValue(CanonicalSQLNumericEvaluator.numeric(offset).value, workMeter: workMeter)
            guard distance.sign >= 0 else { throw invalid("RANGE offset must not be negative") }
            return .decimal(try value.adding(subtract ? distance.negated() : distance))
        case .date, .time, .timestamp, .interval:
            guard let environment else { throw DatabaseExpressionEvaluationError.missingTemporalEnvironment }
            let base = try SQLTemporalIdentity.make(current, type: type, workMeter: workMeter)
            let distance = try SQLTemporalIdentity.make(offset, type: .sql(.interval()), workMeter: workMeter)
            let isTime: Bool
            if case .time(false, _) = selected { isTime = true } else { isTime = false }
            if isTime, case .interval(let value) = offset {
                guard value.microseconds >= 0 else { throw invalid("RANGE offset must not be negative") }
                return .temporalScalar(base.scalar + (subtract ? -Int128(value.microseconds) : Int128(value.microseconds)))
            }
            guard distance.infinityRank >= 0, distance.infinityRank != 0 || distance.scalar >= 0 else {
                throw invalid("RANGE offset must not be negative")
            }
            if distance.infinityRank > 0 {
                if base.infinityRank == (subtract ? 1 : -1) { return .all }
                return .infinity(subtract ? -1 : 1)
            }
            if base.infinityRank != 0 { return .infinity(base.infinityRank) }
            if case .interval = selected {
                let left = try CanonicalSQLTemporalEvaluator.interval(current)
                let right = try CanonicalSQLTemporalEvaluator.interval(offset)
                let sign: Int128 = subtract ? -1 : 1
                let result = try SQLTemporalPrecision.interval(
                    months: Int128(left.period.months) + sign * Int128(right.period.months),
                    days: Int128(left.period.days) + sign * Int128(right.period.days),
                    microseconds: Int128(left.microseconds) + sign * Int128(right.microseconds))
                return .temporalScalar(try SQLTemporalIdentity.make(.interval(result), type: type, workMeter: workMeter).scalar)
            }
            let interval = try CanonicalSQLTemporalEvaluator.interval(offset)
            let threshold = try SQLTemporalArithmetic.adding(interval, to: current, type: type,
                subtract: subtract, environment: environment, workMeter: workMeter)
            let thresholdType: QueryColumnType
            if case .date = selected { thresholdType = .sql(.timestamp(withTimeZone: false)) }
            else { thresholdType = type }
            return .temporal(threshold, thresholdType)
        default: throw mismatch("selected RANGE ordering type")
        }
    }

    private static func compare(_ value: QueryValue, type: QueryColumnType, threshold: Threshold,
        environment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter) throws -> QueryComparison {
        func comparison<T: Comparable>(_ left: T, _ right: T) -> QueryComparison {
            left == right ? .equal : left < right ? .lessThan : .greaterThan
        }
        switch threshold {
        case .integer(let bound): return comparison(Int128(try CanonicalSQLNumericEvaluator.integer(value)), bound)
        case .floating(let bound):
            switch value {
            case .primitive(.float32(let value)): return comparison(Double(value), bound)
            case .primitive(.float64(let value)): return comparison(value, bound)
            default: throw mismatch("floating RANGE comparison")
            }
        case .decimal(let bound):
            let value = try CanonicalSQLDecimalWorkspaceValue(CanonicalSQLNumericEvaluator.numeric(value).value, workMeter: workMeter)
            return comparison(try value.comparison(to: bound), 0)
        case .temporal(let bound, let boundType):
            guard let environment else { throw DatabaseExpressionEvaluationError.missingTemporalEnvironment }
            let selected: QueryValue
            if case .sql(.date) = type {
                selected = try CanonicalSQLTemporalEvaluator.cast(value, from: type, to: .sql(.timestamp(withTimeZone: false)),
                    context: .implicit, environment: environment, workMeter: workMeter)
            } else { selected = value }
            return try CanonicalSQLTemporalEvaluator.compare(selected, bound, leftType: boundType,
                rightType: boundType, environment: environment, workMeter: workMeter)
        case .temporalScalar(let bound):
            let key = try SQLTemporalIdentity.make(value, type: type, workMeter: workMeter)
            if key.infinityRank != 0 { return key.infinityRank < 0 ? .lessThan : .greaterThan }
            return comparison(key.scalar, bound)
        case .infinity(let rank):
            return comparison(try SQLTemporalIdentity.make(value, type: type, workMeter: workMeter).infinityRank, rank)
        case .all: return .equal
        }
    }

    private static func mismatch(_ operation: String) -> DatabaseExpressionEvaluationError { .typeMismatch(operation: operation) }
    private static func invalid(_ reason: String) -> DatabaseExpressionEvaluationError {
        .invalidFunctionArgument(function: "window frame", reason: reason)
    }
}
