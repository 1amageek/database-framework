import DatabaseKit
import DatabaseTypes

/// One series value per pull; widened arithmetic checks the endpoint before narrowing.
struct SQLSeriesCursor: ~Copyable {
    private struct State {
        let type: QueryColumnType
        let end: QueryValue
        let step: QueryValue
        let direction: Int
        let environment: SQLTemporalEnvironment
        let meter: DatabaseWorkMeter
        var current: QueryValue?
        var mustAdvance = false
    }
    private var state: State

    init(bound: CanonicalBoundExpression, values: [QueryValue], environment: SQLTemporalEnvironment,
        meter: DatabaseWorkMeter) throws {
        guard (2...4).contains(values.count), values.count == bound.children.count else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "series selected arity")
        }
        let type = bound.type
        var selectedEnvironment = environment
        let selectedStep: QueryValue
        let direction: Int
        let current: QueryValue?
        // Strictness precedes zero-step and zone validation.
        if values.contains(where: { $0.isNull }) {
            selectedStep = .primitive(.null); direction = 1; current = nil
        } else {
            if values.count > 2 { selectedStep = values[2] }
            else if case .sql(.decimal) = type {
                selectedStep = .numeric(try QueryNumericValue(value: ExactDecimal(coefficient: 1, scale: 0), displayScale: 0))
            } else { selectedStep = type == .sql(.integer) ? .primitive(.int32(1)) : .primitive(.int64(1)) }
            switch type {
            case .sql(.integer), .sql(.bigint):
                let value = try CanonicalSQLNumericEvaluator.integer(selectedStep)
                direction = value == 0 ? 0 : (value < 0 ? -1 : 1)
            case .sql(.decimal):
                let value = try CanonicalSQLNumericEvaluator.numeric(selectedStep).value.coefficient
                direction = value == 0 ? 0 : (value < 0 ? -1 : 1)
            case .sql(.timestamp):
                let identity = try SQLTemporalIdentity.make(selectedStep, type: .sql(.interval()), workMeter: meter)
                guard identity.infinityRank == 0 else {
                    throw DatabaseExpressionEvaluationError.invalidFunctionArgument(function: "generate_series", reason: "step must be finite")
                }
                direction = identity.scalar == 0 ? 0 : (identity.scalar < 0 ? -1 : 1)
                if values.count == 4 {
                    guard case .primitive(.string(let name)) = values[3] else {
                        throw DatabaseExpressionEvaluationError.typeMismatch(operation: "series time zone")
                    }
                    let zone = try CanonicalSQLTemporalEvaluator.zone(name[...], posix: true, environment: environment, workMeter: meter)
                    selectedEnvironment = SQLTemporalEnvironment(transactionInstant: environment.transactionInstant,
                        statementInstant: environment.statementInstant, wallClock: environment.wallClock,
                        configuration: environment.configuration.replacing(timeZone: zone))
                }
            default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "series selected type")
            }
            guard direction != 0 else {
                throw DatabaseExpressionEvaluationError.invalidFunctionArgument(function: "generate_series", reason: "step size cannot equal zero")
            }
            current = values[0]
        }
        // A single nonoptional state avoids partial noncopyable initialization on failure.
        state = State(type: type, end: values[1], step: selectedStep, direction: direction,
            environment: selectedEnvironment, meter: meter, current: current)
    }

    mutating func next() throws -> QueryValue? {
        try state.meter.consume(at: .expressionEvaluation)
        guard let previous = state.current else { return nil }
        let selected: QueryValue
        if state.mustAdvance {
            guard let next = try advance(previous) else { state.current = nil; return nil }
            selected = next
        } else { selected = previous }
        let comparison = try compare(selected, state.end)
        guard state.direction > 0 ? comparison <= 0 : comparison >= 0 else { state.current = nil; return nil }
        state.current = selected
        state.mustAdvance = true
        return selected
    }

    private func compare(_ lhs: QueryValue, _ rhs: QueryValue) throws -> Int {
        switch state.type {
        case .sql(.integer), .sql(.bigint):
            let a = try CanonicalSQLNumericEvaluator.integer(lhs), b = try CanonicalSQLNumericEvaluator.integer(rhs)
            return a == b ? 0 : (a < b ? -1 : 1)
        case .sql(.decimal):
            return try CanonicalSQLNumericEvaluator.decimalOperation(workMeter: state.meter) {
                let a = try CanonicalSQLDecimalWorkspaceValue(CanonicalSQLNumericEvaluator.numeric(lhs).value, workMeter: state.meter)
                let b = try CanonicalSQLDecimalWorkspaceValue(CanonicalSQLNumericEvaluator.numeric(rhs).value, workMeter: state.meter)
                return try a.comparison(to: b)
            }
        default:
            let a = try SQLTemporalIdentity.make(lhs, type: state.type, workMeter: state.meter)
            let b = try SQLTemporalIdentity.make(rhs, type: state.type, workMeter: state.meter)
            return a == b ? 0 : (a < b ? -1 : 1)
        }
    }

    private func advance(_ value: QueryValue) throws -> QueryValue? {
        switch state.type {
        case .sql(.integer), .sql(.bigint):
            let next = Int128(try CanonicalSQLNumericEvaluator.integer(value)) + Int128(try CanonicalSQLNumericEvaluator.integer(state.step))
            let finish = Int128(try CanonicalSQLNumericEvaluator.integer(state.end))
            guard state.direction > 0 ? next <= finish : next >= finish else { return nil }
            guard let result = Int64(exactly: next) else { throw DatabaseExpressionEvaluationError.numericOverflow }
            return try CanonicalSQLNumericEvaluator.integerResult(result, type: state.type)
        case .sql(.decimal):
            return try CanonicalSQLNumericEvaluator.decimalOperation(workMeter: state.meter) {
                let old = try CanonicalSQLNumericEvaluator.numeric(value)
                let increment = try CanonicalSQLNumericEvaluator.numeric(state.step)
                let next = try CanonicalSQLDecimalWorkspaceValue(old.value, workMeter: state.meter)
                    .adding(CanonicalSQLDecimalWorkspaceValue(increment.value, workMeter: state.meter))
                let finish = try CanonicalSQLDecimalWorkspaceValue(CanonicalSQLNumericEvaluator.numeric(state.end).value, workMeter: state.meter)
                let comparison = try next.comparison(to: finish)
                guard state.direction > 0 ? comparison <= 0 : comparison >= 0 else { return nil }
                return try CanonicalSQLNumericEvaluator.numericResult(next.exactDecimal(), scale: max(old.displayScale, increment.displayScale))
            }
        default:
            // S06 infinity arithmetic is absorbing; demand and the common meter bound repetition.
            if SQLTemporalArithmetic.infinity(value) != 0 { return value }
            return try SQLTemporalArithmetic.adding(CanonicalSQLTemporalEvaluator.interval(state.step), to: value,
                type: state.type, environment: state.environment, workMeter: state.meter)
        }
    }
}
