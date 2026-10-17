import DatabaseKit
import DatabaseTypes

extension SQLAggregateEvaluator {
    static func general(_ selected: CanonicalBoundAggregate, bound: CanonicalBoundExpression,
        inputs: [Input], environment: SQLTemporalEnvironment?, memory: SQLAggregateWorkspace) async throws -> Cell {
        let function = selected.call.function
        if function == .count {
            var count: Int64 = 0
            for row in inputs {
                try await checkpoint(memory.meter)
                if row.arguments.isEmpty || !row.arguments[0].value.isNull {
                    let next = count.addingReportingOverflow(1)
                    guard !next.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
                    count = next.partialValue
                }
            }
            return scalar(.primitive(.int64(count)))
        }
        guard let argument = selected.arguments.first else { throw mismatch("aggregate selected input") }
        if function == .sum || function == .avg {
            if case .sql(.interval) = argument.type {
                return try await interval(function, inputs: inputs, memory: memory)
            }
            var accumulator = DatabaseNumericAggregateAccumulator(inputType: argument.type, workMeter: memory.meter)
            do {
                for row in inputs {
                    try await checkpoint(memory.meter)
                    try accumulator.add(row.arguments[0].value)
                }
                return scalar(try (function == .avg ? accumulator.average() : accumulator.sum()) ?? .primitive(.null))
            } catch let error as DatabaseNumericAggregateAccumulator.Failure {
                switch error {
                case .nonFiniteValue: throw DatabaseExpressionEvaluationError.nonFiniteValue
                case .incompatibleNumericKinds, .nonNumericValue: throw mismatch("aggregate selected numeric input")
                case .numericOverflow, .resultNotRepresentable: throw DatabaseExpressionEvaluationError.numericOverflow
                }
            }
        }
        var result: Cell?
        for row in inputs {
            try await checkpoint(memory.meter)
            let cell = row.arguments[0]
            if cell.value.isNull { continue }
            guard let previous = result else { result = cell; continue }
            switch function {
            case .min, .max:
                let comparison = try CanonicalSQLValueSemantics.compareValues(cell.value, previous.value,
                    leftType: argument.type, rightType: argument.type, environment: environment, workMeter: memory.meter)
                if (function == .min && comparison == .lessThan) || (function == .max && comparison == .greaterThan) { result = cell }
            case .boolAnd, .boolOr, .every:
                guard case .primitive(.bool(let left)) = previous.value,
                      case .primitive(.bool(let right)) = cell.value else { throw mismatch("aggregate boolean input") }
                result = scalar(.primitive(.bool(function == .boolOr ? left || right : left && right)))
            case .bitAnd, .bitOr, .bitXor:
                let left = try CanonicalSQLNumericEvaluator.integer(previous.value)
                let right = try CanonicalSQLNumericEvaluator.integer(cell.value)
                let value = function == .bitAnd ? left & right : (function == .bitOr ? left | right : left ^ right)
                result = scalar(try CanonicalSQLNumericEvaluator.integerResult(value, type: bound.type))
            default: throw mismatch("selected general aggregate")
            }
        }
        if function == .boolAnd || function == .boolOr || function == .every
            || function == .bitAnd || function == .bitOr || function == .bitXor {
            return scalar(result?.value ?? .primitive(.null))
        }
        return result ?? null
    }

    private static func interval(_ function: SQLAggregateFunction, inputs: [Input],
        memory: SQLAggregateWorkspace) async throws -> Cell {
        var sum: QueryIntervalValue?
        var count: UInt64 = 0
        var positive = false, negative = false
        for row in inputs {
            try await checkpoint(memory.meter)
            let value = row.arguments[0].value
            if value.isNull { continue }
            let rank = SQLTemporalArithmetic.infinity(value)
            if rank != 0 { positive = positive || rank > 0; negative = negative || rank < 0; continue }
            let interval = try CanonicalSQLTemporalEvaluator.interval(value)
            if let old = sum {
                sum = try SQLTemporalPrecision.interval(months: Int128(old.period.months) + Int128(interval.period.months),
                    days: Int128(old.period.days) + Int128(interval.period.days),
                    microseconds: Int128(old.microseconds) + Int128(interval.microseconds))
            } else { sum = interval }
            let next = count.addingReportingOverflow(1)
            guard !next.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            count = next.partialValue
        }
        if positive || negative {
            guard !(positive && negative) else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            return scalar(.intervalInfinity(negative ? .negative : .positive))
        }
        guard let sum else { return null }
        if function == .avg {
            return scalar(.interval(try SQLTemporalArithmetic.scale(sum, factor: Double(count), dividing: true)))
        }
        return scalar(.interval(sum))
    }
}
