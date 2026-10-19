import DatabaseKit
import DatabaseTypes
import DatabaseMath

extension SQLAggregateEvaluator {
    static func ordered(_ selected: CanonicalBoundAggregate, bound: CanonicalBoundExpression,
        inputs: [Input], direct: [Cell], environment: SQLTemporalEnvironment?,
        memory: SQLAggregateWorkspace, limits: QueryValueLimits) async throws -> Cell {
        guard let key = selected.withinGroup.first else { throw mismatch("ordered-set selected input") }
        let function = selected.call.function
        if function == .mode {
            guard let first = inputs.first else { return null }
            var mode = first.arguments[0], previous = mode
            var frequency = 0, maximum = 0
            for row in inputs {
                try await checkpoint(memory.meter)
                let current = row.arguments[0]
                if try CanonicalSQLValueSemantics.equalValues(current.value, previous.value,
                    leftType: key.expression.type, rightType: key.expression.type,
                    environment: environment, workMeter: memory.meter) { frequency += 1 }
                else { previous = current; frequency = 1 }
                if frequency > maximum { maximum = frequency; mode = current }
            }
            return mode
        }
        guard direct.count == 1 else { throw mismatch("percentile direct argument") }
        let fraction = direct[0].value
        if case .sql(.array) = selected.directArguments[0].type {
            // PostgreSQL finalizes an empty ordered input before deconstructing fractions.
            guard !inputs.isEmpty, !fraction.isNull else { return null }
            guard case .array(let fractions) = fraction,
                  case .sql(.array(let element)) = bound.type else { throw mismatch("percentile fraction array") }
            @AdmittedArray var results: [QueryValue] = []
            for value in fractions.elements {
                try await checkpoint(memory.meter)
                try memory.append(percentile(function, fraction: value, inputs: inputs,
                    meter: memory.meter).value, to: &$results)
            }
            let result = try SQLArrayStorage.makeArray(element: element, rank: fractions.dimensions.count,
                count: fractions.elements.count, meter: memory.meter, limits: limits,
                sourceElementType: key.expression.type, environment: environment,
                dimension: { fractions.dimensions[$0] }, cell: { index, _ in results[index] })
            return try retained(result.value, lease: result.lease, memory: memory)
        }
        // A non-NULL scalar fraction is checked even when the ordered input is empty.
        return try percentile(function, fraction: fraction, inputs: inputs, meter: memory.meter)
    }

    private static func percentile(_ function: SQLAggregateFunction, fraction: QueryValue,
        inputs: [Input], meter: DatabaseWorkMeter) throws -> Cell {
        if fraction.isNull { return null }
        guard case .primitive(.float64(let fraction)) = fraction,
              fraction.isFinite, fraction >= 0, fraction <= 1 else {
            throw invalid(function, "fraction must be between zero and one")
        }
        guard !inputs.isEmpty else { return null }
        try meter.consume(at: .aggregateInput)
        if function == .percentileDisc {
            let row = (fraction * Double(inputs.count)).rounded(.up)
            guard let index = Int(exactly: max(0, row - 1)), inputs.indices.contains(index) else {
                throw DatabaseExpressionEvaluationError.numericOverflow
            }
            return inputs[index].arguments[0]
        }
        let row = fraction * Double(inputs.count - 1)
        guard let lower = Int(exactly: row.rounded(.down)), let upper = Int(exactly: row.rounded(.up)),
              inputs.indices.contains(lower), inputs.indices.contains(upper) else {
            throw DatabaseExpressionEvaluationError.numericOverflow
        }
        let first = inputs[lower].arguments[0]
        if lower == upper { return first }
        let last = inputs[upper].arguments[0]
        let weight = row - Double(lower)
        switch (first.value, last.value) {
        case (.primitive(.float64(let left)), .primitive(.float64(let right))):
            let result = left + weight * (right - left)
            guard result.isFinite else { throw DatabaseExpressionEvaluationError.numericOverflow }
            return scalar(.primitive(.float64(result)))
        case (.interval, .interval), (.intervalInfinity, _), (_, .intervalInfinity):
            let difference = try intervalDifference(last.value, first.value)
            let delta: QueryValue
            let rank = SQLTemporalArithmetic.infinity(difference)
            if rank != 0 { delta = difference }
            else { delta = .interval(try SQLTemporalArithmetic.scale(CanonicalSQLTemporalEvaluator.interval(difference), factor: weight, dividing: false)) }
            return scalar(try intervalAddition(first.value, delta))
        default: throw mismatch("PERCENTILE_CONT selected value")
        }
    }

    private static func intervalDifference(_ left: QueryValue, _ right: QueryValue) throws -> QueryValue {
        let lrank = SQLTemporalArithmetic.infinity(left), rrank = SQLTemporalArithmetic.infinity(right)
        if lrank != 0 || rrank != 0 {
            guard lrank == 0 || rrank == 0 || lrank != rrank else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            return .intervalInfinity((lrank == 0 ? -rrank : lrank) < 0 ? .negative : .positive)
        }
        let a = try CanonicalSQLTemporalEvaluator.interval(left), b = try CanonicalSQLTemporalEvaluator.interval(right)
        return .interval(try SQLTemporalPrecision.interval(months: Int128(a.period.months) - Int128(b.period.months),
            days: Int128(a.period.days) - Int128(b.period.days), microseconds: Int128(a.microseconds) - Int128(b.microseconds)))
    }

    private static func intervalAddition(_ left: QueryValue, _ right: QueryValue) throws -> QueryValue {
        let lrank = SQLTemporalArithmetic.infinity(left), rrank = SQLTemporalArithmetic.infinity(right)
        if lrank != 0 || rrank != 0 {
            guard lrank == 0 || rrank == 0 || lrank == rrank else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            return .intervalInfinity((lrank == 0 ? rrank : lrank) < 0 ? .negative : .positive)
        }
        let a = try CanonicalSQLTemporalEvaluator.interval(left), b = try CanonicalSQLTemporalEvaluator.interval(right)
        return .interval(try SQLTemporalPrecision.interval(months: Int128(a.period.months) + Int128(b.period.months),
            days: Int128(a.period.days) + Int128(b.period.days), microseconds: Int128(a.microseconds) + Int128(b.microseconds)))
    }
}
