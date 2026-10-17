import DatabaseKit
import DatabaseTypes
import DatabaseMath

extension SQLAggregateEvaluator {
    static func variance(_ selected: CanonicalBoundAggregate, inputs: [Input],
        memory: SQLAggregateWorkspace) async throws -> Cell {
        guard let type = selected.arguments.first?.type else { throw mismatch("variance selected input") }
        let function = selected.call.function
        let sample = function == .varSamp || function == .variance || function == .stddevSamp || function == .stddev
        let squareRoot = function == .stddev || function == .stddevPop || function == .stddevSamp
        switch type {
        case .sql(.real), .sql(.doublePrecision):
            var count: UInt64 = 0
            var sum = 0.0, squares = 0.0
            for row in inputs {
                try await checkpoint(memory.meter)
                if row.arguments[0].value.isNull { continue }
                let number = try floating(row.arguments[0].value)
                let next = count + 1
                let updated = sum + number
                if count > 0 {
                    let difference = number * Double(next) - updated
                    squares += difference * difference / (Double(next) * Double(count))
                }
                guard updated.isFinite, squares.isFinite else { throw DatabaseExpressionEvaluationError.numericOverflow }
                sum = updated; count = next
            }
            if count == 0 || sample && count == 1 { return null }
            let variance = squares / Double(sample ? count - 1 : count)
            return scalar(.primitive(.float64(squareRoot ? DatabaseMath.squareRoot(variance) : variance)))
        default:
            typealias Wide = CanonicalSQLDecimalWorkspaceValue
            var sum: Wide?, squares: Wide?
            var count: UInt64 = 0
            var maximumScale: Int32 = 0
            for row in inputs {
                try await checkpoint(memory.meter)
                let value = row.arguments[0].value
                if value.isNull { continue }
                let number = try exact(value)
                let wide = try Wide(number.value, workMeter: memory.meter)
                sum = try sum.map { try $0.adding(wide) } ?? wide
                let square = try wide.multiplied(by: wide)
                squares = try squares.map { try $0.adding(square) } ?? square
                maximumScale = max(maximumScale, number.displayScale)
                let next = count.addingReportingOverflow(1)
                guard !next.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
                count = next.partialValue
            }
            if count == 0 || sample && count == 1 { return null }
            guard let sum, let squares else { throw mismatch("variance transition state") }
            let n = try Wide(ExactDecimal(coefficient: Int128(count), scale: 0), workMeter: memory.meter)
            let numerator = try n.multiplied(by: squares).subtracting(sum.multiplied(by: sum))
            if numerator.sign <= 0 { return scalar(.numeric(try QueryNumericValue(value: ExactDecimal(coefficient: 0, scale: 0), displayScale: 0))) }
            let divisor = try Wide(ExactDecimal(coefficient: Int128(sample ? count - 1 : count), scale: 0), workMeter: memory.meter)
            let denominator = try n.multiplied(by: divisor)
            let left = numerator.leadingBase10000, right = denominator.leadingBase10000
            let doubled = maximumScale.multipliedReportingOverflow(by: 2)
            guard !doubled.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            let scale = CanonicalSQLNumericScale.divisionScale(leftWeight: left.weight, leftDigit: left.digit,
                leftDisplayScale: doubled.partialValue, rightWeight: right.weight, rightDigit: right.digit, rightDisplayScale: 0)
            let quotient = try numerator.divided(by: denominator, toScale: scale)
            let result = try squareRoot ? CanonicalSQLDecimalMath.aggregateSquareRoot(quotient, scale: scale) : quotient
            return scalar(.numeric(try QueryNumericValue(value: result.exactDecimal(), displayScale: scale)))
        }
    }

    static func regression(_ selected: CanonicalBoundAggregate, inputs: [Input],
        memory: SQLAggregateWorkspace) async throws -> Cell {
        guard selected.arguments.count == 2 else { throw mismatch("pairwise aggregate arguments") }
        var count: UInt64 = 0
        var sx = 0.0, sy = 0.0, sxx = 0.0, syy = 0.0, sxy = 0.0
        for row in inputs {
            try await checkpoint(memory.meter)
            if row.arguments[0].value.isNull || row.arguments[1].value.isNull { continue }
            let y = try floating(row.arguments[0].value), x = try floating(row.arguments[1].value)
            let increment = count.addingReportingOverflow(1)
            guard !increment.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            let next = increment.partialValue
            let newX = sx + x, newY = sy + y
            if count > 0 {
                let dx = x * Double(next) - newX, dy = y * Double(next) - newY
                let factor = Double(next) * Double(count)
                sxx += dx * dx / factor
                syy += dy * dy / factor
                sxy += dx * dy / factor
            }
            guard newX.isFinite, newY.isFinite, sxx.isFinite, syy.isFinite, sxy.isFinite else {
                throw DatabaseExpressionEvaluationError.numericOverflow
            }
            count = next; sx = newX; sy = newY
        }
        let function = selected.call.function
        if function == .regrCount {
            guard let count = Int64(exactly: count) else { throw DatabaseExpressionEvaluationError.numericOverflow }
            return scalar(.primitive(.int64(count)))
        }
        guard count > 0 else { return null }
        let n = Double(count)
        let result: Double
        switch function {
        case .regrAvgx: result = sx / n
        case .regrAvgy: result = sy / n
        case .regrSxx: result = sxx
        case .regrSyy: result = syy
        case .regrSxy: result = sxy
        case .covarPop: result = sxy / n
        case .covarSamp:
            guard count > 1 else { return null }; result = sxy / Double(count - 1)
        case .corr:
            guard sxx != 0, syy != 0 else { return null }
            result = sxy / DatabaseMath.squareRoot(sxx * syy)
        case .regrR2:
            guard sxx != 0 else { return null }
            result = syy == 0 ? 1 : sxy * sxy / (sxx * syy)
        case .regrSlope:
            guard sxx != 0 else { return null }; result = sxy / sxx
        case .regrIntercept:
            guard sxx != 0 else { return null }; result = (sy - sx * sxy / sxx) / n
        default: throw mismatch("selected pairwise aggregate")
        }
        guard result.isFinite else { throw DatabaseExpressionEvaluationError.numericOverflow }
        return scalar(.primitive(.float64(result)))
    }

    private static func floating(_ value: QueryValue) throws -> Double {
        let result: Double
        switch value {
        case .primitive(.float64(let number)): result = number
        case .primitive(.float32(let number)): result = Double(number)
        default: throw mismatch("selected floating aggregate input")
        }
        guard result.isFinite else { throw DatabaseExpressionEvaluationError.nonFiniteValue }
        return result
    }

    private static func exact(_ value: QueryValue) throws -> QueryNumericValue {
        switch value {
        case .primitive(.int16(let number)): return try QueryNumericValue(value: ExactDecimal(coefficient: Int128(number), scale: 0), displayScale: 0)
        case .primitive(.int32(let number)): return try QueryNumericValue(value: ExactDecimal(coefficient: Int128(number), scale: 0), displayScale: 0)
        case .primitive(.int64(let number)): return try QueryNumericValue(value: ExactDecimal(coefficient: Int128(number), scale: 0), displayScale: 0)
        default: return try CanonicalSQLNumericEvaluator.numeric(value)
        }
    }
}
