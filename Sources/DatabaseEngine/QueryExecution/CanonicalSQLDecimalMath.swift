import DatabaseKit
import DatabaseTypes
import DatabaseMath

/// Decimal-only transcendental kernels for an already-selected SQL signature.
enum CanonicalSQLDecimalMath {
    private typealias Wide = CanonicalSQLDecimalWorkspaceValue
    // Two base10000 reference guard groups protect series/reduction rounding.
    private static let guardDigits: Int32 = 2 * 4

    static func evaluate(function: String, operands: [QueryNumericValue],
                         workMeter: DatabaseWorkMeter) throws -> QueryNumericValue {
        guard let first = operands.first else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: function)
        }
        try workMeter.checkpoint(at: .expressionEvaluation)
        do {
            let input = try Wide(first.value, workMeter: workMeter)
            let scale: Int32
            let output: Wide
            switch function {
            case "SQRT":
                guard operands.count == 1 else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: function) }
                guard input.sign >= 0 else { throw DatabaseExpressionEvaluationError.numericDomain(operation: function) }
                scale = selectedScale(16 - input.leadingBase10000.weight * 2 - 1, first.displayScale)
                output = try squareRoot(input, scale: scale)
            case "EXP":
                guard operands.count == 1 else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: function) }
                let estimate = approximate(input) * 0.434294481903252
                scale = selectedScale(16 - Int64(clampedEstimate(estimate)), first.displayScale)
                output = try exponential(input, scale: scale)
            case "LN":
                guard operands.count == 1 else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: function) }
                scale = selectedScale(16 - Int64(try logarithmWeight(input)), first.displayScale)
                output = try logarithm(input, scale: scale)
            case "LOG", "LOG10":
                guard operands.count == 1 || (function == "LOG" && operands.count == 2) else {
                    throw DatabaseExpressionEvaluationError.typeMismatch(operation: function)
                }
                let base = operands.count == 2 ? input : try constant(10, meter: workMeter)
                let number = operands.count == 2 ? try Wide(operands[1].value, workMeter: workMeter) : input
                guard base.sign > 0 && number.sign > 0 else { throw DatabaseExpressionEvaluationError.numericDomain(operation: function) }
                let baseWeight = try logarithmWeight(base)
                let numberWeight = try logarithmWeight(number)
                let resultWeight = numberWeight - baseWeight
                scale = selectedScale(16 - Int64(resultWeight), max(first.displayScale, operands.last!.displayScale))
                let numerator = try logarithm(number, scale: max(0, scale + Int32(resultWeight - numberWeight) + guardDigits))
                let denominator = try logarithm(base, scale: max(0, scale + Int32(resultWeight - baseWeight) + guardDigits))
                output = try numerator.divided(by: denominator, toScale: scale)
            case "POWER", "POW":
                guard operands.count == 2 else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: function) }
                let exponent = try Wide(operands[1].value, workMeter: workMeter)
                guard !(input.isZero && exponent.sign < 0), !(input.sign < 0 && exponent.scale > 0) else {
                    throw DatabaseExpressionEvaluationError.numericDomain(operation: function)
                }
                let display = max(first.displayScale, operands[1].displayScale)
                if operands[1].value.scale <= 0,
                   let integer = try integerExponent(operands[1].value) {
                    let estimate = input.isZero ? 0 : Double(integer) * logarithm10Estimate(input)
                    scale = selectedScale(16 - Int64(clampedEstimate(estimate)), display)
                    output = try integerPower(input, exponent: integer, scale: scale,
                                              estimatedResultWeight: Int64(clampedEstimate(estimate)))
                } else if input.isZero {
                    scale = 16
                    output = input
                } else {
                    let absolute = input.sign < 0 ? input.negated() : input
                    let lnWeight = try logarithmWeight(absolute)
                    let lowLog = try logarithm(absolute, scale: max(0, guardDigits - Int32(lnWeight)))
                    let lowProduct = try lowLog.multiplied(by: exponent).rounded(toScale: max(0, guardDigits - Int32(lnWeight)))
                    let estimate = approximate(lowProduct) * 0.434294481903252
                    scale = selectedScale(16 - Int64(clampedEstimate(estimate)), display)
                    let localScale = max(0, scale + Int32(clampedEstimate(estimate)) - Int32(lnWeight) + guardDigits)
                    let ln = try logarithm(absolute, scale: localScale)
                    let product = try ln.multiplied(by: exponent).rounded(toScale: localScale)
                    var result = try exponential(product, scale: scale)
                    if input.sign < 0, operands[1].value.scale == 0,
                       !operands[1].value.coefficient.isMultiple(of: 2) { result = result.negated() }
                    output = result
                }
            default: throw DatabaseExpressionEvaluationError.unsupportedFunction(function)
            }
            let rounded = try output.rounded(toScale: scale)
            do {
                return try QueryNumericValue(value: rounded.exactDecimal(), displayScale: scale)
            } catch is QueryValueError {
                throw DatabaseExpressionEvaluationError.numericOverflow
            }
        } catch let error as ExactDecimalError {
            switch error {
            case .divisionByZero: throw DatabaseExpressionEvaluationError.divisionByZero
            case .numericOverflow: throw DatabaseExpressionEvaluationError.numericOverflow
            case .inexactResult: throw DatabaseExpressionEvaluationError.inexactDecimalResult
            }
        }
    }

    /// Aggregate finalization preserves widened variance until its final rounding.
    static func aggregateSquareRoot(_ input: CanonicalSQLDecimalWorkspaceValue,
        scale: Int32) throws -> CanonicalSQLDecimalWorkspaceValue {
        guard input.sign >= 0 else { throw DatabaseExpressionEvaluationError.numericDomain(operation: "SQRT") }
        return try squareRoot(input, scale: scale)
    }

    private static func squareRoot(_ input: Wide, scale: Int32) throws -> Wide {
        guard !input.isZero else { return input }
        let meter = input.workMeter
        let one = try constant(1, meter: meter)
        let weight = input.decimalDigitCount - input.scale - 1
        let rootWeight = weight >= 0 ? weight / 2 : (weight - 1) / 2
        guard let initialScale = Int32(exactly: -rootWeight - 1) else { throw ExactDecimalError.numericOverflow }
        var estimate = try Wide(ExactDecimal(coefficient: 1, scale: initialScale), workMeter: meter)
        let localScale = scale + guardDigits
        var previous: Wide? = nil
        while true {
            try meter.consume(at: .expressionEvaluation)
            let quotient = try input.divided(by: estimate, toScale: localScale)
            let next = try estimate.adding(quotient).divided(by: 2, toScale: localScale)
            if try next.magnitudeComparison(to: estimate) == 0 || previous?.magnitudeComparison(to: next) == 0 {
                return try next.rounded(toScale: scale)
            }
            previous = estimate
            estimate = next
            if estimate.isZero { return try one.subtracting(one) }
        }
    }

    private static func exponential(_ input: Wide, scale: Int32) throws -> Wide {
        let meter = input.workMeter
        let one = try constant(1, meter: meter)
        let estimate = approximate(input)
        // PostgreSQL's packed numeric result scale determines this range bound.
        if estimate >= Double(2_000 * 3) { throw ExactDecimalError.numericOverflow }
        if estimate <= -Double(2_000 * 3) { return try constant(0, meter: meter) }
        var reducedEstimate = abs(estimate)
        var reductions = 0
        while reducedEstimate > 0.01 {
            try meter.consume(at: .expressionEvaluation)
            reducedEstimate /= 2
            reductions += 1
        }
        let weight = Int32(clampedEstimate(estimate * 0.434294481903252))
        let significant = max(0, 1 + weight + scale + Int32(Double(reductions) * 0.301029995663981)) + guardDigits
        let localScale = max(0, significant - 1)
        var reduced = input
        for _ in 0..<reductions { reduced = try reduced.divided(by: 2, toScale: localScale + Int32(reductions)) }
        var sum = try one.adding(reduced)
        var term = reduced
        var divisor: UInt64 = 1
        while true {
            try meter.consume(at: .expressionEvaluation)
            divisor += 1
            term = try term.multiplied(by: reduced).rounded(toScale: localScale)
            term = try term.divided(by: divisor, toScale: localScale)
            if term.isZero { break }
            sum = try sum.adding(term)
        }
        for _ in 0..<reductions {
            let workingScale = max(0, significant - Int32(sum.leadingBase10000.weight * 8))
            sum = try sum.multiplied(by: sum).rounded(toScale: workingScale)
        }
        return try sum.rounded(toScale: scale)
    }

    private static func logarithm(_ input: Wide, scale: Int32) throws -> Wide {
        guard input.sign > 0 else { throw DatabaseExpressionEvaluationError.numericDomain(operation: "LN") }
        let meter = input.workMeter
        let one = try constant(1, meter: meter)
        let lower = try Wide(ExactDecimal(coefficient: 9, scale: 1), workMeter: meter)
        let upper = try Wide(ExactDecimal(coefficient: 11, scale: 1), workMeter: meter)
        var reduced = input
        var factor = try constant(2, meter: meter)
        var reductions = 0
        while try reduced.magnitudeComparison(to: lower) <= 0 || reduced.magnitudeComparison(to: upper) >= 0 {
            try meter.consume(at: .expressionEvaluation)
            let working = Int64(scale) - reduced.leadingBase10000.weight * 2 + Int64(guardDigits)
            guard let workingScale = Int32(exactly: working) else { throw ExactDecimalError.numericOverflow }
            reduced = try squareRoot(reduced, scale: workingScale)
            factor = try factor.multiplied(by: constant(2, meter: meter))
            reductions += 1
        }
        let localScale = scale + Int32(Double(reductions + 1) * 0.301029995663981) + guardDigits
        let numerator = try reduced.subtracting(one)
        let denominator = try reduced.adding(one)
        let z = try numerator.divided(by: denominator, toScale: localScale)
        let square = try z.multiplied(by: z).rounded(toScale: localScale)
        var power = z
        var sum = z
        var divisor: UInt64 = 1
        while true {
            try meter.consume(at: .expressionEvaluation)
            divisor += 2
            power = try power.multiplied(by: square).rounded(toScale: localScale)
            let term = try power.divided(by: divisor, toScale: localScale)
            if term.isZero { break }
            sum = try sum.adding(term)
        }
        return try sum.multiplied(by: factor).rounded(toScale: scale)
    }

    private static func integerPower(_ base: Wide, exponent: Int32, scale: Int32,
                                     estimatedResultWeight: Int64) throws -> Wide {
        let one = try constant(1, meter: base.workMeter)
        if exponent == 0 { return one }
        if exponent == 1 { return try base.rounded(toScale: scale) }
        if exponent == -1 { return try one.divided(by: base, toScale: scale) }
        let significantDigits = max(1, 1 + Int64(scale) + estimatedResultWeight)
            + Int64(guardDigits) + Int64(DatabaseMath.naturalLogarithm(Double(exponent.magnitude)))
        func product(_ left: Wide, _ right: Wide) throws -> Wide {
            // Reference power_var_int retains significant digits at each product's
            // current weight, including tiny values needed by the final reciprocal.
            let weight = left.leadingBase10000.weight + right.leadingBase10000.weight
            let fractionalScale = left.scale + right.scale
            let requestedScale = max(0, min(significantDigits - weight * 4, fractionalScale))
            guard let localScale = Int32(exactly: requestedScale) else { throw ExactDecimalError.numericOverflow }
            return try left.multiplied(by: right).rounded(toScale: localScale)
        }
        var power = base
        var result = one
        var bits = exponent.magnitude
        while bits != 0 {
            try base.workMeter.consume(at: .expressionEvaluation)
            if bits & 1 != 0 { result = try product(result, power) }
            bits >>= 1
            if bits != 0 { power = try product(power, power) }
        }
        return exponent < 0 ? try one.divided(by: result, toScale: scale) : try result.rounded(toScale: scale)
    }

    private static func logarithmWeight(_ value: Wide) throws -> Int {
        guard value.sign > 0 else { return 0 }
        let estimate = approximate(value)
        if estimate >= 0.9 && estimate <= 1.1 {
            let difference = try value.subtracting(constant(1, meter: value.workMeter))
            return difference.isZero ? 0 : Int(difference.decimalDigitCount - difference.scale - 1)
        }
        let leading = value.leadingEstimate(groups: 2)
        let ln = DatabaseMath.naturalLogarithm(leading.significand) + Double(leading.exponent) * 2.302585092994046
        return Int(DatabaseMath.naturalLogarithm(abs(ln)) / 2.302585092994046)
    }

    private static func logarithm10Estimate(_ value: Wide) -> Double {
        let leading = value.leadingEstimate(groups: 4)
        return DatabaseMath.naturalLogarithm(leading.significand) / 2.302585092994046 + Double(leading.exponent)
    }

    private static func approximate(_ value: Wide) -> Double {
        let leading = value.leadingEstimate(groups: 4)
        return Double(value.sign) * leading.significand * DatabaseMath.power(10, Double(leading.exponent))
    }

    private static func selectedScale(_ estimated: Int64, _ display: Int32) -> Int32 {
        Int32(min(1_000, max(0, estimated, Int64(display))))
    }
    private static func clampedEstimate(_ estimate: Double) -> Int {
        if estimate.isNaN { return 0 }
        return Int(min(2_000, max(-2_000, estimate)))
    }
    private static func constant(_ value: Int128, meter: DatabaseWorkMeter) throws -> Wide {
        try Wide(ExactDecimal(coefficient: value, scale: 0), workMeter: meter)
    }
    private static func integerExponent(_ value: ExactDecimal) throws -> Int32? {
        var coefficient = value.coefficient
        if value.scale < 0 {
            for _ in 0..<min(10, -Int64(value.scale)) {
                let result = coefficient.multipliedReportingOverflow(by: 10)
                if result.overflow { return nil }
                coefficient = result.partialValue
            }
            if value.scale < -10 && coefficient != 0 { return nil }
        }
        return Int32(exactly: coefficient)
    }
}
