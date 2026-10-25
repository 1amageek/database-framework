import DatabaseTypes
import DatabaseKit
import DatabaseMath

/// Executes an already-selected SQL signature. Row values never select overloads.
enum CanonicalSQLNumericEvaluator {
    static func evaluate<Operands: RandomAccessCollection>(
        _ bound: CanonicalBoundExpression,
        operands: Operands,
        workMeter: DatabaseWorkMeter
    ) throws -> QueryValue where Operands.Element == QueryValue, Operands.Index == Int {
        try workMeter.consume(at: .expressionEvaluation)
        guard operands.startIndex == 0, operands.count == bound.children.count else {
            throw mismatch("selected numeric operand count")
        }
        if case .cast(_, let target) = bound.expression {
            guard let source = bound.children.first, operands.count == 1 else { throw mismatch("CAST") }
            return try CanonicalSQLNumericCast.evaluate(operands[0], from: source.type, to: target, workMeter: workMeter)
        }
        for (value, child) in zip(operands, bound.children) {
            if case .primitive(.float32(let x)) = value, !x.isFinite { throw DatabaseExpressionEvaluationError.nonFiniteValue }
            if case .primitive(.float64(let x)) = value, !x.isFinite { throw DatabaseExpressionEvaluationError.nonFiniteValue }
            guard QueryValueValidator.accepts(value, type: child.type) else { throw mismatch("selected numeric input") }
        }
        if operands.contains(where: { $0.isNull }) { return .primitive(.null) }
        switch bound.expression {
        case .add: return try arithmetic(.add, operands, type: bound.type, workMeter: workMeter)
        case .subtract: return try arithmetic(.subtract, operands, type: bound.type, workMeter: workMeter)
        case .multiply: return try arithmetic(.multiply, operands, type: bound.type, workMeter: workMeter)
        case .divide: return try arithmetic(.divide, operands, type: bound.type, workMeter: workMeter)
        case .modulo: return try arithmetic(.modulo, operands, type: bound.type, workMeter: workMeter)
        case .negate: return try arithmetic(.negate, operands, type: bound.type, workMeter: workMeter)
        case .function(let function):
            return try functionValue(function.name, operands, type: bound.type, workMeter: workMeter)
        default: throw DatabaseExpressionEvaluationError.unsupportedExpression("Selected SQL numeric expression")
        }
    }

    private enum Arithmetic { case add, subtract, multiply, divide, modulo, negate }

    private static func arithmetic<Operands: RandomAccessCollection>(_ operation: Arithmetic, _ values: Operands, type: QueryColumnType,
                                   workMeter: DatabaseWorkMeter) throws -> QueryValue where Operands.Element == QueryValue, Operands.Index == Int {
        guard values.count == (operation == .negate ? 1 : 2) else { throw mismatch("arithmetic arity") }
        switch kind(type) {
        case .smallint, .integer, .bigint:
            let left = try integer(values[0])
            let right = values.count > 1 ? try integer(values[1]) : 0
            let result: (partialValue: Int64, overflow: Bool)
            switch operation {
            case .add: result = left.addingReportingOverflow(right)
            case .subtract: result = left.subtractingReportingOverflow(right)
            case .multiply: result = left.multipliedReportingOverflow(by: right)
            case .negate: result = Int64(0).subtractingReportingOverflow(left)
            case .divide:
                guard right != 0 else { throw DatabaseExpressionEvaluationError.divisionByZero }
                guard left != Int64.min || right != -1 else { throw DatabaseExpressionEvaluationError.numericOverflow }
                result = (left / right, false)
            case .modulo:
                guard right != 0 else { throw DatabaseExpressionEvaluationError.divisionByZero }
                result = (right == -1 ? 0 : left % right, false)
            }
            guard !result.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            return try integerResult(result.partialValue, type: type)
        case .real:
            let left = try real(values[0]), right = values.count > 1 ? try real(values[1]) : 0
            let result: Float
            switch operation {
            case .add: result = left + right
            case .subtract: result = left - right
            case .multiply: result = left * right
            case .divide:
                guard right != 0 else { throw DatabaseExpressionEvaluationError.divisionByZero }
                result = left / right
            case .negate: result = -left
            case .modulo: throw mismatch("floating MOD")
            }
            try finiteResult(Double(result), underflow: result == 0 && left != 0 && right != 0 && (operation == .multiply || operation == .divide))
            return .primitive(.float32(result))
        case .doublePrecision:
            let left = try double(values[0]), right = values.count > 1 ? try double(values[1]) : 0
            let result: Double
            switch operation {
            case .add: result = left + right
            case .subtract: result = left - right
            case .multiply: result = left * right
            case .divide:
                guard right != 0 else { throw DatabaseExpressionEvaluationError.divisionByZero }
                result = left / right
            case .negate: result = -left
            case .modulo: throw mismatch("floating MOD")
            }
            try finiteResult(result, underflow: result == 0 && left != 0 && right != 0 && (operation == .multiply || operation == .divide))
            return .primitive(.float64(result))
        case .decimal:
            let left = try numeric(values[0]), right = values.count > 1 ? try numeric(values[1]) : left
            let scale: Int32
            switch operation {
            case .multiply: scale = min(16383, left.displayScale + right.displayScale)
            case .divide: scale = CanonicalSQLNumericScale.divisionScale(left.value, displayScale: left.displayScale,
                                                                        right.value, displayScale: right.displayScale)
            default: scale = max(left.displayScale, right.displayScale)
            }
            let result = try decimalOperation(workMeter: workMeter) {
                let lhs = try CanonicalSQLDecimalWorkspaceValue(left.value, workMeter: workMeter)
                let rhs = try CanonicalSQLDecimalWorkspaceValue(right.value, workMeter: workMeter)
                let output: CanonicalSQLDecimalWorkspaceValue
                switch operation {
                case .add: output = try lhs.adding(rhs)
                case .subtract: output = try lhs.subtracting(rhs)
                case .multiply: output = try lhs.multiplied(by: rhs).rounded(toScale: scale)
                case .divide: output = try lhs.divided(by: rhs, toScale: scale)
                case .modulo: output = try lhs.remainder(dividingBy: rhs)
                case .negate: output = lhs.negated()
                }
                return try output.exactDecimal()
            }
            return try numericResult(result, scale: scale)
        default: throw mismatch("numeric result declaration")
        }
    }

    private static func functionValue<Operands: RandomAccessCollection>(_ name: String, _ values: Operands, type: QueryColumnType,
                                      workMeter: DatabaseWorkMeter) throws -> QueryValue where Operands.Element == QueryValue, Operands.Index == Int {
        if name == "width_bucket" { return try widthBucket(values, workMeter: workMeter) }
        if name == "scale" || name == "min_scale" {
            guard values.count == 1 else { throw mismatch(name) }
            let value = try numeric(values[0])
            return .primitive(.int32(name == "scale" ? value.displayScale : max(0, value.value.scale)))
        }
        switch kind(type) {
        case .smallint, .integer, .bigint:
            guard !values.isEmpty else { throw mismatch(name) }
            let left = try integer(values[0])
            switch name {
            case "abs":
                guard values.count == 1, left != Int64.min else { throw DatabaseExpressionEvaluationError.numericOverflow }
                return try integerResult(left < 0 ? -left : left, type: type)
            case "gcd", "lcm":
                guard values.count == 2 else { throw mismatch(name) }
                let right = try integer(values[1])
                var a = left.magnitude, b = right.magnitude
                while b != 0 {
                    try workMeter.consume(at: .expressionEvaluation)
                    (a, b) = (b, a % b)
                }
                if name == "lcm" {
                    if a == 0 { return try integerResult(0, type: type) }
                    let product = (left.magnitude / a).multipliedReportingOverflow(by: right.magnitude)
                    guard !product.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
                    a = product.partialValue
                }
                guard let result = Int64(exactly: a) else { throw DatabaseExpressionEvaluationError.numericOverflow }
                return try integerResult(result, type: type)
            case "mod":
                // MOD is the function form of the integer % operator.
                guard values.count == 2 else { throw mismatch(name) }
                return try arithmetic(.modulo, values, type: type, workMeter: workMeter)
            default: throw DatabaseExpressionEvaluationError.unsupportedFunction(name)
            }
        case .decimal:
            return try decimalFunction(name, values, workMeter: workMeter)
        case .real, .doublePrecision:
            return try floatingFunction(name, values, type: type)
        default: throw mismatch(name)
        }
    }

    private static func decimalFunction<Operands: RandomAccessCollection>(_ name: String, _ values: Operands,
                                        workMeter: DatabaseWorkMeter) throws -> QueryValue where Operands.Element == QueryValue, Operands.Index == Int {
        guard !values.isEmpty else { throw mismatch(name) }
        let left = try numeric(values[0])
        if ["sqrt", "exp", "ln", "log", "log10", "power"].contains(name) {
            let reservation = try workMeter.reserveIntermediate(bytes: UInt64(values.count * MemoryLayout<QueryNumericValue>.stride), at: .expressionEvaluation)
            defer { reservation.release() }
            let operands = try values.map(numeric)
            return .numeric(try CanonicalSQLDecimalMath.evaluate(function: name.uppercased(), operands: operands, workMeter: workMeter))
        }
        let scale: Int32
        let result: ExactDecimal
        switch name {
        case "abs":
            guard values.count == 1 else { throw mismatch(name) }
            scale = left.displayScale
            result = try decimalOperation(workMeter: workMeter) { try left.value.magnitude() }
        case "sign":
            guard values.count == 1 else { throw mismatch(name) }
            scale = 0
            result = ExactDecimal(coefficient: left.value.coefficient == 0 ? 0 : (left.value.coefficient < 0 ? -1 : 1), scale: 0)
        case "ceil", "ceiling", "floor", "round", "trunc":
            guard values.count == 1 || ((name == "round" || name == "trunc") && values.count == 2) else { throw mismatch(name) }
            let requested = values.count == 2 ? try integer(values[1]) : 0
            // PG clamps arithmetic rounding to a useful result range. Negative scale does not become dscale.
            let minimum: Int64 = name == "trunc" ? -131072 : -131073
            let target = Int32(max(minimum, min(16383, requested)))
            scale = max(0, target)
            let roundScale = target
            let rule: DecimalRoundingRule = name == "floor" ? .down : (name == "ceil" || name == "ceiling") ? .up : name == "trunc" ? .towardZero : .toNearestOrAwayFromZero
            result = try decimalOperation(workMeter: workMeter) {
                try CanonicalSQLDecimalWorkspaceValue(left.value, workMeter: workMeter)
                    .rounded(toScale: roundScale, rounding: rule).exactDecimal()
            }
        case "trim_scale":
            guard values.count == 1 else { throw mismatch(name) }
            scale = max(0, left.value.scale)
            result = left.value
        case "mod", "div", "gcd", "lcm":
            guard values.count == 2 else { throw mismatch(name) }
            let right = try numeric(values[1])
            scale = name == "div" ? 0 : max(left.displayScale, right.displayScale)
            result = try decimalOperation(workMeter: workMeter) {
                let lhs = try CanonicalSQLDecimalWorkspaceValue(left.value, workMeter: workMeter)
                let rhs = try CanonicalSQLDecimalWorkspaceValue(right.value, workMeter: workMeter)
                if name == "div" { return try lhs.divided(by: rhs, toScale: 0, rounding: .towardZero).exactDecimal() }
                if name == "mod" { return try lhs.remainder(dividingBy: rhs).exactDecimal() }
                var a = lhs.sign < 0 ? lhs.negated() : lhs
                var b = rhs.sign < 0 ? rhs.negated() : rhs
                while !b.isZero {
                    try workMeter.consume(at: .expressionEvaluation)
                    let rest = try a.remainder(dividingBy: b)
                    a = b; b = rest
                }
                if name == "gcd" || a.isZero { return try a.exactDecimal() }
                let divided = try lhs.divided(by: a, toScale: Int32(max(0, lhs.scale - a.scale)), rounding: .towardZero)
                let product = try divided.multiplied(by: rhs)
                return try (product.sign < 0 ? product.negated() : product).exactDecimal()
            }
        default: throw DatabaseExpressionEvaluationError.unsupportedFunction(name)
        }
        return try numericResult(result, scale: scale)
    }

    private static func floatingFunction<Operands: RandomAccessCollection>(_ name: String, _ values: Operands, type: QueryColumnType) throws -> QueryValue where Operands.Element == QueryValue, Operands.Index == Int {
        guard values.count == (name == "pi" ? 0 : ["power", "atan2"].contains(name) ? 2 : 1) else { throw mismatch(name) }
        let x = values.isEmpty ? 0 : try double(values[0])
        let y = values.count == 2 ? try double(values[1]) : 0
        let result: Double
        switch name {
        case "pi": result = Double.pi
        case "abs": result = abs(x)
        case "sign": result = x == 0 ? 0 : (x < 0 ? -1 : 1)
        case "ceil", "ceiling": result = x.rounded(.up)
        case "floor": result = x.rounded(.down)
        case "round": result = x.rounded(.toNearestOrEven)
        case "trunc": result = x.rounded(.towardZero)
        case "sqrt":
            guard x >= 0 else { throw domain(name) }
            result = x.squareRoot()
        case "cbrt": result = DatabaseMath.cubeRoot(x)
        case "exp": result = DatabaseMath.exponential(x)
        case "ln":
            guard x > 0 else { throw domain(name) }
            result = DatabaseMath.naturalLogarithm(x)
        case "log", "log10":
            guard x > 0 else { throw domain(name) }
            result = DatabaseMath.commonLogarithm(x)
        case "power":
            guard !(x == 0 && y < 0), !(x < 0 && y.rounded(.towardZero) != y) else { throw domain(name) }
            result = DatabaseMath.power(x, y)
        case "degrees": result = x * (180 / Double.pi)
        case "radians": result = x * (Double.pi / 180)
        case "sin": result = DatabaseMath.sine(x)
        case "cos": result = DatabaseMath.cosine(x)
        case "tan": result = DatabaseMath.tangent(x)
        case "asin":
            guard (-1...1).contains(x) else { throw domain(name) }
            result = DatabaseMath.arcSine(x)
        case "acos":
            guard (-1...1).contains(x) else { throw domain(name) }
            result = DatabaseMath.arcCosine(x)
        case "atan": result = DatabaseMath.arcTangent(x)
        case "atan2": result = DatabaseMath.arcTangent(y: x, x: y)
        default: throw DatabaseExpressionEvaluationError.unsupportedFunction(name)
        }
        let underflow = result == 0 && (name == "exp" || (name == "power" && x != 0))
        try finiteResult(result, underflow: underflow)
        if kind(type) == .real {
            let narrowed = Float(result)
            try finiteResult(Double(narrowed), underflow: narrowed == 0 && result != 0)
            return .primitive(.float32(narrowed))
        }
        return .primitive(.float64(result))
    }

    private static func widthBucket<Operands: RandomAccessCollection>(_ values: Operands, workMeter: DatabaseWorkMeter) throws -> QueryValue where Operands.Element == QueryValue, Operands.Index == Int {
        if values.count == 2 {
            guard case .array(let thresholds) = values[1], thresholds.dimensions.count <= 1 else { throw domain("width_bucket") }
            for threshold in thresholds.elements {
                try workMeter.consume(at: .expressionEvaluation)
                guard !threshold.isNull else { throw domain("width_bucket") }
            }
            var low = 0, high = thresholds.elements.count
            while low < high {
                try workMeter.consume(at: .expressionEvaluation)
                let middle = low + (high - low) / 2
                let comparison = try compare(values[0], thresholds.elements[middle], workMeter: workMeter)
                if comparison >= 0 { low = middle + 1 } else { high = middle }
            }
            guard let bucket = Int32(exactly: low) else { throw DatabaseExpressionEvaluationError.numericOverflow }
            return .primitive(.int32(bucket))
        }
        guard values.count == 4 else { throw mismatch("width_bucket") }
        let count = try integer(values[3])
        guard count > 0, count <= Int32.max else { throw domain("width_bucket") }
        let order = try compare(values[1], values[2], workMeter: workMeter)
        guard order != 0 else { throw domain("width_bucket") }
        let first = try compare(values[0], values[1], workMeter: workMeter), last = try compare(values[0], values[2], workMeter: workMeter)
        if (order < 0 && first < 0) || (order > 0 && first > 0) { return .primitive(.int32(0)) }
        if (order < 0 && last >= 0) || (order > 0 && last <= 0) {
            return try integerResult(count + 1, type: .sql(.integer))
        }
        let isNumeric: Bool
        switch values[0] { case .numeric, .primitive(.decimal): isNumeric = true; default: isNumeric = false }
        if isNumeric {
            let x = try numeric(values[0]).value, a = try numeric(values[1]).value, b = try numeric(values[2]).value
            return try decimalOperation(workMeter: workMeter) {
                let operand = try CanonicalSQLDecimalWorkspaceValue(x, workMeter: workMeter)
                let lower = try CanonicalSQLDecimalWorkspaceValue(a, workMeter: workMeter)
                let upper = try CanonicalSQLDecimalWorkspaceValue(b, workMeter: workMeter)
                let delta = try operand.subtracting(lower)
                let span = try upper.subtracting(lower)
                let multiplier = try CanonicalSQLDecimalWorkspaceValue(ExactDecimal(coefficient: Int128(count), scale: 0), workMeter: workMeter)
                let numerator = try delta.multiplied(by: multiplier)
                let quotient = try numerator.divided(by: span, toScale: 0, rounding: .towardZero)
                return try integerResult(integerFromDecimal(quotient.exactDecimal()) + 1, type: .sql(.integer))
            }
        }
        let x = try double(values[0]), a = try double(values[1]), b = try double(values[2])
        // Halving avoids overflow of opposite-signed bounds without changing the ratio.
        let ratio = (b - a).isFinite ? (x - a) / (b - a) : (x / 2 - a / 2) / (b / 2 - a / 2)
        guard let bucket = Int64(exactly: (ratio * Double(count)).rounded(.down)), bucket < count else {
            // Rounding at the upper inclusive representable neighbor is capped to the last bucket.
            return try integerResult(count, type: .sql(.integer))
        }
        return try integerResult(bucket + 1, type: .sql(.integer))
    }

    static func kind(_ type: QueryColumnType) -> DataType? {
        switch type {
        case .sql(let value): return value
        case .primitive(let value, false):
            switch value {
            case .int16: return .smallint
            case .int32: return .integer
            case .int64: return .bigint
            case .float32: return .real
            case .float64: return .doublePrecision
            case .decimal: return .decimal(precision: nil, scale: nil)
            case .bool: return .boolean
            default: return nil
            }
        default: return nil
        }
    }

    static func integer(_ value: QueryValue) throws -> Int64 {
        switch value {
        case .primitive(.int8(let x)): return Int64(x)
        case .primitive(.int16(let x)): return Int64(x)
        case .primitive(.int32(let x)): return Int64(x)
        case .primitive(.int64(let x)): return x
        case .primitive(.uint8(let x)): return Int64(x)
        case .primitive(.uint16(let x)): return Int64(x)
        case .primitive(.uint32(let x)): return Int64(x)
        case .primitive(.uint64(let x)):
            guard let result = Int64(exactly: x) else { throw DatabaseExpressionEvaluationError.numericOverflow }
            return result
        default: throw mismatch("selected integer carrier")
        }
    }

    static func integerResult(_ value: Int64, type: QueryColumnType) throws -> QueryValue {
        switch kind(type) {
        case .smallint:
            guard let result = Int16(exactly: value) else { throw DatabaseExpressionEvaluationError.numericOverflow }
            return .primitive(.int16(result))
        case .integer:
            guard let result = Int32(exactly: value) else { throw DatabaseExpressionEvaluationError.numericOverflow }
            return .primitive(.int32(result))
        case .bigint: return .primitive(.int64(value))
        default: throw mismatch("selected integer result")
        }
    }

    static func numeric(_ value: QueryValue) throws -> QueryNumericValue {
        switch value {
        case .numeric(let x): return x
        case .primitive(.decimal(let x)): return try QueryNumericValue(value: x, displayScale: max(0, x.scale))
        default: throw mismatch("selected numeric carrier")
        }
    }

    static func real(_ value: QueryValue) throws -> Float {
        guard case .primitive(.float32(let x)) = value else { throw mismatch("selected real carrier") }
        guard x.isFinite else { throw DatabaseExpressionEvaluationError.nonFiniteValue }
        return x
    }

    static func double(_ value: QueryValue) throws -> Double {
        let result: Double
        switch value {
        case .primitive(.float32(let x)): result = Double(x)
        case .primitive(.float64(let x)): result = x
        default: throw mismatch("selected floating carrier")
        }
        guard result.isFinite else { throw DatabaseExpressionEvaluationError.nonFiniteValue }
        return result
    }

    static func numericResult(_ value: ExactDecimal, scale: Int32) throws -> QueryValue {
        do { return .numeric(try QueryNumericValue(value: value, displayScale: scale)) }
        catch { throw DatabaseExpressionEvaluationError.numericOverflow }
    }

    static func integerFromDecimal(_ value: ExactDecimal) throws -> Int64 {
        guard value.scale <= 0 else { throw mismatch("rounded integer numeric") }
        var integer = value.coefficient
        guard integer != 0 else { return 0 }
        guard value.scale >= -19 else { throw DatabaseExpressionEvaluationError.numericOverflow }
        for _ in 0..<(-Int64(value.scale)) {
            let result = integer.multipliedReportingOverflow(by: 10)
            guard !result.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            integer = result.partialValue
        }
        guard let result = Int64(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }
        return result
    }

    static func finiteResult(_ value: Double, underflow: Bool = false) throws {
        guard value.isFinite else { throw DatabaseExpressionEvaluationError.numericOverflow }
        guard !underflow else { throw DatabaseExpressionEvaluationError.numericUnderflow }
    }

    static func decimalOperation<Result>(workMeter: DatabaseWorkMeter, _ body: () throws -> Result) throws -> Result {
        try workMeter.checkpoint(at: .expressionEvaluation)
        do {
            let result = try body()
            try workMeter.checkpoint(at: .expressionEvaluation)
            return result
        } catch let error as ExactDecimalError {
            switch error {
            case .divisionByZero: throw DatabaseExpressionEvaluationError.divisionByZero
            case .numericOverflow: throw DatabaseExpressionEvaluationError.numericOverflow
            case .inexactResult: throw DatabaseExpressionEvaluationError.inexactDecimalResult
            }
        }
    }

    private static func compare(_ left: QueryValue, _ right: QueryValue, workMeter: DatabaseWorkMeter) throws -> Int {
        func meterComparison(_ value: QueryValue) throws {
            switch value {
            case .primitive(.string(let string)):
                try DatabaseByteProcessingMeter.consume(byteCount: string.utf8.count, workMeter: workMeter, stage: .expressionEvaluation)
            case .primitive(.bytes(let bytes)):
                try DatabaseByteProcessingMeter.consume(byteCount: bytes.count, workMeter: workMeter, stage: .expressionEvaluation)
            default: break
            }
        }
        try meterComparison(left)
        try meterComparison(right)
        func primitive(_ value: QueryValue) throws -> FieldValue {
            switch value {
            case .numeric(let numeric): return .decimal(numeric.value)
            case .primitive(let primitive): return primitive
            default: throw mismatch("width_bucket comparable scalar")
            }
        }
        switch try FieldValueComparator.compare(primitive(left), primitive(right)) {
        case .lessThan: return -1
        case .equal: return 0
        case .greaterThan: return 1
        }
    }

    static func mismatch(_ operation: String) -> DatabaseExpressionEvaluationError { .typeMismatch(operation: operation) }
    static func domain(_ operation: String) -> DatabaseExpressionEvaluationError { .numericDomain(operation: operation) }
}
