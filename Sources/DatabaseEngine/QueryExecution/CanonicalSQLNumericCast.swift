import DatabaseTypes
import DatabaseKit
import DatabaseMath

/// SQL scalar conversion policy, invoked only after the cast signature is bound.
enum CanonicalSQLNumericCast {
    static func evaluate(_ value: QueryValue, from source: QueryColumnType, to target: DataType,
                         workMeter: DatabaseWorkMeter) throws -> QueryValue {
        try validate(target)
        if case .primitive(.float32(let x)) = value, !x.isFinite { throw DatabaseExpressionEvaluationError.nonFiniteValue }
        if case .primitive(.float64(let x)) = value, !x.isFinite { throw DatabaseExpressionEvaluationError.nonFiniteValue }
        guard QueryValueValidator.accepts(value, type: source) else {
            throw CanonicalSQLNumericEvaluator.mismatch("selected CAST input")
        }
        if value.isNull { return .primitive(.null) }
        let sourceType = CanonicalSQLNumericEvaluator.kind(source)
        if sourceType == nil, case .primitive(let kind, false) = source {
            switch kind {
            case .int8, .uint8, .uint16, .uint32, .uint64:
                let numericType = QueryColumnType.sql(.decimal(precision: nil, scale: nil))
                let exact = try CanonicalQueryTypeBinder.normalize(value, to: numericType)
                return try evaluate(exact, from: numericType, to: target, workMeter: workMeter)
            default: break
            }
        }
        if target == .boolean {
            if sourceType == .boolean { return value }
            guard sourceType == .smallint || sourceType == .integer || sourceType == .bigint else {
                throw CanonicalSQLNumericEvaluator.mismatch("CAST to boolean")
            }
            return .primitive(.bool(try CanonicalSQLNumericEvaluator.integer(value) != 0))
        }
        switch target {
        case .smallint, .integer, .bigint:
            let result: Int64
            switch sourceType {
            case .smallint, .integer, .bigint: result = try CanonicalSQLNumericEvaluator.integer(value)
            case .boolean:
                guard case .primitive(.bool(let flag)) = value else { throw CanonicalSQLNumericEvaluator.mismatch("boolean CAST") }
                result = flag ? 1 : 0
            case .decimal:
                let decimal = try CanonicalSQLNumericEvaluator.numeric(value).value
                result = try CanonicalSQLNumericEvaluator.decimalOperation(workMeter: workMeter) {
                    try CanonicalSQLNumericEvaluator.integerFromDecimal(
                        CanonicalSQLDecimalWorkspaceValue(decimal, workMeter: workMeter)
                            .rounded(toScale: 0).exactDecimal())
                }
            case .real, .doublePrecision:
                let rounded = try CanonicalSQLNumericEvaluator.double(value).rounded(.toNearestOrEven)
                guard let integer = Int64(exactly: rounded) else { throw DatabaseExpressionEvaluationError.numericOverflow }
                result = integer
            default: throw CanonicalSQLNumericEvaluator.mismatch("CAST to integer")
            }
            return try CanonicalSQLNumericEvaluator.integerResult(result, type: .sql(target))
        case .decimal(let precision, let requestedScale):
            let numeric: QueryNumericValue
            switch sourceType {
            case .decimal: numeric = try CanonicalSQLNumericEvaluator.numeric(value)
            case .smallint, .integer, .bigint:
                numeric = try QueryNumericValue(value: ExactDecimal(coefficient: Int128(CanonicalSQLNumericEvaluator.integer(value)), scale: 0), displayScale: 0)
            case .real, .doublePrecision:
                numeric = try fromFloating(value, significantDigits: sourceType == .real ? 6 : 15, workMeter: workMeter)
            default: throw CanonicalSQLNumericEvaluator.mismatch("CAST to numeric")
            }
            guard let precision else { return .numeric(numeric) }
            let scale = Int32(requestedScale ?? 0)
            let rounded = try CanonicalSQLNumericEvaluator.decimalOperation(workMeter: workMeter) {
                try CanonicalSQLDecimalWorkspaceValue(numeric.value, workMeter: workMeter).rounded(toScale: scale).exactDecimal()
            }
            let maximum = ExactDecimal(coefficient: 1, scale: Int32((requestedScale ?? 0) - precision))
            guard try rounded.magnitude() < maximum else { throw DatabaseExpressionEvaluationError.numericOverflow }
            return try CanonicalSQLNumericEvaluator.numericResult(rounded, scale: max(0, scale))
        case .real, .doublePrecision:
            let result: QueryValue
            switch sourceType {
            case .smallint, .integer, .bigint:
                let integer = try CanonicalSQLNumericEvaluator.integer(value)
                result = target == .real ? .primitive(.float32(Float(integer))) : .primitive(.float64(Double(integer)))
            case .decimal:
                let decimal = try CanonicalSQLNumericEvaluator.numeric(value).value
                result = try withScientificText(decimal, workMeter: workMeter) { text in
                    if target == .real {
                        guard let floating = Float(text) else { throw DatabaseExpressionEvaluationError.numericOverflow }
                        try CanonicalSQLNumericEvaluator.finiteResult(Double(floating), underflow: floating == 0 && decimal.coefficient != 0)
                        return .primitive(.float32(floating))
                    }
                    guard let floating = Double(text) else { throw DatabaseExpressionEvaluationError.numericOverflow }
                    try CanonicalSQLNumericEvaluator.finiteResult(floating, underflow: floating == 0 && decimal.coefficient != 0)
                    return .primitive(.float64(floating))
                }
            case .real, .doublePrecision:
                let floating = try CanonicalSQLNumericEvaluator.double(value)
                if target == .real {
                    let narrowed = Float(floating)
                    try CanonicalSQLNumericEvaluator.finiteResult(Double(narrowed), underflow: narrowed == 0 && floating != 0)
                    result = .primitive(.float32(narrowed))
                } else { result = .primitive(.float64(floating)) }
            default: throw CanonicalSQLNumericEvaluator.mismatch("CAST to floating")
            }
            return result
        default: throw CanonicalSQLNumericEvaluator.mismatch("numeric CAST target")
        }
    }

    private static func withScientificText(_ value: ExactDecimal, workMeter: DatabaseWorkMeter,
                                           _ body: (String) throws -> QueryValue) throws -> QueryValue {
        let exponent = -Int64(value.scale)
        var magnitude = value.coefficient.magnitude, exponentMagnitude = exponent.magnitude
        var coefficientDigits = 1, exponentDigits = 1
        while magnitude >= 10 { magnitude /= 10; coefficientDigits += 1 }
        while exponentMagnitude >= 10 { exponentMagnitude /= 10; exponentDigits += 1 }
        let coefficientStart = value.coefficient < 0 ? 1 : 0
        let marker = coefficientStart + coefficientDigits
        let exponentStart = marker + 1 + (exponent < 0 ? 1 : 0)
        let count = exponentStart + exponentDigits
        // One initializer creates one bounded ASCII owner; no intermediate Strings.
        // Use the existing canonical byte-buffer capacity/owner units, plus its String slot.
        let layout = try DatabaseRetainedArrayLayout.forElement(UInt8.self)
        let growth = try layout.growth(from: 0, toFit: count + 1)
        let reservation = try workMeter.reserveIntermediate(bytes: growth.additionalByteCount
            + layout.sharedOwnerByteCount + UInt64(MemoryLayout<String>.stride), at: .expressionEvaluation)
        defer { reservation.release() }
        try DatabaseByteProcessingMeter.consume(byteCount: count, workMeter: workMeter, stage: .expressionEvaluation)
        let text = String(unsafeUninitializedCapacity: count) { output in
            var coefficient = value.coefficient.magnitude
            if coefficientStart != 0 { output[0] = 45 }
            for index in stride(from: marker - 1, through: coefficientStart, by: -1) {
                output[index] = UInt8(coefficient % 10) + 48; coefficient /= 10
            }
            output[marker] = 101
            if exponent < 0 { output[marker + 1] = 45 }
            var magnitude = exponent.magnitude
            for index in stride(from: count - 1, through: exponentStart, by: -1) {
                output[index] = UInt8(magnitude % 10) + 48; magnitude /= 10
            }
            return count
        }
        return try body(text)
    }

    private static func validate(_ target: DataType) throws {
        if case .decimal(let precision, let scale) = target {
            guard precision.map({ (1...38).contains($0) }) ?? (scale == nil),
                  scale.map({ (-1000...1000).contains($0) }) ?? true else {
                throw CanonicalSQLNumericEvaluator.mismatch("numeric CAST typmod")
            }
        }
    }

    private static func fromFloating(_ value: QueryValue, significantDigits: Int,
                                     workMeter: DatabaseWorkMeter) throws -> QueryNumericValue {
        let floating = try CanonicalSQLNumericEvaluator.double(value)
        try workMeter.consume(128, at: .expressionEvaluation)
        return try DatabaseMath.withDecimalFormatting(of: floating, significantDigits: significantDigits) { bytes in
            var index = 0, negative = false
            if bytes[0] == 45 { negative = true; index += 1 }
            var coefficient: Int128 = 0, fraction: Int32 = 0, point = false, exponent: Int32 = 0
            while index < bytes.count, bytes[index] != 101, bytes[index] != 69 {
                let byte = bytes[index]; index += 1
                if byte == 46 { point = true; continue }
                guard (48...57).contains(byte) else { throw CanonicalSQLNumericEvaluator.mismatch("floating decimal format") }
                coefficient = coefficient * 10 + Int128(byte - 48)
                if point { fraction += 1 }
            }
            if index < bytes.count {
                index += 1
                var exponentNegative = false
                if bytes[index] == 45 || bytes[index] == 43 { exponentNegative = bytes[index] == 45; index += 1 }
                while index < bytes.count { exponent = exponent * 10 + Int32(bytes[index] - 48); index += 1 }
                if exponentNegative { exponent = -exponent }
            }
            let scale = fraction - exponent
            return try QueryNumericValue(value: ExactDecimal(coefficient: negative ? -coefficient : coefficient, scale: scale),
                                         displayScale: max(0, scale))
        }
    }
}
