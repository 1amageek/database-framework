import DatabaseKit
import DatabaseTypes

/// Selected SQL type output; formatted temporaries live only through the borrow.
enum CanonicalSQLScalarText {
    static func withText<Result>(of value: QueryValue, type: QueryColumnType,
        preservingCharacterPadding: Bool, temporalEnvironment: SQLTemporalEnvironment? = nil,
        limits: QueryValueLimits? = nil, workMeter: DatabaseWorkMeter,
        _ body: (borrowing Substring) throws -> Result) throws -> Result {
        guard QueryValueValidator.accepts(value, type: type), !value.isNull else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected scalar text input")
        }
        try workMeter.checkpoint(at: .expressionEvaluation)
        if CanonicalQueryTypeBinder.isCollection(type) {
            let result = try CanonicalSQLScalarCast.evaluateCollection(value, from: type, to: .sql(.text), context: .explicit,
                temporalEnvironment: temporalEnvironment, workMeter: workMeter,
                limits: limits ?? CanonicalSQLScalarCast.collectionLimits(workMeter: workMeter))
            guard case .primitive(.string(let text)) = result.value else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "collection text output")
            }
            return try withExtendedLifetime(result) { try body(text[...]) }
        }
        if CanonicalQueryTypeBinder.temporalType(type) != nil {
            guard let temporalEnvironment else { throw DatabaseExpressionEvaluationError.missingTemporalEnvironment }
            return try SQLTemporalFormatting.withText(of: value, type: type, environment: temporalEnvironment,
                workMeter: workMeter, body)
        }
        if case .primitive(.string(let text)) = value {
            try charge(text.utf8.count, workMeter)
            var view = text[...]
            if !preservingCharacterPadding, case .sql(.char) = type {
                var end = view.unicodeScalars.endIndex
                var examined = 0
                while end != view.unicodeScalars.startIndex {
                    let previous = view.unicodeScalars.index(before: end)
                    guard view.unicodeScalars[previous].value == 32 else { break }
                    end = previous
                    try checkpoint(examined, workMeter); examined += 1
                }
                view = view[..<end]
            }
            return try body(view)
        }
        switch value {
        case .numeric(let numeric):
            return try withNumeric(numeric, workMeter: workMeter, body)
        case .primitive(let primitive):
            switch primitive {
            case .bool(let flag): return try body(flag ? "t"[...] : "f"[...])
            case .int8(let number): return try withInteger(Int128(number), workMeter, body)
            case .int16(let number): return try withInteger(Int128(number), workMeter, body)
            case .int32(let number): return try withInteger(Int128(number), workMeter, body)
            case .int64(let number): return try withInteger(Int128(number), workMeter, body)
            case .uint8(let number): return try withInteger(Int128(number), workMeter, body)
            case .uint16(let number): return try withInteger(Int128(number), workMeter, body)
            case .uint32(let number): return try withInteger(Int128(number), workMeter, body)
            case .uint64(let number): return try withInteger(Int128(number), workMeter, body)
            case .decimal(let decimal):
                return try withNumeric(QueryNumericValue(value: decimal, displayScale: max(0, decimal.scale)),
                    workMeter: workMeter, body)
            case .float32(let number):
                guard number.isFinite else { throw DatabaseExpressionEvaluationError.nonFiniteValue }
                return try withFloating(bits: UInt64(number.bitPattern), fractionBits: 23,
                    exponentBits: 8, bias: 127, precision: 9, fixedLimit: 6, workMeter: workMeter, body)
            case .float64(let number):
                guard number.isFinite else { throw DatabaseExpressionEvaluationError.nonFiniteValue }
                return try withFloating(bits: number.bitPattern, fractionBits: 52,
                    exponentBits: 11, bias: 1023, precision: 17, fixedLimit: 15, workMeter: workMeter, body)
            case .uuid(let uuid):
                return try withTemporaryText(count: 36, workMeter: workMeter, make: {
                    try makeText(count: 36, workMeter: workMeter) { output in
                        var position = 0
                        for index in 0..<16 {
                            if index == 4 || index == 6 || index == 8 || index == 10 {
                                output[position] = 45; position += 1
                            }
                            let byte = uuid[index]
                            output[position] = hexDigit(byte >> 4)
                            output[position + 1] = hexDigit(byte & 15); position += 2
                        }
                    }
                }, body)
            case .bytes(let bytes):
                let count = try checkedCount(UInt64(bytes.count), multiplier: 2, addition: 2)
                return try withTemporaryText(count: count, workMeter: workMeter, make: {
                    try makeText(count: count, workMeter: workMeter) { output in
                        output[0] = 92; output[1] = 120
                        try bytes.withUnsafeBytes { input in
                            for index in input.indices {
                                try checkpoint(index, workMeter)
                                output[2 + index * 2] = hexDigit(input[index] >> 4)
                                output[3 + index * 2] = hexDigit(input[index] & 15)
                            }
                        }
                    }
                }, body)
            default: break
            }
        default: break
        }
        throw DatabaseExpressionEvaluationError.unsupportedExpression("selected scalar type output")
    }

    static func produceText(count: UInt64, workMeter: DatabaseWorkMeter,
        _ make: () throws -> String) throws -> DatabaseQueryScopedFieldValue {
        let footprint = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint(retainedByteCount: count)
        return try .producing(maximumFootprint: footprint, workMeter: workMeter, stage: .expressionEvaluation) {
            .string(try make())
        }
    }

    static func makeText(count: UInt64, workMeter: DatabaseWorkMeter,
        _ fill: (UnsafeMutableBufferPointer<UInt8>) throws -> Void) throws -> String {
        guard let count = Int(exactly: count) else { throw DatabaseIntermediateFootprintError.byteAdditionOverflow(left: count, right: 0) }
        try charge(count, workMeter)
        var failure: (any Error)?
        // The initializer owns all bytes; its pointer is lent synchronously and never escapes.
        let text = String(unsafeUninitializedCapacity: count) { output in
            do { try fill(output); return count }
            catch { failure = error; return 0 }
        }
        if let failure { throw failure }
        return text
    }

    static func checkedCount(_ count: UInt64, multiplier: UInt64 = 1, addition: UInt64 = 0) throws -> UInt64 {
        try DatabaseIntermediateFootprint(bytes: count).multiplied(by: multiplier)
            .adding(DatabaseIntermediateFootprint(bytes: addition)).bytes
    }

    static func charge(_ count: Int, _ meter: DatabaseWorkMeter) throws {
        try DatabaseByteProcessingMeter.consume(byteCount: count, workMeter: meter, stage: .expressionEvaluation)
    }

    static func checkpoint(_ offset: Int, _ meter: DatabaseWorkMeter) throws {
        if offset & 255 == 0 { try meter.checkpoint(at: .expressionEvaluation) }
    }

    static func hexDigit(_ digit: UInt8) -> UInt8 { digit < 10 ? digit + 48 : digit + 87 }

    private static func withTemporaryText<Result>(count: UInt64, workMeter: DatabaseWorkMeter,
        make: () throws -> String, _ body: (borrowing Substring) throws -> Result) throws -> Result {
        let footprint = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint(retainedByteCount: count)
        let reservation = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .expressionEvaluation)
        defer { reservation.release() }
        let text = try make()
        return try body(text[...])
    }

    private static func withInteger<Result>(_ value: Int128, _ meter: DatabaseWorkMeter,
        _ body: (borrowing Substring) throws -> Result) throws -> Result {
        try withNumeric(QueryNumericValue(value: ExactDecimal(coefficient: value, scale: 0), displayScale: 0),
            workMeter: meter, body)
    }

    private static func withNumeric<Result>(_ value: QueryNumericValue, workMeter: DatabaseWorkMeter,
        _ body: (borrowing Substring) throws -> Result) throws -> Result {
        let decimal = value.value
        var magnitude = decimal.coefficient.magnitude, digits = 1
        while magnitude >= 10 { magnitude /= 10; digits += 1 }
        let integral = max(Int64(digits) - Int64(decimal.scale), 1)
        let fractional = UInt64(value.displayScale)
        let sign: UInt64 = decimal.coefficient < 0 ? 1 : 0
        let count = try checkedCount(UInt64(integral), addition: sign + (fractional > 0 ? fractional + 1 : 0))
        return try withTemporaryText(count: count, workMeter: workMeter, make: {
            try makeText(count: count, workMeter: workMeter) { output in
                for index in output.indices {
                    try checkpoint(index, workMeter)
                    output[index] = 48
                }
                if sign != 0 { output[0] = 45 }
                if fractional > 0 { output[Int(sign) + Int(integral)] = 46 }
                var coefficient = decimal.coefficient.magnitude
                for offset in 0..<digits {
                    let power = Int64(offset) - Int64(decimal.scale)
                    let index = power >= 0 ? Int(sign) + Int(integral - 1 - power) :
                        Int(sign) + Int(integral) + 1 + Int(-power - 1)
                    output[index] = UInt8(coefficient % 10) + 48; coefficient /= 10
                }
            }
        }, body)
    }

    private static func binaryPower(_ exponent: Int, meter: DatabaseWorkMeter) throws -> CanonicalSQLDecimalWorkspaceValue {
        var power = try CanonicalSQLDecimalWorkspaceValue(
            ExactDecimal(coefficient: exponent < 0 ? 5 : 2, scale: exponent < 0 ? 1 : 0), workMeter: meter)
        var result = try CanonicalSQLDecimalWorkspaceValue(ExactDecimal(coefficient: 1, scale: 0), workMeter: meter)
        var remaining = exponent.magnitude
        while remaining != 0 {
            try meter.checkpoint(at: .expressionEvaluation)
            if remaining & 1 != 0 { result = try result.multiplied(by: power) }
            remaining >>= 1
            if remaining != 0 { power = try power.multiplied(by: power) }
        }
        return result
    }

    private static func withFloating<Result>(bits: UInt64, fractionBits: Int, exponentBits: Int,
        bias: Int, precision: Int, fixedLimit: Int, workMeter: DatabaseWorkMeter,
        _ body: (borrowing Substring) throws -> Result) throws -> Result {
        let negative = bits >> (fractionBits + exponentBits) != 0
        let fraction = bits & ((UInt64(1) << fractionBits) - 1)
        let exponent = Int((bits >> fractionBits) & ((UInt64(1) << exponentBits) - 1))
        if exponent == 0 && fraction == 0 { return try body(negative ? "-0"[...] : "0"[...]) }
        let mantissa = fraction | (exponent == 0 ? 0 : UInt64(1) << fractionBits)
        let binaryExponent = (exponent == 0 ? 1 : exponent) - bias - fractionBits
        let coefficient = try CanonicalSQLDecimalWorkspaceValue(ExactDecimal(coefficient: Int128(mantissa), scale: 0), workMeter: workMeter)
        let exact = try coefficient.multiplied(by: binaryPower(binaryExponent, meter: workMeter))
        let upper = try exact.adding(binaryPower(binaryExponent - 1, meter: workMeter))
        let lower = try exact.subtracting(binaryPower(binaryExponent - 1 - (fraction == 0 && exponent > 1 ? 1 : 0), meter: workMeter))
        let weight = exact.decimalDigitCount - exact.scale - 1
        for digits in 1...precision {
            let scale = Int32(Int64(digits) - weight - 1)
            let unit = try CanonicalSQLDecimalWorkspaceValue(ExactDecimal(coefficient: 1, scale: scale), workMeter: workMeter)
            var candidate = try exact.rounded(toScale: scale, rounding: .towardZero)
            let remainder = try exact.subtracting(candidate)
            let half = try CanonicalSQLDecimalWorkspaceValue(ExactDecimal(coefficient: 5, scale: scale + 1), workMeter: workMeter)
            let comparison = try remainder.comparison(to: half)
            let truncated = try candidate.exactDecimal()
            let odd = truncated.scale == scale && truncated.coefficient & 1 != 0
            if comparison > 0 || (comparison == 0 && odd) { candidate = try candidate.adding(unit) }
            // REL_18_6 d2s/f2s excludes binary midpoint endpoints (acceptBounds=false).
            if try candidate.comparison(to: lower) <= 0 { candidate = try candidate.adding(unit) }
            if try candidate.comparison(to: upper) >= 0 { candidate = try candidate.subtracting(unit) }
            if try candidate.comparison(to: lower) > 0, try candidate.comparison(to: upper) < 0 {
                let decimal = try candidate.exactDecimal()
                return try withFloatingDecimal(decimal, negative: negative, fixedLimit: fixedLimit,
                    workMeter: workMeter, body)
            }
        }
        throw DatabaseExpressionEvaluationError.invalidCast("finite floating text output")
    }

    private static func withFloatingDecimal<Result>(_ decimal: ExactDecimal, negative: Bool,
        fixedLimit: Int, workMeter: DatabaseWorkMeter,
        _ body: (borrowing Substring) throws -> Result) throws -> Result {
        var magnitude = decimal.coefficient.magnitude, digits = 1
        while magnitude >= 10 { magnitude /= 10; digits += 1 }
        let exponent = digits - Int(decimal.scale) - 1
        if exponent >= -4 && exponent < fixedLimit {
            let signed = ExactDecimal(coefficient: negative ? -decimal.coefficient : decimal.coefficient, scale: decimal.scale)
            return try withNumeric(QueryNumericValue(value: signed, displayScale: max(0, decimal.scale)), workMeter: workMeter, body)
        }
        let exponentDigits = abs(exponent) >= 100 ? 3 : 2
        let sign = negative ? 1 : 0
        let count = UInt64(sign + digits + (digits > 1 ? 1 : 0) + 2 + exponentDigits)
        return try withTemporaryText(count: count, workMeter: workMeter, make: {
            try makeText(count: count, workMeter: workMeter) { output in
                var coefficient = decimal.coefficient.magnitude
                let marker = sign + digits + (digits > 1 ? 1 : 0)
                for offset in 0..<digits {
                    let index = digits - offset - 1
                    output[sign + index + (index > 0 && digits > 1 ? 1 : 0)] = UInt8(coefficient % 10) + 48
                    coefficient /= 10
                }
                if negative { output[0] = 45 }
                if digits > 1 { output[sign + 1] = 46 }
                output[marker] = 101; output[marker + 1] = exponent < 0 ? 45 : 43
                var exponent = abs(exponent)
                for index in stride(from: Int(count) - 1, through: marker + 2, by: -1) {
                    output[index] = UInt8(exponent % 10) + 48; exponent /= 10
                }
            }
        }, body)
    }
}
