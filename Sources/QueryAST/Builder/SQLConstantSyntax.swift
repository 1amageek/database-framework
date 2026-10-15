import DatabaseKit
import DatabaseTypes

/// The common intrinsic constant boundary for SQL parsing and serialization.
enum SQLConstantSyntax {
    static func valueLimits(_ limits: QueryStructuralLimits) -> QueryValueLimits {
        QueryValueLimits(maximumRetainedBytes: limits.maximumRetainedBytes,
                         maximumTotalNodes: limits.maximumTotalNodes,
                         maximumNestingDepth: limits.maximumNestingDepth,
                         maximumCollectionElements: limits.maximumCollectionElements)
    }

    static func numericLiteral(_ text: String, limits: QueryStructuralLimits) throws(SQLConstantError) -> Literal {
        let numeric = try numeric(text, limits: limits)
        let digits = text.utf8.drop(while: { $0 == 43 || $0 == 45 })
        let isHexadecimal = digits.starts(with: [48, 120]) || digits.starts(with: [48, 88])
        let isDecimal = text.utf8.contains(46) || (!isHexadecimal && text.utf8.contains(where: { $0 == 101 || $0 == 69 }))
        if !isDecimal, numeric.value.scale <= 0 {
            let spelling = try numericText(numeric, maximum: limits.maximumRetainedBytes)
            if let value = Int32(spelling) { return .queryValue(.primitive(.int32(value)), type: .sql(.integer)) }
            if let value = Int64(spelling) { return .queryValue(.primitive(.int64(value)), type: .sql(.bigint)) }
        }
        return .queryValue(.numeric(numeric), type: .sql(.decimal(precision: nil, scale: nil)))
    }

    static func numeric(_ text: String, limits: QueryStructuralLimits) throws(SQLConstantError) -> QueryNumericValue {
        guard UInt64(text.utf8.count) <= limits.maximumRetainedBytes else { throw .resourceLimit("Numeric input byte budget exceeded") }
        let bytes = Array(text.utf8)
        guard !bytes.isEmpty else { throw .invalidNumeric("Empty numeric constant") }
        var i = 0
        let negative = bytes[i] == 45
        if bytes[i] == 45 || bytes[i] == 43 { i += 1 }
        guard i < bytes.count else { throw .invalidNumeric("Missing numeric digits") }
        if i + 1 < bytes.count, bytes[i] == 48 {
            let radix: Int128
            switch bytes[i + 1] {
            case 120, 88: radix = 16
            case 111, 79: radix = 8
            case 98, 66: radix = 2
            default: radix = 0
            }
            if radix != 0 {
                i += 2
                if i < bytes.count, bytes[i] == 95 {
                    i += 1
                    guard i < bytes.count, digit(bytes[i]) >= 0, digit(bytes[i]) < radix else {
                        throw .invalidNumeric("Invalid radix prefix separator")
                    }
                }
                var value: Int128 = 0
                var seen = false
                var previousDigit = false
                while i < bytes.count {
                    let byte = bytes[i]
                    i += 1
                    if byte == 95 {
                        guard previousDigit, i < bytes.count, digit(bytes[i]) < radix else { throw .invalidNumeric("Invalid numeric digit separator") }
                        previousDigit = false
                        continue
                    }
                    let number = digit(byte)
                    guard number >= 0, number < radix else { throw .invalidNumeric("Invalid radix digit") }
                    let product = value.multipliedReportingOverflow(by: radix)
                    let sum = product.partialValue.addingReportingOverflow(negative ? -number : number)
                    guard !product.overflow, !sum.overflow else { throw .numericOutOfRange }
                    value = sum.partialValue
                    previousDigit = true
                    seen = true
                }
                guard seen, previousDigit else { throw .invalidNumeric("Missing radix digits") }
                return try makeNumeric(ExactDecimal(coefficient: value, scale: 0), scale: 0)
            }
        }
        var coefficient: Int128 = 0
        var significant = 0
        var pendingZeros: Int64 = 0
        var fractional: Int64 = 0
        var point = false
        var seenDigit = false
        var previousDigit = false
        while i < bytes.count, bytes[i] != 101, bytes[i] != 69 {
            let byte = bytes[i]
            i += 1
            if byte == 46 {
                guard !point else { throw .invalidNumeric("Repeated numeric decimal point") }
                point = true
                previousDigit = false
                continue
            }
            if byte == 95 {
                guard previousDigit, i < bytes.count, (48...57).contains(bytes[i]) else { throw .invalidNumeric("Invalid numeric digit separator") }
                previousDigit = false
                continue
            }
            guard (48...57).contains(byte) else { throw .invalidNumeric("Invalid decimal digit") }
            previousDigit = true
            seenDigit = true
            if point { fractional += 1 }
            let number = Int128(byte - 48)
            if number == 0 { if coefficient != 0 { pendingZeros += 1 }; continue }
            guard Int64(significant) + pendingZeros + 1 <= 38 else { throw .numericOutOfRange }
            while pendingZeros > 0 { coefficient *= 10; significant += 1; pendingZeros -= 1 }
            coefficient = coefficient * 10 + number
            significant += 1
        }
        guard seenDigit else { throw .invalidNumeric("Missing decimal digits") }
        var exponent: Int64 = 0
        if i < bytes.count {
            i += 1
            guard i < bytes.count else { throw .invalidNumeric("Missing exponent digits") }
            let negativeExponent = bytes[i] == 45
            if bytes[i] == 45 || bytes[i] == 43 { i += 1 }
            var seenExponent = false
            previousDigit = false
            while i < bytes.count {
                let byte = bytes[i]
                i += 1
                if byte == 95 {
                    guard previousDigit, i < bytes.count, (48...57).contains(bytes[i]) else { throw .invalidNumeric("Invalid exponent separator") }
                    previousDigit = false
                    continue
                }
                guard (48...57).contains(byte) else { throw .invalidNumeric("Invalid exponent digit") }
                let product = exponent.multipliedReportingOverflow(by: 10)
                let sum = product.partialValue.addingReportingOverflow(Int64(byte - 48))
                guard !product.overflow, !sum.overflow else { throw .numericOutOfRange }
                exponent = sum.partialValue
                seenExponent = true
                previousDigit = true
            }
            guard seenExponent, previousDigit else { throw .invalidNumeric("Missing exponent digits") }
            if negativeExponent { exponent = -exponent }
        }
        let rawScale = fractional.subtractingReportingOverflow(exponent)
        guard !rawScale.overflow, max(0, rawScale.partialValue) <= 16383 else { throw .numericOutOfRange }
        let scale = rawScale.partialValue.subtractingReportingOverflow(pendingZeros)
        guard !scale.overflow, let canonicalScale = Int32(exactly: scale.partialValue),
              coefficient == 0 || Int64(significant) - scale.partialValue <= 131072 else { throw .numericOutOfRange }
        return try makeNumeric(ExactDecimal(coefficient: negative ? -coefficient : coefficient,
                                           scale: coefficient == 0 ? 0 : canonicalScale),
                               scale: Int32(max(0, rawScale.partialValue)))
    }

    private static func digit(_ byte: UInt8) -> Int128 {
        switch byte {
        case 48...57: return Int128(byte - 48)
        case 65...70: return Int128(byte - 65 + 10)
        case 97...102: return Int128(byte - 97 + 10)
        default: return -1
        }
    }

    private static func makeNumeric(_ value: ExactDecimal, scale: Int32) throws(SQLConstantError) -> QueryNumericValue {
        do { return try QueryNumericValue(value: value, displayScale: scale) }
        catch { throw .value(error) }
    }

    static func numericText(_ numeric: QueryNumericValue, maximum: UInt64) throws(SQLConstantError) -> String {
        let value = numeric.value
        let digits = String(value.coefficient.magnitude)
        let integralDigits = value.coefficient == 0 ? 1 : max(1, Int64(digits.count) - Int64(value.scale))
        guard integralDigits <= 131072, numeric.displayScale <= 16383 else { throw .numericOutOfRange }
        let scale = Int(numeric.displayScale)
        let length = UInt64(integralDigits) + (scale == 0 ? 0 : UInt64(scale + 1)) + (value.coefficient < 0 ? 1 : 0)
        guard length <= maximum, length <= UInt64(Int.max) else { throw .resourceLimit("Numeric output byte budget exceeded") }
        let raw: String
        do { raw = try value.decimalLexicalForm(maximumUTF8Count: Int(length)) }
        catch { throw .numericOutOfRange }
        if scale == 0 { return raw }
        let existing = max(0, Int(value.scale))
        if existing == 0 { return raw + "." + String(repeating: "0", count: scale) }
        return raw + String(repeating: "0", count: scale - existing)
    }

    static func normalize(_ expression: Expression, as type: DataType,
                          limits: QueryStructuralLimits) throws(SQLConstantError) -> Literal? {
        guard case .literal(let literal) = expression else { return nil }
        if literal.isNull {
            guard QueryValueValidator.accepts(.primitive(.null), type: .sql(type)) else {
                throw .invalidConstant("Invalid SQL NULL type descriptor")
            }
            return .queryValue(.primitive(.null), type: .sql(type))
        }
        let text: String?
        switch literal {
        case .string(let value): text = value
        case .int(let value): text = String(value)
        case .decimal(let value):
            text = try numericText(try makeNumeric(value, scale: max(0, value.scale)), maximum: limits.maximumRetainedBytes)
        case .queryValue(let value, _):
            switch value {
            case .numeric(let value): text = try numericText(value, maximum: limits.maximumRetainedBytes)
            case .primitive(.int16(let value)): text = String(value)
            case .primitive(.int32(let value)): text = String(value)
            case .primitive(.int64(let value)): text = String(value)
            case .primitive(.float32(let value)): text = String(value)
            case .primitive(.float64(let value)): text = String(value)
            case .primitive(.string(let value)): text = value
            default:
                if QueryValueValidator.accepts(value, type: .sql(type)) { return .queryValue(value, type: .sql(type)) }
                return nil
            }
        case .double(let value): text = String(value)
        case .bool(let value): text = value ? "true" : "false"
        default: text = nil
        }
        guard let text else { return nil }
        guard UInt64(text.utf8.count) <= limits.maximumRetainedBytes else { throw .resourceLimit("Constant input byte budget exceeded") }
        let value: QueryValue
        do {
            switch type {
            case .decimal:
                value = .numeric(try numeric(text, limits: limits))
                // Rounding and typmod coercion belong to runtime; exact casts normalize here.
                if !QueryValueValidator.accepts(value, type: .sql(type)) { return nil }
            case .smallint:
                guard let number = Int16(text) else { throw SQLConstantError.invalidConstant("Invalid SMALLINT constant") }
                value = .primitive(.int16(number))
            case .integer:
                guard let number = Int32(text) else { throw SQLConstantError.invalidConstant("Invalid INTEGER constant") }
                value = .primitive(.int32(number))
            case .bigint:
                guard let number = Int64(text) else { throw SQLConstantError.invalidConstant("Invalid BIGINT constant") }
                value = .primitive(.int64(number))
            case .real:
                guard let number = Float(text), number.isFinite else { throw SQLConstantError.invalidConstant("Invalid finite REAL constant") }
                value = .primitive(.float32(number))
            case .doublePrecision:
                guard let number = Double(text), number.isFinite else { throw SQLConstantError.invalidConstant("Invalid finite DOUBLE PRECISION constant") }
                value = .primitive(.float64(number))
            case .boolean:
                guard text == "true" || text == "false" else { return nil }
                value = .primitive(.bool(text == "true"))
            case .text: value = .primitive(.string(text))
            case .blob, .binary, .varbinary:
                guard text.hasPrefix("\\x"), (text.utf8.count - 2).isMultiple(of: 2) else {
                    throw SQLConstantError.invalidConstant("Invalid hexadecimal BYTEA constant")
                }
                var bytes: [UInt8] = []
                let input = Array(text.utf8.dropFirst(2))
                for offset in stride(from: 0, to: input.count, by: 2) {
                    let high = digit(input[offset]), low = digit(input[offset + 1])
                    guard high >= 0, low >= 0 else { throw SQLConstantError.invalidConstant("Invalid BYTEA hexadecimal digit") }
                    bytes.append(UInt8(high * 16 + low))
                }
                value = .primitive(.bytes(ByteString(bytes)))
            case .char(let length):
                if text.unicodeScalars.count != (length ?? 1) { return nil }
                value = .primitive(.string(text))
            case .varchar(let length):
                if let length, text.unicodeScalars.count > length { return nil }
                value = .primitive(.string(text))
            case .uuid:
                guard let uuid = DatabaseTypes.UUID(canonicalString: text) else { throw SQLConstantError.invalidConstant("Invalid canonical UUID constant") }
                value = .primitive(.uuid(uuid))
            case .json:
                value = .json(try QueryJSONValue(bytes: ByteString(utf8: text), limits: valueLimits(limits)))
            case .jsonb:
                let json = try QueryJSONValue(bytes: ByteString(utf8: text), limits: valueLimits(limits))
                value = .jsonb(try QueryJSONBValue(json: json, limits: valueLimits(limits)))
            case .interval:
                guard let interval = try interval(text) else { return nil }
                value = .interval(interval)
            case .array(let element):
                value = .array(try parseArray(text, element: element, limits: limits))
            default: return nil
            }
        } catch let error as SQLConstantError { throw error }
          catch let error as QueryValueError { throw .value(error) }
          catch { throw .invalidConstant("Intrinsic SQL constant conversion failed") }
        return .queryValue(value, type: .sql(type))
    }

    static func interval(_ text: String) throws(SQLConstantError) -> QueryIntervalValue? {
        let parts = text.split(whereSeparator: { $0.isWhitespace })
        guard parts.count == 6, parts[1] == "mons", parts[3] == "days", parts[5] == "microseconds" else { return nil }
        guard let months = Int32(parts[0]), let days = Int32(parts[2]), let micros = Int64(parts[4]) else {
            throw .invalidConstant("Invalid canonical interval components")
        }
        var seconds = micros / 1_000_000
        var remainder = micros % 1_000_000
        if remainder < 0 { seconds -= 1; remainder += 1_000_000 }
        do {
            return try QueryIntervalValue(period: CalendarPeriod(months: Int64(months), days: Int64(days)),
                                          time: TimeSpan(seconds: seconds, nanoseconds: UInt32(remainder) * 1000))
        } catch let error as QueryValueError { throw .value(error) }
          catch { throw .invalidConstant("Invalid canonical interval time") }
    }

    static func intervalText(_ value: QueryIntervalValue) -> String {
        "\(value.period.months) mons \(value.period.days) days \(value.microseconds) microseconds"
    }
}
