import DatabaseKit
import DatabaseMath
import DatabaseTypes

struct SPARQLNumericValue: Sendable {
    enum ArithmeticOperation: Sendable, Equatable {
        case add
        case subtract
        case multiply
        case divide
        case modulo
    }

    enum RoundingOperation: Sendable {
        case round
        case ceiling
        case floor
    }

    private enum FloatingKind: Int, Sendable {
        case float = 2
        case double = 3
    }

    private enum Storage: Sendable {
        case integer(Int64)
        case unsignedInteger(UInt64)
        case decimal(ExactDecimal)
        case floatingPoint(Double, FloatingKind)
    }

    private static let xsdNamespace = "http://www.w3.org/2001/XMLSchema#"
    private static let decimalDivisionScale: Int32 = 18

    private let storage: Storage

    init?(_ value: FieldValue) {
        switch value {
        case .int8(let integer):
            storage = .integer(Int64(integer))
        case .int16(let integer):
            storage = .integer(Int64(integer))
        case .int32(let integer):
            storage = .integer(Int64(integer))
        case .int64(let integer):
            storage = .integer(integer)
        case .uint8(let integer):
            storage = .unsignedInteger(UInt64(integer))
        case .uint16(let integer):
            storage = .unsignedInteger(UInt64(integer))
        case .uint32(let integer):
            storage = .unsignedInteger(UInt64(integer))
        case .uint64(let integer):
            storage = .unsignedInteger(integer)
        case .float32(let float):
            storage = .floatingPoint(Double(float), .float)
        case .float64(let double):
            storage = .floatingPoint(double, .double)
        case .decimal(let decimal):
            storage = .decimal(decimal)
        case .rdfTerm(.literal(let literal)):
            guard literal.languageTag == nil, literal.baseDirection == nil,
                  let storage = Self.parse(literal) else {
                return nil
            }
            self.storage = storage
        case .bool, .string, .bytes,
             .date, .time, .dateTime, .timestamp,
             .timeSpan, .calendarPeriod,
             .geographicPoint, .geographicPosition, .vector,
             .uuid, .object, .reference, .rdfTerm, .null, .array:
            return nil
        }
    }

    var doubleValue: Double {
        switch storage {
        case .integer(let value):
            return Double(value)
        case .unsignedInteger(let value):
            return Double(value)
        case .decimal(let value):
            return Double(value.coefficient)
                * DatabaseMath.power(10, -Double(value.scale))
        case .floatingPoint(let value, _):
            return value
        }
    }

    var exactInteger: Int64? {
        switch storage {
        case .integer(let value):
            return value
        case .unsignedInteger, .decimal, .floatingPoint:
            return nil
        }
    }

    var isZero: Bool {
        switch storage {
        case .integer(let value): return value == 0
        case .unsignedInteger(let value): return value == 0
        case .decimal(let value): return value.coefficient == 0
        case .floatingPoint(let value, _): return value == 0
        }
    }

    var isNaN: Bool {
        guard case .floatingPoint(let value, _) = storage else { return false }
        return value.isNaN
    }

    func integerConstructorValue() -> Int64? {
        switch storage {
        case .integer(let value):
            return value
        case .unsignedInteger(let value):
            return Int64(exactly: value)
        case .decimal(let value):
            if value.scale <= 0 {
                var result = value.coefficient
                let exponent = -Int64(value.scale)
                for _ in 0..<exponent {
                    let next = result.multipliedReportingOverflow(by: 10)
                    guard !next.overflow else { return nil }
                    result = next.partialValue
                }
                return Int64(exactly: result)
            }
            guard value.scale <= 38 else { return 0 }
            var divisor: Int128 = 1
            for _ in 0..<value.scale {
                let next = divisor.multipliedReportingOverflow(by: 10)
                guard !next.overflow else { return 0 }
                divisor = next.partialValue
            }
            return Int64(exactly: value.coefficient / divisor)
        case .floatingPoint(let value, _):
            guard value.isFinite else { return nil }
            let truncated = value.rounded(.towardZero)
            let upperExclusive = 9_223_372_036_854_775_808.0
            guard truncated >= -upperExclusive,
                  truncated < upperExclusive else {
                return nil
            }
            return Int64(truncated)
        }
    }

    func decimalConstructorLexicalForm() throws(SPARQLNumericError) -> String? {
        let decimal: ExactDecimal
        switch storage {
        case .integer(let value):
            decimal = ExactDecimal(coefficient: Int128(value), scale: 0)
        case .unsignedInteger(let value):
            let coefficient = Int128(value)
            decimal = ExactDecimal(coefficient: coefficient, scale: 0)
        case .decimal(let value):
            decimal = value
        case .floatingPoint(let value, _):
            guard value.isFinite,
                  case .decimal(let parsedDecimal)? =
                    Literal.parseDecimal(String(value)) else {
                return nil
            }
            decimal = parsedDecimal
        }
        do {
            return try decimal.decimalLexicalForm(
                maximumUTF8Count: SPARQLExecutionLimits.maximumLiteralUTF8Count
            )
        } catch let error {
            switch error {
            case .invalidMaximumUTF8Count:
                throw SPARQLNumericError.invalidResultLiteral
            case .representationTooLarge(let required, let maximum):
                throw SPARQLNumericError.resultLiteralTooLarge(
                    requiredUTF8Count: required,
                    maximumUTF8Count: maximum
                )
            }
        }
    }

    func floatingConstructorLexicalForm(asFloat: Bool) -> String {
        let value = asFloat ? Double(Float(doubleValue)) : doubleValue
        return Self.floatingLexicalForm(value)
    }

    func compare(to other: Self) -> SPARQLComparisonOrder? {
        switch (storage, other.storage) {
        case (.integer(let left), .integer(let right)):
            return Self.comparison(left, right)
        case (.unsignedInteger(let left), .unsignedInteger(let right)):
            return Self.comparison(left, right)
        case (.integer(let left), .unsignedInteger(let right)):
            guard left >= 0 else { return .ascending }
            return Self.comparison(UInt64(left), right)
        case (.unsignedInteger(let left), .integer(let right)):
            guard right >= 0 else { return .descending }
            return Self.comparison(left, UInt64(right))
        default:
            break
        }

        let promotion = max(floatingRank, other.floatingRank)
        if promotion == FloatingKind.double.rawValue {
            return Self.floatingComparison(doubleValue, other.doubleValue)
        }
        if promotion == FloatingKind.float.rawValue {
            return Self.floatingComparison(
                Float(doubleValue),
                Float(other.doubleValue)
            )
        }

        guard let left = exactDecimal,
              let right = other.exactDecimal else {
            return nil
        }
        let result = left.compare(to: right)
        if result < 0 { return .ascending }
        if result > 0 { return .descending }
        return .same
    }

    func applying(
        _ operation: ArithmeticOperation,
        to other: Self
    ) throws(SPARQLNumericError) -> Self {
        let promotion = max(floatingRank, other.floatingRank)
        if promotion > 0 {
            return try floatingResult(
                operation,
                other: other,
                kind: promotion == FloatingKind.double.rawValue ? .double : .float
            )
        }

        if isDecimal || other.isDecimal || operation == .divide {
            guard let left = exactDecimal, let right = other.exactDecimal else {
                throw .numericOverflow
            }
            let result = try Self.applyExact(operation, left, right)
            return Self(storage: .decimal(result))
        }

        switch (storage, other.storage) {
        case (.integer(let left), .integer(let right)):
            let result = try Self.apply(operation, left, right)
            return Self(storage: .integer(result))
        case (.unsignedInteger(let left), .unsignedInteger(let right)):
            let result = try Self.apply(operation, left, right)
            return Self(storage: .unsignedInteger(result))
        default:
            guard let left = exactDecimal, let right = other.exactDecimal else {
                throw .numericOverflow
            }
            let result = try Self.applyExact(operation, left, right)
            return Self(storage: .decimal(result))
        }
    }

    func negated() throws(SPARQLNumericError) -> Self {
        switch storage {
        case .integer(let value):
            guard value != Int64.min else { throw .numericOverflow }
            return Self(storage: .integer(-value))
        case .unsignedInteger(let value):
            guard value <= UInt64(Int64.max) else { throw .numericOverflow }
            return Self(storage: .integer(-Int64(value)))
        case .decimal(let value):
            let result = try Self.performExactDecimalOperation {
                () throws(ExactDecimalError) -> ExactDecimal in
                try value.negated()
            }
            return Self(storage: .decimal(result))
        case .floatingPoint(let value, let kind):
            return Self(storage: .floatingPoint(-value, kind))
        }
    }

    func magnitude() throws(SPARQLNumericError) -> Self {
        switch storage {
        case .integer(let value):
            if value < 0 { return try negated() }
            return self
        case .unsignedInteger:
            return self
        case .decimal(let value):
            let result = try Self.performExactDecimalOperation {
                () throws(ExactDecimalError) -> ExactDecimal in
                try value.magnitude()
            }
            return Self(storage: .decimal(result))
        case .floatingPoint(let value, let kind):
            return Self(storage: .floatingPoint(value.magnitude, kind))
        }
    }

    func rounded(
        _ operation: RoundingOperation
    ) throws(SPARQLNumericError) -> Self {
        switch storage {
        case .integer, .unsignedInteger:
            return self
        case .decimal(let value):
            return Self(
                storage: .decimal(
                    try Self.roundDecimal(value, operation: operation)
                )
            )
        case .floatingPoint(let value, let kind):
            let rounded: Double
            switch operation {
            case .round:
                rounded = DatabaseMath.floor(value + 0.5)
            case .ceiling:
                rounded = DatabaseMath.ceiling(value)
            case .floor:
                rounded = DatabaseMath.floor(value)
            }
            return Self(storage: .floatingPoint(rounded, kind))
        }
    }

    func fieldValue() throws(SPARQLNumericError) -> FieldValue {
        let lexicalForm: String
        let datatype: String
        switch storage {
        case .integer(let value):
            lexicalForm = String(value)
            datatype = Self.xsdNamespace + "integer"
        case .unsignedInteger(let value):
            lexicalForm = String(value)
            datatype = Self.xsdNamespace + "unsignedLong"
        case .decimal(let value):
            do {
                lexicalForm = try value.decimalLexicalForm(
                    maximumUTF8Count: SPARQLExecutionLimits.maximumLiteralUTF8Count
                )
            } catch let error {
                switch error {
                case .invalidMaximumUTF8Count:
                    throw .invalidResultLiteral
                case .representationTooLarge(let required, let maximum):
                    throw .resultLiteralTooLarge(
                        requiredUTF8Count: required,
                        maximumUTF8Count: maximum
                    )
                }
            }
            datatype = Self.xsdNamespace + "decimal"
        case .floatingPoint(let value, let kind):
            lexicalForm = Self.floatingLexicalForm(value)
            datatype = Self.xsdNamespace + (kind == .float ? "float" : "double")
        }

        do {
            return .rdfTerm(
                .literal(
                    try RDFLiteral(
                        lexicalForm: lexicalForm,
                        datatype: datatype
                    )
                )
            )
        } catch {
            throw .invalidResultLiteral
        }
    }

    private init(storage: Storage) {
        self.storage = storage
    }

    private var floatingRank: Int {
        guard case .floatingPoint(_, let kind) = storage else { return 0 }
        return kind.rawValue
    }

    private var isDecimal: Bool {
        if case .decimal = storage { return true }
        return false
    }

    private var exactDecimal: ExactDecimal? {
        switch storage {
        case .integer(let value):
            return ExactDecimal(coefficient: Int128(value), scale: 0)
        case .unsignedInteger(let value):
            let coefficient = Int128(value)
            return ExactDecimal(coefficient: coefficient, scale: 0)
        case .decimal(let value):
            return value
        case .floatingPoint:
            return nil
        }
    }

    private func floatingResult(
        _ operation: ArithmeticOperation,
        other: Self,
        kind: FloatingKind
    ) throws(SPARQLNumericError) -> Self {
        if kind == .float {
            let left = Float(doubleValue)
            let right = Float(other.doubleValue)
            let result: Float
            switch operation {
            case .add: result = left + right
            case .subtract: result = left - right
            case .multiply: result = left * right
            case .divide: result = left / right
            case .modulo: result = left.truncatingRemainder(dividingBy: right)
            }
            return Self(storage: .floatingPoint(Double(result), .float))
        }

        let left = doubleValue
        let right = other.doubleValue
        let result: Double
        switch operation {
        case .add: result = left + right
        case .subtract: result = left - right
        case .multiply: result = left * right
        case .divide: result = left / right
        case .modulo: result = left.truncatingRemainder(dividingBy: right)
        }
        return Self(storage: .floatingPoint(result, .double))
    }

    private static func apply(
        _ operation: ArithmeticOperation,
        _ left: Int64,
        _ right: Int64
    ) throws(SPARQLNumericError) -> Int64 {
        let result: (partialValue: Int64, overflow: Bool)
        switch operation {
        case .add: result = left.addingReportingOverflow(right)
        case .subtract: result = left.subtractingReportingOverflow(right)
        case .multiply: result = left.multipliedReportingOverflow(by: right)
        case .divide:
            guard right != 0 else { throw .divisionByZero }
            guard !(left == Int64.min && right == -1) else {
                throw .numericOverflow
            }
            return left / right
        case .modulo:
            guard right != 0 else { throw .divisionByZero }
            if left == Int64.min, right == -1 { return 0 }
            return left % right
        }
        guard !result.overflow else { throw .numericOverflow }
        return result.partialValue
    }

    private static func apply(
        _ operation: ArithmeticOperation,
        _ left: UInt64,
        _ right: UInt64
    ) throws(SPARQLNumericError) -> UInt64 {
        let result: (partialValue: UInt64, overflow: Bool)
        switch operation {
        case .add: result = left.addingReportingOverflow(right)
        case .subtract: result = left.subtractingReportingOverflow(right)
        case .multiply: result = left.multipliedReportingOverflow(by: right)
        case .divide:
            guard right != 0 else { throw .divisionByZero }
            return left / right
        case .modulo:
            guard right != 0 else { throw .divisionByZero }
            return left % right
        }
        guard !result.overflow else { throw .numericOverflow }
        return result.partialValue
    }

    private static func applyExact(
        _ operation: ArithmeticOperation,
        _ left: ExactDecimal,
        _ right: ExactDecimal
    ) throws(SPARQLNumericError) -> ExactDecimal {
        switch operation {
        case .add:
            return try performExactDecimalOperation {
                () throws(ExactDecimalError) -> ExactDecimal in
                try left.adding(right)
            }
        case .subtract:
            return try performExactDecimalOperation {
                () throws(ExactDecimalError) -> ExactDecimal in
                try left.subtracting(right)
            }
        case .multiply:
            return try performExactDecimalOperation {
                () throws(ExactDecimalError) -> ExactDecimal in
                try left.multiplying(by: right)
            }
        case .divide:
            return try divideDecimal(left, by: right)
        case .modulo:
            return try performExactDecimalOperation {
                () throws(ExactDecimalError) -> ExactDecimal in
                try left.remainder(dividingBy: right)
            }
        }
    }

    private static func roundDecimal(
        _ value: ExactDecimal,
        operation: RoundingOperation
    ) throws(SPARQLNumericError) -> ExactDecimal {
        guard value.scale > 0, value.coefficient != 0 else { return value }

        if value.scale > 38 {
            switch operation {
            case .round:
                return ExactDecimal(coefficient: 0, scale: 0)
            case .ceiling:
                return ExactDecimal(
                    coefficient: value.coefficient > 0 ? 1 : 0,
                    scale: 0
                )
            case .floor:
                return ExactDecimal(
                    coefficient: value.coefficient < 0 ? -1 : 0,
                    scale: 0
                )
            }
        }

        var divisor: Int128 = 1
        for _ in 0..<value.scale {
            let next = divisor.multipliedReportingOverflow(by: 10)
            guard !next.overflow else { throw .numericOverflow }
            divisor = next.partialValue
        }
        let quotient = value.coefficient / divisor
        let remainder = value.coefficient % divisor
        guard remainder != 0 else {
            return ExactDecimal(coefficient: quotient, scale: 0)
        }

        let coefficient: Int128
        switch operation {
        case .round:
            let threshold = divisor.magnitude / 2
            if remainder > 0, remainder.magnitude >= threshold {
                let result = quotient.addingReportingOverflow(1)
                guard !result.overflow else { throw .numericOverflow }
                coefficient = result.partialValue
            } else if remainder < 0, remainder.magnitude > threshold {
                let result = quotient.subtractingReportingOverflow(1)
                guard !result.overflow else { throw .numericOverflow }
                coefficient = result.partialValue
            } else {
                coefficient = quotient
            }
        case .ceiling:
            if remainder > 0 {
                let result = quotient.addingReportingOverflow(1)
                guard !result.overflow else { throw .numericOverflow }
                coefficient = result.partialValue
            } else {
                coefficient = quotient
            }
        case .floor:
            if remainder < 0 {
                let result = quotient.subtractingReportingOverflow(1)
                guard !result.overflow else { throw .numericOverflow }
                coefficient = result.partialValue
            } else {
                coefficient = quotient
            }
        }
        return ExactDecimal(coefficient: coefficient, scale: 0)
    }

    private static func performExactDecimalOperation<T>(
        _ operation: () throws(ExactDecimalError) -> T
    ) throws(SPARQLNumericError) -> T {
        do {
            return try operation()
        } catch let error {
            switch error {
            case .numericOverflow: throw .numericOverflow
            case .divisionByZero: throw .divisionByZero
            case .inexactResult: throw .inexactDecimalResult
            }
        }
    }

    /// Divides two exact decimals using the SPARQL execution precision when the
    /// mathematical quotient has no finite decimal representation.
    ///
    /// ExactDecimal deliberately rejects recurring results. SPARQL decimal
    /// arithmetic instead requires an implementation-defined finite precision,
    /// owned here by the query runtime rather than by the primitive value type.
    private static func divideDecimal(
        _ left: ExactDecimal,
        by right: ExactDecimal
    ) throws(SPARQLNumericError) -> ExactDecimal {
        do {
            return try left.dividing(by: right)
        } catch let error {
            switch error {
            case .divisionByZero:
                throw .divisionByZero
            case .numericOverflow:
                throw .numericOverflow
            case .inexactResult:
                // Continue with the runtime's bounded recurring-decimal policy.
                break
            }
        }

        var numerator = left.coefficient.magnitude
        var denominator = right.coefficient.magnitude
        guard denominator != 0 else { throw .divisionByZero }

        let divisor = greatestCommonDivisor(numerator, denominator)
        numerator /= divisor
        denominator /= divisor

        var resultScale = decimalDivisionScale
        var scaledNumerator: UInt128?
        var scaledDenominator: UInt128?
        while resultScale >= 0 {
            let exponent = Int64(resultScale)
                + Int64(right.scale)
                - Int64(left.scale)
            if exponent >= 0 {
                scaledNumerator = scaleMagnitude(
                    numerator,
                    by: exponent
                )
                scaledDenominator = denominator
            } else {
                scaledNumerator = numerator
                scaledDenominator = scaleMagnitude(
                    denominator,
                    by: -exponent
                )
            }
            if scaledNumerator != nil, scaledDenominator != nil {
                break
            }
            resultScale -= 1
        }

        guard resultScale >= 0,
              let scaledNumerator,
              let scaledDenominator else {
            throw .numericOverflow
        }

        var quotient = scaledNumerator / scaledDenominator
        let remainder = scaledNumerator % scaledDenominator
        let roundingThreshold = scaledDenominator / 2
            + scaledDenominator % 2
        if remainder >= roundingThreshold {
            let rounded = quotient.addingReportingOverflow(1)
            guard !rounded.overflow else { throw .numericOverflow }
            quotient = rounded.partialValue
        }

        let isNegative = (left.coefficient < 0) != (right.coefficient < 0)
        let maximumMagnitude = isNegative
            ? UInt128(Int128.max) + 1
            : UInt128(Int128.max)
        guard quotient <= maximumMagnitude else { throw .numericOverflow }

        let coefficient: Int128
        if isNegative {
            if quotient == UInt128(Int128.max) + 1 {
                coefficient = Int128.min
            } else {
                guard let signed = Int128(exactly: quotient) else {
                    throw .numericOverflow
                }
                coefficient = -signed
            }
        } else {
            guard let signed = Int128(exactly: quotient) else {
                throw .numericOverflow
            }
            coefficient = signed
        }
        return ExactDecimal(
            coefficient: coefficient,
            scale: resultScale
        )
    }

    private static func scaleMagnitude(
        _ value: UInt128,
        by exponent: Int64
    ) -> UInt128? {
        guard exponent >= 0 else { return nil }
        var result = value
        for _ in 0..<exponent {
            guard result <= UInt128.max / 10 else { return nil }
            result *= 10
        }
        return result
    }

    private static func greatestCommonDivisor(
        _ left: UInt128,
        _ right: UInt128
    ) -> UInt128 {
        var left = left
        var right = right
        while right != 0 {
            let remainder = left % right
            left = right
            right = remainder
        }
        return left
    }

    private static func parse(_ literal: RDFLiteral) -> Storage? {
        switch literal.datatypeIRI.rawValue {
        case xsdNamespace + "integer":
            return parseInteger(literal.lexicalForm)
        case xsdNamespace + "nonPositiveInteger":
            return parseInteger(literal.lexicalForm, range: ...0)
        case xsdNamespace + "negativeInteger":
            return parseInteger(literal.lexicalForm, range: ...(-1))
        case xsdNamespace + "long":
            return parseInteger(literal.lexicalForm)
        case xsdNamespace + "int":
            return parseInteger(
                literal.lexicalForm,
                range: Int64(Int32.min)...Int64(Int32.max)
            )
        case xsdNamespace + "short":
            return parseInteger(
                literal.lexicalForm,
                range: Int64(Int16.min)...Int64(Int16.max)
            )
        case xsdNamespace + "byte":
            return parseInteger(
                literal.lexicalForm,
                range: Int64(Int8.min)...Int64(Int8.max)
            )
        case xsdNamespace + "nonNegativeInteger":
            return parseInteger(literal.lexicalForm, range: 0...)
        case xsdNamespace + "positiveInteger":
            return parseInteger(literal.lexicalForm, range: 1...)
        case xsdNamespace + "unsignedLong":
            return parseUnsignedInteger(literal.lexicalForm, maximum: UInt64.max)
        case xsdNamespace + "unsignedInt":
            return parseUnsignedInteger(
                literal.lexicalForm,
                maximum: UInt64(UInt32.max)
            )
        case xsdNamespace + "unsignedShort":
            return parseUnsignedInteger(
                literal.lexicalForm,
                maximum: UInt64(UInt16.max)
            )
        case xsdNamespace + "unsignedByte":
            return parseUnsignedInteger(
                literal.lexicalForm,
                maximum: UInt64(UInt8.max)
            )
        case xsdNamespace + "decimal":
            guard isDecimalLexicalForm(literal.lexicalForm),
                  case .decimal(let decimal)? =
                    Literal.parseDecimal(literal.lexicalForm) else {
                return nil
            }
            return .decimal(decimal)
        case xsdNamespace + "float":
            guard let value = parseFloatingPoint(literal.lexicalForm) else {
                return nil
            }
            return .floatingPoint(Double(Float(value)), .float)
        case xsdNamespace + "double":
            guard let value = parseFloatingPoint(literal.lexicalForm) else {
                return nil
            }
            return .floatingPoint(value, .double)
        default:
            return nil
        }
    }

    private static func parseInteger(
        _ lexicalForm: String,
        range: ClosedRange<Int64> = Int64.min...Int64.max
    ) -> Storage? {
        guard isIntegerLexicalForm(lexicalForm),
              let value = Int64(lexicalForm), range.contains(value) else {
            return nil
        }
        return .integer(value)
    }

    private static func parseInteger(
        _ lexicalForm: String,
        range: PartialRangeThrough<Int64>
    ) -> Storage? {
        guard isIntegerLexicalForm(lexicalForm),
              let value = Int64(lexicalForm), range.contains(value) else {
            return nil
        }
        return .integer(value)
    }

    private static func parseInteger(
        _ lexicalForm: String,
        range: PartialRangeFrom<Int64>
    ) -> Storage? {
        guard isIntegerLexicalForm(lexicalForm),
              let value = Int64(lexicalForm), range.contains(value) else {
            return nil
        }
        return .integer(value)
    }

    private static func parseUnsignedInteger(
        _ lexicalForm: String,
        maximum: UInt64
    ) -> Storage? {
        guard isIntegerLexicalForm(lexicalForm),
              lexicalForm.first != "-",
              let value = UInt64(lexicalForm), value <= maximum else {
            return nil
        }
        if let signed = Int64(exactly: value) {
            return .integer(signed)
        }
        return .unsignedInteger(value)
    }

    private static func isIntegerLexicalForm(_ value: String) -> Bool {
        var iterator = value.utf8.makeIterator()
        guard let first = iterator.next() else { return false }
        if first != 0x2B && first != 0x2D && !isASCIIDigit(first) {
            return false
        }
        var hasDigit = first != 0x2B && first != 0x2D
        while let byte = iterator.next() {
            guard isASCIIDigit(byte) else { return false }
            hasDigit = true
        }
        return hasDigit
    }

    private static func isDecimalLexicalForm(_ value: String) -> Bool {
        var iterator = value.utf8.makeIterator()
        guard var byte = iterator.next() else { return false }
        if byte == 0x2B || byte == 0x2D {
            guard let next = iterator.next() else { return false }
            byte = next
        }
        var decimalPointCount = 0
        var digitCount = 0
        while true {
            if byte == 0x2E {
                decimalPointCount += 1
                if decimalPointCount > 1 { return false }
            } else if isASCIIDigit(byte) {
                digitCount += 1
            } else {
                return false
            }
            guard let next = iterator.next() else { break }
            byte = next
        }
        return digitCount > 0
    }

    private static func parseFloatingPoint(_ value: String) -> Double? {
        switch value {
        case "INF": return .infinity
        case "-INF": return -.infinity
        case "NaN": return .nan
        default:
            guard isFloatingPointLexicalForm(value),
                  let parsed = Double(value) else {
                return nil
            }
            return parsed
        }
    }

    private static func isFloatingPointLexicalForm(_ value: String) -> Bool {
        let parts = value.split(
            omittingEmptySubsequences: false,
            whereSeparator: { $0 == "e" || $0 == "E" }
        )
        guard parts.count <= 2,
              isDecimalLexicalForm(String(parts[0])) else {
            return false
        }
        if parts.count == 1 { return true }
        return isIntegerLexicalForm(String(parts[1]))
    }

    private static func isASCIIDigit(_ byte: UInt8) -> Bool {
        byte >= 0x30 && byte <= 0x39
    }

    private static func floatingLexicalForm(_ value: Double) -> String {
        if value.isNaN { return "NaN" }
        if value == .infinity { return "INF" }
        if value == -.infinity { return "-INF" }
        return String(value)
    }

    private static func comparison<T: Comparable>(
        _ left: T,
        _ right: T
    ) -> SPARQLComparisonOrder {
        if left < right { return .ascending }
        if left > right { return .descending }
        return .same
    }

    private static func floatingComparison<T: BinaryFloatingPoint>(
        _ left: T,
        _ right: T
    ) -> SPARQLComparisonOrder? {
        guard !left.isNaN, !right.isNaN else { return nil }
        return comparison(left, right)
    }
}
