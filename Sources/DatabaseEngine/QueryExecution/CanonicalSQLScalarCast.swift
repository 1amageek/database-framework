import DatabaseKit
import DatabaseTypes

/// Conversion of an already selected SQL scalar, including assignment typmods.
enum CanonicalSQLScalarCast {
    static func usesPayload(_ target: QueryColumnType) -> Bool {
        switch target {
        case .sql(.text), .sql(.char), .sql(.varchar), .sql(.blob), .sql(.binary), .sql(.varbinary),
             .primitive(.string, false), .primitive(.bytes, false): return true
        default: return false
        }
    }

    static func collectionLimits(workMeter: DatabaseWorkMeter) -> QueryValueLimits {
        let structural = QueryStructuralLimits.default
        return QueryValueLimits(maximumRetainedBytes: min(structural.maximumRetainedBytes, workMeter.budget.maximumIntermediateBytes),
            maximumTotalNodes: min(structural.maximumTotalNodes, workMeter.budget.maximumWorkUnits),
            maximumNestingDepth: min(structural.maximumNestingDepth, workMeter.budget.maximumWorkUnits),
            maximumCollectionElements: min(structural.maximumCollectionElements, workMeter.budget.maximumWorkUnits))
    }

    static func evaluateCollection(_ value: QueryValue, from source: QueryColumnType,
        to target: QueryColumnType, context: CanonicalQueryTypeBinder.CastContext,
        temporalEnvironment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter,
        limits: QueryValueLimits) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        try validate(value, source: source, target: target, workMeter: workMeter)
        if CanonicalQueryTypeBinder.arrayElement(source) != nil || CanonicalQueryTypeBinder.arrayElement(target) != nil {
            return try CanonicalSQLArrayEvaluator.cast(value, from: source, to: target, context: context,
                temporalEnvironment: temporalEnvironment, workMeter: workMeter, limits: limits)
        }
        guard CanonicalQueryTypeBinder.isJSON(source) || CanonicalQueryTypeBinder.isJSON(target) else { throw invalid("selected collection target") }
        return try CanonicalSQLJSONEvaluator.cast(value, from: source, to: target, context: context,
            temporalEnvironment: temporalEnvironment, workMeter: workMeter, limits: limits)
    }

    static func evaluateValue(_ value: QueryValue, from source: QueryColumnType,
        to target: QueryColumnType, context: CanonicalQueryTypeBinder.CastContext,
        temporalEnvironment: SQLTemporalEnvironment? = nil, workMeter: DatabaseWorkMeter) throws -> QueryValue {
        try validate(value, source: source, target: target, workMeter: workMeter)
        if case .primitive = target, source == target { return value }
        if source == target, CanonicalQueryTypeBinder.isJSON(target) { return value }
        if CanonicalQueryTypeBinder.temporalType(target) != nil {
            guard let temporalEnvironment else { throw DatabaseExpressionEvaluationError.missingTemporalEnvironment }
            return try CanonicalSQLTemporalEvaluator.cast(value, from: source, to: target, context: context,
                environment: temporalEnvironment, workMeter: workMeter)
        }
        if target == .sql(.uuid) || target == .primitive(.uuid, isArray: false) {
            guard isText(source) || isUUID(source) else { throw invalid("UUID") }
            if value.isNull { return .primitive(.null) }
            if case .primitive(.uuid) = value { return value }
            return try withInputText(value, source: source, workMeter: workMeter) { text in
                try DatabaseByteProcessingMeter.consume(byteCount: text.utf8.count, passes: 2,
                    workMeter: workMeter, stage: .expressionEvaluation)
                do { return try QueryValue.uuid(sqlLexicalForm: text, maximumUTF8Count: UInt64(text.utf8.count)) }
                catch let error as QueryValueError { throw lexicalFailure(error, target: "UUID") }
            }
        }
        let sqlTarget: DataType
        if case .sql(let type) = target { sqlTarget = type }
        else {
            guard let type = primitiveNumericType(target) else { throw invalid("selected scalar target") }
            sqlTarget = type
        }
        switch sqlTarget {
        case .smallint, .integer, .bigint, .real, .doublePrecision, .decimal, .boolean: break
        default: throw invalid("selected scalar target")
        }
        let result: QueryValue
        if isText(source) {
            if value.isNull { return .primitive(.null) }
            result = try withInputText(value, source: source, workMeter: workMeter) { text in
                switch sqlTarget {
                case .boolean:
                    return .primitive(.bool(try boolean(text, workMeter: workMeter)))
                case .real, .doublePrecision:
                    let trimmed = try asciiTrim(text, workMeter: workMeter)
                    let footprint = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint(retainedByteCount: UInt64(trimmed.utf8.count))
                    let reservation = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .expressionEvaluation)
                    defer { reservation.release() }
                    try DatabaseByteProcessingMeter.consume(byteCount: trimmed.utf8.count, passes: 2,
                        workMeter: workMeter, stage: .expressionEvaluation)
                    let string = String(trimmed)
                    if sqlTarget == .real {
                        guard let number = Float(string) else { throw invalid("REAL") }
                        guard number.isFinite else { throw DatabaseExpressionEvaluationError.nonFiniteValue }
                        try CanonicalSQLNumericEvaluator.finiteResult(Double(number), underflow: number == 0 && nonzeroMantissa(trimmed, workMeter: workMeter))
                        return .primitive(.float32(number))
                    }
                    guard let number = Double(string) else { throw invalid("DOUBLE PRECISION") }
                    guard number.isFinite else { throw DatabaseExpressionEvaluationError.nonFiniteValue }
                    try CanonicalSQLNumericEvaluator.finiteResult(number, underflow: number == 0 && nonzeroMantissa(trimmed, workMeter: workMeter))
                    return .primitive(.float64(number))
                default:
                    if sqlTarget == .smallint || sqlTarget == .integer || sqlTarget == .bigint {
                        guard try integerLexical(text, workMeter: workMeter) else { throw invalid("integer") }
                    }
                    let numeric: QueryNumericValue
                    // The pure constructor counts input, trims its exterior, then scans the lexical body.
                    try DatabaseByteProcessingMeter.consume(byteCount: text.utf8.count, passes: 3,
                        workMeter: workMeter, stage: .expressionEvaluation)
                    do { numeric = try QueryNumericValue(sqlLexicalForm: text, maximumUTF8Count: UInt64(text.utf8.count)) }
                    catch let error as QueryValueError { throw lexicalFailure(error, target: "NUMERIC") }
                    return try CanonicalSQLNumericCast.evaluate(.numeric(numeric),
                        from: .sql(.decimal(precision: nil, scale: nil)), to: sqlTarget, workMeter: workMeter)
                }
            }
        } else {
            result = try CanonicalSQLNumericCast.evaluate(value, from: source, to: sqlTarget, workMeter: workMeter)
        }
        if case .primitive = target {
            return try CanonicalQueryTypeBinder.normalize(result, to: target, permitsFloatingRounding: true)
        }
        return result
    }

    static func evaluatePayload(_ value: QueryValue, from source: QueryColumnType,
        to target: QueryColumnType, context: CanonicalQueryTypeBinder.CastContext,
        temporalEnvironment: SQLTemporalEnvironment? = nil, limits: QueryValueLimits? = nil,
        workMeter: DatabaseWorkMeter) throws -> DatabaseQueryScopedFieldValue {
        try validate(value, source: source, target: target, workMeter: workMeter)
        guard usesPayload(target) else { throw invalid("selected payload target") }
        if isBinary(target) {
            guard isBinary(source) || isText(source) else { throw invalid("BYTEA") }
            if value.isNull { return .borrowing(.null) }
            if case .primitive(.bytes(let bytes)) = value { return .borrowing(.bytes(bytes)) }
            return try withInputText(value, source: source, workMeter: workMeter) { text in
                let maximum = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint(retainedByteCount: UInt64(text.utf8.count))
                return try .producing(maximumFootprint: maximum, workMeter: workMeter, stage: .expressionEvaluation) {
                    // The shared parser counts input, measures decoded bytes, then replays into final storage.
                    try DatabaseByteProcessingMeter.consume(byteCount: text.utf8.count, passes: 3,
                        workMeter: workMeter, stage: .expressionEvaluation)
                    do {
                        let parsed = try QueryValue.binary(sqlLexicalForm: text, maximumUTF8Count: UInt64(text.utf8.count))
                        guard case .primitive(let primitive) = parsed else { throw invalid("BYTEA") }
                        return primitive
                    } catch let error as QueryValueError { throw lexicalFailure(error, target: "BYTEA") }
                }
            }
        }
        guard supportsTextOutput(source) else { throw invalid("TEXT") }
        if value.isNull { return .borrowing(.null) }
        if context == .implicit, target == .sql(.char(length: nil)),
           case .primitive(.string(let text)) = value {
            return .borrowing(.string(text))
        }
        if case .primitive(.bool(let flag)) = value {
            return try characterResult(flag ? "true"[...] : "false"[...], target: target,
                context: context, workMeter: workMeter)
        }
        return try CanonicalSQLScalarText.withText(of: value, type: source,
            preservingCharacterPadding: isCharacter(target), temporalEnvironment: temporalEnvironment, limits: limits, workMeter: workMeter) { text in
                try characterResult(text, target: target, context: context, workMeter: workMeter)
            }
    }

    private static func characterResult(_ text: borrowing Substring, target: QueryColumnType,
        context: CanonicalQueryTypeBinder.CastContext, workMeter: DatabaseWorkMeter) throws -> DatabaseQueryScopedFieldValue {
        let limit: Int?
        let padding: Bool
        switch target {
        case .sql(.char(let length)):
            limit = context == .implicit ? length : (length ?? 1)
            padding = limit != nil
        case .sql(.varchar(let length)): limit = length; padding = false
        default: limit = nil; padding = false
        }
        try CanonicalSQLScalarText.charge(text.utf8.count, workMeter)
        var count = 0, end = text.unicodeScalars.endIndex
        for index in text.unicodeScalars.indices {
            try CanonicalSQLScalarText.checkpoint(count, workMeter)
            if let limit, count == limit { end = index }
            if let limit, count >= limit, context != .explicit, text.unicodeScalars[index].value != 32 {
                throw invalid("character length")
            }
            count += 1
        }
        let visible = text[..<end]
        let spaces = padding ? max(0, (limit ?? 0) - count) : 0
        let outputCount = try CanonicalSQLScalarText.checkedCount(UInt64(visible.utf8.count), addition: UInt64(spaces))
        return try CanonicalSQLScalarText.produceText(count: outputCount, workMeter: workMeter) {
            try CanonicalSQLScalarText.makeText(count: outputCount, workMeter: workMeter) { output in
                var offset = 0
                for byte in visible.utf8 {
                    try CanonicalSQLScalarText.checkpoint(offset, workMeter)
                    output[offset] = byte; offset += 1
                }
                while offset < output.count {
                    try CanonicalSQLScalarText.checkpoint(offset, workMeter)
                    output[offset] = 32; offset += 1
                }
            }
        }
    }

    private static func withInputText<Result>(_ value: QueryValue, source: QueryColumnType,
        workMeter: DatabaseWorkMeter, _ body: (borrowing Substring) throws -> Result) throws -> Result {
        try CanonicalSQLScalarText.withText(of: value, type: source, preservingCharacterPadding: false,
            workMeter: workMeter, body)
    }

    private static func validate(_ value: QueryValue, source: QueryColumnType, target: QueryColumnType,
        workMeter: DatabaseWorkMeter) throws {
        try workMeter.consume(at: .expressionEvaluation)
        guard QueryValueValidator.accepts(value, type: source),
              QueryValueValidator.accepts(.primitive(.null), type: target) else { throw invalid("selected declaration") }
    }

    static func supportsTextOutput(_ type: QueryColumnType) -> Bool {
        if CanonicalQueryTypeBinder.isCollection(type) { return true }
        if CanonicalQueryTypeBinder.temporalType(type) != nil { return true }
        switch type {
        case .sql(.smallint), .sql(.integer), .sql(.bigint), .sql(.real), .sql(.doublePrecision),
             .sql(.decimal), .sql(.boolean), .sql(.text), .sql(.char), .sql(.varchar), .sql(.uuid),
             .sql(.blob), .sql(.binary), .sql(.varbinary): return true
        case .primitive(let kind, false):
            switch kind {
            case .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64,
                 .float32, .float64, .decimal, .bool, .string, .uuid, .bytes: return true
            default: return false
            }
        default: return false
        }
    }

    private static func primitiveNumericType(_ type: QueryColumnType) -> DataType? {
        switch type {
        case .primitive(.bool, false): return .boolean
        case .primitive(.int16, false): return .smallint
        case .primitive(.int32, false): return .integer
        case .primitive(.int64, false): return .bigint
        case .primitive(.float32, false): return .real
        case .primitive(.float64, false): return .doublePrecision
        case .primitive(.int8, false), .primitive(.uint8, false), .primitive(.uint16, false),
             .primitive(.uint32, false), .primitive(.uint64, false), .primitive(.decimal, false):
            return .decimal(precision: nil, scale: nil)
        default: return nil
        }
    }

    private static func isText(_ type: QueryColumnType) -> Bool {
        switch type {
        case .sql(.text), .sql(.char), .sql(.varchar), .primitive(.string, false): return true
        default: return false
        }
    }
    private static func isCharacter(_ type: QueryColumnType) -> Bool {
        if case .sql(.char) = type { return true }; return false
    }
    private static func isUUID(_ type: QueryColumnType) -> Bool {
        type == .sql(.uuid) || type == .primitive(.uuid, isArray: false)
    }
    private static func isBinary(_ type: QueryColumnType) -> Bool {
        switch type {
        case .sql(.blob), .sql(.binary), .sql(.varbinary), .primitive(.bytes, false): return true
        default: return false
        }
    }
    private static func asciiTrim(_ text: borrowing Substring, workMeter: DatabaseWorkMeter) throws -> Substring {
        try CanonicalSQLScalarText.charge(text.utf8.count, workMeter)
        var start = text.utf8.startIndex, end = text.utf8.endIndex
        var examined = 0
        func whitespace(_ byte: UInt8) -> Bool { byte == 32 || (9...13).contains(byte) }
        while start != end, whitespace(text.utf8[start]) {
            try CanonicalSQLScalarText.checkpoint(examined, workMeter); examined += 1
            text.utf8.formIndex(after: &start)
        }
        while start != end {
            try CanonicalSQLScalarText.checkpoint(examined, workMeter); examined += 1
            let previous = text.utf8.index(before: end)
            guard whitespace(text.utf8[previous]) else { break }; end = previous
        }
        return text[start..<end]
    }
    private static func boolean(_ text: borrowing Substring, workMeter: DatabaseWorkMeter) throws -> Bool {
        let input = try asciiTrim(text, workMeter: workMeter)
        func prefix(_ word: StaticString, minimum: Int = 1) -> Bool {
            let count = input.utf8.count
            guard count >= minimum, count <= word.utf8CodeUnitCount else { return false }
            return word.withUTF8Buffer { expected in
                var index = 0
                for byte in input.utf8 {
                    let folded = (65...90).contains(byte) ? byte + 32 : byte
                    if folded != expected[index] { return false }; index += 1
                }
                return true
            }
        }
        if input == "1" || prefix("true") || prefix("yes") || prefix("on", minimum: 2) { return true }
        if input == "0" || prefix("false") || prefix("no") || prefix("off", minimum: 2) { return false }
        throw invalid("BOOLEAN")
    }
    private static func integerLexical(_ text: borrowing Substring, workMeter: DatabaseWorkMeter) throws -> Bool {
        let text = try asciiTrim(text, workMeter: workMeter)
        try CanonicalSQLScalarText.charge(text.utf8.count, workMeter)
        var bytes = text.utf8.makeIterator()
        var first = bytes.next()
        if first == 43 || first == 45 { first = bytes.next() }
        if first == 48, let second = bytes.next(), second == 120 || second == 88 || second == 111 || second == 79 || second == 98 || second == 66 {
            return true
        }
        for (offset, byte) in text.utf8.enumerated() {
            try CanonicalSQLScalarText.checkpoint(offset, workMeter)
            if byte == 46 || byte == 101 || byte == 69 { return false }
        }
        return true
    }
    private static func nonzeroMantissa(_ text: borrowing Substring, workMeter: DatabaseWorkMeter) throws -> Bool {
        try CanonicalSQLScalarText.charge(text.utf8.count, workMeter)
        var bytes = text.utf8.makeIterator()
        var first = bytes.next()
        if first == 43 || first == 45 { first = bytes.next() }
        let second = bytes.next()
        let hexadecimal = first == 48 && (second == 120 || second == 88)
        var byte = first
        var next = second
        var examined = 0
        while let current = byte {
            try CanonicalSQLScalarText.checkpoint(examined, workMeter); examined += 1
            if hexadecimal ? (current == 112 || current == 80) : (current == 101 || current == 69) { break }
            if (49...57).contains(current) || (hexadecimal && ((65...70).contains(current) || (97...102).contains(current))) { return true }
            byte = next; next = bytes.next()
        }
        return false
    }
    private static func lexicalFailure(_ error: QueryValueError, target: String) -> any Error {
        switch error {
        case .numericOutOfRange: return DatabaseExpressionEvaluationError.numericOverflow
        case .resourceLimit, .arithmeticOverflow, .unknownRetainedByteCount: return error
        default: return invalid(target)
        }
    }
    private static func invalid(_ target: String) -> DatabaseExpressionEvaluationError { .invalidCast(target) }
}
