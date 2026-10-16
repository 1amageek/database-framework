import DatabaseKit
import DatabaseTypes

/// Executes selected C/POSIX text signatures without changing raw evaluator semantics.
enum CanonicalSQLStringEvaluator {
    static func evaluate(
        _ bound: CanonicalBoundExpression,
        operands: [QueryValue],
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseQueryScopedFieldValue {
        guard operands.count == bound.children.count else { throw mismatch("selected text signature") }
        let name: String
        switch bound.expression {
        case .function(let function): name = function.name
        case .concatenate: name = "||"
        default: throw mismatch("selected text signature")
        }
        try validate(name, bound: bound)
        for index in operands.indices {
            guard QueryValueValidator.accepts(operands[index], type: bound.children[index].type) else {
                throw mismatch("selected text input")
            }
        }
        try workMeter.consume(at: .expressionEvaluation)
        func text<Result>(_ index: Int, padding: Bool = false,
                          _ body: (borrowing Substring) throws -> Result) throws -> Result {
            try CanonicalSQLScalarText.withText(of: operands[index], type: bound.children[index].type,
                preservingCharacterPadding: padding, workMeter: workMeter, body)
        }
        func integer(_ index: Int) throws -> Int64 {
            guard case .primitive(.int32(let value)) = operands[index] else { throw mismatch(name) }
            return Int64(value)
        }
        if name == "concat" || name == "concat_ws" {
            if name == "concat_ws", operands[0].isNull { return .borrowing(.null) }
            func concatenate(_ separator: Substring) throws -> DatabaseQueryScopedFieldValue {
                let start = name == "concat_ws" ? 1 : 0
                var bytes: UInt64 = 0
                var count: UInt64 = 0
                for index in start..<operands.count where !operands[index].isNull {
                    try text(index, padding: true) { value in
                        bytes = try adding(bytes, UInt64(value.utf8.count))
                    }
                    count += 1
                }
                if count > 1 { bytes = try adding(bytes, multiplying(UInt64(separator.utf8.count), count - 1)) }
                return try produce(bytes: bytes, workMeter: workMeter) { output in
                    var first = true
                    for index in start..<operands.count where !operands[index].isNull {
                        if !first { try output.append(separator) }
                        try text(index, padding: true) { value in try output.append(value) }
                        first = false
                    }
                }
            }
            if name == "concat_ws" { return try text(0) { try concatenate($0) } }
            return try concatenate(Substring())
        }
        if operands.contains(where: { $0.isNull }) { return .borrowing(.null) }
        if name == "chr" {
            let value = try integer(0)
            guard value > 0, value <= 0x10FFFF, !(0xD800...0xDFFF).contains(value),
                  let scalar = Unicode.Scalar(UInt32(value)) else { throw invalid(name, "invalid Unicode scalar") }
            return try produce(bytes: UInt64(width(scalar)), workMeter: workMeter) { try $0.append(scalar) }
        }
        return try text(0, padding: name == "octet_length" || name == "bit_length") { source in
            switch name {
            case "||":
                return try text(1) { right in
                    let bytes = try adding(UInt64(source.utf8.count), UInt64(right.utf8.count))
                    return try produce(bytes: bytes, workMeter: workMeter) { output in
                        try output.append(source)
                        try output.append(right)
                    }
                }
            case "lower", "upper":
                return try produce(bytes: UInt64(source.utf8.count), workMeter: workMeter) { output in
                    for byte in source.utf8 {
                        let mapped: UInt8
                        if name == "lower", (65...90).contains(byte) { mapped = byte + 32 }
                        else if name == "upper", (97...122).contains(byte) { mapped = byte - 32 }
                        else { mapped = byte }
                        try output.append(mapped)
                    }
                }
            case "length", "char_length", "character_length":
                return try countResult(count(source, workMeter: workMeter))
            case "octet_length", "bit_length":
                let bytes = UInt64(source.utf8.count)
                try DatabaseByteProcessingMeter.consume(byteCount: bytes, workMeter: workMeter, stage: .expressionEvaluation)
                return try countResult(name == "bit_length" ? multiplying(bytes, 8) : bytes)
            case "ascii":
                return .borrowing(.int32(Int32(source.unicodeScalars.first?.value ?? 0)))
            case "reverse":
                return try produce(bytes: UInt64(source.utf8.count), workMeter: workMeter) { output in
                    for scalar in source.unicodeScalars.reversed() { try output.append(scalar) }
                }
            case "substring", "substr":
                let start = try integer(1)
                let length = operands.count == 3 ? try integer(2) : nil
                if let length, length < 0 { throw invalid(name, "negative substring length") }
                let total = try count(source, workMeter: workMeter)
                let lower = min(total, UInt64(max(0, start - 1)))
                let upper = length.map { min(total, UInt64(max(0, start - 1 + $0))) } ?? total
                return try copy(slice(source, start: lower, end: max(lower, upper), workMeter: workMeter), workMeter: workMeter)
            case "left", "right":
                let requested = try integer(1)
                let total = try count(source, workMeter: workMeter)
                let length = requested >= 0 ? min(UInt64(requested), total)
                    : total - min(UInt64(-requested), total)
                let start = name == "left" ? 0 : total - length
                return try copy(slice(source, start: start, end: start + length, workMeter: workMeter), workMeter: workMeter)
            case "repeat":
                let repetitions = max(0, try integer(1))
                let bytes = try multiplying(UInt64(source.utf8.count), UInt64(repetitions))
                return try produce(bytes: bytes, workMeter: workMeter) { output in
                    if !source.isEmpty { for _ in 0..<repetitions { try output.append(source) } }
                }
            case "strpos", "position":
                return try text(1) { pattern in
                    if pattern.isEmpty { return .borrowing(.int32(1)) }
                    guard let range = try firstRange(pattern, in: source, workMeter: workMeter) else {
                        return .borrowing(.int32(0))
                    }
                    return try countResult(adding(count(source[..<range.lowerBound], workMeter: workMeter), 1))
                }
            case "btrim", "trim", "ltrim", "rtrim":
                func trimmed(_ characters: Substring) throws -> DatabaseQueryScopedFieldValue {
                    var lower = source.unicodeScalars.startIndex
                    var upper = source.unicodeScalars.endIndex
                    var scan = Scan(workMeter: workMeter)
                    func contains(_ scalar: Unicode.Scalar) throws -> Bool {
                        for candidate in characters.unicodeScalars {
                            try scan.step()
                            if candidate == scalar { return true }
                        }
                        return false
                    }
                    if name != "rtrim" {
                        while lower < upper, try contains(source.unicodeScalars[lower]) {
                            source.unicodeScalars.formIndex(after: &lower)
                        }
                    }
                    if name != "ltrim" {
                        while lower < upper {
                            let previous = source.unicodeScalars.index(before: upper)
                            guard try contains(source.unicodeScalars[previous]) else { break }
                            upper = previous
                        }
                    }
                    try scan.finish()
                    return try copy(source[lower..<upper], workMeter: workMeter)
                }
                if operands.count == 2 { return try text(1) { try trimmed($0) } }
                return try trimmed(" ")
            case "lpad", "rpad":
                let target = max(0, try integer(1))
                let sourceCount = try count(source, workMeter: workMeter)
                if UInt64(target) <= sourceCount {
                    return try copy(slice(source, start: 0, end: UInt64(target), workMeter: workMeter), workMeter: workMeter)
                }
                func padded(_ fill: Substring) throws -> DatabaseQueryScopedFieldValue {
                    let fillCount = try count(fill, workMeter: workMeter)
                    if fillCount == 0 { return try copy(source, workMeter: workMeter) }
                    let missing = UInt64(target) - sourceCount
                    let repetitions = missing / fillCount
                    let suffix = try slice(fill, start: 0, end: missing % fillCount, workMeter: workMeter)
                    let bytes = try adding(UInt64(source.utf8.count),
                        adding(multiplying(UInt64(fill.utf8.count), repetitions), UInt64(suffix.utf8.count)))
                    return try produce(bytes: bytes, workMeter: workMeter) { output in
                        if name == "rpad" { try output.append(source) }
                        for _ in 0..<repetitions { try output.append(fill) }
                        try output.append(suffix)
                        if name == "lpad" { try output.append(source) }
                    }
                }
                if operands.count == 3 { return try text(2) { try padded($0) } }
                return try padded(" ")
            case "replace":
                return try text(1) { pattern in try text(2) { replacement in
                    if pattern.isEmpty { return try copy(source, workMeter: workMeter) }
                    var bytes = UInt64(source.utf8.count)
                    var remaining = source
                    while let range = try firstRange(pattern, in: remaining, workMeter: workMeter) {
                        bytes -= UInt64(pattern.utf8.count)
                        bytes = try adding(bytes, UInt64(replacement.utf8.count))
                        remaining = remaining[range.upperBound...]
                    }
                    return try produce(bytes: bytes, workMeter: workMeter) { output in
                        var remaining = source
                        while let range = try firstRange(pattern, in: remaining, workMeter: workMeter) {
                            try output.append(remaining[..<range.lowerBound])
                            try output.append(replacement)
                            remaining = remaining[range.upperBound...]
                        }
                        try output.append(remaining)
                    }
                } }
            case "translate":
                return try text(1) { from in try text(2) { to in
                    func translated(_ scalar: Unicode.Scalar, scan: inout Scan) throws -> Unicode.Scalar? {
                        var target = to.unicodeScalars.makeIterator()
                        for candidate in from.unicodeScalars {
                            try scan.step()
                            let replacement = target.next()
                            if candidate == scalar { return replacement }
                        }
                        return scalar
                    }
                    var scan = Scan(workMeter: workMeter)
                    var bytes: UInt64 = 0
                    for scalar in source.unicodeScalars {
                        if let value = try translated(scalar, scan: &scan) { bytes = try adding(bytes, UInt64(width(value))) }
                        try scan.step()
                    }
                    try scan.finish()
                    return try produce(bytes: bytes, workMeter: workMeter) { output in
                        var scan = Scan(workMeter: workMeter)
                        for scalar in source.unicodeScalars {
                            if let value = try translated(scalar, scan: &scan) { try output.append(value) }
                            try scan.step()
                        }
                        try scan.finish()
                    }
                } }
            case "split_part":
                let requested = try integer(2)
                guard requested != 0 else { throw invalid(name, "field index must not be zero") }
                return try text(1) { delimiter in
                    if delimiter.isEmpty {
                        return try copy(requested == 1 || requested == -1 ? source : Substring(), workMeter: workMeter)
                    }
                    var total: UInt64 = 1
                    if requested < 0 {
                        var remaining = source
                        while let range = try firstRange(delimiter, in: remaining, workMeter: workMeter) {
                            total = try adding(total, 1)
                            remaining = remaining[range.upperBound...]
                        }
                    }
                    let target: UInt64
                    if requested > 0 { target = UInt64(requested) }
                    else {
                        let offset = UInt64(-requested)
                        if offset > total { return try copy(Substring(), workMeter: workMeter) }
                        target = try adding(total - offset, 1)
                    }
                    var field: UInt64 = 1
                    var remaining = source
                    while let range = try firstRange(delimiter, in: remaining, workMeter: workMeter) {
                        if field == target { return try copy(remaining[..<range.lowerBound], workMeter: workMeter) }
                        remaining = remaining[range.upperBound...]
                        field = try adding(field, 1)
                    }
                    return try copy(field == target ? remaining : Substring(), workMeter: workMeter)
                }
            default: throw DatabaseExpressionEvaluationError.unsupportedFunction(name)
            }
        }
    }

    private static func validate(_ name: String, bound: CanonicalBoundExpression) throws {
        let types = bound.children
        func text(_ index: Int) -> Bool {
            guard types.indices.contains(index) else { return false }
            switch types[index].type {
            case .sql(.text), .sql(.char), .sql(.varchar), .primitive(.string, false): return true
            default: return false
            }
        }
        func integer(_ index: Int) -> Bool {
            types.indices.contains(index) && (types[index].type == .sql(.integer) || types[index].type == .primitive(.int32))
        }
        let valid: Bool
        let integerResult: Bool
        switch name {
        case "length", "char_length", "character_length", "octet_length", "bit_length", "ascii":
            valid = types.count == 1 && text(0); integerResult = true
        case "lower", "upper", "reverse": valid = types.count == 1 && text(0); integerResult = false
        case "chr": valid = types.count == 1 && integer(0); integerResult = false
        case "strpos", "position": valid = types.count == 2 && text(0) && text(1); integerResult = true
        case "||": valid = types.count == 2 && text(0) && text(1); integerResult = false
        case "substring", "substr":
            valid = (types.count == 2 || types.count == 3) && text(0) && integer(1) && (types.count == 2 || integer(2))
            integerResult = false
        case "left", "right", "repeat": valid = types.count == 2 && text(0) && integer(1); integerResult = false
        case "lpad", "rpad":
            valid = (types.count == 2 || types.count == 3) && text(0) && integer(1) && (types.count == 2 || text(2))
            integerResult = false
        case "trim", "btrim", "ltrim", "rtrim":
            valid = (types.count == 1 || types.count == 2) && text(0) && (types.count == 1 || text(1)); integerResult = false
        case "replace", "translate": valid = types.count == 3 && text(0) && text(1) && text(2); integerResult = false
        case "split_part": valid = types.count == 3 && text(0) && text(1) && integer(2); integerResult = false
        case "concat": valid = true; integerResult = false
        case "concat_ws": valid = !types.isEmpty && text(0); integerResult = false
        default: throw DatabaseExpressionEvaluationError.unsupportedFunction(name)
        }
        guard valid, bound.type == .sql(integerResult ? .integer : .text) else { throw mismatch(name) }
        if name == "concat" || name == "concat_ws" {
            for index in (name == "concat_ws" ? 1 : 0)..<types.count {
                let admitted: Bool
                switch types[index].type {
                case .sql(.boolean), .sql(.smallint), .sql(.integer), .sql(.bigint), .sql(.real),
                     .sql(.doublePrecision), .sql(.decimal), .sql(.text), .sql(.char), .sql(.varchar),
                     .sql(.uuid), .sql(.binary), .sql(.varbinary), .sql(.blob): admitted = true
                case .primitive(let kind, false):
                    switch kind {
                    case .bool, .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64,
                         .float32, .float64, .decimal, .string, .uuid, .bytes: admitted = true
                    default: admitted = false
                    }
                default: admitted = false
                }
                guard admitted else {
                    throw DatabaseExpressionEvaluationError.unsupportedExpression("CONCAT argument output type")
                }
            }
        }
    }

    private static func count(_ value: Substring, workMeter: DatabaseWorkMeter) throws -> UInt64 {
        var result: UInt64 = 0
        var scan = Scan(workMeter: workMeter)
        for scalar in value.unicodeScalars { result += 1; try scan.step(width(scalar)) }
        try scan.finish()
        return result
    }

    private static func slice(_ value: Substring, start: UInt64, end: UInt64,
                              workMeter: DatabaseWorkMeter) throws -> Substring {
        var index = value.unicodeScalars.startIndex
        var lower = index
        var offset: UInt64 = 0
        var scan = Scan(workMeter: workMeter)
        while offset < end, index < value.unicodeScalars.endIndex {
            if offset == start { lower = index }
            try scan.step(width(value.unicodeScalars[index]))
            value.unicodeScalars.formIndex(after: &index)
            offset += 1
        }
        if start == end { lower = index }
        try scan.finish()
        return value[lower..<index]
    }

    private static func firstRange(_ pattern: Substring, in source: Substring,
                                   workMeter: DatabaseWorkMeter) throws -> Range<String.Index>? {
        if pattern.isEmpty { return source.startIndex..<source.startIndex }
        var candidate = source.utf8.startIndex
        var scan = Scan(workMeter: workMeter)
        while candidate < source.utf8.endIndex {
            var input = candidate
            var wanted = pattern.utf8.startIndex
            while input < source.utf8.endIndex, wanted < pattern.utf8.endIndex {
                try scan.step()
                if source.utf8[input] != pattern.utf8[wanted] { break }
                source.utf8.formIndex(after: &input)
                pattern.utf8.formIndex(after: &wanted)
            }
            if wanted == pattern.utf8.endIndex { try scan.finish(); return candidate..<input }
            source.utf8.formIndex(after: &candidate)
        }
        try scan.finish()
        return nil
    }

    private static func countResult(_ value: UInt64) throws -> DatabaseQueryScopedFieldValue {
        guard let count = Int32(exactly: value) else { throw DatabaseExpressionEvaluationError.numericOverflow }
        return .borrowing(.int32(count))
    }

    private static func copy(_ source: Substring, workMeter: DatabaseWorkMeter) throws -> DatabaseQueryScopedFieldValue {
        try produce(bytes: UInt64(source.utf8.count), workMeter: workMeter) { try $0.append(source) }
    }

    private static func produce(bytes: UInt64, workMeter: DatabaseWorkMeter,
                                _ write: (inout Output) throws -> Void) throws -> DatabaseQueryScopedFieldValue {
        guard let capacity = Int(exactly: bytes) else { throw DatabaseIntermediateFootprintError.canonicalValueByteCountUnavailable }
        let footprint = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint(retainedByteCount: bytes)
        return try .producing(maximumFootprint: footprint, workMeter: workMeter, stage: .expressionEvaluation) {
            let result = try String(unsafeUninitializedCapacity: capacity) { buffer in
                var output = Output(buffer: buffer, scan: Scan(workMeter: workMeter))
                try write(&output)
                try output.scan.finish()
                guard output.offset == capacity else { throw mismatch("text output byte count") }
                return output.offset
            }
            return .string(result)
        }
    }

    // The String initializer owns and frees the buffer exactly once, including throws.
    // Output borrows that initialized-prefix buffer only inside its synchronous closure.
    // Every write checks its offset; only 0..<offset is initialized before publication.
    // UInt8 stride/alignment are one, with no rebinding; the inout writer is the sole mutable borrower.
    // Valid UTF-8 inputs and validated scalars supply encoding; no pointer or writer crosses a Sendable boundary.
    private struct Output {
        let buffer: UnsafeMutableBufferPointer<UInt8>
        var scan: Scan
        var offset = 0

        mutating func append(_ byte: UInt8) throws {
            guard offset < buffer.count else { throw mismatch("text output byte bound") }
            buffer[offset] = byte
            offset += 1
            try scan.step()
        }

        mutating func append(_ value: Substring) throws {
            for byte in value.utf8 { try append(byte) }
        }

        mutating func append(_ scalar: Unicode.Scalar) throws {
            let value = scalar.value
            switch value {
            case ...0x7F: try append(UInt8(value))
            case ...0x7FF:
                try append(UInt8(0xC0 | (value >> 6)))
                try append(UInt8(0x80 | (value & 0x3F)))
            case ...0xFFFF:
                try append(UInt8(0xE0 | (value >> 12)))
                try append(UInt8(0x80 | ((value >> 6) & 0x3F)))
                try append(UInt8(0x80 | (value & 0x3F)))
            default:
                try append(UInt8(0xF0 | (value >> 18)))
                try append(UInt8(0x80 | ((value >> 12) & 0x3F)))
                try append(UInt8(0x80 | ((value >> 6) & 0x3F)))
                try append(UInt8(0x80 | (value & 0x3F)))
            }
        }
    }

    private struct Scan {
        let workMeter: DatabaseWorkMeter
        var pending = 0
        mutating func step(_ amount: Int = 1) throws {
            pending += amount
            if pending >= 256 {
                try workMeter.consume(UInt64(pending / 256), at: .expressionEvaluation)
                pending %= 256
            }
        }
        mutating func finish() throws {
            if pending > 0 { try workMeter.consume(at: .expressionEvaluation); pending = 0 }
            else { try workMeter.checkpoint(at: .expressionEvaluation) }
        }
    }

    private static func width(_ scalar: Unicode.Scalar) -> Int {
        switch scalar.value { case ...0x7F: return 1; case ...0x7FF: return 2; case ...0xFFFF: return 3; default: return 4 }
    }
    private static func adding(_ left: UInt64, _ right: UInt64) throws -> UInt64 {
        try DatabaseIntermediateFootprint(bytes: left).adding(DatabaseIntermediateFootprint(bytes: right)).bytes
    }
    private static func multiplying(_ left: UInt64, _ right: UInt64) throws -> UInt64 {
        try DatabaseIntermediateFootprint(bytes: left).multiplied(by: right).bytes
    }
    private static func mismatch(_ operation: String) -> DatabaseExpressionEvaluationError { .typeMismatch(operation: operation) }
    private static func invalid(_ function: String, _ reason: String) -> DatabaseExpressionEvaluationError {
        .invalidFunctionArgument(function: function, reason: reason)
    }
}
