import DatabaseKit
import DatabaseTypes

/// BYTEA kernels for signatures selected before row evaluation.
enum CanonicalSQLBinaryEvaluator {
    static func evaluate(_ bound: CanonicalBoundExpression, operands: [QueryValue],
        workMeter: DatabaseWorkMeter) throws -> DatabaseQueryScopedFieldValue {
        guard operands.count == bound.children.count else { throw mismatch("binary arity") }
        for index in operands.indices {
            guard QueryValueValidator.accepts(operands[index], type: bound.children[index].type) else {
                throw mismatch("binary selected input")
            }
        }
        try workMeter.consume(at: .expressionEvaluation)
        let name: String
        switch bound.expression {
        case .concatenate: name = "bytea concatenation"
        case .function(let function): name = function.name
        default: throw mismatch("binary expression")
        }
        if operands.contains(where: { $0.isNull }) { return .borrowing(.null) }
        func value(_ index: Int) throws -> QueryValue {
            guard operands.indices.contains(index) else { throw mismatch("binary arity") }
            return operands[index]
        }
        func bytes(_ index: Int) throws -> ByteString {
            guard case .primitive(.bytes(let bytes)) = try value(index) else { throw mismatch(name) }
            return bytes
        }
        func integer(_ index: Int) throws -> Int64 { try CanonicalSQLNumericEvaluator.integer(value(index)) }
        func intResult(_ count: Int64) throws -> DatabaseQueryScopedFieldValue {
            guard let count = Int32(exactly: count) else { throw DatabaseExpressionEvaluationError.numericOverflow }
            return .borrowing(.int32(count))
        }
        switch name {
        case "length", "octet_length": return try intResult(Int64(bytes(0).count))
        case "bit_length":
            let count = try bytes(0).count
            guard count <= Int(Int32.max) / 8 else { throw DatabaseExpressionEvaluationError.numericOverflow }
            return try intResult(Int64(count) * 8)
        case "get_byte", "get_bit", "set_byte", "set_bit":
            let input = try bytes(0), index = try integer(1)
            let bit = name == "get_bit" || name == "set_bit"
            guard index >= 0, (bit ? index / 8 : index) < Int64(input.count) else {
                throw invalid(name, "offset outside the input")
            }
            let byteIndex = Int(bit ? index / 8 : index), bitIndex = Int(index % 8)
            if name == "get_byte" { return .borrowing(.int32(Int32(input[byteIndex]))) }
            if name == "get_bit" { return .borrowing(.int32(Int32((input[byteIndex] >> bitIndex) & 1))) }
            let replacement = try integer(2)
            if bit && replacement != 0 && replacement != 1 { throw invalid(name, "bit must be zero or one") }
            try CanonicalSQLScalarText.charge(input.count, workMeter)
            return try produceBytes(count: UInt64(input.count), workMeter: workMeter) {
                try ByteString.copying(count: input.count) { output in
                    try input.withUnsafeBytes { source in
                        for offset in source.indices {
                            try CanonicalSQLScalarText.checkpoint(offset, workMeter)
                            output[offset] = source[offset]
                        }
                    }
                    if bit {
                        let mask = UInt8(1) << bitIndex
                        output[byteIndex] = replacement == 0 ? output[byteIndex] & ~mask : output[byteIndex] | mask
                    } else { output[byteIndex] = UInt8(truncatingIfNeeded: replacement) }
                }
            }
        case "substr", "substring":
            let input = try bytes(0), start = try integer(1)
            let end: Int64
            if operands.count == 3 {
                let length = try integer(2)
                guard length >= 0 else { throw invalid(name, "negative substring length") }
                let sum = start.addingReportingOverflow(length)
                // PostgreSQL's selected INT32 substring overflow runs through the end.
                end = sum.overflow || sum.partialValue > Int64(Int32.max) ? Int64(input.count) + 1 : sum.partialValue
            } else { end = Int64(input.count) + 1 }
            let lower = min(Int64(input.count), max(1, start) - 1)
            let upper = min(Int64(input.count), max(lower, end - 1))
            return .borrowing(.bytes(input[Int(lower)..<Int(upper)]))
        case "bytea concatenation":
            let left = try bytes(0), right = try bytes(1)
            let count = try CanonicalSQLScalarText.checkedCount(UInt64(left.count), addition: UInt64(right.count))
            guard let capacity = Int(exactly: count) else { throw DatabaseIntermediateFootprintError.byteAdditionOverflow(left: count, right: 0) }
            try CanonicalSQLScalarText.charge(capacity, workMeter)
            return try produceBytes(count: count, workMeter: workMeter) {
                try ByteString.copying(count: capacity) { output in
                    var offset = 0
                    try left.withUnsafeBytes { source in
                        for byte in source {
                            try CanonicalSQLScalarText.checkpoint(offset, workMeter)
                            output[offset] = byte; offset += 1
                        }
                    }
                    try right.withUnsafeBytes { source in
                        for byte in source {
                            try CanonicalSQLScalarText.checkpoint(offset, workMeter)
                            output[offset] = byte; offset += 1
                        }
                    }
                }
            }
        case "encode", "decode":
            guard case .primitive(.string(let format)) = try value(1) else { throw mismatch(name) }
            let encoding = try encoding(format, function: name, workMeter: workMeter)
            if name == "encode" { return try encode(bytes(0), encoding: encoding, workMeter: workMeter) }
            guard case .primitive(.string(let text)) = try value(0) else { throw mismatch(name) }
            let count = try decode(text[...], encoding: encoding, workMeter: workMeter) { _ in }
            return try produceBytes(count: UInt64(count), workMeter: workMeter) {
                try ByteString.copying(count: count) { output in
                    var index = 0
                    _ = try decode(text[...], encoding: encoding, workMeter: workMeter) { byte in
                        output[index] = byte; index += 1
                    }
                }
            }
        default: throw DatabaseExpressionEvaluationError.unsupportedFunction(name)
        }
    }

    private enum Encoding { case hex, base64, escape }

    private static func encoding(_ name: String, function: String, workMeter: DatabaseWorkMeter) throws -> Encoding {
        try CanonicalSQLScalarText.charge(name.utf8.count, workMeter)
        func equal(_ expected: StaticString) -> Bool {
            guard name.utf8.count == expected.utf8CodeUnitCount else { return false }
            return expected.withUTF8Buffer { expected in
                var index = 0
                for byte in name.utf8 {
                    if ((65...90).contains(byte) ? byte + 32 : byte) != expected[index] { return false }
                    index += 1
                }
                return true
            }
        }
        if equal("hex") { return .hex }
        if equal("base64") { return .base64 }
        if equal("escape") { return .escape }
        throw invalid(function, "unrecognized encoding")
    }

    private static func produceBytes(count: UInt64, workMeter: DatabaseWorkMeter,
        _ make: () throws -> ByteString) throws -> DatabaseQueryScopedFieldValue {
        let footprint = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint(retainedByteCount: count)
        return try .producing(maximumFootprint: footprint, workMeter: workMeter, stage: .expressionEvaluation) {
            .bytes(try make())
        }
    }

    private static func encode(_ input: ByteString, encoding: Encoding,
        workMeter: DatabaseWorkMeter) throws -> DatabaseQueryScopedFieldValue {
        let count: UInt64
        switch encoding {
        case .hex: count = try CanonicalSQLScalarText.checkedCount(UInt64(input.count), multiplier: 2)
        case .base64:
            let groups = try CanonicalSQLScalarText.checkedCount(UInt64(input.count), addition: 2) / 3
            count = try CanonicalSQLScalarText.checkedCount(groups, multiplier: 4, addition: UInt64(input.count / 57))
        case .escape:
            try CanonicalSQLScalarText.charge(input.count, workMeter)
            var result: UInt64 = 0
            try input.withUnsafeBytes { source in
                for index in source.indices {
                    try CanonicalSQLScalarText.checkpoint(index, workMeter)
                    let byte = source[index]
                    result = try CanonicalSQLScalarText.checkedCount(result, addition: byte == 0 || byte >= 128 ? 4 : (byte == 92 ? 2 : 1))
                }
            }
            count = result
        }
        try CanonicalSQLScalarText.charge(input.count, workMeter)
        return try CanonicalSQLScalarText.produceText(count: count, workMeter: workMeter) {
            try CanonicalSQLScalarText.makeText(count: count, workMeter: workMeter) { output in
                try input.withUnsafeBytes { source in
                    var position = 0
                    switch encoding {
                    case .hex:
                        for index in source.indices {
                            try CanonicalSQLScalarText.checkpoint(index, workMeter)
                            output[position] = CanonicalSQLScalarText.hexDigit(source[index] >> 4)
                            output[position + 1] = CanonicalSQLScalarText.hexDigit(source[index] & 15); position += 2
                        }
                    case .escape:
                        for index in source.indices {
                            try CanonicalSQLScalarText.checkpoint(index, workMeter)
                            let byte = source[index]
                            if byte == 0 || byte >= 128 {
                                output[position] = 92; output[position + 1] = (byte >> 6) + 48
                                output[position + 2] = ((byte >> 3) & 7) + 48; output[position + 3] = (byte & 7) + 48; position += 4
                            } else if byte == 92 {
                                output[position] = 92; output[position + 1] = 92; position += 2
                            } else { output[position] = byte; position += 1 }
                        }
                    case .base64:
                        let alphabet: StaticString = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
                        func digit(_ bits: UInt32) -> UInt8 {
                            alphabet.withUTF8Buffer { $0[Int(bits & 63)] }
                        }
                        var index = 0, column = 0
                        while index < source.count {
                            try CanonicalSQLScalarText.checkpoint(index, workMeter)
                            let remaining = source.count - index
                            let bits = UInt32(source[index]) << 16 |
                                (remaining > 1 ? UInt32(source[index + 1]) << 8 : 0) |
                                (remaining > 2 ? UInt32(source[index + 2]) : 0)
                            output[position] = digit(bits >> 18); output[position + 1] = digit(bits >> 12)
                            output[position + 2] = remaining > 1 ? digit(bits >> 6) : 61
                            output[position + 3] = remaining > 2 ? digit(bits) : 61
                            position += 4; column += 4; index += min(3, remaining)
                            if column == 76 && remaining >= 3 { output[position] = 10; position += 1; column = 0 }
                        }
                    }
                }
            }
        }
    }

    private static func decode(_ text: borrowing Substring, encoding: Encoding,
        workMeter: DatabaseWorkMeter, emit: (UInt8) throws -> Void) throws -> Int {
        try CanonicalSQLScalarText.charge(text.utf8.count, workMeter)
        var iterator = text.utf8.makeIterator(), offset = 0, count = 0
        func next() throws -> UInt8? {
            try CanonicalSQLScalarText.checkpoint(offset, workMeter); offset += 1
            return iterator.next()
        }
        func write(_ byte: UInt8) throws { try emit(byte); count += 1 }
        func whitespace(_ byte: UInt8) -> Bool { byte == 32 || byte == 9 || byte == 10 || byte == 13 }
        switch encoding {
        case .hex:
            func hex(_ byte: UInt8) -> UInt8? {
                switch byte {
                case 48...57: return byte - 48
                case 65...70: return byte - 55
                case 97...102: return byte - 87
                default: return nil
                }
            }
            while let first = try next() {
                if whitespace(first) { continue }
                guard let high = hex(first), let second = try next(), let low = hex(second) else {
                    throw invalid("decode", "invalid hexadecimal pair")
                }
                try write(high << 4 | low)
            }
        case .escape:
            while let byte = try next() {
                if byte != 92 { try write(byte); continue }
                guard let second = try next() else { throw invalid("decode", "incomplete escape") }
                if second == 92 { try write(92); continue }
                guard (48...51).contains(second), let third = try next(), (48...55).contains(third),
                      let fourth = try next(), (48...55).contains(fourth) else {
                    throw invalid("decode", "invalid octal escape")
                }
                try write((second - 48) << 6 | (third - 48) << 3 | (fourth - 48))
            }
        case .base64:
            var bits: UInt32 = 0, position = 0, ending = 0
            while let byte = try next() {
                if whitespace(byte) { continue }
                let value: UInt32
                switch byte {
                case 65...90: value = UInt32(byte - 65)
                case 97...122: value = UInt32(byte - 71)
                case 48...57: value = UInt32(byte + 4)
                case 43: value = 62
                case 47: value = 63
                case 61:
                    if ending == 0 {
                        guard position == 2 || position == 3 else { throw invalid("decode", "unexpected base64 padding") }
                        ending = position == 2 ? 1 : 2
                    }
                    value = 0
                default: throw invalid("decode", "invalid base64 symbol")
                }
                bits = bits << 6 | value; position += 1
                if position == 4 {
                    try write(UInt8(truncatingIfNeeded: bits >> 16))
                    if ending == 0 || ending > 1 { try write(UInt8(truncatingIfNeeded: bits >> 8)) }
                    if ending == 0 || ending > 2 { try write(UInt8(truncatingIfNeeded: bits)) }
                    bits = 0; position = 0
                }
            }
            guard position == 0 else { throw invalid("decode", "incomplete base64 sequence") }
        }
        return count
    }

    private static func invalid(_ function: String, _ reason: String) -> DatabaseExpressionEvaluationError {
        .invalidFunctionArgument(function: function, reason: reason)
    }
    private static func mismatch(_ operation: String) -> DatabaseExpressionEvaluationError { .typeMismatch(operation: operation) }
}
