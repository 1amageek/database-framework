import DatabaseKit
import DatabaseTypes

/// Retains the original input and scans bounded offsets without collecting rows.
struct SQLCopyDecoder: ~Copyable {
    private let bytes: ByteString
    private let options: SQLCopyOptions
    private let meter: DatabaseWorkMeter
    private let inputReservation: DatabaseIntermediateReservation
    private var offset = 0
    private var record = 0
    private var lineEnding: UInt8?
    private let delimiter: UInt8
    private let quote: UInt8
    private let escape: UInt8

    init(bytes: ByteString, options: SQLCopyOptions, workMeter: DatabaseWorkMeter) throws {
        guard let retained = bytes.retainedByteCount else { throw SQLCopyExecutionError.unknownRetainedInputSize }
        inputReservation = try workMeter.reserveIntermediate(bytes: UInt64(retained), at: .resultMaterialization)
        self.bytes = bytes; self.options = options; meter = workMeter
        delimiter = options.delimiter.utf8.first!; quote = options.quote.utf8.first!; escape = options.escape.utf8.first!
    }

    mutating func next(expectedColumnCount: Int) throws -> SQLCopyDecodedRow? {
        try meter.checkpoint(at: .resultMaterialization)
        guard offset < bytes.count else { return nil }
        record += 1
        var fields = try SQLArrayBuffer<String?>(meter: meter)
        try fields.reservation.reserveAdditional(rows: 1, at: .resultMaterialization)
        while true {
            let decoded = try field(number: fields.values.count + 1)
            defer { withExtendedLifetime(decoded.reservation) {} }
            if let value = decoded.value {
                // The String is the SQL literal ownership boundary; its UTF-8 bytes remain charged with this row.
                fields.reservation.absorbGuaranteedPartial(from: decoded.reservation, bytes: UInt64(value.utf8.count))
            }
            try fields.append(decoded.value)
            if decoded.endOfRecord { break }
        }
        guard fields.values.count == expectedColumnCount else {
            throw SQLCopyExecutionError.columnCount(record: record, expected: expectedColumnCount, actual: fields.values.count)
        }
        let result = fields.finish()
        return SQLCopyDecodedRow(fields: result.values, reservation: result.reservation)
    }

    private mutating func field(number: Int) throws -> (value: String?, endOfRecord: Bool, reservation: DatabaseIntermediateReservation) {
        let start = offset
        var decoded = try SQLArrayBuffer<UInt8>(meter: meter)
        var quoted = false
        var inQuotes = false
        while offset < bytes.count {
            let byte = try read()
            if options.format == .csv, inQuotes {
                offset += 1
                if byte == escape, offset < bytes.count {
                    let following = try read()
                    if following == quote || following == escape {
                        offset += 1; try decoded.append(following); continue
                    }
                }
                if byte == quote { inQuotes = false; continue }
                try decoded.append(byte)
                continue
            }
            if byte == delimiter || byte == 10 || byte == 13 { break }
            offset += 1
            if options.format == .csv, byte == quote {
                quoted = true; inQuotes = true; continue
            }
            if options.format == .text, byte == 92 {
                // Keep raw bytes until NULL detection; escapes may protect delimiters and newlines.
                try decoded.append(byte)
                guard offset < bytes.count else { throw malformed(number) }
                try decoded.append(try read()); offset += 1
            } else { try decoded.append(byte) }
        }
        guard !inQuotes else { throw malformed(number) }
        let end = offset
        let endOfRecord: Bool
        if offset == bytes.count { endOfRecord = true }
        else {
            let separator = try read(); offset += 1
            if separator == delimiter { endOfRecord = false }
            else if separator == 10 || separator == 13 {
                var ending = separator
                if separator == 13, offset < bytes.count, try read() == 10 {
                    offset += 1; ending = 14
                }
                if let lineEnding, lineEnding != ending { throw malformed(number) }
                lineEnding = ending
                endOfRecord = true
            } else { throw malformed(number) }
        }
        if !quoted, try rawEqualsNull(start..<end) {
            return (nil, endOfRecord, decoded.reservation)
        }
        if options.format == .text {
            let raw = decoded.finish()
            defer { withExtendedLifetime(raw.reservation) {} }
            decoded = try SQLArrayBuffer<UInt8>(meter: meter)
            var index = 0
            while index < raw.values.count {
                try meter.consume(at: .resultMaterialization)
                let byte = raw.values[index]; index += 1
                if byte != 92 { try decoded.append(byte); continue }
                guard index < raw.values.count else { throw malformed(number) }
                let escaped = raw.values[index]; index += 1
                switch escaped {
                case 98: try decoded.append(8)
                case 102: try decoded.append(12)
                case 110: try decoded.append(10)
                case 114: try decoded.append(13)
                case 116: try decoded.append(9)
                case 118: try decoded.append(11)
                case 48...55:
                    var value = UInt16(escaped - 48)
                    var digits = 1
                    while digits < 3, index < raw.values.count, (48...55).contains(raw.values[index]) {
                        value = value * 8 + UInt16(raw.values[index] - 48); digits += 1; index += 1
                    }
                    try decoded.append(UInt8(truncatingIfNeeded: value))
                case 120:
                    var value: UInt8 = 0
                    var digits = 0
                    while digits < 2, index < raw.values.count, let digit = hex(raw.values[index]) {
                        value = value * 16 + digit; digits += 1; index += 1
                    }
                    try decoded.append(digits == 0 ? 120 : value)
                default: try decoded.append(escaped)
                }
            }
        }
        try meter.consume(UInt64(decoded.values.count), at: .resultMaterialization)
        guard !decoded.values.contains(0) else { throw malformed(number) }
        // Reserve the String before materializing while decoded byte storage still exists.
        try decoded.reservation.reserveAdditional(bytes: UInt64(decoded.values.count), at: .resultMaterialization)
        guard let value = String(validating: decoded.values, as: UTF8.self) else {
            throw SQLCopyExecutionError.invalidUTF8(record: record, field: number)
        }
        return (value, endOfRecord, decoded.reservation)
    }

    private func read() throws -> UInt8 {
        try meter.consume(at: .resultMaterialization)
        return bytes[offset]
    }
    private func rawEqualsNull(_ range: Range<Int>) throws -> Bool {
        guard range.count == options.null.utf8.count else { return false }
        try meter.consume(UInt64(range.count), at: .resultMaterialization)
        return zip(bytes[range], options.null.utf8).allSatisfy { $0 == $1 }
    }
    private func malformed(_ field: Int) -> SQLCopyExecutionError { .malformedInput(record: record, field: field) }
    private func hex(_ byte: UInt8) -> UInt8? {
        switch byte { case 48...57: byte - 48; case 65...70: byte - 55; case 97...102: byte - 87; default: nil }
    }
}
