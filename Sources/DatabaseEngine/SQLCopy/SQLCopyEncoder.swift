import DatabaseKit
import DatabaseTypes

/// Grows one admitted output owner and borrows scalar UTF-8 only synchronously.
struct SQLCopyEncoder: ~Copyable {
    private var output: SQLArrayBuffer<UInt8>
    private let options: SQLCopyOptions
    private let delimiter: UInt8
    private let quote: UInt8
    private let escape: UInt8
    private let meter: DatabaseWorkMeter

    init(options: SQLCopyOptions, workMeter: DatabaseWorkMeter) throws {
        self.options = options; meter = workMeter
        delimiter = options.delimiter.utf8.first!; quote = options.quote.utf8.first!; escape = options.escape.utf8.first!
        output = try SQLArrayBuffer<UInt8>(meter: workMeter)
    }

    mutating func separator() throws { try output.append(delimiter) }
    mutating func endRecord() throws { try output.append(10) }
    mutating func null() throws {
        for byte in options.null.utf8 { try output.append(byte) }
    }

    mutating func field(_ text: borrowing Substring) throws {
        try meter.consume(UInt64(text.utf8.count), at: .resultMaterialization)
        switch options.format {
        case .csv:
            let quoted = text.utf8.elementsEqual(options.null.utf8)
                || text.utf8.contains { $0 == delimiter || $0 == quote || $0 == 10 || $0 == 13 }
            if quoted { try output.append(quote) }
            for byte in text.utf8 {
                if quoted, byte == quote || byte == escape { try output.append(escape) }
                try output.append(byte)
            }
            if quoted { try output.append(quote) }
        case .text:
            // An exact NULL marker must be distinguished even if it has no normally escaped byte.
            let nullCollision = text.utf8.elementsEqual(options.null.utf8)
            var position = 0
            for byte in text.utf8 {
                switch byte {
                case 8: try escaped(98)
                case 9: try escaped(116)
                case 10: try escaped(110)
                case 11: try escaped(118)
                case 12: try escaped(102)
                case 13: try escaped(114)
                case 92: try escaped(92)
                default:
                    if byte == delimiter { try escaped(byte) }
                    else if nullCollision && position == 0 {
                        // Three octal digits avoid consuming following numeric field bytes.
                        try output.append(92)
                        try output.append(48 + ((byte >> 6) & 7))
                        try output.append(48 + ((byte >> 3) & 7))
                        try output.append(48 + (byte & 7))
                    } else { try output.append(byte) }
                }
                position += 1
            }
            // TEXT with an empty NULL marker cannot represent a non-NULL empty value.
            if nullCollision && text.isEmpty { throw SQLCopyExecutionError.invalidTarget }
        }
    }

    consuming func finish(affectedRows: UInt64) -> (SQLCopyResult, DatabaseIntermediateReservation) {
        let result = output.finish()
        return (SQLCopyResult(bytes: ByteString(result.values), affectedRows: affectedRows), result.reservation)
    }

    private mutating func escaped(_ byte: UInt8) throws {
        try output.append(92); try output.append(byte)
    }
}
