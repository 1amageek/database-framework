import DatabaseTypes
import StorageKit

extension DatabaseTypes.UUID: @retroactive TupleElement {
    public func encodeTuple(to sink: inout TupleEncodingSink) {
        sink.writeByte(TupleTypeCode.uuid.rawValue)
        for byte in self {
            sink.writeByte(byte)
        }
    }

    public static func decodeTuple(
        from bytes: Bytes,
        at offset: inout Int
    ) throws -> DatabaseTypes.UUID {
        guard offset > 0, offset <= bytes.count else {
            throw TupleError.unexpectedEndOfData
        }
        guard bytes[offset - 1] == TupleTypeCode.uuid.rawValue else {
            throw TupleError.invalidTypeCode(bytes[offset - 1])
        }
        let (endOffset, overflow) = offset.addingReportingOverflow(16)
        guard !overflow, endOffset <= bytes.count else {
            throw TupleError.unexpectedEndOfData
        }

        var high: UInt64 = 0
        var low: UInt64 = 0
        for byteOffset in 0..<16 {
            let byte = bytes[offset + byteOffset]
            if byteOffset < 8 {
                high = (high << 8) | UInt64(byte)
            } else {
                low = (low << 8) | UInt64(byte)
            }
        }
        offset = endOffset
        return DatabaseTypes.UUID(high: high, low: low)
    }

    var storageBytes: Bytes {
        Bytes.copying(count: 16) { output in
            for byteOffset in 0..<16 {
                output[byteOffset] = self[byteOffset]
            }
        }
    }
}
