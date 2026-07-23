import DatabaseValue
import StorageKit

/// Emits one canonical record identifier directly into a tuple sink.
///
/// The value retains string and byte storage through their existing owners.
/// Packing performs the single allocation required for the final key bytes.
struct RecordIdentifierTupleElement: TupleElement {
    let value: RecordIdentifierValue

    func encodeTuple(to sink: inout TupleEncodingSink) {
        Self.encode(value, to: &sink)
    }

    static func decodeTuple(
        from bytes: Bytes,
        at offset: inout Int
    ) throws -> Self {
        guard offset > 0 else {
            throw TupleError.unexpectedEndOfData
        }
        throw TupleError.invalidTypeCode(bytes[offset - 1])
    }

    private static func encode(
        _ value: RecordIdentifierValue,
        to sink: inout TupleEncodingSink
    ) {
        switch value {
        case .bool(let value):
            value.encodeTuple(to: &sink)
        case .int64(let value):
            value.encodeTuple(to: &sink)
        case .uint64(let value):
            value.encodeTuple(to: &sink)
        case .string(let value):
            value.encodeTuple(to: &sink)
        case .bytes(let value):
            sink.writeByte(TupleTypeCode.bytes.rawValue)
            value.withUnsafeBytes { bytes in
                var chunkStart = 0
                for index in bytes.indices where bytes[index] == 0 {
                    if chunkStart < index {
                        sink.writeBytes(
                            UnsafeRawBufferPointer(
                                rebasing: bytes[chunkStart..<index]
                            )
                        )
                    }
                    sink.writeByte(0)
                    sink.writeByte(0xff)
                    chunkStart = index + 1
                }
                if chunkStart < bytes.count {
                    sink.writeBytes(
                        UnsafeRawBufferPointer(
                            rebasing: bytes[chunkStart..<bytes.count]
                        )
                    )
                }
            }
            sink.writeByte(0)
        case .uuid(let value):
            sink.writeByte(TupleTypeCode.uuid.rawValue)
            sink.writeByte(UInt8(truncatingIfNeeded: value.high >> 56))
            sink.writeByte(UInt8(truncatingIfNeeded: value.high >> 48))
            sink.writeByte(UInt8(truncatingIfNeeded: value.high >> 40))
            sink.writeByte(UInt8(truncatingIfNeeded: value.high >> 32))
            sink.writeByte(UInt8(truncatingIfNeeded: value.high >> 24))
            sink.writeByte(UInt8(truncatingIfNeeded: value.high >> 16))
            sink.writeByte(UInt8(truncatingIfNeeded: value.high >> 8))
            sink.writeByte(UInt8(truncatingIfNeeded: value.high))
            sink.writeByte(UInt8(truncatingIfNeeded: value.low >> 56))
            sink.writeByte(UInt8(truncatingIfNeeded: value.low >> 48))
            sink.writeByte(UInt8(truncatingIfNeeded: value.low >> 40))
            sink.writeByte(UInt8(truncatingIfNeeded: value.low >> 32))
            sink.writeByte(UInt8(truncatingIfNeeded: value.low >> 24))
            sink.writeByte(UInt8(truncatingIfNeeded: value.low >> 16))
            sink.writeByte(UInt8(truncatingIfNeeded: value.low >> 8))
            sink.writeByte(UInt8(truncatingIfNeeded: value.low))
        case .composite(let components):
            sink.writeByte(TupleTypeCode.nested.rawValue)
            sink.withNullEscaping { nestedSink in
                for component in components {
                    encode(component, to: &nestedSink)
                }
            }
            sink.writeByte(0)
        }
    }
}
