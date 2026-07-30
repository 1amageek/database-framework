import DatabaseTypes
import DatabaseKit
import StorageKit

package struct CanonicalFieldValueTupleElement: TupleElement {
    package let prepared: FieldValueTupleCodec.Prepared

    package var tupleValue: TupleValue? {
        let packed = encodeTuple()
        // Canonical field payloads contain no NUL bytes, so the physical tuple
        // spelling is exactly: bytes type code, payload, terminator.
        return .bytes(packed[(packed.startIndex + 1)..<(packed.endIndex - 1)])
    }

    package static func == (
        lhs: CanonicalFieldValueTupleElement,
        rhs: CanonicalFieldValueTupleElement
    ) -> Bool {
        lhs.prepared.value == rhs.prepared.value
    }

    package func hash(into hasher: inout Hasher) {
        hasher.combine(prepared.value)
    }

    package func encodeTuple(to sink: inout TupleEncodingSink) {
        FieldValueTupleCodec.write(prepared, to: &sink)
    }

    package static func decodeTuple(
        from bytes: ByteString,
        at offset: inout Int
    ) throws -> CanonicalFieldValueTupleElement {
        let payload = try ByteString.decodeTuple(from: bytes, at: &offset)
        let value = try FieldValueTupleCodec.decode(payload)
        return try CanonicalFieldValueTupleElement(
            prepared: FieldValueTupleCodec.prepareComposite(
                value,
                limits: .default
            )
        )
    }
}
