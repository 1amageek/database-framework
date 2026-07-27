import DatabaseKit
import StorageKit

package struct CanonicalFieldValueTupleElement: TupleElement {
    package let prepared: FieldValueTupleCodec.Prepared

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
        from bytes: Bytes,
        at offset: inout Int
    ) throws -> CanonicalFieldValueTupleElement {
        let payload = try Bytes.decodeTuple(from: bytes, at: &offset)
        let value = try FieldValueTupleCodec.decode(payload)
        return try CanonicalFieldValueTupleElement(
            prepared: FieldValueTupleCodec.prepareComposite(
                value,
                limits: .default
            )
        )
    }
}
