import DatabaseOperationCore
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

package enum PersistentJobPayloadStorage {
    package static func encode<Value: PersistentJobPayload>(
        _ value: Value,
        limits: DatabaseWireLimits
    ) throws -> ByteString {
        try DatabaseRuntimePayloadEncoder.encode(
            PersistentJobValue(value: try value.persistentJobValue()),
            limits: limits
        )
    }

    package static func decode<Value: PersistentJobPayload>(
        _ type: Value.Type,
        from bytes: ByteString,
        limits: DatabaseWireLimits
    ) throws -> Value {
        let storedValue = try DatabaseRuntimePayloadDecoder.decode(
            PersistentJobValue.self,
            from: bytes,
            limits: limits
        )
        return try Value(persistentJobValue: storedValue.value)
    }

    package static func encodedByteCount<Value: PersistentJobPayload>(
        _ value: Value,
        limits: DatabaseWireLimits
    ) throws -> Int {
        let storedValue = PersistentJobValue(
            value: try value.persistentJobValue()
        )
        return try DatabaseWireWriter.encodedByteCount(limits: limits) {
            (
                writer: inout DatabaseWireWriter
            ) throws(DatabaseWireError) in
            try storedValue.encode(into: &writer)
        }
    }
}
