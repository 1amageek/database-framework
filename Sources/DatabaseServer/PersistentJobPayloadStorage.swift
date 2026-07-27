import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

enum PersistentJobPayloadStorage {
    static func encode<Value: PersistentJobPayload>(
        _ value: Value,
        limits: DatabaseWireLimits
    ) throws -> ByteString {
        try ServerPayloadEncoder.encode(
            PersistentJobValue(value: try value.persistentJobValue()),
            limits: limits
        )
    }

    static func decode<Value: PersistentJobPayload>(
        _ type: Value.Type,
        from bytes: ByteString,
        limits: DatabaseWireLimits
    ) throws -> Value {
        let storedValue = try ServerPayloadDecoder.decode(
            PersistentJobValue.self,
            from: bytes,
            limits: limits
        )
        return try Value(persistentJobValue: storedValue.value)
    }

    static func encodedByteCount<Value: PersistentJobPayload>(
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
