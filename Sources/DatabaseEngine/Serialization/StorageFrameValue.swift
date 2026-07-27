import DatabaseTypes

/// A value encoded exclusively for engine-owned persistent metadata.
package protocol StorageFrameValue: Sendable {
    func encode(
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError)

    init(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError)
}

/// Encodes and decodes one complete bounded engine-owned metadata frame.
package enum StorageFrameCodec {
    package static func encode<Value: StorageFrameValue>(
        _ value: Value,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> ByteString {
        try StorageFrameEncoder.encode(limits: limits) {
            (encoder: inout StorageFrameEncoder) throws(StorageFrameError) in
            try value.encode(to: &encoder)
        }
    }

    package static func decode<Value: StorageFrameValue>(
        _ type: Value.Type,
        from bytes: ByteString,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> Value {
        var decoder = try StorageFrameDecoder(bytes, limits: limits)
        let value = try Value(from: &decoder)
        try decoder.ensureFullyRead()
        return value
    }
}
