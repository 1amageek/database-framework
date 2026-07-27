import DatabaseTypes

enum CanonicalDirectoryPartitionCodec {
    private static let prefix = "dbp1-"

    static func encode(_ value: FieldValue) throws -> String {
        // Directory APIs require an owned String. The bounded storage frame is
        // the only intermediate materialization before that output boundary.
        let bytes = try StorageFrameEncoder.encode {
            writer throws(StorageFrameError) in
            try StorageValueEncoder.write(value, into: &writer)
        }

        var encoded = prefix
        encoded.reserveCapacity(prefix.count + bytes.count * 2)
        for byte in bytes {
            encoded.append(hexDigit(byte >> 4))
            encoded.append(hexDigit(byte & 0x0f))
        }
        return encoded
    }

    private static func hexDigit(_ value: UInt8) -> Character {
        Character(UnicodeScalar(value < 10 ? 48 + value : 87 + value))
    }
}
