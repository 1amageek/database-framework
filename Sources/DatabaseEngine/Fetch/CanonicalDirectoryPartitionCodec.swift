import DatabaseValue
import DatabaseWire

enum CanonicalDirectoryPartitionCodec {
    private static let prefix = "dbp1-"

    static func encode(_ value: DatabaseValue) throws -> String {
        var writer = DatabaseWireWriter()
        try value.encode(into: &writer)

        var encoded = prefix
        encoded.reserveCapacity(prefix.count + writer.bytes.count * 2)
        for byte in writer.bytes {
            encoded.append(hexDigit(byte >> 4))
            encoded.append(hexDigit(byte & 0x0f))
        }
        return encoded
    }

    private static func hexDigit(_ value: UInt8) -> Character {
        Character(UnicodeScalar(value < 10 ? 48 + value : 87 + value))
    }
}
