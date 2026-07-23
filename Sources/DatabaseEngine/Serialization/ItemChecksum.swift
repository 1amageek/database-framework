import StorageKit

/// Portable CRC32C used to reject corrupted entity payloads before decoding.
enum ItemChecksum {
    private static let table: [UInt32] = (0..<256).map { value in
        var crc = UInt32(value)
        for _ in 0..<8 {
            crc = (crc & 1) == 0
                ? crc >> 1
                : (crc >> 1) ^ 0x82F6_3B78
        }
        return crc
    }

    static func crc32c(_ bytes: Bytes) -> UInt32 {
        bytes.withUnsafeBytes { source in
            crc32c(source)
        }
    }

    static func crc32c(_ bytes: UnsafeRawBufferPointer) -> UInt32 {
        var crc = UInt32.max
        for byte in bytes {
            let index = Int(UInt8(truncatingIfNeeded: crc) ^ byte)
            crc = table[index] ^ (crc >> 8)
        }
        return ~crc
    }
}
