import StorageKit

/// Immutable physical format contract stored with every database store.
public struct DatabaseFormatDescriptor: Sendable, Equatable {
    public static let descriptorVersion: UInt8 = 1
    public static let serializedSize = 45

    private static let magic: Bytes = [0x44, 0x42, 0x46, 0x4D]
    private static let checksumOffset = 41

    public let persistableFormatVersion: UInt16
    public let envelopeVersion: UInt8
    public let itemStorage: ItemStorageConfiguration

    public static func v1(
        itemStorage: ItemStorageConfiguration
    ) -> DatabaseFormatDescriptor {
        DatabaseFormatDescriptor(
            persistableFormatVersion: PersistableStorageCodec.formatVersion,
            envelopeVersion: ItemEnvelope.currentVersion,
            itemStorage: itemStorage
        )
    }

    private init(
        persistableFormatVersion: UInt16,
        envelopeVersion: UInt8,
        itemStorage: ItemStorageConfiguration
    ) {
        self.persistableFormatVersion = persistableFormatVersion
        self.envelopeVersion = envelopeVersion
        self.itemStorage = itemStorage
    }

    /// Encodes the fixed-size descriptor in one allocation.
    public func serialize() -> Bytes {
        Bytes.copying(count: Self.serializedSize) { output in
            output[0] = Self.magic[0]
            output[1] = Self.magic[1]
            output[2] = Self.magic[2]
            output[3] = Self.magic[3]
            output[4] = Self.descriptorVersion
            Self.writeUInt16(persistableFormatVersion, to: output, at: 5)
            output[7] = envelopeVersion
            output[8] = itemStorage.encoding.rawValue
            Self.writeUInt64(
                UInt64(itemStorage.maximumPlainByteCount),
                to: output,
                at: 9
            )
            Self.writeUInt64(
                UInt64(itemStorage.maximumStoredByteCount),
                to: output,
                at: 17
            )
            Self.writeUInt64(
                UInt64(itemStorage.maximumInlineByteCount),
                to: output,
                at: 25
            )
            Self.writeUInt64(
                UInt64(itemStorage.chunkByteCount),
                to: output,
                at: 33
            )
            let checksum = ItemChecksum.crc32c(
                UnsafeRawBufferPointer(
                    rebasing: output[0..<Self.checksumOffset]
                )
            )
            Self.writeUInt32(
                checksum,
                to: output,
                at: Self.checksumOffset
            )
        }
    }

    public static func deserialize(
        _ bytes: Bytes
    ) throws -> DatabaseFormatDescriptor {
        guard bytes.count == serializedSize else {
            throw DatabaseFormatDescriptorError.invalidSize(
                actual: bytes.count,
                expected: serializedSize
            )
        }
        guard bytes[0] == magic[0],
              bytes[1] == magic[1],
              bytes[2] == magic[2],
              bytes[3] == magic[3] else {
            throw DatabaseFormatDescriptorError.invalidMagic
        }
        guard bytes[4] == descriptorVersion else {
            throw DatabaseFormatDescriptorError.unsupportedDescriptorVersion(
                bytes[4]
            )
        }

        let expectedChecksum = readUInt32(bytes, at: checksumOffset)
        let actualChecksum = bytes.withUnsafeBytes { source in
            ItemChecksum.crc32c(
                UnsafeRawBufferPointer(
                    rebasing: source[0..<checksumOffset]
                )
            )
        }
        guard actualChecksum == expectedChecksum else {
            throw DatabaseFormatDescriptorError.checksumMismatch(
                expected: expectedChecksum,
                actual: actualChecksum
            )
        }

        let persistableFormatVersion = readUInt16(bytes, at: 5)
        guard persistableFormatVersion == PersistableStorageCodec.formatVersion else {
            throw DatabaseFormatDescriptorError.unsupportedPersistableFormatVersion(
                persistableFormatVersion
            )
        }
        let envelopeVersion = bytes[7]
        guard envelopeVersion == ItemEnvelope.currentVersion else {
            throw DatabaseFormatDescriptorError.unsupportedEnvelopeVersion(
                envelopeVersion
            )
        }
        guard let encoding = ItemPayloadEncoding(rawValue: bytes[8]) else {
            throw DatabaseFormatDescriptorError.unsupportedPayloadEncoding(
                bytes[8]
            )
        }

        guard let maximumPlainByteCount = Int(
                  exactly: readUInt64(bytes, at: 9)
              ),
              let maximumStoredByteCount = Int(
                  exactly: readUInt64(bytes, at: 17)
              ),
              let maximumInlineByteCount = Int(
                  exactly: readUInt64(bytes, at: 25)
              ),
              let chunkByteCount = Int(
                  exactly: readUInt64(bytes, at: 33)
              ) else {
            throw DatabaseFormatDescriptorError.integerOutOfRange
        }

        let itemStorage: ItemStorageConfiguration
        do {
            itemStorage = try ItemStorageConfiguration(
                encoding: encoding,
                maximumPlainByteCount: maximumPlainByteCount,
                maximumStoredByteCount: maximumStoredByteCount,
                maximumInlineByteCount: maximumInlineByteCount,
                chunkByteCount: chunkByteCount
            )
        } catch {
            throw DatabaseFormatDescriptorError.invalidStorageConfiguration(
                error
            )
        }
        return DatabaseFormatDescriptor(
            persistableFormatVersion: persistableFormatVersion,
            envelopeVersion: envelopeVersion,
            itemStorage: itemStorage
        )
    }

    private static func writeUInt16(
        _ value: UInt16,
        to output: UnsafeMutableRawBufferPointer,
        at offset: Int
    ) {
        output[offset] = UInt8(truncatingIfNeeded: value >> 8)
        output[offset + 1] = UInt8(truncatingIfNeeded: value)
    }

    private static func writeUInt32(
        _ value: UInt32,
        to output: UnsafeMutableRawBufferPointer,
        at offset: Int
    ) {
        for index in 0..<4 {
            output[offset + index] = UInt8(
                truncatingIfNeeded: value >> UInt32((3 - index) * 8)
            )
        }
    }

    private static func writeUInt64(
        _ value: UInt64,
        to output: UnsafeMutableRawBufferPointer,
        at offset: Int
    ) {
        for index in 0..<8 {
            output[offset + index] = UInt8(
                truncatingIfNeeded: value >> UInt64((7 - index) * 8)
            )
        }
    }

    private static func readUInt16(_ bytes: Bytes, at offset: Int) -> UInt16 {
        (UInt16(bytes[offset]) << 8) | UInt16(bytes[offset + 1])
    }

    private static func readUInt32(_ bytes: Bytes, at offset: Int) -> UInt32 {
        var value: UInt32 = 0
        for index in 0..<4 {
            value = (value << 8) | UInt32(bytes[offset + index])
        }
        return value
    }

    private static func readUInt64(_ bytes: Bytes, at offset: Int) -> UInt64 {
        var value: UInt64 = 0
        for index in 0..<8 {
            value = (value << 8) | UInt64(bytes[offset + index])
        }
        return value
    }
}
