import DatabaseTypes
import StorageKit

/// Immutable physical format contract stored with every database store.
///
/// The descriptor carries two independent versions and they are bumped for
/// different reasons.
///
/// | Version | Describes | Bumped when |
/// |---|---|---|
/// | `descriptorVersion` | the 48-byte encoding of this descriptor | a field is added, removed, resized, or moved |
/// | `layoutVersion` | the item layout named by `layoutKind` | stored item bytes stop being readable by the previous reader |
///
/// Neither version describes the Directory layout of the database. Directory
/// existence and compatibility are owned by StorageKit's Directory catalog,
/// which rejects an incompatible store before the descriptor is read.
public struct DatabaseFormatDescriptor: Sendable, Equatable {
    /// Encoding version of this descriptor's own serialized bytes.
    public static let descriptorVersion: UInt8 = 2
    /// Item layout version this build writes for its `layoutKind`.
    public static let currentLayoutVersion: UInt16 = 1
    public static let serializedSize = 48

    private static let magic: ByteString = [0x44, 0x42, 0x46, 0x4D]
    private static let checksumOffset = 44

    public let layoutKind: DatabaseStorageLayoutKind
    /// Version of the item layout `layoutKind` names, not of this descriptor
    /// and not of the Directory layout.
    public let layoutVersion: UInt16
    public let persistableFormatVersion: UInt16
    public let envelopeVersion: UInt8
    public let itemStorage: ItemStorageConfiguration

    public static func current(
        layoutKind: DatabaseStorageLayoutKind,
        itemStorage: ItemStorageConfiguration
    ) -> DatabaseFormatDescriptor {
        DatabaseFormatDescriptor(
            layoutKind: layoutKind,
            layoutVersion: currentLayoutVersion,
            persistableFormatVersion: PersistableStorageCodec.formatVersion,
            envelopeVersion: ItemEnvelope.currentVersion,
            itemStorage: itemStorage
        )
    }

    private init(
        layoutKind: DatabaseStorageLayoutKind,
        layoutVersion: UInt16,
        persistableFormatVersion: UInt16,
        envelopeVersion: UInt8,
        itemStorage: ItemStorageConfiguration
    ) {
        self.layoutKind = layoutKind
        self.layoutVersion = layoutVersion
        self.persistableFormatVersion = persistableFormatVersion
        self.envelopeVersion = envelopeVersion
        self.itemStorage = itemStorage
    }

    /// Encodes the fixed-size descriptor in one allocation.
    public func serialize() -> ByteString {
        ByteString.copying(count: Self.serializedSize) { output in
            output[0] = Self.magic[0]
            output[1] = Self.magic[1]
            output[2] = Self.magic[2]
            output[3] = Self.magic[3]
            output[4] = Self.descriptorVersion
            output[5] = layoutKind.rawValue
            Self.writeUInt16(layoutVersion, to: output, at: 6)
            Self.writeUInt16(persistableFormatVersion, to: output, at: 8)
            output[10] = envelopeVersion
            output[11] = itemStorage.encoding.rawValue
            Self.writeUInt64(
                UInt64(itemStorage.maximumPlainByteCount),
                to: output,
                at: 12
            )
            Self.writeUInt64(
                UInt64(itemStorage.maximumStoredByteCount),
                to: output,
                at: 20
            )
            Self.writeUInt64(
                UInt64(itemStorage.maximumInlineByteCount),
                to: output,
                at: 28
            )
            Self.writeUInt64(
                UInt64(itemStorage.chunkByteCount),
                to: output,
                at: 36
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
        _ bytes: ByteString
    ) throws -> DatabaseFormatDescriptor {
        guard bytes.count >= 5 else {
            throw DatabaseFormatDescriptorError.invalidSize(
                actual: bytes.count,
                expected: serializedSize
            )
        }
        guard byte(in: bytes, at: 0) == magic[0],
              byte(in: bytes, at: 1) == magic[1],
              byte(in: bytes, at: 2) == magic[2],
              byte(in: bytes, at: 3) == magic[3] else {
            throw DatabaseFormatDescriptorError.invalidMagic
        }
        let encodedDescriptorVersion = byte(in: bytes, at: 4)
        guard encodedDescriptorVersion == descriptorVersion else {
            throw DatabaseFormatDescriptorError.unsupportedDescriptorVersion(
                encodedDescriptorVersion
            )
        }
        guard bytes.count == serializedSize else {
            throw DatabaseFormatDescriptorError.invalidSize(
                actual: bytes.count,
                expected: serializedSize
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

        let encodedLayoutKind = byte(in: bytes, at: 5)
        guard let layoutKind = DatabaseStorageLayoutKind(
            rawValue: encodedLayoutKind
        ) else {
            throw DatabaseFormatDescriptorError.unsupportedLayoutKind(
                encodedLayoutKind
            )
        }
        let layoutVersion = readUInt16(bytes, at: 6)
        guard layoutVersion == currentLayoutVersion else {
            throw DatabaseFormatDescriptorError.unsupportedLayoutVersion(
                layoutVersion
            )
        }
        let persistableFormatVersion = readUInt16(bytes, at: 8)
        guard persistableFormatVersion == PersistableStorageCodec.formatVersion else {
            throw DatabaseFormatDescriptorError.unsupportedPersistableFormatVersion(
                persistableFormatVersion
            )
        }
        let envelopeVersion = byte(in: bytes, at: 10)
        guard envelopeVersion == ItemEnvelope.currentVersion else {
            throw DatabaseFormatDescriptorError.unsupportedEnvelopeVersion(
                envelopeVersion
            )
        }
        let encodingByte = byte(in: bytes, at: 11)
        guard let encoding = ItemPayloadEncoding(rawValue: encodingByte) else {
            throw DatabaseFormatDescriptorError.unsupportedPayloadEncoding(
                encodingByte
            )
        }

        guard let maximumPlainByteCount = Int(
                  exactly: readUInt64(bytes, at: 12)
              ),
              let maximumStoredByteCount = Int(
                  exactly: readUInt64(bytes, at: 20)
              ),
              let maximumInlineByteCount = Int(
                  exactly: readUInt64(bytes, at: 28)
              ),
              let chunkByteCount = Int(
                  exactly: readUInt64(bytes, at: 36)
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
            layoutKind: layoutKind,
            layoutVersion: layoutVersion,
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

    private static func readUInt16(_ bytes: ByteString, at offset: Int) -> UInt16 {
        (UInt16(byte(in: bytes, at: offset)) << 8)
            | UInt16(byte(in: bytes, at: offset + 1))
    }

    private static func readUInt32(_ bytes: ByteString, at offset: Int) -> UInt32 {
        var value: UInt32 = 0
        for index in 0..<4 {
            value = (value << 8) | UInt32(byte(in: bytes, at: offset + index))
        }
        return value
    }

    private static func readUInt64(_ bytes: ByteString, at offset: Int) -> UInt64 {
        var value: UInt64 = 0
        for index in 0..<8 {
            value = (value << 8) | UInt64(byte(in: bytes, at: offset + index))
        }
        return value
    }

    private static func byte(
        in bytes: ByteString,
        at offset: Int
    ) -> UInt8 {
        bytes[bytes.index(bytes.startIndex, offsetBy: offset)]
    }
}
