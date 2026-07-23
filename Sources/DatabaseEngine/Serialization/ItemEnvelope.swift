import StorageKit

/// Canonical v1 envelope for persisted database entities.
///
/// The fixed header is encoded in network byte order:
///
/// ```text
/// magic[4] | version[1] | storage[1] | encoding[1]
/// plainByteCount[8] | storedByteCount[8] | crc32c[4] | content[n]
/// ```
///
/// Inline content is the stored payload itself. External content is an
/// eight-byte `ExternalRef`; the payload is stored under the entity's blob
/// subspace. Deserialization returns inline content as a view into the owned
/// envelope buffer.
public struct ItemEnvelope: Sendable, Equatable {
    public static let magic: Bytes = [0x49, 0x54, 0x45, 0x4D]
    public static let currentVersion: UInt8 = 1
    public static let headerSize = 27

    public enum StorageKind: UInt8, Sendable, Equatable {
        case inline = 0
        case external = 1
    }

    public enum Content: Sendable, Equatable {
        case inline(Bytes)
        case external(ExternalRef)
    }

    public let version: UInt8
    public let storageKind: StorageKind
    public let encoding: ItemPayloadEncoding
    public let plainByteCount: UInt64
    public let storedByteCount: UInt64
    public let checksum: UInt32
    public let content: Content

    public static func inline(
        payload: Bytes,
        encoding: ItemPayloadEncoding,
        plainByteCount: UInt64,
        checksum: UInt32
    ) throws -> ItemEnvelope {
        let storedByteCount = UInt64(payload.count)
        try validateEncodingSizes(
            encoding: encoding,
            plainByteCount: plainByteCount,
            storedByteCount: storedByteCount
        )
        return ItemEnvelope(
            version: currentVersion,
            storageKind: .inline,
            encoding: encoding,
            plainByteCount: plainByteCount,
            storedByteCount: storedByteCount,
            checksum: checksum,
            content: .inline(payload)
        )
    }

    public static func external(
        reference: ExternalRef,
        encoding: ItemPayloadEncoding,
        plainByteCount: UInt64,
        storedByteCount: UInt64,
        checksum: UInt32
    ) throws -> ItemEnvelope {
        try validateEncodingSizes(
            encoding: encoding,
            plainByteCount: plainByteCount,
            storedByteCount: storedByteCount
        )
        try reference.validate(storedByteCount: storedByteCount)
        return ItemEnvelope(
            version: currentVersion,
            storageKind: .external,
            encoding: encoding,
            plainByteCount: plainByteCount,
            storedByteCount: storedByteCount,
            checksum: checksum,
            content: .external(reference)
        )
    }

    private init(
        version: UInt8,
        storageKind: StorageKind,
        encoding: ItemPayloadEncoding,
        plainByteCount: UInt64,
        storedByteCount: UInt64,
        checksum: UInt32,
        content: Content
    ) {
        self.version = version
        self.storageKind = storageKind
        self.encoding = encoding
        self.plainByteCount = plainByteCount
        self.storedByteCount = storedByteCount
        self.checksum = checksum
        self.content = content
    }

    /// Produces the final persisted value in one allocation.
    public func serialize() -> Bytes {
        let contentByteCount: Int
        switch content {
        case .inline(let payload):
            contentByteCount = payload.count
        case .external:
            contentByteCount = ExternalRef.serializedSize
        }

        return Bytes.copying(count: Self.headerSize + contentByteCount) { output in
            output[0] = Self.magic[0]
            output[1] = Self.magic[1]
            output[2] = Self.magic[2]
            output[3] = Self.magic[3]
            output[4] = version
            output[5] = storageKind.rawValue
            output[6] = encoding.rawValue
            Self.writeUInt64(plainByteCount, to: output, at: 7)
            Self.writeUInt64(storedByteCount, to: output, at: 15)
            Self.writeUInt32(checksum, to: output, at: 23)

            switch content {
            case .inline(let payload):
                payload.withUnsafeBytes { source in
                    UnsafeMutableRawBufferPointer(
                        rebasing: output[Self.headerSize..<output.count]
                    ).copyMemory(from: source)
                }
            case .external(let reference):
                reference.write(to: output, at: Self.headerSize)
            }
        }
    }

    /// Parses and structurally validates an envelope without copying its inline payload.
    public static func deserialize(_ bytes: Bytes) throws -> ItemEnvelope {
        guard bytes.count >= headerSize else {
            throw ItemEnvelopeError.invalidHeader
        }
        guard bytes[0] == magic[0],
              bytes[1] == magic[1],
              bytes[2] == magic[2],
              bytes[3] == magic[3] else {
            throw ItemEnvelopeError.invalidMagic
        }
        let version = bytes[4]
        guard version == currentVersion else {
            throw ItemEnvelopeError.unsupportedVersion(version)
        }
        guard let storageKind = StorageKind(rawValue: bytes[5]) else {
            throw ItemEnvelopeError.invalidStorageKind(bytes[5])
        }
        guard let encoding = ItemPayloadEncoding(rawValue: bytes[6]) else {
            throw ItemEnvelopeError.unsupportedEncoding(bytes[6])
        }

        let plainByteCount = readUInt64(bytes, at: 7)
        let storedByteCount = readUInt64(bytes, at: 15)
        let checksum = readUInt32(bytes, at: 23)
        try validateEncodingSizes(
            encoding: encoding,
            plainByteCount: plainByteCount,
            storedByteCount: storedByteCount
        )

        let contentBytes = bytes[headerSize..<bytes.count]
        let content: Content
        switch storageKind {
        case .inline:
            guard let expectedCount = Int(exactly: storedByteCount),
                  contentBytes.count == expectedCount else {
                throw ItemEnvelopeError.payloadSizeMismatch(
                    expected: storedByteCount,
                    actual: UInt64(contentBytes.count)
                )
            }
            content = .inline(contentBytes)
        case .external:
            let reference = try ExternalRef.deserialize(contentBytes)
            try reference.validate(storedByteCount: storedByteCount)
            content = .external(reference)
        }

        return ItemEnvelope(
            version: version,
            storageKind: storageKind,
            encoding: encoding,
            plainByteCount: plainByteCount,
            storedByteCount: storedByteCount,
            checksum: checksum,
            content: content
        )
    }

    public static func isEnvelope(_ bytes: Bytes) -> Bool {
        bytes.count >= headerSize
            && bytes[0] == magic[0]
            && bytes[1] == magic[1]
            && bytes[2] == magic[2]
            && bytes[3] == magic[3]
    }

    private static func validateEncodingSizes(
        encoding: ItemPayloadEncoding,
        plainByteCount: UInt64,
        storedByteCount: UInt64
    ) throws {
        switch encoding {
        case .identity:
            guard plainByteCount == storedByteCount else {
                throw ItemEnvelopeError.invalidSizeMetadata
            }
        }
    }

    public struct ExternalRef: Sendable, Equatable {
        public static let serializedSize = 8

        public let chunkCount: UInt32
        public let chunkByteCount: UInt32

        public init(
            chunkCount: UInt32,
            chunkByteCount: UInt32,
            storedByteCount: UInt64
        ) throws {
            self.chunkCount = chunkCount
            self.chunkByteCount = chunkByteCount
            try validate(storedByteCount: storedByteCount)
        }

        public func serialize() -> Bytes {
            Bytes.copying(count: Self.serializedSize) { output in
                write(to: output, at: 0)
            }
        }

        public static func deserialize(_ bytes: Bytes) throws -> ExternalRef {
            guard bytes.count == serializedSize else {
                throw ItemEnvelopeError.invalidExternalRef
            }
            return ExternalRef(
                uncheckedChunkCount: ItemEnvelope.readUInt32(bytes, at: 0),
                chunkByteCount: ItemEnvelope.readUInt32(bytes, at: 4)
            )
        }

        fileprivate init(
            uncheckedChunkCount: UInt32,
            chunkByteCount: UInt32
        ) {
            self.chunkCount = uncheckedChunkCount
            self.chunkByteCount = chunkByteCount
        }

        fileprivate func validate(storedByteCount: UInt64) throws {
            guard storedByteCount > 0,
                  chunkCount > 0,
                  chunkByteCount > 0 else {
                throw ItemEnvelopeError.invalidExternalRef
            }
            let chunkBytes = UInt64(chunkByteCount)
            let (roundedSize, overflow) = storedByteCount.addingReportingOverflow(
                chunkBytes - 1
            )
            guard !overflow,
                  UInt64(chunkCount) == roundedSize / chunkBytes else {
                throw ItemEnvelopeError.invalidExternalRef
            }
        }

        fileprivate func write(
            to output: UnsafeMutableRawBufferPointer,
            at offset: Int
        ) {
            ItemEnvelope.writeUInt32(chunkCount, to: output, at: offset)
            ItemEnvelope.writeUInt32(
                chunkByteCount,
                to: output,
                at: offset + 4
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

    private static func readUInt64(_ bytes: Bytes, at offset: Int) -> UInt64 {
        var value: UInt64 = 0
        for index in 0..<8 {
            value = (value << 8) | UInt64(bytes[offset + index])
        }
        return value
    }

    private static func readUInt32(_ bytes: Bytes, at offset: Int) -> UInt32 {
        var value: UInt32 = 0
        for index in 0..<4 {
            value = (value << 8) | UInt32(bytes[offset + index])
        }
        return value
    }
}

public enum ItemEnvelopeError: Error, CustomStringConvertible, Sendable, Equatable {
    case invalidMagic
    case invalidHeader
    case unsupportedVersion(UInt8)
    case invalidStorageKind(UInt8)
    case unsupportedEncoding(UInt8)
    case invalidSizeMetadata
    case invalidExternalRef
    case payloadSizeMismatch(expected: UInt64, actual: UInt64)
    case chunkMissing(index: Int)
    case chunkSizeMismatch(index: Int, expected: Int, actual: Int)
    case checksumMismatch(expected: UInt32, actual: UInt32)

    public var description: String {
        switch self {
        case .invalidMagic:
            return "Invalid item envelope magic"
        case .invalidHeader:
            return "Invalid item envelope header"
        case .unsupportedVersion(let version):
            return "Unsupported item envelope version: \(version)"
        case .invalidStorageKind(let kind):
            return "Invalid item envelope storage kind: \(kind)"
        case .unsupportedEncoding(let encoding):
            return "Unsupported item payload encoding: \(encoding)"
        case .invalidSizeMetadata:
            return "Invalid item envelope size metadata"
        case .invalidExternalRef:
            return "Invalid item external reference"
        case .payloadSizeMismatch(let expected, let actual):
            return "Item payload size mismatch: expected \(expected), got \(actual)"
        case .chunkMissing(let index):
            return "Missing item blob chunk at index \(index)"
        case .chunkSizeMismatch(let index, let expected, let actual):
            return "Item blob chunk \(index) size mismatch: expected \(expected), got \(actual)"
        case .checksumMismatch(let expected, let actual):
            return "Item checksum mismatch: expected \(expected), got \(actual)"
        }
    }
}
