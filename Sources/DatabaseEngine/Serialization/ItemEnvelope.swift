// ItemEnvelope.swift
// DatabaseEngine - Item envelope format for storage
//
// Defines the wire format for stored items, supporting both inline data
// and external blob references for large values.

import Foundation
import FoundationDB

// MARK: - ItemEnvelope

/// Envelope format for stored items
///
/// **Wire Format**:
/// ```
/// Bytes 0-3:  Magic number "ITEM" (0x49 0x54 0x45 0x4D)
/// Byte 4:     Format version (currently 0x01)
/// Byte 5:     Flags (inline/external)
/// Byte 6:     Codec ID (compression/encryption algorithm)
/// Bytes 7...: Payload (inline data) or ExternalRef (blob reference)
/// ```
///
/// **Design Goals**:
/// - Items subspace contains only item envelopes (1 key per item)
/// - Large values stored in separate blobs subspace
/// - Range scans over items return consistent item envelopes
/// - Magic number prevents misidentification of non-envelope data
public struct ItemEnvelope: Sendable, Equatable {
    // MARK: - Constants

    /// Magic number "ITEM" to identify envelope format
    public static let magic: [UInt8] = [0x49, 0x54, 0x45, 0x4D]  // "ITEM"

    /// Current format version
    public static let currentVersion: UInt8 = 0x01

    /// Header size: magic (4) + version (1) + flags (1) + codec (1)
    public static let headerSize: Int = 7

    /// Maximum inline payload size (90KB - leave room for FDB overhead)
    public static let maxInlineSize: Int = 90_000

    // MARK: - Properties

    /// Format version
    public let version: UInt8

    /// Storage flags
    public let flags: Flags

    /// Codec identifier for compression/encryption
    public let codec: Codec

    /// Payload data (inline) or external reference
    public let content: Content

    // MARK: - Initialization

    /// Create an inline envelope
    public static func inline(
        data: FDB.Bytes,
        codec: Codec = .zlibCompressed
    ) -> ItemEnvelope {
        ItemEnvelope(
            version: currentVersion,
            flags: .inline,
            codec: codec,
            content: .inline(data)
        )
    }

    /// Create an external reference envelope
    public static func external(
        ref: ExternalRef,
        codec: Codec = .zlibCompressed
    ) -> ItemEnvelope {
        ItemEnvelope(
            version: currentVersion,
            flags: .external,
            codec: codec,
            content: .external(ref)
        )
    }

    private init(version: UInt8, flags: Flags, codec: Codec, content: Content) {
        self.version = version
        self.flags = flags
        self.codec = codec
        self.content = content
    }

    // MARK: - Serialization

    /// Serialize envelope to bytes for storage
    public func serialize() -> FDB.Bytes {
        var result: [UInt8] = []

        // Magic
        result.append(contentsOf: Self.magic)

        // Header
        result.append(version)
        result.append(flags.rawValue)
        result.append(codec.rawValue)

        // Content
        switch content {
        case .inline(let data):
            result.append(contentsOf: data)

        case .external(let ref):
            result.append(contentsOf: ref.serialize())
        }

        return result
    }

    /// Deserialize envelope from stored bytes
    public static func deserialize(_ bytes: FDB.Bytes) throws -> ItemEnvelope {
        guard bytes.count >= headerSize else {
            throw ItemEnvelopeError.invalidHeader
        }

        // Verify magic
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

        guard let flags = Flags(rawValue: bytes[5]) else {
            throw ItemEnvelopeError.invalidFlags(bytes[5])
        }

        let codec = Codec(rawValue: bytes[6]) ?? .none

        let payloadBytes = Array(bytes[headerSize...])

        let content: Content
        switch flags {
        case .inline:
            content = .inline(payloadBytes)

        case .external:
            let ref = try ExternalRef.deserialize(payloadBytes)
            content = .external(ref)
        }

        return ItemEnvelope(
            version: version,
            flags: flags,
            codec: codec,
            content: content
        )
    }

    /// Check if bytes represent a valid ItemEnvelope
    /// Only checks magic number - no weak heuristics
    public static func isEnvelope(_ bytes: FDB.Bytes) -> Bool {
        guard bytes.count >= headerSize else { return false }

        // Check magic only - this is unambiguous
        return bytes[0] == magic[0] &&
               bytes[1] == magic[1] &&
               bytes[2] == magic[2] &&
               bytes[3] == magic[3]
    }
}

// MARK: - Flags

extension ItemEnvelope {
    /// Storage flags
    public enum Flags: UInt8, Sendable {
        /// Data is stored inline in this envelope
        case inline = 0x00

        /// Data is stored externally in blobs subspace
        case external = 0x01
    }
}

// MARK: - Codec

extension ItemEnvelope {
    /// Codec identifier for compression/encryption
    public enum Codec: UInt8, Sendable {
        /// No transformation (raw data)
        case none = 0x00

        /// zlib compression (default)
        case zlibCompressed = 0x01

        /// LZ4 compression (faster)
        case lz4Compressed = 0x02

        /// LZMA compression (better ratio)
        case lzmaCompressed = 0x03

        /// LZFSE compression (Apple optimized)
        case lzfseCompressed = 0x04

        /// AES-256-GCM encryption (no compression)
        case aesEncrypted = 0x10

        /// zlib + AES-256-GCM
        case zlibEncrypted = 0x11
    }
}

// MARK: - Content

extension ItemEnvelope {
    /// Envelope content: either inline data or external reference
    public enum Content: Sendable, Equatable {
        /// Data stored inline in this envelope
        case inline(FDB.Bytes)

        /// Reference to external blob storage
        case external(ExternalRef)
    }
}

// MARK: - ExternalRef

extension ItemEnvelope {
    /// Reference to externally stored blob data
    ///
    /// **Wire Format**:
    /// ```
    /// Bytes 0-7:   Total size (Int64, big-endian)
    /// Bytes 8-11:  Chunk count (Int32, big-endian)
    /// Bytes 12-15: Chunk size (Int32, big-endian)
    /// ```
    public struct ExternalRef: Sendable, Equatable {
        /// Total size of the original data
        public let totalSize: Int64

        /// Number of chunks
        public let chunkCount: Int32

        /// Size of each chunk (last chunk may be smaller)
        public let chunkSize: Int32

        /// Wire format size
        public static let serializedSize: Int = 16

        public init(totalSize: Int64, chunkCount: Int32, chunkSize: Int32) {
            self.totalSize = totalSize
            self.chunkCount = chunkCount
            self.chunkSize = chunkSize
        }

        /// Serialize to bytes
        public func serialize() -> FDB.Bytes {
            var result: [UInt8] = []
            result.reserveCapacity(Self.serializedSize)

            // totalSize (8 bytes, big-endian)
            var size = totalSize.bigEndian
            withUnsafeBytes(of: &size) { result.append(contentsOf: $0) }

            // chunkCount (4 bytes, big-endian)
            var count = chunkCount.bigEndian
            withUnsafeBytes(of: &count) { result.append(contentsOf: $0) }

            // chunkSize (4 bytes, big-endian)
            var cSize = chunkSize.bigEndian
            withUnsafeBytes(of: &cSize) { result.append(contentsOf: $0) }

            return result
        }

        /// Deserialize from bytes
        public static func deserialize(_ bytes: FDB.Bytes) throws -> ExternalRef {
            guard bytes.count >= serializedSize else {
                throw ItemEnvelopeError.invalidExternalRef
            }

            let totalSize = bytes.withUnsafeBytes { ptr -> Int64 in
                var value: Int64 = 0
                withUnsafeMutableBytes(of: &value) { dest in
                    _ = ptr.copyBytes(to: dest, count: 8)
                }
                return Int64(bigEndian: value)
            }

            let chunkCount = Array(bytes[8..<12]).withUnsafeBytes { ptr -> Int32 in
                var value: Int32 = 0
                withUnsafeMutableBytes(of: &value) { dest in
                    _ = ptr.copyBytes(to: dest, count: 4)
                }
                return Int32(bigEndian: value)
            }

            let chunkSize = Array(bytes[12..<16]).withUnsafeBytes { ptr -> Int32 in
                var value: Int32 = 0
                withUnsafeMutableBytes(of: &value) { dest in
                    _ = ptr.copyBytes(to: dest, count: 4)
                }
                return Int32(bigEndian: value)
            }

            return ExternalRef(
                totalSize: totalSize,
                chunkCount: chunkCount,
                chunkSize: chunkSize
            )
        }
    }
}

// MARK: - ItemEnvelopeError

/// Errors from ItemEnvelope operations
public enum ItemEnvelopeError: Error, CustomStringConvertible, Sendable {
    case invalidMagic
    case invalidHeader
    case unsupportedVersion(UInt8)
    case invalidFlags(UInt8)
    case invalidExternalRef
    case chunkMissing(index: Int)
    case sizeMismatch(expected: Int, actual: Int)

    public var description: String {
        switch self {
        case .invalidMagic:
            return "Invalid item envelope: missing or incorrect magic number"
        case .invalidHeader:
            return "Invalid item envelope header"
        case .unsupportedVersion(let v):
            return "Unsupported item envelope version: \(v)"
        case .invalidFlags(let f):
            return "Invalid item envelope flags: \(f)"
        case .invalidExternalRef:
            return "Invalid external reference data"
        case .chunkMissing(let index):
            return "Missing blob chunk at index \(index)"
        case .sizeMismatch(let expected, let actual):
            return "Size mismatch: expected \(expected) bytes, got \(actual)"
        }
    }
}
