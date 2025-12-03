// TransformingSerializer.swift
// DatabaseEngine - Serializer with compression and encryption support
//
// Reference: FDB Record Layer TransformedRecordSerializer.java
// Applies transformations (compression, encryption) to serialized data.

import Foundation
import Compression
import Crypto

// MARK: - TransformationType

/// Type of transformation applied to data
public struct TransformationType: OptionSet, Sendable, Equatable {
    public let rawValue: UInt8

    public init(rawValue: UInt8) {
        self.rawValue = rawValue
    }

    /// No transformation
    public static let none: TransformationType = []

    /// Data is compressed
    public static let compressed = TransformationType(rawValue: 0x01)

    /// Data is encrypted
    public static let encrypted = TransformationType(rawValue: 0x02)

    /// Data is both compressed and encrypted
    public static let compressedAndEncrypted: TransformationType = [.compressed, .encrypted]
}

// MARK: - TransformConfiguration

/// Configuration for data transformation
public struct TransformConfiguration: Sendable, Equatable {
    /// Whether compression is enabled
    public let compressionEnabled: Bool

    /// Compression algorithm to use
    public let compressionAlgorithm: CompressionAlgorithm

    /// Minimum data size for compression (smaller data won't be compressed)
    public let compressionMinSize: Int

    /// Whether to skip compression if it doesn't reduce size
    public let skipIneffectiveCompression: Bool

    /// Whether encryption is enabled
    public let encryptionEnabled: Bool

    /// Encryption key provider (required if encryption is enabled)
    public let keyProvider: (any EncryptionKeyProvider)?

    /// Default configuration (compression only)
    public static let `default` = TransformConfiguration(
        compressionEnabled: true,
        compressionAlgorithm: .zlib,
        compressionMinSize: 100,
        skipIneffectiveCompression: true,
        encryptionEnabled: false,
        keyProvider: nil
    )

    /// No transformation
    public static let none = TransformConfiguration(
        compressionEnabled: false,
        compressionAlgorithm: .zlib,
        compressionMinSize: 0,
        skipIneffectiveCompression: true,
        encryptionEnabled: false,
        keyProvider: nil
    )

    public init(
        compressionEnabled: Bool = true,
        compressionAlgorithm: CompressionAlgorithm = .zlib,
        compressionMinSize: Int = 100,
        skipIneffectiveCompression: Bool = true,
        encryptionEnabled: Bool = false,
        keyProvider: (any EncryptionKeyProvider)? = nil
    ) {
        self.compressionEnabled = compressionEnabled
        self.compressionAlgorithm = compressionAlgorithm
        self.compressionMinSize = compressionMinSize
        self.skipIneffectiveCompression = skipIneffectiveCompression
        self.encryptionEnabled = encryptionEnabled
        self.keyProvider = keyProvider
    }

    public static func == (lhs: TransformConfiguration, rhs: TransformConfiguration) -> Bool {
        lhs.compressionEnabled == rhs.compressionEnabled &&
        lhs.compressionAlgorithm == rhs.compressionAlgorithm &&
        lhs.compressionMinSize == rhs.compressionMinSize &&
        lhs.skipIneffectiveCompression == rhs.skipIneffectiveCompression &&
        lhs.encryptionEnabled == rhs.encryptionEnabled
    }
}

// MARK: - CompressionAlgorithm

/// Compression algorithm
public enum CompressionAlgorithm: UInt8, Sendable {
    case lz4 = 0x00
    case zlib = 0x01
    case lzma = 0x02
    case lzfse = 0x03

    var algorithm: compression_algorithm {
        switch self {
        case .lz4: return COMPRESSION_LZ4
        case .zlib: return COMPRESSION_ZLIB
        case .lzma: return COMPRESSION_LZMA
        case .lzfse: return COMPRESSION_LZFSE
        }
    }
}

// MARK: - EncryptionKeyProvider

/// Protocol for providing encryption keys
public protocol EncryptionKeyProvider: Sendable {
    /// Get encryption key for the given key ID
    func getKey(for keyId: String) async throws -> Data

    /// Get the current key ID to use for encryption
    func currentKeyId() -> String
}

// MARK: - TransformingSerializer

/// Serializer that applies transformations (compression, encryption) to data
///
/// **Data Format**:
/// ```
/// [1 byte: flags][optional: algorithm][optional: keyId][payload]
///
/// Flags byte:
///   bit 0: compressed
///   bit 1: encrypted
///   bits 2-7: reserved
///
/// If compressed:
///   [1 byte: algorithm]
///
/// If encrypted:
///   [1 byte: keyId length][keyId bytes][16 bytes: IV][payload][16 bytes: auth tag]
/// ```
///
/// **Usage**:
/// ```swift
/// let serializer = TransformingSerializer(configuration: .default)
///
/// // Serialize with compression
/// let transformed = try serializer.serialize(data)
///
/// // Deserialize (automatically detects and reverses transformation)
/// let original = try await serializer.deserialize(transformed)
/// ```
public struct TransformingSerializer: Sendable {
    // MARK: - Properties

    /// Configuration
    public let configuration: TransformConfiguration

    // MARK: - Initialization

    public init(configuration: TransformConfiguration = .default) {
        self.configuration = configuration
    }

    // MARK: - Public API

    /// Serialize data with configured transformations
    ///
    /// - Parameter data: The data to transform
    /// - Returns: Transformed data with header
    public func serialize(_ data: Data) async throws -> Data {
        var result = data
        var flags = TransformationType.none

        // Apply compression if enabled and data is large enough
        if configuration.compressionEnabled && data.count >= configuration.compressionMinSize {
            if let compressed = compress(data, algorithm: configuration.compressionAlgorithm) {
                // Only use compression if it actually reduces size
                if !configuration.skipIneffectiveCompression || compressed.count < data.count {
                    result = compressed
                    flags.insert(.compressed)
                }
            }
        }

        // Apply encryption if enabled
        if configuration.encryptionEnabled, let keyProvider = configuration.keyProvider {
            result = try await encrypt(result, keyProvider: keyProvider)
            flags.insert(.encrypted)
        }

        // Prepend header
        var output = Data()
        output.append(flags.rawValue)

        if flags.contains(.compressed) {
            output.append(configuration.compressionAlgorithm.rawValue)
        }

        output.append(result)
        return output
    }

    /// Serialize data synchronously (only for cases where encryption is not used)
    ///
    /// - Parameter data: The data to transform
    /// - Returns: Transformed data with header
    /// - Throws: `TransformError.asyncRequired` if encryption is enabled
    public func serializeSync(_ data: Data) throws -> Data {
        // Encrypted data requires async handling
        if configuration.encryptionEnabled {
            throw TransformError.asyncRequired
        }

        var result = data
        var flags = TransformationType.none

        // Apply compression if enabled and data is large enough
        if configuration.compressionEnabled && data.count >= configuration.compressionMinSize {
            if let compressed = compress(data, algorithm: configuration.compressionAlgorithm) {
                // Only use compression if it actually reduces size
                if !configuration.skipIneffectiveCompression || compressed.count < data.count {
                    result = compressed
                    flags.insert(.compressed)
                }
            }
        }

        // Prepend header
        var output = Data()
        output.append(flags.rawValue)

        if flags.contains(.compressed) {
            output.append(configuration.compressionAlgorithm.rawValue)
        }

        output.append(result)
        return output
    }

    /// Deserialize data, reversing any transformations
    ///
    /// - Parameter data: The transformed data
    /// - Returns: Original data
    public func deserialize(_ data: Data) async throws -> Data {
        guard !data.isEmpty else {
            return data
        }

        var offset = 0
        let flags = TransformationType(rawValue: data[offset])
        offset += 1

        var compressionAlgorithm: CompressionAlgorithm = .zlib
        if flags.contains(.compressed) {
            guard offset < data.count else {
                throw TransformError.invalidFormat("Missing compression algorithm byte")
            }
            guard let algo = CompressionAlgorithm(rawValue: data[offset]) else {
                throw TransformError.invalidFormat("Unknown compression algorithm")
            }
            compressionAlgorithm = algo
            offset += 1
        }

        var payload = Data(data[offset...])

        // Reverse encryption if applied
        if flags.contains(.encrypted) {
            guard let keyProvider = configuration.keyProvider else {
                throw TransformError.missingKeyProvider
            }
            payload = try await decrypt(payload, keyProvider: keyProvider)
        }

        // Reverse compression if applied
        if flags.contains(.compressed) {
            guard let decompressed = decompress(payload, algorithm: compressionAlgorithm) else {
                throw TransformError.decompressionFailed
            }
            payload = decompressed
        }

        return payload
    }

    /// Deserialize data synchronously (for cases where encryption is not used)
    ///
    /// - Parameter data: The transformed data
    /// - Returns: Original data
    public func deserializeSync(_ data: Data) throws -> Data {
        guard !data.isEmpty else {
            return data
        }

        var offset = 0
        let flags = TransformationType(rawValue: data[offset])
        offset += 1

        // Encrypted data requires async handling
        if flags.contains(.encrypted) {
            throw TransformError.asyncRequired
        }

        var compressionAlgorithm: CompressionAlgorithm = .zlib
        if flags.contains(.compressed) {
            guard offset < data.count else {
                throw TransformError.invalidFormat("Missing compression algorithm byte")
            }
            guard let algo = CompressionAlgorithm(rawValue: data[offset]) else {
                throw TransformError.invalidFormat("Unknown compression algorithm")
            }
            compressionAlgorithm = algo
            offset += 1
        }

        var payload = Data(data[offset...])

        // Reverse compression if applied
        if flags.contains(.compressed) {
            guard let decompressed = decompress(payload, algorithm: compressionAlgorithm) else {
                throw TransformError.decompressionFailed
            }
            payload = decompressed
        }

        return payload
    }

    // MARK: - Compression

    /// Compress data using the specified algorithm
    private func compress(_ data: Data, algorithm: CompressionAlgorithm) -> Data? {
        let destinationBufferSize = data.count + 64  // Some overhead for small data
        var destinationBuffer = [UInt8](repeating: 0, count: destinationBufferSize)

        let compressedSize = data.withUnsafeBytes { sourceBuffer -> Int in
            guard let sourcePtr = sourceBuffer.baseAddress else { return 0 }
            return compression_encode_buffer(
                &destinationBuffer,
                destinationBufferSize,
                sourcePtr.assumingMemoryBound(to: UInt8.self),
                data.count,
                nil,
                algorithm.algorithm
            )
        }

        guard compressedSize > 0 else { return nil }
        return Data(destinationBuffer[0..<compressedSize])
    }

    /// Decompress data using the specified algorithm
    ///
    /// Uses progressive buffer sizing to handle various compression ratios.
    /// Highly compressible data (e.g., repeated bytes) can have compression
    /// ratios over 100:1, requiring large decompression buffers.
    private func decompress(_ data: Data, algorithm: CompressionAlgorithm) -> Data? {
        // Progressive buffer sizes: 8x, 64x, 256x, 1024x
        // Handles compression ratios from typical (2-10x) to extreme (100x+)
        let multipliers = [8, 64, 256, 1024]

        for multiplier in multipliers {
            let destinationBufferSize = max(data.count * multiplier, 1024)
            var destinationBuffer = [UInt8](repeating: 0, count: destinationBufferSize)

            let decompressedSize = data.withUnsafeBytes { sourceBuffer -> Int in
                guard let sourcePtr = sourceBuffer.baseAddress else { return 0 }
                return compression_decode_buffer(
                    &destinationBuffer,
                    destinationBufferSize,
                    sourcePtr.assumingMemoryBound(to: UInt8.self),
                    data.count,
                    nil,
                    algorithm.algorithm
                )
            }

            // Success: decompressed size is less than buffer (buffer was large enough)
            if decompressedSize > 0 && decompressedSize < destinationBufferSize {
                return Data(destinationBuffer[0..<decompressedSize])
            }

            // Buffer too small or error: try next size
        }

        // Final attempt with very large buffer (handles extreme cases)
        let finalBufferSize = data.count * 4096
        var finalBuffer = [UInt8](repeating: 0, count: finalBufferSize)

        let finalSize = data.withUnsafeBytes { sourceBuffer -> Int in
            guard let sourcePtr = sourceBuffer.baseAddress else { return 0 }
            return compression_decode_buffer(
                &finalBuffer,
                finalBufferSize,
                sourcePtr.assumingMemoryBound(to: UInt8.self),
                data.count,
                nil,
                algorithm.algorithm
            )
        }

        guard finalSize > 0 else { return nil }
        return Data(finalBuffer[0..<finalSize])
    }

    // MARK: - Encryption
    //
    // Uses AES-256-GCM (Authenticated Encryption with Associated Data)
    // Reference: NIST SP 800-38D - Recommendation for Block Cipher Modes of Operation: GCM
    //
    // Format: [1 byte: keyId length][keyId bytes][12 bytes: nonce][ciphertext + 16 bytes: tag]
    //
    // Security properties:
    // - Confidentiality: AES-256 encryption
    // - Integrity: GCM authentication tag
    // - Nonce: Random 12-byte nonce per encryption (96 bits as recommended by NIST)

    /// Encrypt data using the key provider
    ///
    /// - Parameters:
    ///   - data: The plaintext data to encrypt
    ///   - keyProvider: Provider for encryption keys
    /// - Returns: Encrypted data with nonce and authentication tag
    /// - Throws: `TransformError.encryptionFailed` if encryption fails
    private func encrypt(_ data: Data, keyProvider: any EncryptionKeyProvider) async throws -> Data {
        let keyId = keyProvider.currentKeyId()

        // Validate key ID length (max 255 bytes)
        guard let keyIdData = keyId.data(using: .utf8), keyIdData.count <= 255 else {
            throw TransformError.encryptionFailed("Key ID too long or invalid encoding")
        }

        // Get the encryption key
        let rawKey: Data
        do {
            rawKey = try await keyProvider.getKey(for: keyId)
        } catch {
            throw TransformError.encryptionFailed("Failed to get key: \(error)")
        }

        guard rawKey.count == 32 else {
            throw TransformError.encryptionFailed("Invalid key size. Expected 32 bytes for AES-256.")
        }

        // Create symmetric key
        let symmetricKey = SymmetricKey(data: rawKey)

        // Generate random nonce (12 bytes for GCM)
        let nonce = AES.GCM.Nonce()

        // Encrypt with AES-GCM
        let sealedBox: AES.GCM.SealedBox
        do {
            sealedBox = try AES.GCM.seal(data, using: symmetricKey, nonce: nonce)
        } catch {
            throw TransformError.encryptionFailed("AES-GCM encryption failed: \(error)")
        }

        // Build output: [keyId length][keyId][nonce][ciphertext + tag]
        var output = Data()
        output.append(UInt8(keyIdData.count))
        output.append(keyIdData)
        output.append(contentsOf: nonce)
        output.append(sealedBox.ciphertext)
        output.append(sealedBox.tag)

        return output
    }

    /// Decrypt data using the key provider
    ///
    /// - Parameters:
    ///   - data: The encrypted data (nonce + ciphertext + tag)
    ///   - keyProvider: Provider for encryption keys
    /// - Returns: Decrypted plaintext data
    /// - Throws: `TransformError.decryptionFailed` if decryption fails
    private func decrypt(_ data: Data, keyProvider: any EncryptionKeyProvider) async throws -> Data {
        guard !data.isEmpty else {
            throw TransformError.decryptionFailed("Empty encrypted data")
        }

        var offset = 0

        // Read key ID length
        let keyIdLength = Int(data[offset])
        offset += 1

        guard offset + keyIdLength <= data.count else {
            throw TransformError.decryptionFailed("Invalid key ID length")
        }

        // Read key ID
        let keyIdData = data[offset..<(offset + keyIdLength)]
        guard let keyId = String(data: keyIdData, encoding: .utf8) else {
            throw TransformError.decryptionFailed("Invalid key ID encoding")
        }
        offset += keyIdLength

        // Nonce is 12 bytes, tag is 16 bytes
        let nonceSize = 12
        let tagSize = 16
        let minSize = nonceSize + tagSize

        guard data.count - offset >= minSize else {
            throw TransformError.decryptionFailed("Encrypted data too short")
        }

        // Read nonce
        let nonceData = data[offset..<(offset + nonceSize)]
        offset += nonceSize

        // Remaining is ciphertext + tag
        let ciphertextAndTag = data[offset...]
        let ciphertextLength = ciphertextAndTag.count - tagSize

        guard ciphertextLength >= 0 else {
            throw TransformError.decryptionFailed("Invalid ciphertext length")
        }

        let ciphertext = ciphertextAndTag.prefix(ciphertextLength)
        let tag = ciphertextAndTag.suffix(tagSize)

        // Get the decryption key
        let rawKey = try await keyProvider.getKey(for: keyId)

        guard rawKey.count == 32 else {
            throw TransformError.decryptionFailed("Invalid key size. Expected 32 bytes for AES-256.")
        }

        // Create symmetric key
        let symmetricKey = SymmetricKey(data: rawKey)

        // Create nonce
        let nonce: AES.GCM.Nonce
        do {
            nonce = try AES.GCM.Nonce(data: nonceData)
        } catch {
            throw TransformError.decryptionFailed("Invalid nonce: \(error)")
        }

        // Create sealed box and decrypt
        let sealedBox: AES.GCM.SealedBox
        do {
            sealedBox = try AES.GCM.SealedBox(nonce: nonce, ciphertext: ciphertext, tag: tag)
        } catch {
            throw TransformError.decryptionFailed("Invalid sealed box: \(error)")
        }

        do {
            let plaintext = try AES.GCM.open(sealedBox, using: symmetricKey)
            return plaintext
        } catch {
            throw TransformError.decryptionFailed("AES-GCM decryption failed (authentication failure or corrupted data): \(error)")
        }
    }
}

// MARK: - TransformError

/// Errors from transformation operations
public enum TransformError: Error, CustomStringConvertible, Sendable {
    case invalidFormat(String)
    case compressionFailed
    case decompressionFailed
    case missingKeyProvider
    case encryptionFailed(String)
    case decryptionFailed(String)
    case asyncRequired

    public var description: String {
        switch self {
        case .invalidFormat(let message):
            return "Invalid data format: \(message)"
        case .compressionFailed:
            return "Compression failed"
        case .decompressionFailed:
            return "Decompression failed"
        case .missingKeyProvider:
            return "Encryption key provider is required but not configured"
        case .encryptionFailed(let message):
            return "Encryption failed: \(message)"
        case .decryptionFailed(let message):
            return "Decryption failed: \(message)"
        case .asyncRequired:
            return "Async serialization/deserialization required for encrypted data"
        }
    }
}

// MARK: - TransformStatistics

/// Statistics about transformation operations
public struct TransformStatistics: Sendable {
    /// Original size before transformation
    public let originalSize: Int

    /// Size after transformation
    public let transformedSize: Int

    /// Compression ratio (transformed / original)
    public var compressionRatio: Double {
        guard originalSize > 0 else { return 1.0 }
        return Double(transformedSize) / Double(originalSize)
    }

    /// Space saved in bytes
    public var spaceSaved: Int {
        max(0, originalSize - transformedSize)
    }

    /// Whether compression was effective (reduced size)
    public var compressionEffective: Bool {
        transformedSize < originalSize
    }
}
