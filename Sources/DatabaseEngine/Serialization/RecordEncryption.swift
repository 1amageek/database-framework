// RecordEncryption.swift
// DatabaseEngine - Record encryption key management
//
// Reference: FDB Record Layer ScopedDirectoryLayer encryption support
// Provides key providers and utilities for record-level encryption.

import Foundation
import Crypto
import Synchronization

// MARK: - StaticKeyProvider

/// Simple key provider using a static key
///
/// **Warning**: For development/testing only. Use a proper key management
/// system (KMS) in production.
///
/// **Usage**:
/// ```swift
/// let key = SymmetricKey(size: .bits256)
/// let provider = StaticKeyProvider(key: key, keyId: "test-key-1")
///
/// let config = TransformConfiguration(
///     encryptionEnabled: true,
///     keyProvider: provider
/// )
/// ```
public struct StaticKeyProvider: EncryptionKeyProvider, Sendable {
    private let key: SymmetricKey
    private let keyId: String

    /// Create a provider with a static key
    ///
    /// - Parameters:
    ///   - key: The AES-256 symmetric key (32 bytes)
    ///   - keyId: Identifier for this key
    public init(key: SymmetricKey, keyId: String) {
        self.key = key
        self.keyId = keyId
    }

    /// Create a provider from raw key bytes
    ///
    /// - Parameters:
    ///   - keyData: The raw key bytes (must be 32 bytes for AES-256)
    ///   - keyId: Identifier for this key
    public init(keyData: Data, keyId: String) {
        self.key = SymmetricKey(data: keyData)
        self.keyId = keyId
    }

    public func getKey(for keyId: String) async throws -> Data {
        guard keyId == self.keyId else {
            throw KeyProviderError.keyNotFound(keyId: keyId)
        }
        return key.withUnsafeBytes { Data($0) }
    }

    public func currentKeyId() throws -> String {
        keyId
    }
}

// MARK: - RotatingKeyProvider

/// Key provider supporting key rotation
///
/// Manages multiple keys for encryption key rotation. The current key is used
/// for new encryptions, while historical keys are used for decryption.
///
/// **Key Rotation Pattern**:
/// 1. Add new key with `addKey()`
/// 2. Set it as current with `setCurrentKeyId()`
/// 3. Gradually re-encrypt data with new key
/// 4. Remove old key with `removeKey()` after all data is migrated
///
/// **Usage**:
/// ```swift
/// let provider = RotatingKeyProvider()
/// provider.addKey(key: key1, keyId: "key-v1")
/// provider.setCurrentKeyId("key-v1")
///
/// // Later, rotate to new key
/// provider.addKey(key: key2, keyId: "key-v2")
/// provider.setCurrentKeyId("key-v2")
/// // Old data still readable with key-v1
/// ```
public final class RotatingKeyProvider: EncryptionKeyProvider, Sendable {
    private struct State: Sendable {
        var keys: [String: SymmetricKey] = [:]
        var currentKeyId: String?
    }

    private let state: Mutex<State>

    public init() {
        self.state = Mutex(State())
    }

    /// Add a key
    ///
    /// - Parameters:
    ///   - key: The symmetric key
    ///   - keyId: Identifier for this key
    public func addKey(key: SymmetricKey, keyId: String) {
        state.withLock { $0.keys[keyId] = key }
    }

    /// Add a key from raw bytes
    ///
    /// - Parameters:
    ///   - keyData: Raw key bytes (32 bytes for AES-256)
    ///   - keyId: Identifier for this key
    public func addKey(keyData: Data, keyId: String) {
        let key = SymmetricKey(data: keyData)
        addKey(key: key, keyId: keyId)
    }

    /// Remove a key
    ///
    /// - Parameter keyId: The key to remove
    /// - Returns: True if the key existed and was removed
    @discardableResult
    public func removeKey(keyId: String) -> Bool {
        state.withLock { $0.keys.removeValue(forKey: keyId) != nil }
    }

    /// Set the current key for new encryptions
    ///
    /// - Parameter keyId: The key ID to use for encryption
    /// - Throws: `KeyProviderError.keyNotFound` if the key doesn't exist
    public func setCurrentKeyId(_ keyId: String) throws {
        try state.withLock { state in
            guard state.keys[keyId] != nil else {
                throw KeyProviderError.keyNotFound(keyId: keyId)
            }
            state.currentKeyId = keyId
        }
    }

    /// Get all available key IDs
    public var keyIds: [String] {
        state.withLock { Array($0.keys.keys) }
    }

    // MARK: - EncryptionKeyProvider

    public func getKey(for keyId: String) async throws -> Data {
        guard let key = state.withLock({ $0.keys[keyId] }) else {
            throw KeyProviderError.keyNotFound(keyId: keyId)
        }
        return key.withUnsafeBytes { Data($0) }
    }

    public func currentKeyId() throws -> String {
        guard let keyId = state.withLock({ $0.currentKeyId }) else {
            throw KeyProviderError.notConfigured
        }
        return keyId
    }
}

// MARK: - DerivedKeyProvider

/// Key provider that derives keys from a master key
///
/// Uses HKDF (HMAC-based Key Derivation Function) to derive keys from a
/// master key. Each key ID generates a unique derived key.
///
/// **Advantages**:
/// - Only need to store/protect one master key
/// - Unlimited number of derived keys
/// - Cryptographically secure derivation
///
/// **Usage**:
/// ```swift
/// let masterKey = SymmetricKey(size: .bits256)
/// let provider = DerivedKeyProvider(masterKey: masterKey)
///
/// // Keys are derived on demand based on key ID
/// // Same key ID always produces same derived key
/// ```
///
/// **Reference**: RFC 5869 - HMAC-based Extract-and-Expand Key Derivation Function
public struct DerivedKeyProvider: EncryptionKeyProvider, Sendable {
    private let masterKey: SymmetricKey
    private let info: Data
    private let _currentKeyId: String

    /// Create a provider from a master key
    ///
    /// - Parameters:
    ///   - masterKey: The master key for derivation
    ///   - info: Additional context info for HKDF (default: "record-encryption")
    ///   - currentKeyId: The key ID to use for new encryptions
    public init(
        masterKey: SymmetricKey,
        info: String = "record-encryption",
        currentKeyId: String = "default"
    ) {
        self.masterKey = masterKey
        self.info = Data(info.utf8)
        self._currentKeyId = currentKeyId
    }

    public func getKey(for keyId: String) async throws -> Data {
        // Derive key using HKDF
        // Salt is the key ID to ensure different keys for different IDs
        let salt = Data(keyId.utf8)

        let derivedKey = HKDF<SHA256>.deriveKey(
            inputKeyMaterial: masterKey,
            salt: salt,
            info: info,
            outputByteCount: 32  // AES-256 key size
        )

        return derivedKey.withUnsafeBytes { Data($0) }
    }

    public func currentKeyId() throws -> String {
        _currentKeyId
    }
}

// MARK: - EnvironmentKeyProvider

/// Key provider that reads keys from environment variables
///
/// **Usage**:
/// Set environment variable: `FDB_ENCRYPTION_KEY_v1=<base64-encoded-key>`
///
/// ```swift
/// let provider = EnvironmentKeyProvider(
///     prefix: "FDB_ENCRYPTION_KEY_",
///     currentKeyId: "v1"
/// )
/// ```
public struct EnvironmentKeyProvider: EncryptionKeyProvider, Sendable {
    private let prefix: String
    private let _currentKeyId: String

    /// Create a provider that reads keys from environment
    ///
    /// - Parameters:
    ///   - prefix: Environment variable prefix (default: "FDB_ENCRYPTION_KEY_")
    ///   - currentKeyId: The key ID to use for new encryptions
    public init(
        prefix: String = "FDB_ENCRYPTION_KEY_",
        currentKeyId: String
    ) {
        self.prefix = prefix
        self._currentKeyId = currentKeyId
    }

    public func getKey(for keyId: String) async throws -> Data {
        let envName = prefix + keyId

        guard let envValue = ProcessInfo.processInfo.environment[envName] else {
            throw KeyProviderError.keyNotFound(keyId: keyId)
        }

        // Expect base64-encoded key
        guard let keyData = Data(base64Encoded: envValue) else {
            throw KeyProviderError.invalidKeyFormat(keyId: keyId, reason: "Invalid base64 encoding")
        }

        guard keyData.count == 32 else {
            throw KeyProviderError.invalidKeyFormat(keyId: keyId, reason: "Key must be 32 bytes for AES-256")
        }

        return keyData
    }

    public func currentKeyId() throws -> String {
        _currentKeyId
    }
}

// MARK: - KeyProviderError

/// Errors from key providers
public enum KeyProviderError: Error, CustomStringConvertible, Sendable {
    /// Key with specified ID not found
    case keyNotFound(keyId: String)

    /// Key has invalid format
    case invalidKeyFormat(keyId: String, reason: String)

    /// Key provider not configured
    case notConfigured

    public var description: String {
        switch self {
        case .keyNotFound(let keyId):
            return "Encryption key not found: '\(keyId)'"
        case .invalidKeyFormat(let keyId, let reason):
            return "Invalid key format for '\(keyId)': \(reason)"
        case .notConfigured:
            return "Encryption key provider not configured"
        }
    }
}

// MARK: - Key Generation Utilities

/// Utilities for generating encryption keys
public enum EncryptionKeyUtils {

    /// Generate a new random AES-256 key
    ///
    /// - Returns: A cryptographically secure random 32-byte key
    public static func generateKey() -> SymmetricKey {
        SymmetricKey(size: .bits256)
    }

    /// Generate a key from a password using PBKDF2
    ///
    /// - Parameters:
    ///   - password: The password to derive from
    ///   - salt: Salt for derivation (should be random and stored)
    ///   - iterations: Number of PBKDF2 iterations (default: 100000)
    /// - Returns: A derived AES-256 key
    ///
    /// **Reference**: RFC 8018 - PKCS #5: Password-Based Cryptography Specification
    public static func deriveKey(
        from password: String,
        salt: Data,
        iterations: Int = 100_000
    ) -> SymmetricKey {
        let derivedKey = pbkdf2SHA256(
            password: Data(password.utf8),
            salt: salt,
            iterations: max(1, iterations),
            outputByteCount: 32
        )
        return SymmetricKey(data: derivedKey)
    }

    /// Generate a random salt
    ///
    /// - Parameter size: Salt size in bytes (default: 32)
    /// - Returns: Random salt data
    public static func generateSalt(size: Int = 32) -> Data {
        guard size > 0 else {
            return Data()
        }

        var generator = SystemRandomNumberGenerator()
        var bytes: [UInt8] = []
        bytes.reserveCapacity(size)
        for _ in 0..<size {
            bytes.append(UInt8.random(in: UInt8.min...UInt8.max, using: &generator))
        }
        return Data(bytes)
    }

    /// Export a key to base64 for storage
    ///
    /// - Parameter key: The key to export
    /// - Returns: Base64-encoded key string
    public static func exportKey(_ key: SymmetricKey) -> String {
        key.withUnsafeBytes { Data($0).base64EncodedString() }
    }

    /// Import a key from base64
    ///
    /// - Parameter base64String: The base64-encoded key
    /// - Returns: The symmetric key
    /// - Throws: `KeyProviderError` if invalid
    public static func importKey(from base64String: String) throws -> SymmetricKey {
        guard let data = Data(base64Encoded: base64String) else {
            throw KeyProviderError.invalidKeyFormat(keyId: "", reason: "Invalid base64 encoding")
        }
        guard data.count == 32 else {
            throw KeyProviderError.invalidKeyFormat(keyId: "", reason: "Key must be 32 bytes")
        }
        return SymmetricKey(data: data)
    }

    private static func pbkdf2SHA256(
        password: Data,
        salt: Data,
        iterations: Int,
        outputByteCount: Int
    ) -> Data {
        let key = SymmetricKey(data: password)
        let hmacLength = SHA256.Digest.byteCount
        let blockCount = (outputByteCount + hmacLength - 1) / hmacLength
        var derivedBytes: [UInt8] = []
        derivedBytes.reserveCapacity(blockCount * hmacLength)

        for blockIndex in 1...blockCount {
            var blockSalt = [UInt8](salt)
            blockSalt.append(UInt8((blockIndex >> 24) & 0xff))
            blockSalt.append(UInt8((blockIndex >> 16) & 0xff))
            blockSalt.append(UInt8((blockIndex >> 8) & 0xff))
            blockSalt.append(UInt8(blockIndex & 0xff))

            var u = Array(HMAC<SHA256>.authenticationCode(for: Data(blockSalt), using: key))
            var block = u

            if iterations > 1 {
                for _ in 1..<iterations {
                    u = Array(HMAC<SHA256>.authenticationCode(for: Data(u), using: key))
                    for index in block.indices {
                        block[index] ^= u[index]
                    }
                }
            }

            derivedBytes.append(contentsOf: block)
        }

        return Data(derivedBytes.prefix(outputByteCount))
    }
}
