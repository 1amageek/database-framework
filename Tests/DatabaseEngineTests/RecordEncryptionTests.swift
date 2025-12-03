// RecordEncryptionTests.swift
// DatabaseEngine Tests - Record encryption tests

import Testing
import Foundation
import Crypto
@testable import DatabaseEngine

// MARK: - StaticKeyProvider Tests

@Suite("StaticKeyProvider Tests")
struct StaticKeyProviderTests {

    @Test("Provider returns correct key")
    func providerReturnsKey() async throws {
        let key = SymmetricKey(size: .bits256)
        let provider = StaticKeyProvider(key: key, keyId: "test-key")

        let retrieved = try await provider.getKey(for: "test-key")

        #expect(retrieved.count == 32) // AES-256 key
    }

    @Test("Provider returns correct key ID")
    func providerReturnsKeyId() throws {
        let key = SymmetricKey(size: .bits256)
        let provider = StaticKeyProvider(key: key, keyId: "my-key-id")

        #expect(try provider.currentKeyId() == "my-key-id")
    }

    @Test("Provider throws for wrong key ID")
    func providerThrowsForWrongId() async throws {
        let key = SymmetricKey(size: .bits256)
        let provider = StaticKeyProvider(key: key, keyId: "test-key")

        do {
            _ = try await provider.getKey(for: "wrong-key")
            Issue.record("Should have thrown")
        } catch let error as KeyProviderError {
            if case .keyNotFound(let keyId) = error {
                #expect(keyId == "wrong-key")
            } else {
                Issue.record("Wrong error type")
            }
        }
    }

    @Test("Provider from data")
    func providerFromData() async throws {
        let keyData = Data(repeating: 0x42, count: 32)
        let provider = StaticKeyProvider(keyData: keyData, keyId: "data-key")

        let retrieved = try await provider.getKey(for: "data-key")
        #expect(retrieved == keyData)
    }
}

// MARK: - RotatingKeyProvider Tests

@Suite("RotatingKeyProvider Tests")
struct RotatingKeyProviderTests {

    @Test("Add and retrieve key")
    func addAndRetrieveKey() async throws {
        let provider = RotatingKeyProvider()
        let key = SymmetricKey(size: .bits256)

        provider.addKey(key: key, keyId: "key-v1")
        try provider.setCurrentKeyId("key-v1")

        let retrieved = try await provider.getKey(for: "key-v1")
        #expect(retrieved.count == 32)
    }

    @Test("Multiple keys")
    func multipleKeys() async throws {
        let provider = RotatingKeyProvider()

        provider.addKey(key: SymmetricKey(size: .bits256), keyId: "key-v1")
        provider.addKey(key: SymmetricKey(size: .bits256), keyId: "key-v2")
        try provider.setCurrentKeyId("key-v2")

        #expect(provider.keyIds.count == 2)
        #expect(try provider.currentKeyId() == "key-v2")
    }

    @Test("Remove key")
    func removeKey() async throws {
        let provider = RotatingKeyProvider()

        provider.addKey(key: SymmetricKey(size: .bits256), keyId: "key-v1")
        let removed = provider.removeKey(keyId: "key-v1")

        #expect(removed == true)
        #expect(provider.keyIds.isEmpty)
    }

    @Test("Set current key requires existing key")
    func setCurrentKeyRequiresExisting() async throws {
        let provider = RotatingKeyProvider()

        do {
            try provider.setCurrentKeyId("nonexistent")
            Issue.record("Should have thrown")
        } catch let error as KeyProviderError {
            if case .keyNotFound = error {
                // Expected
            } else {
                Issue.record("Wrong error type")
            }
        }
    }
}

// MARK: - DerivedKeyProvider Tests

@Suite("DerivedKeyProvider Tests")
struct DerivedKeyProviderTests {

    @Test("Derived keys are consistent")
    func derivedKeysConsistent() async throws {
        let masterKey = SymmetricKey(size: .bits256)
        let provider = DerivedKeyProvider(masterKey: masterKey)

        let key1 = try await provider.getKey(for: "user-123")
        let key2 = try await provider.getKey(for: "user-123")

        #expect(key1 == key2)
    }

    @Test("Different IDs produce different keys")
    func differentIdsDifferentKeys() async throws {
        let masterKey = SymmetricKey(size: .bits256)
        let provider = DerivedKeyProvider(masterKey: masterKey)

        let key1 = try await provider.getKey(for: "user-123")
        let key2 = try await provider.getKey(for: "user-456")

        #expect(key1 != key2)
    }

    @Test("Derived key is correct size")
    func derivedKeyCorrectSize() async throws {
        let masterKey = SymmetricKey(size: .bits256)
        let provider = DerivedKeyProvider(masterKey: masterKey)

        let key = try await provider.getKey(for: "any-key")
        #expect(key.count == 32) // AES-256
    }
}

// MARK: - EncryptionKeyUtils Tests

@Suite("EncryptionKeyUtils Tests")
struct EncryptionKeyUtilsTests {

    @Test("Generate random key")
    func generateRandomKey() {
        let key1 = EncryptionKeyUtils.generateKey()
        let key2 = EncryptionKeyUtils.generateKey()

        // Keys should be different (extremely unlikely to collide)
        let data1 = key1.withUnsafeBytes { Data($0) }
        let data2 = key2.withUnsafeBytes { Data($0) }
        #expect(data1 != data2)
    }

    @Test("Generate salt")
    func generateSalt() {
        let salt1 = EncryptionKeyUtils.generateSalt(size: 32)
        let salt2 = EncryptionKeyUtils.generateSalt(size: 32)

        #expect(salt1.count == 32)
        #expect(salt2.count == 32)
        #expect(salt1 != salt2)
    }

    @Test("Export and import key")
    func exportImportKey() throws {
        let key = EncryptionKeyUtils.generateKey()
        let exported = EncryptionKeyUtils.exportKey(key)

        let imported = try EncryptionKeyUtils.importKey(from: exported)

        let keyData = key.withUnsafeBytes { Data($0) }
        let importedData = imported.withUnsafeBytes { Data($0) }
        #expect(keyData == importedData)
    }

    @Test("Import invalid base64 throws")
    func importInvalidBase64Throws() throws {
        do {
            _ = try EncryptionKeyUtils.importKey(from: "not-valid-base64!!!")
            Issue.record("Should have thrown")
        } catch let error as KeyProviderError {
            if case .invalidKeyFormat = error {
                // Expected
            } else {
                Issue.record("Wrong error type")
            }
        }
    }

    @Test("Import wrong size key throws")
    func importWrongSizeThrows() throws {
        let shortKey = Data(repeating: 0x42, count: 16).base64EncodedString()

        do {
            _ = try EncryptionKeyUtils.importKey(from: shortKey)
            Issue.record("Should have thrown")
        } catch let error as KeyProviderError {
            if case .invalidKeyFormat = error {
                // Expected
            } else {
                Issue.record("Wrong error type")
            }
        }
    }

    @Test("Derive key from password")
    func deriveKeyFromPassword() {
        let salt = EncryptionKeyUtils.generateSalt()
        let key = EncryptionKeyUtils.deriveKey(
            from: "my-password",
            salt: salt,
            iterations: 1000 // Low for testing
        )

        let keyData = key.withUnsafeBytes { Data($0) }
        #expect(keyData.count == 32)
    }

    @Test("Same password and salt produce same key")
    func samePasswordSameSalt() {
        let salt = Data(repeating: 0x42, count: 32)
        let key1 = EncryptionKeyUtils.deriveKey(from: "password", salt: salt, iterations: 1000)
        let key2 = EncryptionKeyUtils.deriveKey(from: "password", salt: salt, iterations: 1000)

        let data1 = key1.withUnsafeBytes { Data($0) }
        let data2 = key2.withUnsafeBytes { Data($0) }
        #expect(data1 == data2)
    }
}
