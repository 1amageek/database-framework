// DeterministicHash.swift
// DatabaseEngine - Deterministic hashing utilities
//
// Swift's Hasher uses a per-process random seed for security.
// For persisted data (continuation tokens, statistics), we need deterministic hashing.

import Foundation

/// Deterministic hash utilities
///
/// Use instead of Swift's `Hasher` when hashing data that will be:
/// - Persisted to storage
/// - Compared across process restarts
/// - Shared between different server instances
internal enum DeterministicHash {

    /// Hash a value conforming to HashBytesConvertible
    @usableFromInline
    static func hash<T: HashBytesConvertible>(_ value: T) -> UInt64 {
        MurmurHash3.hash(value.toHashBytes())
    }

    /// Hash raw bytes
    @usableFromInline
    static func hash(bytes: [UInt8]) -> UInt64 {
        MurmurHash3.hash(bytes)
    }
}

/// Deterministic hasher for combining multiple values
///
/// Similar API to Swift's `Hasher`, but produces deterministic results.
///
/// ```swift
/// var hasher = DeterministicHasher()
/// hasher.combine("operator")
/// hasher.combine(["idx1", "idx2"])
/// let hash = hasher.finalize()
/// ```
internal struct DeterministicHasher: Sendable {

    @usableFromInline
    var accumulated: [UInt8] = []

    @inlinable
    init() {}

    /// Combine a value into the hash
    @usableFromInline
    mutating func combine<T: HashBytesConvertible>(_ value: T) {
        accumulated.append(contentsOf: value.toHashBytes())
    }

    /// Combine raw bytes into the hash
    @usableFromInline
    mutating func combine(bytes: [UInt8]) {
        accumulated.append(contentsOf: bytes)
    }

    /// Finalize and return the hash value
    @usableFromInline
    func finalize() -> UInt64 {
        MurmurHash3.hash(accumulated)
    }

    /// Finalize and return as byte array
    ///
    /// **Important**: Uses little-endian byte order for cross-platform consistency.
    /// This ensures continuation tokens are valid across different server architectures
    /// (e.g., ARM and x86 machines in the same cluster).
    @usableFromInline
    func finalizeToBytes() -> [UInt8] {
        let hash = finalize()
        // Use explicit little-endian for cross-platform consistency
        return withUnsafeBytes(of: hash.littleEndian) { Array($0) }
    }
}
