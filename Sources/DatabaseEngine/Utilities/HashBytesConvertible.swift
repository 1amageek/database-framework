// HashBytesConvertible.swift
// DatabaseEngine - Internal protocol for deterministic hashing
//
// This protocol enables type-safe, deterministic byte conversion for hashing.
// It stays internal to avoid polluting the global namespace with conformances.

import Foundation
import Core

/// Protocol for types that can be converted to bytes for deterministic hashing
///
/// Unlike Swift's `Hashable`, this guarantees:
/// - Same value → always same bytes
/// - Consistent across process restarts
/// - Consistent across platforms
///
/// **Internal only**: Not exported to avoid global namespace pollution.
/// Standard type conformances are module-internal.
internal protocol HashBytesConvertible {
    /// Convert to deterministic byte representation
    func toHashBytes() -> [UInt8]
}

// MARK: - Standard Type Conformances

extension String: HashBytesConvertible {
    @usableFromInline
    func toHashBytes() -> [UInt8] {
        Array(self.utf8)
    }
}

extension Int: HashBytesConvertible {
    @usableFromInline
    func toHashBytes() -> [UInt8] {
        // Use Int64 for cross-platform consistency
        // (Int is 32-bit on some platforms, 64-bit on others)
        Int64(self).toHashBytes()
    }
}

extension Int64: HashBytesConvertible {
    @usableFromInline
    func toHashBytes() -> [UInt8] {
        withUnsafeBytes(of: self) { Array($0) }
    }
}

extension UInt64: HashBytesConvertible {
    @usableFromInline
    func toHashBytes() -> [UInt8] {
        withUnsafeBytes(of: self) { Array($0) }
    }
}

extension Double: HashBytesConvertible {
    @usableFromInline
    func toHashBytes() -> [UInt8] {
        // Use bit pattern for consistent representation
        // (avoids issues with -0.0 vs 0.0, NaN representations)
        withUnsafeBytes(of: self.bitPattern) { Array($0) }
    }
}

extension Bool: HashBytesConvertible {
    @usableFromInline
    func toHashBytes() -> [UInt8] {
        [self ? 1 : 0]
    }
}

extension Data: HashBytesConvertible {
    @usableFromInline
    func toHashBytes() -> [UInt8] {
        Array(self)
    }
}

extension Array: HashBytesConvertible where Element: HashBytesConvertible {
    @usableFromInline
    func toHashBytes() -> [UInt8] {
        var result: [UInt8] = []
        // Length prefix to disambiguate (e.g., ["a"] vs ["a", ""])
        Swift.withUnsafeBytes(of: Int64(count)) { result.append(contentsOf: $0) }
        for element in self {
            let bytes = element.toHashBytes()
            // Per-element length prefix for unambiguous parsing
            Swift.withUnsafeBytes(of: Int64(bytes.count)) { result.append(contentsOf: $0) }
            result.append(contentsOf: bytes)
        }
        return result
    }
}

extension FieldValue: HashBytesConvertible {
    @usableFromInline
    func toHashBytes() -> [UInt8] {
        // Type tag prefix to disambiguate types
        switch self {
        case .null:
            return [0x00]
        case .bool(let v):
            return [0x01] + v.toHashBytes()
        case .int64(let v):
            return [0x02] + v.toHashBytes()
        case .double(let v):
            return [0x03] + v.toHashBytes()
        case .string(let v):
            return [0x04] + v.toHashBytes()
        case .data(let v):
            return [0x05] + v.toHashBytes()
        case .array(let values):
            var result: [UInt8] = [0x06]
            Swift.withUnsafeBytes(of: Int64(values.count)) { result.append(contentsOf: $0) }
            for value in values {
                result.append(contentsOf: value.toHashBytes())
            }
            return result
        }
    }
}
