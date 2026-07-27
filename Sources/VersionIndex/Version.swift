#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import DatabaseEngine
import StorageKit

/// Represents an entity version (FDB Versionstamp)
///
/// A Version is a 10-byte value assigned by FoundationDB at commit time.
/// This is the native 80-bit versionstamp used by SET_VERSIONSTAMPED_KEY.
/// It consists of:
/// - 8 bytes: database commit version (big-endian, globally unique)
/// - 2 bytes: batch order within same commit version (big-endian)
///
/// Versions are comparable and provide total ordering for optimistic concurrency control.
public struct Version: Sendable, Comparable, Hashable, CustomStringConvertible {
    public let bytes: Bytes  // Must be exactly 10 bytes

    // MARK: - Initialization

    /// Create a Version from versionstamp bytes
    public init(bytes: Bytes) {
        precondition(bytes.count == 10, "Version must be 10 bytes (80-bit versionstamp)")
        self.bytes = bytes
    }

    /// Create incomplete versionstamp placeholder (0xFF bytes)
    /// Used when setting keys/values that will be filled by FDB at commit time
    public static func incomplete() -> Version {
        return Version(bytes: Bytes(repeating: 0xFF, count: 10))
    }

    // MARK: - Comparable

    public static func < (lhs: Version, rhs: Version) -> Bool {
        return lhs.bytes.lexicographicallyPrecedes(rhs.bytes)
    }

    public static func == (lhs: Version, rhs: Version) -> Bool {
        return lhs.bytes == rhs.bytes
    }

    // MARK: - Hashable

    public func hash(into hasher: inout Hasher) {
        hasher.combine(bytes)
    }

    // MARK: - CustomStringConvertible

    public var description: String {
        DatabaseTextFormatting.lowercaseHex(bytes)
    }

    // MARK: - Conversion

    /// Extract database commit version (first 8 bytes, big-endian)
    public var databaseVersion: UInt64 {
        return bytes.prefix(8).withUnsafeBytes {
            $0.load(as: UInt64.self).bigEndian
        }
    }

    /// Extract batch order (last 2 bytes, big-endian)
    public var batchOrder: UInt16 {
        return UInt16(bytes[8]) << 8 | UInt16(bytes[9])
    }
}
