// VersionIndexKind+Maintainable.swift
// VersionIndexLayer - IndexKindMaintainable extension for VersionIndexKind
//
// This file provides the bridge between VersionIndexKind (defined in FDBModel)
// and VersionIndexMaintainer (defined in this package).

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB

// MARK: - IndexKindMaintainable Extension

/// Extends VersionIndexKind (from FDBModel) with IndexKindMaintainable conformance
extension VersionIndexKind: IndexKindMaintainable {
    /// Create a VersionIndexMaintainer for this index kind
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        // VersionIndexKind doesn't use configurations
        return VersionIndexMaintainer<Item>(
            index: index,
            strategy: self.strategy,
            subspace: subspace,  // Already index-specific from caller
            idExpression: idExpression
        )
    }
}

// MARK: - Version Struct (FDB Versionstamp)

/// Represents a record version (FDB Versionstamp)
///
/// A Version is a 10-byte value assigned by FoundationDB at commit time.
/// This is the native 80-bit versionstamp used by SET_VERSIONSTAMPED_KEY.
/// It consists of:
/// - 8 bytes: database commit version (big-endian, globally unique)
/// - 2 bytes: batch order within same commit version (big-endian)
///
/// Versions are comparable and provide total ordering for optimistic concurrency control.
public struct Version: Sendable, Comparable, Hashable, CustomStringConvertible {
    public let bytes: FDB.Bytes  // Must be exactly 10 bytes

    // MARK: - Initialization

    /// Create a Version from versionstamp bytes
    public init(bytes: FDB.Bytes) {
        precondition(bytes.count == 10, "Version must be 10 bytes (80-bit versionstamp)")
        self.bytes = bytes
    }

    /// Create incomplete versionstamp placeholder (0xFF bytes)
    /// Used when setting keys/values that will be filled by FDB at commit time
    public static func incomplete() -> Version {
        return Version(bytes: [UInt8](repeating: 0xFF, count: 10))
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
        return bytes.map { String(format: "%02x", $0) }.joined()
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
