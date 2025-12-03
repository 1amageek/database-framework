// FormatVersion.swift
// DatabaseEngine - Storage format versioning for backward compatibility
//
// Reference: FDB Record Layer FDBRecordStore.FormatVersion
// Manages storage format versions to support schema evolution and migrations.

import Foundation
import FoundationDB
import Core

// MARK: - FormatVersion

/// Represents the storage format version
///
/// Format versions allow the system to:
/// - Detect incompatible storage formats
/// - Perform automatic upgrades when possible
/// - Reject opens of incompatible databases
/// - Support gradual rollouts of format changes
///
/// **Version Components**:
/// - Major: Breaking changes that require migration
/// - Minor: Backward-compatible feature additions
/// - Patch: Bug fixes to format handling
///
/// **Usage**:
/// ```swift
/// // Check compatibility when opening store
/// let stored = try await versionManager.loadVersion(transaction: tx)
/// if let stored = stored {
///     try versionManager.checkCompatibility(stored)
/// }
/// ```
public struct FormatVersion: Sendable, Equatable, Comparable, Hashable, CustomStringConvertible {
    /// Major version - breaking changes
    public let major: Int

    /// Minor version - backward-compatible additions
    public let minor: Int

    /// Patch version - bug fixes
    public let patch: Int

    // MARK: - Well-Known Versions

    /// Initial format version
    public static let v1_0_0 = FormatVersion(major: 1, minor: 0, patch: 0)

    /// Current format version
    public static let current = v1_0_0

    /// Minimum supported version for reading
    public static let minimumSupported = v1_0_0

    /// Minimum supported version for writing
    public static let minimumWritable = v1_0_0

    // MARK: - Initialization

    public init(major: Int, minor: Int, patch: Int) {
        precondition(major >= 0, "Major version must be non-negative")
        precondition(minor >= 0, "Minor version must be non-negative")
        precondition(patch >= 0, "Patch version must be non-negative")

        self.major = major
        self.minor = minor
        self.patch = patch
    }

    // MARK: - Comparable

    public static func < (lhs: FormatVersion, rhs: FormatVersion) -> Bool {
        if lhs.major != rhs.major {
            return lhs.major < rhs.major
        }
        if lhs.minor != rhs.minor {
            return lhs.minor < rhs.minor
        }
        return lhs.patch < rhs.patch
    }

    // MARK: - CustomStringConvertible

    public var description: String {
        "\(major).\(minor).\(patch)"
    }

    // MARK: - Serialization

    /// Encode to tuple elements for storage
    public var tupleElements: [any TupleElement] {
        [major, minor, patch]
    }

    /// Decode from tuple
    public static func fromTuple(_ tuple: Tuple) -> FormatVersion? {
        guard tuple.count >= 3,
              let major = tuple[0] as? Int,
              let minor = tuple[1] as? Int,
              let patch = tuple[2] as? Int else {
            return nil
        }
        return FormatVersion(major: major, minor: minor, patch: patch)
    }

    /// Encode to bytes
    public func toBytes() -> [UInt8] {
        var bytes: [UInt8] = []
        bytes.append(contentsOf: withUnsafeBytes(of: Int32(major).bigEndian) { Array($0) })
        bytes.append(contentsOf: withUnsafeBytes(of: Int32(minor).bigEndian) { Array($0) })
        bytes.append(contentsOf: withUnsafeBytes(of: Int32(patch).bigEndian) { Array($0) })
        return bytes
    }

    /// Decode from bytes
    public static func fromBytes(_ bytes: [UInt8]) -> FormatVersion? {
        guard bytes.count >= 12 else { return nil }

        let major = Int(Int32(bigEndian: bytes[0..<4].withUnsafeBytes { $0.load(as: Int32.self) }))
        let minor = Int(Int32(bigEndian: bytes[4..<8].withUnsafeBytes { $0.load(as: Int32.self) }))
        let patch = Int(Int32(bigEndian: bytes[8..<12].withUnsafeBytes { $0.load(as: Int32.self) }))

        return FormatVersion(major: major, minor: minor, patch: patch)
    }
}

// MARK: - FormatVersionManager

/// Manages format version checking and upgrades
///
/// **Responsibilities**:
/// - Store and retrieve format version
/// - Check compatibility on store open
/// - Perform automatic upgrades when safe
/// - Track format features
///
/// **Usage**:
/// ```swift
/// let manager = FormatVersionManager(subspace: metadataSubspace)
///
/// try await database.withTransaction { tx in
///     // Check version on open
///     let stored = try await manager.loadVersion(transaction: tx)
///
///     if let stored = stored {
///         try manager.checkCompatibility(stored)
///         if manager.needsUpgrade(stored) {
///             try await manager.upgrade(from: stored, transaction: tx)
///         }
///     } else {
///         // New store - write current version
///         manager.saveVersion(.current, transaction: tx)
///     }
/// }
/// ```
public struct FormatVersionManager: Sendable {
    // MARK: - Storage Keys

    /// Key for storing format version
    private let versionKey: FDB.Bytes

    // MARK: - Initialization

    /// Create a version manager
    ///
    /// - Parameter subspace: Metadata subspace for storing version info
    public init(subspace: Subspace) {
        let metadataSubspace = subspace.subspace("_format")
        self.versionKey = metadataSubspace.pack(Tuple("version"))
    }

    // MARK: - Version Management

    /// Load the stored format version
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: The stored version, or nil if not set
    public func loadVersion(transaction: any TransactionProtocol) async throws -> FormatVersion? {
        guard let bytes = try await transaction.getValue(for: versionKey) else {
            return nil
        }

        return FormatVersion.fromBytes(Array(bytes))
    }

    /// Save a format version
    ///
    /// - Parameters:
    ///   - version: The version to save
    ///   - transaction: The transaction to use
    public func saveVersion(_ version: FormatVersion, transaction: any TransactionProtocol) {
        transaction.setValue(version.toBytes(), for: versionKey)
    }

    /// Check if a stored version is compatible with the current code
    ///
    /// - Parameter stored: The stored version
    /// - Throws: `FormatVersionError` if incompatible
    public func checkCompatibility(_ stored: FormatVersion) throws {
        // Check if too old
        if stored < FormatVersion.minimumSupported {
            throw FormatVersionError.tooOld(
                stored: stored,
                minimum: FormatVersion.minimumSupported
            )
        }

        // Check if too new (from a newer version of the code)
        if stored > FormatVersion.current {
            throw FormatVersionError.tooNew(
                stored: stored,
                current: FormatVersion.current
            )
        }

        // Check if major version is compatible
        if stored.major != FormatVersion.current.major {
            throw FormatVersionError.majorVersionMismatch(
                stored: stored,
                current: FormatVersion.current
            )
        }
    }

    /// Check if an upgrade is needed
    ///
    /// - Parameter stored: The stored version
    /// - Returns: Whether an upgrade is available
    public func needsUpgrade(_ stored: FormatVersion) -> Bool {
        stored < FormatVersion.current
    }

    /// Check if an upgrade is safe (can be done online)
    ///
    /// - Parameters:
    ///   - from: Source version
    ///   - to: Target version
    /// - Returns: Whether the upgrade can be done online
    public func canUpgradeOnline(from: FormatVersion, to: FormatVersion) -> Bool {
        // Same major version upgrades are typically safe
        return from.major == to.major
    }

    /// Upgrade the format version
    ///
    /// - Parameters:
    ///   - from: Source version
    ///   - transaction: Transaction to use
    public func upgrade(from: FormatVersion, transaction: any TransactionProtocol) {
        // Save new version
        saveVersion(FormatVersion.current, transaction: transaction)
    }
}

// MARK: - FormatVersionError

/// Errors from format version checking
public enum FormatVersionError: Error, CustomStringConvertible, Sendable {
    /// Stored version is too old to read
    case tooOld(stored: FormatVersion, minimum: FormatVersion)

    /// Stored version is newer than this code supports
    case tooNew(stored: FormatVersion, current: FormatVersion)

    /// Major version doesn't match (incompatible)
    case majorVersionMismatch(stored: FormatVersion, current: FormatVersion)

    /// Upgrade failed
    case upgradeFailed(from: FormatVersion, to: FormatVersion, reason: String)

    public var description: String {
        switch self {
        case .tooOld(let stored, let minimum):
            return "Format version \(stored) is too old. Minimum supported: \(minimum)"

        case .tooNew(let stored, let current):
            return "Format version \(stored) is too new for this code (version \(current)). Please upgrade the software."

        case .majorVersionMismatch(let stored, let current):
            return "Format version \(stored) is incompatible with current version \(current). Major version mismatch."

        case .upgradeFailed(let from, let to, let reason):
            return "Failed to upgrade from \(from) to \(to): \(reason)"
        }
    }
}
