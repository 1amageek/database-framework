// UniquenessViolation.swift
// DatabaseEngine - Uniqueness violation tracking for indexes
//
// Reference: FDB Record Layer StandardIndexMaintainer.java
// https://github.com/FoundationDB/fdb-record-layer/blob/main/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/StandardIndexMaintainer.java

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import StorageKit
import Core
import Synchronization

// MARK: - UniquenessViolation

/// Represents a uniqueness constraint violation
///
/// Records information about duplicate values found in a unique index,
/// including all conflicting primary keys.
///
/// **Usage**:
/// ```swift
/// let violations = try await tracker.scanViolations(indexName: "email_idx")
/// for violation in violations {
///     print("Duplicate value \(violation.valueDescription) found for entities: \(violation.primaryKeys)")
/// }
/// ```
public struct UniquenessViolation: Sendable, Equatable {
    // MARK: - Properties

    /// Name of the violated index
    public let indexName: String

    /// Type name of the affected Persistable
    public let persistableType: String

    /// The duplicate value (packed tuple bytes)
    ///
    /// Use `unpackedValue()` to get the tuple elements.
    public let valueKey: Bytes

    /// All primary keys that have this duplicate value
    ///
    /// Contains at least 2 entries (otherwise it's not a violation).
    public let primaryKeys: [Bytes]

    /// When the violation was first detected
    public let detectedAt: Date

    // MARK: - Initialization

    public init(
        indexName: String,
        persistableType: String,
        valueKey: Bytes,
        primaryKeys: [Bytes],
        detectedAt: Date = Date()
    ) {
        self.indexName = indexName
        self.persistableType = persistableType
        self.valueKey = valueKey
        self.primaryKeys = primaryKeys
        self.detectedAt = detectedAt
    }

    // MARK: - Convenience

    /// Unpack the value key into tuple elements
    ///
    /// - Returns: Array of tuple element descriptions
    public func unpackedValue() throws -> [String] {
        try Tuple.unpack(from: valueKey).map { String(describing: $0) }
    }

    /// Human-readable description of the duplicate value
    public var valueDescription: String {
        do {
            return try unpackedValue().joined(separator: ", ")
        } catch {
            return "<invalid tuple: \(error)>"
        }
    }

    /// Unpack primary keys into tuples
    ///
    /// - Returns: Array of primary key tuples
    public func unpackedPrimaryKeys() throws -> [Tuple] {
        try primaryKeys.map { bytes in
            Tuple(try Tuple.unpack(from: bytes))
        }
    }
}

// MARK: - CustomStringConvertible

extension UniquenessViolation: CustomStringConvertible {
    public var description: String {
        let pkDescriptions: [String]
        do {
            pkDescriptions = try unpackedPrimaryKeys().map { String(describing: $0) }
        } catch {
            pkDescriptions = ["<invalid tuple: \(error)>"]
        }
        return """
        UniquenessViolation(
            index: \(indexName),
            type: \(persistableType),
            value: [\(valueDescription)],
            conflictingEntities: \(pkDescriptions.count),
            primaryKeys: [\(pkDescriptions.joined(separator: ", "))],
            detectedAt: \(detectedAt)
        )
        """
    }
}

// MARK: - UniquenessViolationError

/// Error thrown when uniqueness constraint is violated
///
/// Provides detailed information about the conflict including:
/// - Which index was violated
/// - What value caused the conflict
/// - Which entities have the duplicate value
///
/// **Usage**:
/// ```swift
/// do {
///     try await context.save()
/// } catch let error as UniquenessViolationError {
///     print("Duplicate \(error.indexName): \(error.valueDescription)")
///     print("Existing entity: \(error.existingPrimaryKey)")
///     print("New entity: \(error.newPrimaryKey)")
/// }
/// ```
public struct UniquenessViolationError: Error, Sendable, CustomStringConvertible {
    /// Name of the violated index
    public let indexName: String

    /// Type name of the affected Persistable
    public let persistableType: String

    /// The duplicate value (as string descriptions)
    public let conflictingValues: [String]

    /// Primary key of the existing entity
    public let existingPrimaryKey: Tuple

    /// Primary key of the new entity attempting to insert
    public let newPrimaryKey: Tuple

    public init(
        indexName: String,
        persistableType: String,
        conflictingValues: [String],
        existingPrimaryKey: Tuple,
        newPrimaryKey: Tuple
    ) {
        self.indexName = indexName
        self.persistableType = persistableType
        self.conflictingValues = conflictingValues
        self.existingPrimaryKey = existingPrimaryKey
        self.newPrimaryKey = newPrimaryKey
    }

    /// Human-readable description of the duplicate value
    public var valueDescription: String {
        conflictingValues.joined(separator: ", ")
    }

    public var description: String {
        """
        Uniqueness violation on index '\(indexName)' for type '\(persistableType)':
        Value [\(valueDescription)] already exists.
        Existing entity: \(existingPrimaryKey)
        Conflicting entity: \(newPrimaryKey)
        """
    }
}

// MARK: - UniquenessCheckMode

/// Mode for handling uniqueness violations
///
/// Controls whether violations are immediately rejected or tracked for later resolution.
public enum UniquenessCheckMode: Sendable, Hashable {
    /// Throw error immediately on first violation (default for readable indexes)
    case immediate

    /// Track violations for later resolution (for write-only indexes during online indexing)
    ///
    /// Violations are stored in `[index_subspace]/_violations/` and can be
    /// scanned using `UniquenessViolationTracker.scanViolations()`.
    case track

    /// Skip uniqueness checks entirely (for disabled indexes)
    case skip
}

// MARK: - ViolationResolution

/// Result of attempting to resolve a uniqueness violation
public enum ViolationResolution: Sendable {
    /// Violation was resolved (duplicate entities no longer exist)
    case resolved

    /// Violation still exists (duplicate entities remain)
    case unresolved(UniquenessViolation)

    /// Violation entry was not found (may have been resolved already)
    case notFound
}
