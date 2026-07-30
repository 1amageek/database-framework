// UniquenessViolation.swift
// DatabaseEngine - Uniqueness violation tracking for indexes
//
// Reference: FDB Record Layer StandardIndexMaintainer.java
// https://github.com/FoundationDB/fdb-record-layer/blob/main/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/StandardIndexMaintainer.java

import StorageKit
import DatabaseKit
import DatabaseTypes
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
    public let valueKey: ByteString

    /// All primary keys that have this duplicate value
    ///
    /// Contains at least 2 entries (otherwise it's not a violation).
    public let primaryKeys: [ByteString]

    /// When the violation was first detected
    public let detectedAt: Timestamp

    // MARK: - Initialization

    public init(
        indexName: String,
        persistableType: String,
        valueKey: ByteString,
        primaryKeys: [ByteString],
        detectedAt: Timestamp
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
        try Tuple.unpack(from: valueKey).map {
            TupleElementSemanticName.describe($0)
        }
    }

    /// Human-readable description of the duplicate value
    public var valueDescription: String {
        do {
            return try unpackedValue().joined(separator: ", ")
        } catch {
            return "<invalid tuple>"
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
            pkDescriptions = try unpackedPrimaryKeys().map {
                DatabaseTextFormatting.lowercaseHex($0.pack())
            }
        } catch {
            pkDescriptions = ["<invalid tuple>"]
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

    /// Canonical values that conflict with an existing index entry.
    public let conflictingValues: [FieldValue]

    /// Primary key of the existing entity
    public let existingPrimaryKey: Tuple

    /// Primary key of the new entity attempting to insert
    public let newPrimaryKey: Tuple

    public init(
        indexName: String,
        persistableType: String,
        conflictingValues: [FieldValue],
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
        conflictingValues.map { value in
            if case .string(let text) = value {
                return text
            }
            return TupleElementSemanticName.describe(value)
        }
            .joined(separator: ", ")
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
