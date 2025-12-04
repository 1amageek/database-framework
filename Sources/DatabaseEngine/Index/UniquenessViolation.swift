// UniquenessViolation.swift
// DatabaseEngine - Uniqueness violation tracking for indexes
//
// Reference: FDB Record Layer StandardIndexMaintainer.java
// https://github.com/FoundationDB/fdb-record-layer/blob/main/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/StandardIndexMaintainer.java

import Foundation
import FoundationDB
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
///     print("Duplicate value \(violation.valueDescription) found for records: \(violation.primaryKeys)")
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
    public let valueKey: [UInt8]

    /// All primary keys that have this duplicate value
    ///
    /// Contains at least 2 entries (otherwise it's not a violation).
    public let primaryKeys: [[UInt8]]

    /// When the violation was first detected
    public let detectedAt: Date

    // MARK: - Initialization

    public init(
        indexName: String,
        persistableType: String,
        valueKey: [UInt8],
        primaryKeys: [[UInt8]],
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
    public func unpackedValue() -> [String] {
        do {
            let elements = try Tuple.unpack(from: valueKey)
            return elements.map { String(describing: $0) }
        } catch {
            return ["<unpacking failed>"]
        }
    }

    /// Human-readable description of the duplicate value
    public var valueDescription: String {
        unpackedValue().joined(separator: ", ")
    }

    /// Unpack primary keys into tuples
    ///
    /// - Returns: Array of primary key tuples
    public func unpackedPrimaryKeys() -> [Tuple] {
        primaryKeys.compactMap { bytes in
            guard let elements = try? Tuple.unpack(from: bytes) else { return nil }
            return Tuple(elements)
        }
    }
}

// MARK: - CustomStringConvertible

extension UniquenessViolation: CustomStringConvertible {
    public var description: String {
        let pkDescriptions = unpackedPrimaryKeys().map { String(describing: $0) }
        return """
        UniquenessViolation(
            index: \(indexName),
            type: \(persistableType),
            value: [\(valueDescription)],
            conflictingRecords: \(pkDescriptions.count),
            primaryKeys: [\(pkDescriptions.joined(separator: ", "))],
            detectedAt: \(detectedAt)
        )
        """
    }
}

// MARK: - Codable

extension UniquenessViolation: Codable {
    enum CodingKeys: String, CodingKey {
        case indexName
        case persistableType
        case valueKey
        case primaryKeys
        case detectedAt
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        indexName = try container.decode(String.self, forKey: .indexName)
        persistableType = try container.decode(String.self, forKey: .persistableType)
        valueKey = try container.decode([UInt8].self, forKey: .valueKey)
        primaryKeys = try container.decode([[UInt8]].self, forKey: .primaryKeys)
        detectedAt = try container.decode(Date.self, forKey: .detectedAt)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(indexName, forKey: .indexName)
        try container.encode(persistableType, forKey: .persistableType)
        try container.encode(valueKey, forKey: .valueKey)
        try container.encode(primaryKeys, forKey: .primaryKeys)
        try container.encode(detectedAt, forKey: .detectedAt)
    }
}

// MARK: - UniquenessViolationError

/// Error thrown when uniqueness constraint is violated
///
/// Provides detailed information about the conflict including:
/// - Which index was violated
/// - What value caused the conflict
/// - Which records have the duplicate value
///
/// **Usage**:
/// ```swift
/// do {
///     try await context.save()
/// } catch let error as UniquenessViolationError {
///     print("Duplicate \(error.indexName): \(error.valueDescription)")
///     print("Existing record: \(error.existingPrimaryKey)")
///     print("New record: \(error.newPrimaryKey)")
/// }
/// ```
public struct UniquenessViolationError: Error, Sendable, CustomStringConvertible {
    /// Name of the violated index
    public let indexName: String

    /// Type name of the affected Persistable
    public let persistableType: String

    /// The duplicate value (as string descriptions)
    public let conflictingValues: [String]

    /// Primary key of the existing record
    public let existingPrimaryKey: Tuple

    /// Primary key of the new record attempting to insert
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
        Existing record: \(existingPrimaryKey)
        Conflicting record: \(newPrimaryKey)
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
    /// Violation was resolved (duplicate records no longer exist)
    case resolved

    /// Violation still exists (duplicate records remain)
    case unresolved(UniquenessViolation)

    /// Violation record was not found (may have been resolved already)
    case notFound
}
