// UniquenessViolationTracker.swift
// DatabaseEngine - Tracks and manages uniqueness violations
//
// Reference: FDB Record Layer IndexingMerger.java (violation tracking during merge)
// https://github.com/FoundationDB/fdb-record-layer/blob/main/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexingMerger.java

import Foundation
import FoundationDB
import Core
import Logging

// MARK: - UniquenessViolationTracker

/// Tracks uniqueness violations for indexes during online indexing
///
/// When building an index online (write-only mode), violations are recorded
/// instead of immediately throwing errors. This allows the indexing to complete
/// and violations to be reviewed and resolved afterward.
///
/// **Storage Format**:
/// ```
/// [metadataSubspace]/_violations/[indexName]/[valueKey] → ViolationRecord (JSON)
/// ```
///
/// **Lifecycle**:
/// 1. During online indexing: `recordViolation()` stores conflicts
/// 2. After indexing: `scanViolations()` retrieves all violations
/// 3. User resolves conflicts (deletes duplicates or updates values)
/// 4. Call `verifyResolution()` to confirm fix
/// 5. Call `clearViolation()` to remove the violation record
///
/// **Usage**:
/// ```swift
/// let tracker = UniquenessViolationTracker(
///     database: database,
///     metadataSubspace: metadataSubspace
/// )
///
/// // After online indexing completes
/// let violations = try await tracker.scanViolations(indexName: "email_idx")
/// if !violations.isEmpty {
///     print("Found \(violations.count) uniqueness violations")
///     for violation in violations {
///         print(violation)
///     }
/// }
/// ```
///
/// **Reference**: FDB Record Layer tracks violations during index merging
public final class UniquenessViolationTracker: Sendable {
    // MARK: - Constants

    /// Subspace key for violations storage
    private static let violationsKey = "_violations"

    // MARK: - Properties

    /// Database connection
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Metadata subspace containing violation records
    private let metadataSubspace: Subspace

    /// Logger
    private let logger: Logger

    // MARK: - Initialization

    /// Create a violation tracker
    ///
    /// - Parameters:
    ///   - database: FDB database connection
    ///   - metadataSubspace: Subspace for metadata storage (usually `[store]/M/`)
    public init(
        database: any DatabaseProtocol,
        metadataSubspace: Subspace
    ) {
        self.database = database
        self.metadataSubspace = metadataSubspace
        self.logger = Logger(label: "com.fdb.uniqueness.tracker")
    }

    // MARK: - Violation Subspace

    /// Get the subspace for storing violations
    private var violationsSubspace: Subspace {
        metadataSubspace.subspace(Self.violationsKey)
    }

    /// Get subspace for a specific index's violations
    private func indexViolationsSubspace(indexName: String) -> Subspace {
        violationsSubspace.subspace(indexName)
    }

    // MARK: - Record Violations

    /// Record a uniqueness violation
    ///
    /// Called during online indexing when a duplicate is detected.
    /// If a violation for the same value already exists, the new primary key
    /// is added to the existing violation record.
    ///
    /// - Parameters:
    ///   - indexName: Name of the violated index
    ///   - persistableType: Type name of the affected model
    ///   - valueKey: The duplicate index value (packed tuple)
    ///   - primaryKey: Primary key of the conflicting record
    ///   - transaction: Current transaction
    public func recordViolation(
        indexName: String,
        persistableType: String,
        valueKey: [UInt8],
        primaryKey: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let subspace = indexViolationsSubspace(indexName: indexName)
        let key = subspace.pack(Tuple(valueKey))
        let pkBytes = primaryKey.pack()

        // Check if violation already exists
        if let existingData = try await transaction.getValue(for: key, snapshot: false) {
            // Parse existing violation and add new primary key
            let violation = try JSONDecoder().decode(UniquenessViolation.self, from: Data(existingData))

            // Check if this PK is already recorded
            if !violation.primaryKeys.contains(where: { $0 == pkBytes }) {
                // Add new primary key
                let updatedViolation = UniquenessViolation(
                    indexName: violation.indexName,
                    persistableType: violation.persistableType,
                    valueKey: violation.valueKey,
                    primaryKeys: violation.primaryKeys + [pkBytes],
                    detectedAt: violation.detectedAt
                )

                let data = try JSONEncoder().encode(updatedViolation)
                transaction.setValue(Array(data), for: key)

                logger.debug(
                    "Added primary key to existing violation",
                    metadata: [
                        "indexName": "\(indexName)",
                        "totalConflicts": "\(updatedViolation.primaryKeys.count)"
                    ]
                )
            }
        } else {
            // Create new violation record
            // We need at least 2 primary keys for a violation, but we might be
            // called with just the second conflicting key. The first key is
            // already in the index, so we need to find it.
            let violation = UniquenessViolation(
                indexName: indexName,
                persistableType: persistableType,
                valueKey: valueKey,
                primaryKeys: [pkBytes],  // Will be updated when second conflict is found
                detectedAt: Date()
            )

            let data = try JSONEncoder().encode(violation)
            transaction.setValue(Array(data), for: key)

            logger.info(
                "Recorded new uniqueness violation",
                metadata: [
                    "indexName": "\(indexName)",
                    "type": "\(persistableType)"
                ]
            )
        }
    }

    /// Record a complete violation with both conflicting primary keys
    ///
    /// - Parameters:
    ///   - indexName: Name of the violated index
    ///   - persistableType: Type name of the affected model
    ///   - valueKey: The duplicate index value (packed tuple)
    ///   - existingPrimaryKey: Primary key of the existing record
    ///   - newPrimaryKey: Primary key of the new conflicting record
    ///   - transaction: Current transaction
    public func recordViolation(
        indexName: String,
        persistableType: String,
        valueKey: [UInt8],
        existingPrimaryKey: Tuple,
        newPrimaryKey: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let subspace = indexViolationsSubspace(indexName: indexName)
        let key = subspace.pack(Tuple(valueKey))

        let existingBytes = existingPrimaryKey.pack()
        let newBytes = newPrimaryKey.pack()

        // Check if violation already exists
        if let existingData = try await transaction.getValue(for: key, snapshot: false) {
            let violation = try JSONDecoder().decode(UniquenessViolation.self, from: Data(existingData))

            // Merge primary keys
            var allPKs = violation.primaryKeys
            if !allPKs.contains(where: { $0 == existingBytes }) {
                allPKs.append(existingBytes)
            }
            if !allPKs.contains(where: { $0 == newBytes }) {
                allPKs.append(newBytes)
            }

            let updatedViolation = UniquenessViolation(
                indexName: violation.indexName,
                persistableType: violation.persistableType,
                valueKey: violation.valueKey,
                primaryKeys: allPKs,
                detectedAt: violation.detectedAt
            )

            let data = try JSONEncoder().encode(updatedViolation)
            transaction.setValue(Array(data), for: key)
        } else {
            // Create new violation with both keys
            let violation = UniquenessViolation(
                indexName: indexName,
                persistableType: persistableType,
                valueKey: valueKey,
                primaryKeys: [existingBytes, newBytes],
                detectedAt: Date()
            )

            let data = try JSONEncoder().encode(violation)
            transaction.setValue(Array(data), for: key)

            logger.info(
                "Recorded uniqueness violation",
                metadata: [
                    "indexName": "\(indexName)",
                    "type": "\(persistableType)",
                    "conflictCount": "2"
                ]
            )
        }
    }

    // MARK: - Scan Violations

    /// Scan all violations for an index
    ///
    /// - Parameters:
    ///   - indexName: Name of the index to scan
    ///   - limit: Maximum number of violations to return (nil = all)
    /// - Returns: Array of violations
    public func scanViolations(
        indexName: String,
        limit: Int? = nil
    ) async throws -> [UniquenessViolation] {
        try await database.withTransaction(configuration: .batch) { transaction in
            try await self.scanViolations(
                indexName: indexName,
                limit: limit,
                transaction: transaction
            )
        }
    }

    /// Scan all violations for an index within a transaction
    ///
    /// - Parameters:
    ///   - indexName: Name of the index to scan
    ///   - limit: Maximum number of violations to return (nil = all)
    ///   - transaction: Current transaction
    /// - Returns: Array of violations
    public func scanViolations(
        indexName: String,
        limit: Int? = nil,
        transaction: any TransactionProtocol
    ) async throws -> [UniquenessViolation] {
        let subspace = indexViolationsSubspace(indexName: indexName)
        let (begin, end) = subspace.range()

        var violations: [UniquenessViolation] = []
        var count = 0

        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        for try await (_, value) in sequence {
            if let maxLimit = limit, count >= maxLimit {
                break
            }

            let violation = try JSONDecoder().decode(
                UniquenessViolation.self,
                from: Data(value)
            )
            violations.append(violation)
            count += 1
        }

        return violations
    }

    /// Scan all violations across all indexes
    ///
    /// - Parameter limit: Maximum number of violations to return per index
    /// - Returns: Dictionary of index name to violations
    public func scanAllViolations(
        limit: Int? = nil
    ) async throws -> [String: [UniquenessViolation]] {
        try await database.withTransaction(configuration: .batch) { transaction in
            try await self.scanAllViolations(limit: limit, transaction: transaction)
        }
    }

    /// Scan all violations across all indexes within a transaction
    public func scanAllViolations(
        limit: Int? = nil,
        transaction: any TransactionProtocol
    ) async throws -> [String: [UniquenessViolation]] {
        let (begin, end) = violationsSubspace.range()

        var result: [String: [UniquenessViolation]] = [:]

        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        for try await (_, value) in sequence {
            let violation = try JSONDecoder().decode(
                UniquenessViolation.self,
                from: Data(value)
            )

            if let maxLimit = limit {
                let currentCount = result[violation.indexName]?.count ?? 0
                if currentCount >= maxLimit {
                    continue
                }
            }

            result[violation.indexName, default: []].append(violation)
        }

        return result
    }

    /// Check if an index has any violations
    ///
    /// - Parameter indexName: Name of the index to check
    /// - Returns: True if violations exist
    public func hasViolations(indexName: String) async throws -> Bool {
        try await database.withTransaction(configuration: .batch) { transaction in
            try await self.hasViolations(indexName: indexName, transaction: transaction)
        }
    }

    /// Check if an index has any violations within a transaction
    public func hasViolations(
        indexName: String,
        transaction: any TransactionProtocol
    ) async throws -> Bool {
        let subspace = indexViolationsSubspace(indexName: indexName)
        let (begin, end) = subspace.range()

        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        for try await _ in sequence {
            return true
        }

        return false
    }

    /// Count violations for an index
    ///
    /// - Parameter indexName: Name of the index
    /// - Returns: Number of distinct value violations (not total conflicting records)
    public func countViolations(indexName: String) async throws -> Int {
        try await database.withTransaction(configuration: .batch) { transaction in
            try await self.countViolations(indexName: indexName, transaction: transaction)
        }
    }

    /// Count violations for an index within a transaction
    public func countViolations(
        indexName: String,
        transaction: any TransactionProtocol
    ) async throws -> Int {
        let subspace = indexViolationsSubspace(indexName: indexName)
        let (begin, end) = subspace.range()

        var count = 0
        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        for try await _ in sequence {
            count += 1
        }

        return count
    }

    // MARK: - Resolution

    /// Verify if a violation has been resolved
    ///
    /// Checks the actual index to see if duplicates still exist.
    ///
    /// - Parameters:
    ///   - indexName: Name of the index
    ///   - valueKey: The duplicate value to check
    ///   - indexSubspace: Subspace of the index
    /// - Returns: Resolution result
    public func verifyResolution(
        indexName: String,
        valueKey: [UInt8],
        indexSubspace: Subspace
    ) async throws -> ViolationResolution {
        try await database.withTransaction(configuration: .batch) { transaction in
            try await self.verifyResolution(
                indexName: indexName,
                valueKey: valueKey,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }
    }

    /// Verify if a violation has been resolved within a transaction
    public func verifyResolution(
        indexName: String,
        valueKey: [UInt8],
        indexSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> ViolationResolution {
        // Check violation record
        let violationSubspace = indexViolationsSubspace(indexName: indexName)
        let violationKey = violationSubspace.pack(Tuple(valueKey))

        guard let violationData = try await transaction.getValue(for: violationKey, snapshot: false) else {
            return .notFound
        }

        let violation = try JSONDecoder().decode(UniquenessViolation.self, from: Data(violationData))

        // Check actual index for duplicates
        let valueSubspace = Subspace(prefix: indexSubspace.prefix + valueKey)
        let (begin, end) = valueSubspace.range()

        var foundPrimaryKeys: [[UInt8]] = []
        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        for try await (key, _) in sequence {
            // Extract primary key from index key
            let keyBytes = Array(key)
            if keyBytes.count > valueSubspace.prefix.count {
                let pkBytes = Array(keyBytes.dropFirst(valueSubspace.prefix.count))
                foundPrimaryKeys.append(pkBytes)
            }
        }

        if foundPrimaryKeys.count <= 1 {
            // No longer a violation
            return .resolved
        } else {
            // Still has duplicates
            let updatedViolation = UniquenessViolation(
                indexName: violation.indexName,
                persistableType: violation.persistableType,
                valueKey: violation.valueKey,
                primaryKeys: foundPrimaryKeys,
                detectedAt: violation.detectedAt
            )
            return .unresolved(updatedViolation)
        }
    }

    /// Clear a violation record
    ///
    /// Call this after confirming the violation has been resolved.
    ///
    /// - Parameters:
    ///   - indexName: Name of the index
    ///   - valueKey: The value key to clear
    public func clearViolation(
        indexName: String,
        valueKey: [UInt8]
    ) async throws {
        try await database.withTransaction(configuration: .batch) { transaction in
            try await self.clearViolation(
                indexName: indexName,
                valueKey: valueKey,
                transaction: transaction
            )
        }
    }

    /// Clear a violation record within a transaction
    public func clearViolation(
        indexName: String,
        valueKey: [UInt8],
        transaction: any TransactionProtocol
    ) async throws {
        let subspace = indexViolationsSubspace(indexName: indexName)
        let key = subspace.pack(Tuple(valueKey))
        transaction.clear(key: key)

        logger.info(
            "Cleared violation record",
            metadata: ["indexName": "\(indexName)"]
        )
    }

    /// Clear all violations for an index
    ///
    /// Use after all violations have been resolved or when resetting the index.
    ///
    /// - Parameter indexName: Name of the index
    public func clearAllViolations(indexName: String) async throws {
        try await database.withTransaction(configuration: .batch) { transaction in
            try await self.clearAllViolations(indexName: indexName, transaction: transaction)
        }
    }

    /// Clear all violations for an index within a transaction
    public func clearAllViolations(
        indexName: String,
        transaction: any TransactionProtocol
    ) async throws {
        let subspace = indexViolationsSubspace(indexName: indexName)
        let (begin, end) = subspace.range()
        transaction.clearRange(beginKey: begin, endKey: end)

        logger.info(
            "Cleared all violations for index",
            metadata: ["indexName": "\(indexName)"]
        )
    }
}

// MARK: - Violation Summary

/// Summary of uniqueness violations for an index
public struct ViolationSummary: Sendable {
    /// Index name
    public let indexName: String

    /// Number of distinct duplicate values
    public let violationCount: Int

    /// Total number of conflicting records
    public let totalConflictingRecords: Int

    /// Whether violations exist
    public var hasViolations: Bool {
        violationCount > 0
    }
}

extension UniquenessViolationTracker {
    /// Get summary of violations for an index
    public func violationSummary(indexName: String) async throws -> ViolationSummary {
        let violations = try await scanViolations(indexName: indexName)

        let totalRecords = violations.reduce(0) { $0 + $1.primaryKeys.count }

        return ViolationSummary(
            indexName: indexName,
            violationCount: violations.count,
            totalConflictingRecords: totalRecords
        )
    }
}
