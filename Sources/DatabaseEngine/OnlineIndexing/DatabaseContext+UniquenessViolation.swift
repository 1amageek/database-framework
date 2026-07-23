// DatabaseContext+UniquenessViolation.swift
// DatabaseEngine - Uniqueness violation API extension for DatabaseContext
//
// This extension provides APIs for managing uniqueness violations that occur
// during online index building. It is separated from the core DatabaseContext to
// follow the extension pattern for optional features.

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import StorageKit

// MARK: - Uniqueness Violation API

extension DatabaseContext {
    /// Scan uniqueness violations for an index
    ///
    /// Returns all violations for the specified index on the given Persistable type.
    /// Use this after online indexing completes to review any uniqueness violations
    /// that were tracked during the build process.
    ///
    /// **Usage**:
    /// ```swift
    /// let violations = try await context.scanUniquenessViolations(
    ///     for: User.self,
    ///     indexName: "email_idx"
    /// )
    /// for violation in violations {
    ///     print("Duplicate value: \(violation.valueDescription)")
    ///     print("Conflicting records: \(violation.primaryKeys.count)")
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type to scan
    ///   - indexName: Name of the index to scan for violations
    ///   - limit: Maximum number of violations to return (nil = all)
    /// - Returns: Array of uniqueness violations
    public func scanUniquenessViolations<T: Persistable>(
        for type: T.Type,
        indexName: String,
        limit: Int? = nil
    ) async throws -> [UniquenessViolation] {
        let databaseStore = try await container.store(for: type)
        return try await databaseStore.violationTracker.scanViolations(
            indexName: indexName,
            limit: limit
        )
    }

    /// Scan uniqueness violations for a partitioned type
    ///
    /// Required for types with dynamic directories (`Field(\.keyPath)` in `#Directory`).
    /// Directory is resolved before data retrieval.
    ///
    /// **Usage**:
    /// ```swift
    /// var partition = DirectoryPath<TenantUser>()
    /// partition.set(\.tenantID, to: "tenant_123")
    /// let violations = try await context.scanUniquenessViolations(
    ///     for: TenantUser.self,
    ///     indexName: "email_idx",
    ///     partition: partition
    /// )
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type to scan
    ///   - indexName: Name of the index to scan for violations
    ///   - limit: Maximum number of violations to return (nil = all)
    ///   - partition: Partition binding specifying directory field values
    /// - Returns: Array of uniqueness violations
    public func scanUniquenessViolations<T: Persistable>(
        for type: T.Type,
        indexName: String,
        limit: Int? = nil,
        partition: DirectoryPath<T>
    ) async throws -> [UniquenessViolation] {
        let databaseStore = try await container.store(for: type, path: partition)
        return try await databaseStore.violationTracker.scanViolations(
            indexName: indexName,
            limit: limit
        )
    }

    /// Check if an index has any uniqueness violations
    ///
    /// Fast check without loading all violations.
    ///
    /// **Usage**:
    /// ```swift
    /// if try await context.hasUniquenessViolations(for: User.self, indexName: "email_idx") {
    ///     print("Index has violations - review before making readable")
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the index to check
    /// - Returns: True if violations exist
    public func hasUniquenessViolations<T: Persistable>(
        for type: T.Type,
        indexName: String
    ) async throws -> Bool {
        let databaseStore = try await container.store(for: type)
        return try await databaseStore.violationTracker.hasViolations(indexName: indexName)
    }

    /// Check if an index has any uniqueness violations (partitioned type)
    ///
    /// Required for types with dynamic directories.
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the index to check
    ///   - partition: Partition binding specifying directory field values
    /// - Returns: True if violations exist
    public func hasUniquenessViolations<T: Persistable>(
        for type: T.Type,
        indexName: String,
        partition: DirectoryPath<T>
    ) async throws -> Bool {
        let databaseStore = try await container.store(for: type, path: partition)
        return try await databaseStore.violationTracker.hasViolations(indexName: indexName)
    }

    /// Get a summary of uniqueness violations for an index
    ///
    /// Returns violation count and total conflicting records without loading
    /// all violation details.
    ///
    /// **Usage**:
    /// ```swift
    /// let summary = try await context.uniquenessViolationSummary(
    ///     for: User.self,
    ///     indexName: "email_idx"
    /// )
    /// if summary.hasViolations {
    ///     print("\(summary.violationCount) duplicate values")
    ///     print("\(summary.totalConflictingRecords) total conflicting records")
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the index
    /// - Returns: Violation summary
    public func uniquenessViolationSummary<T: Persistable>(
        for type: T.Type,
        indexName: String
    ) async throws -> ViolationSummary {
        let databaseStore = try await container.store(for: type)
        return try await databaseStore.violationTracker.violationSummary(indexName: indexName)
    }

    /// Get a summary of uniqueness violations for a partitioned type
    ///
    /// Required for types with dynamic directories.
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the index
    ///   - partition: Partition binding specifying directory field values
    /// - Returns: Violation summary
    public func uniquenessViolationSummary<T: Persistable>(
        for type: T.Type,
        indexName: String,
        partition: DirectoryPath<T>
    ) async throws -> ViolationSummary {
        let databaseStore = try await container.store(for: type, path: partition)
        return try await databaseStore.violationTracker.violationSummary(indexName: indexName)
    }

    /// Clear a resolved uniqueness violation
    ///
    /// Call this after confirming the violation has been resolved by
    /// deleting or updating the duplicate records.
    ///
    /// **Usage**:
    /// ```swift
    /// // After resolving a violation
    /// try await context.clearUniquenessViolation(
    ///     for: User.self,
    ///     indexName: "email_idx",
    ///     valueKey: violation.valueKey
    /// )
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the index
    ///   - valueKey: The duplicate value key to clear
    public func clearUniquenessViolation<T: Persistable>(
        for type: T.Type,
        indexName: String,
        valueKey: Bytes
    ) async throws {
        let databaseStore = try await container.store(for: type)
        try await databaseStore.violationTracker.clearViolation(
            indexName: indexName,
            valueKey: valueKey
        )
    }

    /// Clear a resolved uniqueness violation (partitioned type)
    ///
    /// Required for types with dynamic directories.
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the index
    ///   - valueKey: The duplicate value key to clear
    ///   - partition: Partition binding specifying directory field values
    public func clearUniquenessViolation<T: Persistable>(
        for type: T.Type,
        indexName: String,
        valueKey: Bytes,
        partition: DirectoryPath<T>
    ) async throws {
        let databaseStore = try await container.store(for: type, path: partition)
        try await databaseStore.violationTracker.clearViolation(
            indexName: indexName,
            valueKey: valueKey
        )
    }

    /// Clear all uniqueness violations for an index
    ///
    /// Use after all violations have been resolved or when resetting the index.
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the index
    public func clearAllUniquenessViolations<T: Persistable>(
        for type: T.Type,
        indexName: String
    ) async throws {
        let databaseStore = try await container.store(for: type)
        try await databaseStore.violationTracker.clearAllViolations(indexName: indexName)
    }

    /// Clear all uniqueness violations for a partitioned type
    ///
    /// Required for types with dynamic directories.
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the index
    ///   - partition: Partition binding specifying directory field values
    public func clearAllUniquenessViolations<T: Persistable>(
        for type: T.Type,
        indexName: String,
        partition: DirectoryPath<T>
    ) async throws {
        let databaseStore = try await container.store(for: type, path: partition)
        try await databaseStore.violationTracker.clearAllViolations(indexName: indexName)
    }

    /// Verify if a uniqueness violation has been resolved
    ///
    /// Checks the actual index to see if duplicate entries still exist.
    ///
    /// **Usage**:
    /// ```swift
    /// let resolution = try await context.verifyUniquenessViolationResolution(
    ///     for: User.self,
    ///     indexName: "email_idx",
    ///     valueKey: violation.valueKey
    /// )
    /// switch resolution {
    /// case .resolved:
    ///     try await context.clearUniquenessViolation(...)
    /// case .unresolved(let updatedViolation):
    ///     print("Still has \(updatedViolation.primaryKeys.count) duplicates")
    /// case .notFound:
    ///     print("Violation was already cleared")
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the index
    ///   - valueKey: The duplicate value key to verify
    /// - Returns: Resolution status
    public func verifyUniquenessViolationResolution<T: Persistable>(
        for type: T.Type,
        indexName: String,
        valueKey: Bytes
    ) async throws -> ViolationResolution {
        let databaseStore = try await container.store(for: type)

        let indexSubspace = databaseStore.indexSubspace.subspace(indexName)
        return try await databaseStore.violationTracker.verifyResolution(
            indexName: indexName,
            valueKey: valueKey,
            indexSubspace: indexSubspace
        )
    }

    /// Verify if a uniqueness violation has been resolved (partitioned type)
    ///
    /// Required for types with dynamic directories.
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the index
    ///   - valueKey: The duplicate value key to verify
    ///   - partition: Partition binding specifying directory field values
    /// - Returns: Resolution status
    public func verifyUniquenessViolationResolution<T: Persistable>(
        for type: T.Type,
        indexName: String,
        valueKey: Bytes,
        partition: DirectoryPath<T>
    ) async throws -> ViolationResolution {
        let databaseStore = try await container.store(for: type, path: partition)

        let indexSubspace = databaseStore.indexSubspace.subspace(indexName)
        return try await databaseStore.violationTracker.verifyResolution(
            indexName: indexName,
            valueKey: valueKey,
            indexSubspace: indexSubspace
        )
    }
}
