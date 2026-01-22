// VersionQuery+Diff.swift
// VersionIndex - Diff extension for version queries
//
// Provides diff functionality between version history entries.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - VersionQueryBuilder+Diff

extension VersionQueryBuilder {

    /// Compute diff between two specific versions
    ///
    /// Retrieves items at both versions and computes the diff.
    ///
    /// **Usage**:
    /// ```swift
    /// let diff = try await context.versions(Document.self)
    ///     .forItem(documentId)
    ///     .diff(from: oldVersion, to: newVersion)
    ///
    /// print("Changed fields: \(diff.modifiedFields)")
    /// ```
    ///
    /// - Parameters:
    ///   - oldVersion: The older version (base)
    ///   - newVersion: The newer version (current)
    ///   - options: Diff computation options
    /// - Returns: ModelDiff with version information
    /// - Throws: DiffError if models not found or diff fails
    public func diff(
        from oldVersion: Version,
        to newVersion: Version,
        options: DiffOptions = DiffOptions()
    ) async throws -> ModelDiff {
        // Retrieve both versions
        guard let oldItem: T = try await self.at(oldVersion) else {
            throw DiffError.modelNotFoundAtVersion(
                id: primaryKeyDescription,
                version: oldVersion.description
            )
        }
        guard let newItem: T = try await self.at(newVersion) else {
            throw DiffError.modelNotFoundAtVersion(
                id: primaryKeyDescription,
                version: newVersion.description
            )
        }

        // Compute diff using ModelDiffBuilder
        let baseDiff = try ModelDiffBuilder.diff(
            old: oldItem,
            new: newItem,
            options: options
        )

        // Add version information
        return ModelDiff(
            typeName: baseDiff.typeName,
            idString: baseDiff.idString,
            changes: baseDiff.changes,
            timestamp: baseDiff.timestamp,
            oldVersion: VersionInfo(
                versionID: oldVersion.description,
                timestamp: nil
            ),
            newVersion: VersionInfo(
                versionID: newVersion.description,
                timestamp: nil
            )
        )
    }

    /// Compute diff from the previous version
    ///
    /// Gets the latest version and the version before it, then computes the diff.
    /// Returns nil if there's no previous version to compare against.
    ///
    /// **Usage**:
    /// ```swift
    /// if let diff = try await context.versions(Document.self)
    ///     .forItem(documentId)
    ///     .diffFromPrevious() {
    ///     print("Latest changes: \(diff.modifiedFields)")
    /// }
    /// ```
    ///
    /// - Parameter options: Diff computation options
    /// - Returns: ModelDiff if there are at least 2 versions, nil otherwise
    /// - Throws: DiffError if diff computation fails
    public func diffFromPrevious(
        options: DiffOptions = DiffOptions()
    ) async throws -> ModelDiff? {
        // Get the two most recent versions
        let history = try await self.limit(2).execute()

        guard history.count >= 2 else {
            // Not enough versions to compute diff
            return nil
        }

        let (newVersion, newItem) = history[0]
        let (oldVersion, oldItem) = history[1]

        // Compute diff using ModelDiffBuilder
        let baseDiff = try ModelDiffBuilder.diff(
            old: oldItem,
            new: newItem,
            options: options
        )

        // Add version information
        return ModelDiff(
            typeName: baseDiff.typeName,
            idString: baseDiff.idString,
            changes: baseDiff.changes,
            timestamp: baseDiff.timestamp,
            oldVersion: VersionInfo(
                versionID: oldVersion.description,
                timestamp: nil
            ),
            newVersion: VersionInfo(
                versionID: newVersion.description,
                timestamp: nil
            )
        )
    }

    /// Compute diff between the latest version and a specific older version
    ///
    /// **Usage**:
    /// ```swift
    /// let diff = try await context.versions(Document.self)
    ///     .forItem(documentId)
    ///     .diffFromLatest(since: someOldVersion)
    ///
    /// print("All changes since v1: \(diff.changedFields)")
    /// ```
    ///
    /// - Parameters:
    ///   - oldVersion: The older version to compare from
    ///   - options: Diff computation options
    /// - Returns: ModelDiff comparing old version to latest
    /// - Throws: DiffError if models not found or diff fails
    public func diffFromLatest(
        since oldVersion: Version,
        options: DiffOptions = DiffOptions()
    ) async throws -> ModelDiff {
        // Get the older version
        guard let oldItem: T = try await self.at(oldVersion) else {
            throw DiffError.modelNotFoundAtVersion(
                id: primaryKeyDescription,
                version: oldVersion.description
            )
        }

        // Get the latest version
        guard let (latestVersion, latestItem) = try await self.limit(1).execute().first else {
            throw DiffError.insufficientVersionHistory(
                id: primaryKeyDescription,
                required: 1,
                available: 0
            )
        }

        // Compute diff using ModelDiffBuilder
        let baseDiff = try ModelDiffBuilder.diff(
            old: oldItem,
            new: latestItem,
            options: options
        )

        // Add version information
        return ModelDiff(
            typeName: baseDiff.typeName,
            idString: baseDiff.idString,
            changes: baseDiff.changes,
            timestamp: baseDiff.timestamp,
            oldVersion: VersionInfo(
                versionID: oldVersion.description,
                timestamp: nil
            ),
            newVersion: VersionInfo(
                versionID: latestVersion.description,
                timestamp: nil
            )
        )
    }

    /// Get all diffs between consecutive versions in history
    ///
    /// Useful for audit logs that need to show the complete change history.
    ///
    /// **Usage**:
    /// ```swift
    /// let diffs = try await context.versions(Document.self)
    ///     .forItem(documentId)
    ///     .limit(10)
    ///     .allDiffs()
    ///
    /// for diff in diffs {
    ///     print("Version \(diff.oldVersion?.versionID ?? "?") -> \(diff.newVersion?.versionID ?? "?")")
    ///     print("  Changes: \(diff.changedFields)")
    /// }
    /// ```
    ///
    /// - Parameter options: Diff computation options
    /// - Returns: Array of diffs between consecutive versions (newest first)
    /// - Throws: DiffError if diff computation fails
    public func allDiffs(
        options: DiffOptions = DiffOptions()
    ) async throws -> [ModelDiff] {
        let history = try await self.execute()

        guard history.count >= 2 else {
            return []
        }

        var diffs: [ModelDiff] = []

        for i in 0..<(history.count - 1) {
            let (newVersion, newItem) = history[i]
            let (oldVersion, oldItem) = history[i + 1]

            let baseDiff = try ModelDiffBuilder.diff(
                old: oldItem,
                new: newItem,
                options: options
            )

            let versionedDiff = ModelDiff(
                typeName: baseDiff.typeName,
                idString: baseDiff.idString,
                changes: baseDiff.changes,
                timestamp: baseDiff.timestamp,
                oldVersion: VersionInfo(
                    versionID: oldVersion.description,
                    timestamp: nil
                ),
                newVersion: VersionInfo(
                    versionID: newVersion.description,
                    timestamp: nil
                )
            )

            diffs.append(versionedDiff)
        }

        return diffs
    }

    /// Check if there are any changes from the previous version
    ///
    /// More efficient than computing full diff when you only need to know
    /// whether changes exist.
    ///
    /// - Parameter excludeFields: Fields to exclude from comparison
    /// - Returns: True if there are changes, false if same or no previous version
    public func hasChangesFromPrevious(
        excludeFields: Set<String> = []
    ) async throws -> Bool {
        let history = try await self.limit(2).execute()

        guard history.count >= 2 else {
            return false
        }

        let (_, newItem) = history[0]
        let (_, oldItem) = history[1]

        return ModelDiffBuilder.hasChanges(
            old: oldItem,
            new: newItem,
            excludeFields: excludeFields
        )
    }

    // MARK: - Private Helpers

    /// String description of the primary key for error messages
    private var primaryKeyDescription: String {
        primaryKey.map { "\($0)" }.joined(separator: ", ")
    }
}
