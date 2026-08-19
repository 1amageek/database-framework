import DatabaseKit

/// Persisted migration progress for the selected data root and compiled schema.
public struct DatabaseMigrationStatus: Sendable, Hashable {
    public let currentVersion: Schema.Version?
    public let targetVersion: Schema.Version
    public let pendingMigrationIdentifiers: [String]

    public init(
        currentVersion: Schema.Version?,
        targetVersion: Schema.Version,
        pendingMigrationIdentifiers: [String]
    ) {
        self.currentVersion = currentVersion
        self.targetVersion = targetVersion
        self.pendingMigrationIdentifiers = pendingMigrationIdentifiers
    }
}
