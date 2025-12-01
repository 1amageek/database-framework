import Core

/// SchemaMigrationPlan - SwiftData-compatible migration plan protocol
///
/// **Design**: A migration plan defines the complete migration path between
/// schema versions. It lists all schemas (in version order) and the stages
/// to transition between them.
///
/// **Example usage**:
/// ```swift
/// enum AppMigrationPlan: SchemaMigrationPlan {
///     static var schemas: [any VersionedSchema.Type] {
///         [AppSchemaV1.self, AppSchemaV2.self, AppSchemaV3.self]
///     }
///
///     static var stages: [MigrationStage] {
///         [migrateV1toV2, migrateV2toV3]
///     }
///
///     static let migrateV1toV2 = MigrationStage.lightweight(
///         fromVersion: AppSchemaV1.self,
///         toVersion: AppSchemaV2.self
///     )
///
///     static let migrateV2toV3 = MigrationStage.custom(
///         fromVersion: AppSchemaV2.self,
///         toVersion: AppSchemaV3.self,
///         willMigrate: { context in
///             // Pre-migration cleanup
///         },
///         didMigrate: nil
///     )
/// }
///
/// // Use with FDBContainer
/// let container = try await FDBContainer(
///     for: AppSchemaV3.self,
///     migrationPlan: AppMigrationPlan.self
/// )
/// try await container.migrateIfNeeded()
/// ```
public protocol SchemaMigrationPlan: Sendable {
    /// All schema versions in order (oldest to newest)
    ///
    /// Must include all versions in the migration chain.
    /// Order is important: first element should be the oldest version,
    /// last element should be the current version.
    static var schemas: [any VersionedSchema.Type] { get }

    /// Migration stages
    ///
    /// Defines how to migrate between consecutive versions.
    /// Should have N-1 stages for N schemas (one for each version transition).
    static var stages: [MigrationStage] { get }
}

// MARK: - SchemaMigrationPlan Extensions

extension SchemaMigrationPlan {
    /// Get the current (latest) schema version
    public static var currentSchema: (any VersionedSchema.Type)? {
        return schemas.last
    }

    /// Get the current version identifier
    public static var currentVersion: Schema.Version? {
        return currentSchema?.versionIdentifier
    }

    /// Find migration path between versions
    ///
    /// Returns the stages needed to migrate from one version to another.
    ///
    /// - Parameters:
    ///   - from: Source version
    ///   - to: Target version
    /// - Returns: Array of MigrationStage in execution order
    /// - Throws: Error if no path exists
    public static func findPath(
        from: Schema.Version,
        to: Schema.Version
    ) throws -> [MigrationStage] {
        guard from < to else {
            // Downgrades not supported
            if from == to {
                return []  // Already at target version
            }
            throw MigrationPlanError.downgradeNotSupported(from: from, to: to)
        }

        var path: [MigrationStage] = []
        var currentVersion = from

        while currentVersion < to {
            // Find stage that starts at currentVersion
            guard let nextStage = stages.first(where: { $0.fromVersionIdentifier == currentVersion }) else {
                throw MigrationPlanError.noMigrationPath(from: currentVersion, to: to)
            }

            path.append(nextStage)
            currentVersion = nextStage.toVersionIdentifier

            // Safety check to prevent infinite loops
            if path.count > schemas.count {
                throw MigrationPlanError.cyclicMigrationPath(from: from, to: to)
            }
        }

        return path
    }

    /// Validate the migration plan
    ///
    /// Checks that:
    /// - Schemas are in ascending version order
    /// - Stages form a complete chain
    /// - No duplicate versions
    ///
    /// - Throws: MigrationPlanError if validation fails
    public static func validate() throws {
        // Check at least 2 schemas for migration
        guard schemas.count >= 2 else {
            if schemas.count == 1 {
                // Single schema is valid (no migrations needed)
                return
            }
            throw MigrationPlanError.emptySchemaList
        }

        // Check version ordering
        var previousVersion: Schema.Version? = nil
        var seenVersions: Set<String> = []

        for schema in schemas {
            let version = schema.versionIdentifier
            let versionString = version.description

            // Check for duplicates
            if seenVersions.contains(versionString) {
                throw MigrationPlanError.duplicateVersion(version)
            }
            seenVersions.insert(versionString)

            // Check ascending order
            if let prev = previousVersion, prev >= version {
                throw MigrationPlanError.versionsNotOrdered(prev, version)
            }
            previousVersion = version
        }

        // Check stages form a chain
        let expectedStageCount = schemas.count - 1
        guard stages.count == expectedStageCount else {
            throw MigrationPlanError.stageCountMismatch(
                expected: expectedStageCount,
                actual: stages.count
            )
        }

        // Verify each stage connects consecutive schemas
        for (index, stage) in stages.enumerated() {
            let expectedFrom = schemas[index].versionIdentifier
            let expectedTo = schemas[index + 1].versionIdentifier

            if stage.fromVersionIdentifier != expectedFrom {
                throw MigrationPlanError.stageMismatch(
                    stageIndex: index,
                    expected: expectedFrom,
                    actual: stage.fromVersionIdentifier
                )
            }

            if stage.toVersionIdentifier != expectedTo {
                throw MigrationPlanError.stageMismatch(
                    stageIndex: index,
                    expected: expectedTo,
                    actual: stage.toVersionIdentifier
                )
            }
        }
    }
}

// MARK: - MigrationPlanError

/// Errors that can occur during migration plan validation or execution
public enum MigrationPlanError: Error, CustomStringConvertible {
    /// Schema list is empty
    case emptySchemaList

    /// Duplicate schema version detected
    case duplicateVersion(Schema.Version)

    /// Schema versions are not in ascending order
    case versionsNotOrdered(Schema.Version, Schema.Version)

    /// Number of stages doesn't match schemas
    case stageCountMismatch(expected: Int, actual: Int)

    /// Stage doesn't connect expected versions
    case stageMismatch(stageIndex: Int, expected: Schema.Version, actual: Schema.Version)

    /// No migration path exists between versions
    case noMigrationPath(from: Schema.Version, to: Schema.Version)

    /// Downgrade migrations are not supported
    case downgradeNotSupported(from: Schema.Version, to: Schema.Version)

    /// Cyclic migration path detected
    case cyclicMigrationPath(from: Schema.Version, to: Schema.Version)

    public var description: String {
        switch self {
        case .emptySchemaList:
            return "Migration plan has no schemas defined"

        case .duplicateVersion(let version):
            return "Duplicate schema version: \(version)"

        case .versionsNotOrdered(let v1, let v2):
            return "Schema versions not in ascending order: \(v1) should come before \(v2)"

        case .stageCountMismatch(let expected, let actual):
            return "Stage count mismatch: expected \(expected) stages, got \(actual)"

        case .stageMismatch(let index, let expected, let actual):
            return "Stage \(index) version mismatch: expected \(expected), got \(actual)"

        case .noMigrationPath(let from, let to):
            return "No migration path from \(from) to \(to)"

        case .downgradeNotSupported(let from, let to):
            return "Downgrade not supported: \(from) to \(to)"

        case .cyclicMigrationPath(let from, let to):
            return "Cyclic migration path detected from \(from) to \(to)"
        }
    }
}
