import Core

/// MigrationStage - Migration stage definition
///
/// **Design**: Each migration step is represented as a MigrationStage.
/// Stages can be lightweight (automatic) or custom (with manual code).
///
/// **Lightweight Migration**:
/// Automatically handles:
/// - Index additions (builds via OnlineIndexer)
/// - Index removals (clears data and marks deleted)
/// - Field additions (new fields use default values)
///
/// **Custom Migration**:
/// Provides hooks for:
/// - `willMigrate`: Pre-processing (e.g., data cleanup, duplicate removal)
/// - `didMigrate`: Post-processing (e.g., setting default values, data transformation)
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
///     // Lightweight migration (index additions only)
///     static let migrateV1toV2 = MigrationStage.lightweight(
///         fromVersion: AppSchemaV1.self,
///         toVersion: AppSchemaV2.self
///     )
///
///     // Custom migration with data transformation
///     static let migrateV2toV3 = MigrationStage.custom(
///         fromVersion: AppSchemaV2.self,
///         toVersion: AppSchemaV3.self,
///         willMigrate: { context in
///             // Pre-migration processing
///         },
///         didMigrate: { context in
///             // Post-migration processing
///         }
///     )
/// }
/// ```
public enum MigrationStage: Sendable {
    /// Lightweight migration (automatic index and field changes)
    ///
    /// Automatically detects and applies:
    /// - Index additions → enables and builds via OnlineIndexer
    /// - Index removals → disables and clears data
    /// - Field additions → no action needed (default values)
    ///
    /// - Parameters:
    ///   - fromVersion: Source schema version type
    ///   - toVersion: Target schema version type
    case lightweight(
        fromVersion: any VersionedSchema.Type,
        toVersion: any VersionedSchema.Type
    )

    /// Custom migration with hooks
    ///
    /// Provides control over migration process with pre/post hooks.
    /// The lightweight migration steps are executed between willMigrate and didMigrate.
    ///
    /// **Execution Order**:
    /// 1. `willMigrate` closure (if provided)
    /// 2. Lightweight migration (index additions/removals)
    /// 3. `didMigrate` closure (if provided)
    /// 4. Version update
    ///
    /// - Parameters:
    ///   - fromVersion: Source schema version type
    ///   - toVersion: Target schema version type
    ///   - willMigrate: Pre-migration hook (async, optional)
    ///   - didMigrate: Post-migration hook (async, optional)
    case custom(
        fromVersion: any VersionedSchema.Type,
        toVersion: any VersionedSchema.Type,
        willMigrate: (@Sendable (MigrationContext) async throws -> Void)?,
        didMigrate: (@Sendable (MigrationContext) async throws -> Void)?
    )

    // MARK: - Properties

    /// Source schema version type
    public var fromVersion: any VersionedSchema.Type {
        switch self {
        case .lightweight(let from, _):
            return from
        case .custom(let from, _, _, _):
            return from
        }
    }

    /// Target schema version type
    public var toVersion: any VersionedSchema.Type {
        switch self {
        case .lightweight(_, let to):
            return to
        case .custom(_, let to, _, _):
            return to
        }
    }

    /// Source version identifier
    public var fromVersionIdentifier: Schema.Version {
        return fromVersion.versionIdentifier
    }

    /// Target version identifier
    public var toVersionIdentifier: Schema.Version {
        return toVersion.versionIdentifier
    }

    /// Check if this is a lightweight migration
    public var isLightweight: Bool {
        switch self {
        case .lightweight:
            return true
        case .custom:
            return false
        }
    }

    /// Will migrate closure (nil for lightweight)
    public var willMigrate: (@Sendable (MigrationContext) async throws -> Void)? {
        switch self {
        case .lightweight:
            return nil
        case .custom(_, _, let willMigrate, _):
            return willMigrate
        }
    }

    /// Did migrate closure (nil for lightweight)
    public var didMigrate: (@Sendable (MigrationContext) async throws -> Void)? {
        switch self {
        case .lightweight:
            return nil
        case .custom(_, _, _, let didMigrate):
            return didMigrate
        }
    }
}

// MARK: - MigrationStage Extensions

extension MigrationStage {
    /// Get index changes for this migration stage
    ///
    /// Returns the indexes that need to be added and removed.
    ///
    /// - Returns: Tuple of (added indexes, removed indexes)
    public var indexChanges: (added: Set<String>, removed: Set<String>) {
        return toVersion.indexChanges(from: fromVersion)
    }

    /// Get added index descriptors for this migration stage
    ///
    /// - Returns: Array of IndexDescriptor objects to add
    public var addedIndexDescriptors: [IndexDescriptor] {
        let addedNames = indexChanges.added
        return toVersion.allIndexDescriptors.filter { addedNames.contains($0.name) }
    }

    /// Get removed index names for this migration stage
    ///
    /// - Returns: Set of index names to remove
    public var removedIndexNames: Set<String> {
        return indexChanges.removed
    }

    /// Description for logging
    public var migrationDescription: String {
        let type = isLightweight ? "lightweight" : "custom"
        return "\(type) migration: \(fromVersionIdentifier) → \(toVersionIdentifier)"
    }
}

// MARK: - MigrationStage Factory Methods

extension MigrationStage {
    /// Create a lightweight migration if possible, otherwise custom
    ///
    /// Automatically determines if lightweight migration is safe based on
    /// schema differences.
    ///
    /// - Parameters:
    ///   - from: Source schema version
    ///   - to: Target schema version
    ///   - willMigrate: Optional pre-migration hook
    ///   - didMigrate: Optional post-migration hook
    /// - Returns: Appropriate MigrationStage
    public static func automatic(
        from: any VersionedSchema.Type,
        to: any VersionedSchema.Type,
        willMigrate: (@Sendable (MigrationContext) async throws -> Void)? = nil,
        didMigrate: (@Sendable (MigrationContext) async throws -> Void)? = nil
    ) -> MigrationStage {
        // If there are hooks, always use custom
        if willMigrate != nil || didMigrate != nil {
            return .custom(
                fromVersion: from,
                toVersion: to,
                willMigrate: willMigrate,
                didMigrate: didMigrate
            )
        }

        // Check if lightweight is possible
        if to.canLightweightMigrate(from: from) {
            return .lightweight(fromVersion: from, toVersion: to)
        }

        // Default to custom for safety
        return .custom(
            fromVersion: from,
            toVersion: to,
            willMigrate: nil,
            didMigrate: nil
        )
    }
}
