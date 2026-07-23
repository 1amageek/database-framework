#if !os(WASI)
#if FOUNDATION_DB
import DatabaseEngine
import FDBStorage

extension DBContainer {
    /// Opens a container backed by the default FoundationDB configuration.
    public static func open(
        for schema: Schema,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexConfiguration] = []
    ) async throws -> DBContainer {
        try await open(
            for: schema,
            configuration: FDBStorageEngine.Configuration(),
            security: security,
            indexConfigurations: indexConfigurations
        )
    }

    /// Opens a versioned container backed by the default FoundationDB configuration.
    public static func open<
        S: VersionedSchema,
        P: SchemaMigrationPlan
    >(
        for schema: S.Type,
        migrationPlan: P.Type,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexConfiguration] = []
    ) async throws -> DBContainer {
        try await open(
            for: schema,
            migrationPlan: migrationPlan,
            configuration: FDBStorageEngine.Configuration(),
            security: security,
            indexConfigurations: indexConfigurations
        )
    }
}
#endif

#endif
