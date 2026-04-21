#if FOUNDATION_DB
import DatabaseEngine
import FDBStorage

extension DBContainer {
    /// Create a container backed by the default FoundationDB configuration.
    public convenience init(
        for schema: Schema,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexConfiguration] = []
    ) async throws {
        try await self.init(
            for: schema,
            configuration: FDBStorageEngine.Configuration(),
            security: security,
            indexConfigurations: indexConfigurations
        )
    }

    /// Create a versioned container backed by the default FoundationDB configuration.
    public convenience init<S: VersionedSchema, P: SchemaMigrationPlan>(
        for schema: S.Type,
        migrationPlan: P.Type,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexConfiguration] = []
    ) async throws {
        try await self.init(
            for: schema,
            migrationPlan: migrationPlan,
            configuration: FDBStorageEngine.Configuration(),
            security: security,
            indexConfigurations: indexConfigurations
        )
    }
}
#endif
