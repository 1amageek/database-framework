import Core
import DatabaseEngine

/// Backend-specific container configuration used by the `Database` facade.
///
/// `DatabaseEngine` stays backend-neutral and accepts `DBConfiguration`.
/// The `Database` module lifts concrete backend configuration types such as
/// `FDBStorageEngine.Configuration`, `SQLiteStorageEngine.Configuration`, and
/// `PostgreSQLStorageEngine.Configuration` into that generic representation.
public protocol DatabaseContainerConfiguration: Sendable {
    func makeDBConfiguration(
        indexConfigurations: [any IndexConfiguration]
    ) async throws -> DBConfiguration
}

extension DBContainer {
    /// Create a container from a backend-specific configuration.
    ///
    /// The `configuration` label is shared across backends; the concrete value
    /// selects the storage engine.
    public convenience init(
        for schema: Schema,
        configuration: any DatabaseContainerConfiguration,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexConfiguration] = []
    ) async throws {
        let dbConfiguration = try await configuration.makeDBConfiguration(
            indexConfigurations: indexConfigurations
        )
        try await self.init(
            for: schema,
            configuration: dbConfiguration,
            security: security
        )
    }

    /// Create a versioned container from a backend-specific configuration.
    public convenience init<S: VersionedSchema, P: SchemaMigrationPlan>(
        for schema: S.Type,
        migrationPlan: P.Type,
        configuration: any DatabaseContainerConfiguration,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexConfiguration] = []
    ) async throws {
        let dbConfiguration = try await configuration.makeDBConfiguration(
            indexConfigurations: indexConfigurations
        )
        try await self.init(
            for: schema,
            migrationPlan: migrationPlan,
            configuration: dbConfiguration,
            security: security
        )
    }
}
