import DatabaseKit
import DatabaseEngine

/// Backend-specific container configuration used by the `Database` facade.
///
/// `DatabaseEngine` stays backend-neutral and accepts `DBConfiguration`.
/// The `Database` module lifts concrete backend configuration types such as
/// `FDBStorageEngine.Configuration`, `SQLiteStorageEngine.Configuration`, and
/// `PostgreSQLStorageEngine.Configuration` into that generic representation.
public protocol DatabaseContainerConfiguration: Sendable {
    func makeDBConfiguration(
        indexConfigurations: [any IndexRuntimeConfiguration]
    ) async throws -> DBConfiguration
}

extension DBContainer {
    /// Opens a container from a backend-specific configuration.
    ///
    /// The `configuration` label is shared across backends; the concrete value
    /// selects the storage engine.
    public static func open(
        for schema: Schema,
        configuration: any DatabaseContainerConfiguration,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) async throws -> DBContainer {
        let dbConfiguration = try await configuration.makeDBConfiguration(
            indexConfigurations: indexConfigurations
        )
        return try await open(
            for: schema,
            configuration: dbConfiguration,
            runtimeConfiguration: runtimeConfiguration,
            security: security
        )
    }

    /// Opens a versioned container from a backend-specific configuration.
    public static func open<
        S: VersionedSchema,
        P: SchemaMigrationPlan
    >(
        for schema: S.Type,
        migrationPlan: P.Type,
        configuration: any DatabaseContainerConfiguration,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) async throws -> DBContainer {
        let dbConfiguration = try await configuration.makeDBConfiguration(
            indexConfigurations: indexConfigurations
        )
        return try await open(
            for: schema,
            migrationPlan: migrationPlan,
            configuration: dbConfiguration,
            runtimeConfiguration: runtimeConfiguration,
            security: security
        )
    }
}
