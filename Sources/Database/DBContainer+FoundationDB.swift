#if !os(WASI)
#if FOUNDATION_DB
import DatabaseEngine
import FDBStorage
import StorageKit

extension DBContainer {
    /// Opens a container backed by the default FoundationDB configuration.
    public static func open(
        for schema: Schema,
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) async throws -> DBContainer {
        try await open(
            for: schema,
            configuration: FDBStorageEngine.Configuration(),
            monotonicClock: monotonicClock,
            wallClock: wallClock,
            runtimeConfiguration: runtimeConfiguration,
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
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) async throws -> DBContainer {
        try await open(
            for: schema,
            migrationPlan: migrationPlan,
            configuration: FDBStorageEngine.Configuration(),
            monotonicClock: monotonicClock,
            wallClock: wallClock,
            runtimeConfiguration: runtimeConfiguration,
            security: security,
            indexConfigurations: indexConfigurations
        )
    }
}
#endif

#endif
