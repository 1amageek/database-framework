#if SQLITE
import Database
import DatabaseEngine
import TestSupport

extension DBContainer {
    static func open(
        for schema: Schema,
        configuration: SQLiteStorageEngine.Configuration,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) async throws -> DBContainer {
        try await open(
            for: schema,
            configuration: configuration,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            runtimeConfiguration: runtimeConfiguration,
            security: security,
            indexConfigurations: indexConfigurations
        )
    }

    static func open<S: VersionedSchema, P: SchemaMigrationPlan>(
        for schema: S.Type,
        migrationPlan: P.Type,
        configuration: SQLiteStorageEngine.Configuration,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) async throws -> DBContainer {
        try await open(
            for: schema,
            migrationPlan: migrationPlan,
            configuration: configuration,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            runtimeConfiguration: runtimeConfiguration,
            security: security,
            indexConfigurations: indexConfigurations
        )
    }

    static func inMemory(
        for schema: Schema,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) async throws -> DBContainer {
        try await inMemory(
            for: schema,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            runtimeConfiguration: runtimeConfiguration,
            security: security,
            indexConfigurations: indexConfigurations
        )
    }

    static func sqlite(
        for schema: Schema,
        path: String,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        security: SecurityConfiguration = .enabled(),
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) async throws -> DBContainer {
        try await sqlite(
            for: schema,
            path: path,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            runtimeConfiguration: runtimeConfiguration,
            security: security,
            indexConfigurations: indexConfigurations
        )
    }
}
#endif
