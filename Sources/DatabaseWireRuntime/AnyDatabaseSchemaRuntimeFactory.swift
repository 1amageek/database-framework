import DatabaseEngine
import DatabaseKit

public struct AnyDatabaseSchemaRuntimeFactory: DatabaseSchemaRuntimeFactory,
    Sendable {
    private let makeConfiguration: @Sendable (
        Schema
    ) async throws -> DatabaseRuntimeConfiguration

    public init<Factory: DatabaseSchemaRuntimeFactory>(_ factory: Factory) {
        self.makeConfiguration = { schema in
            try await factory.makeRuntimeConfiguration(for: schema)
        }
    }

    public func makeRuntimeConfiguration(
        for schema: Schema
    ) async throws -> DatabaseRuntimeConfiguration {
        try await makeConfiguration(schema)
    }
}
