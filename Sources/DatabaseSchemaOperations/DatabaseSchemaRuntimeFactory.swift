import DatabaseJobRuntime
import DatabaseEngine
import DatabaseKit

/// Builds the complete runtime registration set paired with an applied schema.
public protocol DatabaseSchemaRuntimeFactory: Sendable {
    func makeOperationConfiguration(
        for schema: Schema
    ) async throws -> DatabaseRuntimeConfiguration
}
