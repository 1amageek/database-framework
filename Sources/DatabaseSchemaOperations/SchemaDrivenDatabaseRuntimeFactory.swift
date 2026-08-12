import DatabaseJobRuntime
import DatabaseEngine
import DatabaseKit
import DatabaseRuntime

/// Runtime factory for a database whose complete execution model comes from a
/// canonical `SchemaManifest` rather than compiled `Persistable` types.
public struct SchemaDrivenDatabaseRuntimeFactory: DatabaseSchemaRuntimeFactory,
    Sendable {
    private let authorizationPolicies: [AuthorizationPolicyHandler]

    public init(
        authorizationPolicies: [AuthorizationPolicyHandler] = []
    ) {
        self.authorizationPolicies = authorizationPolicies
    }

    public func makeOperationConfiguration(
        for schema: Schema
    ) async throws -> DatabaseRuntimeConfiguration {
        try DatabaseFrameworkRuntime.configuration(
            schema: schema,
            authorizationPolicies: authorizationPolicies
        )
    }
}
