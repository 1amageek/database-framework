/// Immutable container-scoped authorization policy lookup.
public struct AuthorizationPolicyRegistry: Sendable {
    private let handlers: [String: AuthorizationPolicyHandler]

    init(
        handlers: [AuthorizationPolicyHandler]
    ) throws(DatabaseRuntimeConfigurationError) {
        var registered: [String: AuthorizationPolicyHandler] = [:]
        registered.reserveCapacity(handlers.count)
        for handler in handlers {
            guard registered[handler.entityName] == nil else {
                throw .duplicateAuthorizationPolicy(
                    entityName: handler.entityName
                )
            }
            registered[handler.entityName] = handler
        }
        self.handlers = registered
    }

    func adding(_ additions: [AuthorizationPolicyHandler]) throws -> Self {
        try Self(handlers: Array(handlers.values) + additions)
    }

    func handler(
        for entityName: String
    ) -> AuthorizationPolicyHandler? {
        handlers[entityName]
    }
}
