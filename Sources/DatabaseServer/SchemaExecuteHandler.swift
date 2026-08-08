@_spi(DatabaseServer) import DatabaseWire

public struct SchemaExecuteHandler: DatabaseOperationHandler {
    public typealias Operation = SchemaExecuteOperation

    private let coordinator: DatabaseSchemaCoordinator

    public init(coordinator: DatabaseSchemaCoordinator) {
        self.coordinator = coordinator
    }

    public func handle(
        _ request: SchemaExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> SchemaExecuteOperation.Response {
        _ = context
        switch request.invocation {
        case .plan(let manifest, let expectedFingerprint):
            return .plan(
                try await coordinator.plan(
                    manifest: manifest,
                    expectedFingerprint: expectedFingerprint
                )
            )
        case .apply(
            let manifest,
            let expectedFingerprint,
            let idempotencyKey
        ):
            return .applied(
                try await coordinator.apply(
                    manifest: manifest,
                    expectedFingerprint: expectedFingerprint,
                    idempotencyKey: idempotencyKey,
                    context: context
                )
            )
        }
    }
}
