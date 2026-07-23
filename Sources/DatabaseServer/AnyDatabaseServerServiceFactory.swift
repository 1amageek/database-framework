/// Type-erased application service factory.
public final class AnyDatabaseServerServiceFactory: Sendable {
    private let createServices: @Sendable (
        DatabaseServerServiceContext
    ) async throws -> DatabaseServerServices

    public init<Factory: DatabaseServerServiceFactory>(_ factory: Factory) {
        self.createServices = { context in
            try await factory.makeServices(context: context)
        }
    }

    public init(
        makeServices: @escaping @Sendable (
            DatabaseServerServiceContext
        ) async throws -> DatabaseServerServices
    ) {
        self.createServices = makeServices
    }

    public func makeServices(
        context: DatabaseServerServiceContext
    ) async throws -> DatabaseServerServices {
        try await createServices(context)
    }
}
