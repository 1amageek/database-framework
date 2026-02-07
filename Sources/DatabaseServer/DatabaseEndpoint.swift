import Foundation
import Core
import DatabaseEngine
import DatabaseClientProtocol

/// Server endpoint that processes client requests via ServiceEnvelope
///
/// Provides a framework-agnostic request handler that can be integrated
/// into any WebSocket server (Vapor, Hummingbird, NIO, etc.).
///
/// **Embedded mode** (integrate into existing server):
/// ```swift
/// let container = try await FDBContainer(for: schema)
/// let endpoint = DatabaseEndpoint(container: container)
///
/// // Vapor example
/// app.webSocket("db") { req, ws in
///     ws.onBinary { ws, buffer in
///         let data = Data(buffer: buffer)
///         let response = try await endpoint.handleRequest(data)
///         try await ws.send(response)
///     }
/// }
/// ```
///
/// **Middleware** (authentication, rate limiting, etc.):
/// ```swift
/// let endpoint = DatabaseEndpoint(container: container)
///     .middleware(AuthMiddleware(verifier: jwtVerifier))
///     .middleware(RateLimitMiddleware(maxRequests: 100))
/// ```
public final class DatabaseEndpoint: Sendable {

    private let router: OperationRouter
    private let middlewares: [any ServerMiddleware]

    /// Create an endpoint backed by an FDBContainer
    ///
    /// Automatically registers handlers for all entity types in the schema.
    /// Creates a new FDBContext per request for stateless processing.
    ///
    /// - Parameter container: The FDBContainer managing database resources
    public init(container: FDBContainer) {
        self.router = OperationRouter(container: container)
        self.middlewares = []
    }

    private init(router: OperationRouter, middlewares: [any ServerMiddleware]) {
        self.router = router
        self.middlewares = middlewares
    }

    /// Add a middleware to the processing pipeline
    ///
    /// Middlewares are executed in the order they are added.
    /// Each middleware can inspect, modify, or reject requests.
    ///
    /// - Parameter middleware: The middleware to add
    /// - Returns: A new endpoint with the middleware added
    public func middleware(_ middleware: any ServerMiddleware) -> DatabaseEndpoint {
        var newMiddlewares = self.middlewares
        newMiddlewares.append(middleware)
        return DatabaseEndpoint(router: router, middlewares: newMiddlewares)
    }

    /// Process a single request (JSON-encoded ServiceEnvelope)
    ///
    /// This is the main entry point for integrating with any transport.
    /// Decodes the request, runs it through the middleware pipeline,
    /// routes to the appropriate handler, and returns the response.
    ///
    /// - Parameter data: JSON-encoded ServiceEnvelope
    /// - Returns: JSON-encoded response ServiceEnvelope
    public func handleRequest(_ data: Data) async -> Data {
        let decoder = JSONDecoder()
        let encoder = JSONEncoder()

        // Decode the request envelope
        let envelope: ServiceEnvelope
        do {
            envelope = try decoder.decode(ServiceEnvelope.self, from: data)
        } catch {
            let errorResponse = ServiceEnvelope(
                responseTo: "unknown",
                operationID: "error",
                errorCode: "INVALID_REQUEST",
                errorMessage: "Failed to decode request: \(error.localizedDescription)"
            )
            return (try? encoder.encode(errorResponse)) ?? Data()
        }

        // Build the handler chain (middlewares → router)
        let handler = buildHandlerChain(for: envelope)

        do {
            let response = try await handler(envelope)
            return try encoder.encode(response)
        } catch let error as ServiceError {
            let errorResponse = ServiceEnvelope(
                responseTo: envelope.requestID,
                operationID: envelope.operationID,
                errorCode: error.code,
                errorMessage: error.message
            )
            return (try? encoder.encode(errorResponse)) ?? Data()
        } catch {
            let errorResponse = ServiceEnvelope(
                responseTo: envelope.requestID,
                operationID: envelope.operationID,
                errorCode: "INTERNAL_ERROR",
                errorMessage: error.localizedDescription
            )
            return (try? encoder.encode(errorResponse)) ?? Data()
        }
    }

    /// Build the middleware chain ending with the router
    private func buildHandlerChain(
        for envelope: ServiceEnvelope
    ) -> @Sendable (ServiceEnvelope) async throws -> ServiceEnvelope {
        // Start with the router as the innermost handler
        var handler: @Sendable (ServiceEnvelope) async throws -> ServiceEnvelope = { [router] envelope in
            try await router.handle(envelope)
        }

        // Wrap with middlewares in reverse order (so first-added middleware runs first)
        for middleware in middlewares.reversed() {
            let next = handler
            handler = { envelope in
                try await middleware.handle(request: envelope, next: next)
            }
        }

        return handler
    }
}
