import Foundation
import DatabaseClientProtocol

/// Middleware for intercepting and transforming request/response envelopes
///
/// Middlewares form a pipeline: each middleware can inspect, modify, or reject
/// requests before they reach the operation handler, and transform responses
/// on the way back.
///
/// **Usage**:
/// ```swift
/// struct AuthMiddleware: ServerMiddleware {
///     let verifier: JWTVerifier
///
///     func handle(
///         request: ServiceEnvelope,
///         next: @Sendable (ServiceEnvelope) async throws -> ServiceEnvelope
///     ) async throws -> ServiceEnvelope {
///         guard let token = request.metadata["authorization"] else {
///             throw ServiceError(code: "UNAUTHORIZED", message: "Missing token")
///         }
///         try verifier.verify(token)
///         return try await next(request)
///     }
/// }
/// ```
public protocol ServerMiddleware: Sendable {

    /// Process a request envelope and optionally delegate to the next handler
    ///
    /// - Parameters:
    ///   - request: The incoming request envelope
    ///   - next: The next handler in the pipeline (call to continue processing)
    /// - Returns: The response envelope
    func handle(
        request: ServiceEnvelope,
        next: @Sendable (ServiceEnvelope) async throws -> ServiceEnvelope
    ) async throws -> ServiceEnvelope
}
