import DatabaseEngine
import DatabaseValue
import DatabaseWire

public final class DatabaseEndpoint: Sendable {
    private let container: DBContainer
    private let registry: DatabaseOperationRegistry
    private let authorizationPolicy: AnyDatabaseOperationAuthorizationPolicy
    private let middlewares: [AnyDatabaseRequestMiddleware]
    private let limits: DatabaseWireLimits
    private let errorMapper: AnyDatabaseErrorMapper

    public init(
        container: DBContainer,
        registry: DatabaseOperationRegistry,
        authorizationPolicy: AnyDatabaseOperationAuthorizationPolicy,
        middlewares: [AnyDatabaseRequestMiddleware] = [],
        limits: DatabaseWireLimits = .default
    ) {
        self.container = container
        self.registry = registry
        self.authorizationPolicy = authorizationPolicy
        self.middlewares = middlewares
        self.limits = limits
        self.errorMapper = AnyDatabaseErrorMapper(CanonicalDatabaseErrorMapper())
    }

    public init<Mapper: DatabaseErrorMapper>(
        container: DBContainer,
        registry: DatabaseOperationRegistry,
        authorizationPolicy: AnyDatabaseOperationAuthorizationPolicy,
        middlewares: [AnyDatabaseRequestMiddleware] = [],
        limits: DatabaseWireLimits = .default,
        errorMapper: Mapper
    ) {
        self.container = container
        self.registry = registry
        self.authorizationPolicy = authorizationPolicy
        self.middlewares = middlewares
        self.limits = limits
        self.errorMapper = AnyDatabaseErrorMapper(errorMapper)
    }

    public func execute(_ bytes: DatabaseBytes) async throws -> DatabaseBytes {
        let request: DatabaseWireRequestEnvelope
        do {
            request = try DatabaseEnvelopeCodec.decodeRequest(bytes, limits: limits)
        } catch {
            throw DatabaseEndpointError.invalidRequestFrame(error)
        }

        let context = DatabaseOperationContext(
            container: container,
            requestID: request.requestID,
            metadata: request.metadata,
            requestPayload: request.payload
        )
        let result: DatabaseOperationResult
        do {
            try await authorizationPolicy.authorize(
                request: request,
                context: context
            )
            result = try await handlerChain()(request, context)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            return try encodeFailureResponse(
                for: request,
                error: errorMapper.remoteError(
                    for: error,
                    context: context,
                    limits: limits
                )
            )
        }

        guard result.operation == request.operation else {
            throw DatabaseEndpointError.responseOperationMismatch(
                expected: request.operation,
                actual: result.operation
            )
        }
        do {
            return try result.encodeResponse(
                requestID: request.requestID,
                limits: limits
            )
        } catch let wireError as DatabaseWireError {
            return try encodeFailureResponse(
                for: request,
                error: errorMapper.remoteError(
                    for: DatabaseResponsePreparationError(
                        wireError: wireError
                    ),
                    context: context,
                    limits: limits
                )
            )
        } catch {
            return try encodeFailureResponse(
                for: request,
                error: errorMapper.remoteError(
                    for: error,
                    context: context,
                    limits: limits
                )
            )
        }
    }

    private func handlerChain() -> DatabaseRequestHandler {
        var handler: DatabaseRequestHandler = { [registry, limits] request, context in
            guard let operationHandler = registry.resolve(request.operation) else {
                throw DatabaseEndpointError.missingHandler(request.operation)
            }
            return try await operationHandler.invoke(
                payload: request.payload,
                context: context,
                limits: limits
            )
        }
        for middleware in middlewares.reversed() {
            let next = handler
            handler = { request, context in
                try await middleware.handle(
                    request: request,
                    context: context,
                    next: next
                )
            }
        }
        return handler
    }

    private func encodeFailureResponse(
        for request: DatabaseWireRequestEnvelope,
        error remoteError: DatabaseRemoteError
    ) throws -> DatabaseBytes {
        do {
            return try encodeFailureEnvelope(
                for: request,
                error: remoteError
            )
        } catch {
            let reduced = DatabaseRemoteError(
                category: remoteError.category,
                code: Self.stringPrefix(
                    remoteError.code,
                    maximumBytes: limits.maximumStringBytes
                ),
                message: Self.stringPrefix(
                    remoteError.message,
                    maximumBytes: limits.maximumStringBytes
                ),
                retryability: remoteError.retryability
            )
            do {
                return try encodeFailureEnvelope(
                    for: request,
                    error: reduced
                )
            } catch {
                let fallback = DatabaseRemoteError(
                    category: .internalFailure,
                    code: Self.stringPrefix(
                        "FAILURE_RESPONSE_ENCODING_FAILED",
                        maximumBytes: limits.maximumStringBytes
                    ),
                    message: Self.stringPrefix(
                        "Failure response exceeded configured wire limits",
                        maximumBytes: limits.maximumStringBytes
                    ),
                    retryability: .never
                )
                do {
                    return try encodeFailureEnvelope(
                        for: request,
                        error: fallback
                    )
                } catch let wireError {
                    throw DatabaseEndpointError.responseEncodingFailed(
                        wireError
                    )
                }
            }
        }
    }

    private func encodeFailureEnvelope(
        for request: DatabaseWireRequestEnvelope,
        error: DatabaseRemoteError
    ) throws(DatabaseWireError) -> DatabaseBytes {
        try DatabaseEnvelopeCodec.encode(
            response: DatabaseWireResponseEnvelope(
                requestID: request.requestID,
                operation: request.operation,
                payload: .failure(error)
            ),
            limits: limits
        )
    }

    private static func stringPrefix(
        _ string: String,
        maximumBytes: Int
    ) -> String {
        guard maximumBytes > 0 else { return "" }
        guard string.utf8.count > maximumBytes else { return string }

        // A new String is required at the wire ownership boundary. Iterating
        // scalars avoids an intermediate UTF-8 array and never splits a scalar.
        var result = ""
        result.reserveCapacity(maximumBytes)
        var byteCount = 0
        for scalar in string.unicodeScalars {
            let scalarByteCount = scalar.utf8.count
            let addition = byteCount.addingReportingOverflow(scalarByteCount)
            guard !addition.overflow,
                  addition.partialValue <= maximumBytes else {
                break
            }
            result.unicodeScalars.append(scalar)
            byteCount = addition.partialValue
        }
        return result
    }
}
