import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

public final class DatabaseEndpoint: Sendable {
    private let container: DBContainer
    private let registry: DatabaseOperationRegistry
    private let admissionPolicy: AnyDatabaseOperationAdmissionPolicy
    private let middlewares: [AnyDatabaseRequestMiddleware]
    private let limits: DatabaseWireLimits
    private let errorMapper: AnyDatabaseErrorMapper

    public init(
        container: DBContainer,
        registry: DatabaseOperationRegistry,
        admissionPolicy: AnyDatabaseOperationAdmissionPolicy,
        middlewares: [AnyDatabaseRequestMiddleware] = [],
        limits: DatabaseWireLimits = .default
    ) {
        self.container = container
        self.registry = registry
        self.admissionPolicy = admissionPolicy
        self.middlewares = middlewares
        self.limits = limits
        self.errorMapper = AnyDatabaseErrorMapper(CanonicalDatabaseErrorMapper())
    }

    public init<Mapper: DatabaseErrorMapper>(
        container: DBContainer,
        registry: DatabaseOperationRegistry,
        admissionPolicy: AnyDatabaseOperationAdmissionPolicy,
        middlewares: [AnyDatabaseRequestMiddleware] = [],
        limits: DatabaseWireLimits = .default,
        errorMapper: Mapper
    ) {
        self.container = container
        self.registry = registry
        self.admissionPolicy = admissionPolicy
        self.middlewares = middlewares
        self.limits = limits
        self.errorMapper = AnyDatabaseErrorMapper(errorMapper)
    }

    public func execute(
        _ bytes: ByteString,
        context executionContext: DatabaseRequestExecutionContext
    ) async throws -> ByteString {
        let request: DatabaseWireRequestEnvelope
        do {
            request = try DatabaseWireDecoder(limits: limits)
                .decodeRequestEnvelope(bytes)
        } catch {
            throw DatabaseEndpointError.invalidRequestFrame(error)
        }

        return try await container.withSchemaLease { _ in
            try await RequestAuthorization.$context.withValue(
                executionContext.authorization
            ) {
                try await execute(
                    request,
                    executionContext: executionContext
                )
            }
        }
    }

    private func execute(
        _ request: DatabaseWireRequestEnvelope,
        executionContext: DatabaseRequestExecutionContext
    ) async throws -> ByteString {
        let admissionRequest = DatabaseOperationAdmissionRequest(
            requestID: request.requestID,
            operation: request.operation,
            metadata: request.metadata,
            authorization: executionContext.authorization
        )
        if case .deny(let denial) = admissionPolicy.decision(
            for: admissionRequest
        ) {
            return try encodeFailureResponse(
                for: request,
                error: RemoteOperationError(
                    category: .authorization,
                    code: denial.code,
                    message: denial.message,
                    retryability: denial.retryability,
                    details: denial.details
                )
            )
        }

        let context = DatabaseOperationContext(
            container: container,
            requestID: request.requestID,
            metadata: request.metadata,
            authorization: executionContext.authorization,
            requestPayload: request.payload
        )
        let result: DatabaseOperationResult
        do {
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
                envelope: request,
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
        error remoteError: RemoteOperationError
    ) throws -> ByteString {
        do {
            return try encodeFailureEnvelope(
                for: request,
                error: remoteError
            )
        } catch {
            let reduced = RemoteOperationError(
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
                let fallback = RemoteOperationError(
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
        error: RemoteOperationError
    ) throws(DatabaseWireError) -> ByteString {
        try DatabaseWireEncoder(limits: limits).encodeFailure(
            requestID: request.requestID,
            operation: request.operation,
            error: error
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
