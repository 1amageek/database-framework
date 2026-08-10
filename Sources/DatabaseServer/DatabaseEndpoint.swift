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
        guard let operationHandler = registry.resolve(request.operation) else {
            let requirement = DatabaseOperationRequirement.canonical(
                for: request.operation
            )
            let context = makeContext(
                request: request,
                executionContext: executionContext,
                baseContext: nil,
                composition: nil,
                requirement: requirement
            )
            return try encodeFailureResponse(
                for: request,
                error: errorMapper.remoteError(
                    for: DatabaseEndpointError.missingHandler(
                        request.operation
                    ),
                    context: context,
                    limits: limits
                )
            )
        }
        let prepared: PreparedDatabaseOperation
        do {
            prepared = try operationHandler.prepare(
                envelope: request,
                limits: limits
            )
        } catch {
            let context = DatabaseOperationContext(
                container: container,
                target: request.target,
                baseContext: nil,
                composition: nil,
                requirement: DatabaseOperationRequirement.canonical(
                    for: request.operation
                ),
                requestID: request.requestID,
                metadata: request.metadata,
                authorization: executionContext.authorization,
                requestPayload: request.payload,
                wireLimits: limits
            )
            return try encodeFailureResponse(
                for: request,
                error: errorMapper.remoteError(
                    for: error,
                    context: context,
                    limits: limits
                )
            )
        }
        guard prepared.requirement.acceptedTargets.accepts(request.target) else {
            let context = DatabaseOperationContext(
                container: container,
                target: request.target,
                baseContext: nil,
                composition: nil,
                requirement: prepared.requirement,
                requestID: request.requestID,
                metadata: request.metadata,
                authorization: executionContext.authorization,
                requestPayload: request.payload,
                wireLimits: limits
            )
            return try encodeFailureResponse(
                for: request,
                error: errorMapper.remoteError(
                    for: DatabaseEndpointError.targetKindNotAccepted(
                        request.target
                    ),
                    context: context,
                    limits: limits
                )
            )
        }
        let admissionRequest = DatabaseOperationAdmissionRequest(
            requestID: request.requestID,
            operation: request.operation,
            target: request.target,
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
        guard container.layoutStatus == .current
                || prepared.requirement.permitsMigrationRequiredLayout else {
            let context = makeContext(
                request: request,
                executionContext: executionContext,
                baseContext: nil,
                composition: nil,
                requirement: prepared.requirement
            )
            return try encodeFailureResponse(
                for: request,
                error: errorMapper.remoteError(
                    for: DatabaseEndpointError.migrationRequired,
                    context: context,
                    limits: limits
                )
            )
        }

        return try await execute(
            prepared,
            request: request,
            executionContext: executionContext
        )
    }

    private func execute(
        _ prepared: PreparedDatabaseOperation,
        request: DatabaseWireRequestEnvelope,
        executionContext: DatabaseRequestExecutionContext
    ) async throws -> ByteString {
        switch request.target {
        case .database:
            let context = makeContext(
                request: request,
                executionContext: executionContext,
                baseContext: nil,
                composition: nil,
                requirement: prepared.requirement
            )
            return try await invoke(
                prepared,
                request: request,
                context: context
            )
        case .base(let baseID):
            let lease: DatabaseBaseLease
            do {
                switch prepared.requirement.baseAdmission {
                case .activeData:
                    lease = try container.acquireBaseLease(baseID)
                case .administration, .lifecycleJob:
                    lease = try container.acquireBaseAdministrationLease(baseID)
                }
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                if prepared.requirement.baseAdmission == .lifecycleJob {
                    let context = self.makeContext(
                        request: request,
                        executionContext: executionContext,
                        baseContext: nil,
                        composition: nil,
                        requirement: prepared.requirement
                    )
                    return try await self.invoke(
                        prepared,
                        request: request,
                        context: context
                    )
                }
                return try encodeFailureResponse(
                    for: request,
                    error: Self.baseUnavailableError()
                )
            }
            return try await container.withBaseLease(lease) {
                let baseContext = self.container.session(
                    authorization: executionContext.authorization
                ).base(baseID).newContext()
                let context = self.makeContext(
                    request: request,
                    executionContext: executionContext,
                    baseContext: baseContext,
                    composition: nil,
                    requirement: prepared.requirement
                )
                return try await self.invoke(
                    prepared,
                    request: request,
                    context: context
                )
            }
        case .composition(let compositionID):
            let source = container.session(
                authorization: executionContext.authorization
            ).composition(compositionID)
            let context = makeContext(
                request: request,
                executionContext: executionContext,
                baseContext: nil,
                composition: source,
                requirement: prepared.requirement
            )
            return try await invoke(
                prepared,
                request: request,
                context: context
            )
        }
    }

    private static func baseUnavailableError() -> RemoteOperationError {
        RemoteOperationError(
            category: .authorization,
            code: "BASE_UNAVAILABLE",
            message: "The Base is unavailable",
            retryability: .never
        )
    }

    private func makeContext(
        request: DatabaseWireRequestEnvelope,
        executionContext: DatabaseRequestExecutionContext,
        baseContext: DatabaseContext?,
        composition: CompositionDataSource?,
        requirement: DatabaseOperationRequirement
    ) -> DatabaseOperationContext {
        DatabaseOperationContext(
            container: container,
            target: request.target,
            baseContext: baseContext,
            composition: composition,
            requirement: requirement,
            requestID: request.requestID,
            metadata: request.metadata,
            authorization: executionContext.authorization,
            requestPayload: request.payload,
            wireLimits: limits
        )
    }

    private func invoke(
        _ prepared: PreparedDatabaseOperation,
        request: DatabaseWireRequestEnvelope,
        context: DatabaseOperationContext
    ) async throws -> ByteString {
        let result: DatabaseOperationResult
        do {
            result = try await handlerChain(prepared: prepared)(request, context)
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

    private func handlerChain(
        prepared: PreparedDatabaseOperation
    ) -> DatabaseRequestHandler {
        var handler: DatabaseRequestHandler = { request, context in
            _ = request
            return try await prepared.invoke(context)
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
