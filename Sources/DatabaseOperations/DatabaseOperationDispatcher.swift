import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

/// Executes one already-decoded database operation against a target-bound
/// container. Wire framing is owned by `DatabaseWireAdapter`.
package final class DatabaseOperationDispatcher: Sendable {
    private let container: DBContainer
    private let registry: DatabaseOperationRegistry
    private let admissionPolicy: AnyDatabaseOperationAdmissionPolicy
    private let middlewares: [AnyDatabaseRequestMiddleware]

    package init(
        container: DBContainer,
        registry: DatabaseOperationRegistry,
        admissionPolicy: AnyDatabaseOperationAdmissionPolicy,
        middlewares: [AnyDatabaseRequestMiddleware] = []
    ) {
        self.container = container
        self.registry = registry
        self.admissionPolicy = admissionPolicy
        self.middlewares = middlewares
    }

    package func execute(
        _ request: DatabaseWireRequestEnvelope,
        context executionContext: DatabaseRequestExecutionContext,
        requestLimits: DatabaseWireLimits,
        responseLimits: DatabaseWireLimits
    ) async throws -> DatabaseOperationDispatchOutcome {
        try await container.withSchemaLease { _ in
            try await RequestAuthorization.$context.withValue(
                executionContext.authorization
            ) {
                try await executePreparedRequest(
                    request,
                    executionContext: executionContext,
                    requestLimits: requestLimits,
                    responseLimits: responseLimits
                )
            }
        }
    }

    private func executePreparedRequest(
        _ request: DatabaseWireRequestEnvelope,
        executionContext: DatabaseRequestExecutionContext,
        requestLimits: DatabaseWireLimits,
        responseLimits: DatabaseWireLimits
    ) async throws -> DatabaseOperationDispatchOutcome {
        guard let operationHandler = registry.resolve(request.operation) else {
            let requirement = DatabaseOperationRequirement.canonical(
                for: request.operation
            )
            let context = makeContext(
                request: request,
                executionContext: executionContext,
                requirement: requirement,
                wireLimits: responseLimits
            )
            return .failure(
                DatabaseOperationDispatchFailure(
                    error: DatabaseOperationError.missingHandler(
                        request.operation
                    ),
                    context: context
                )
            )
        }
        let prepared: PreparedDatabaseOperation
        do {
            prepared = try operationHandler.prepare(
                envelope: request,
                limits: requestLimits
            )
        } catch {
            let context = makeContext(
                request: request,
                executionContext: executionContext,
                requirement: DatabaseOperationRequirement.canonical(
                    for: request.operation
                ),
                wireLimits: responseLimits
            )
            return .failure(
                DatabaseOperationDispatchFailure(
                    error: error,
                    context: context
                )
            )
        }
        guard prepared.requirement.acceptedTargets.accepts(request.target) else {
            let context = makeContext(
                request: request,
                executionContext: executionContext,
                requirement: prepared.requirement,
                wireLimits: responseLimits
            )
            return .failure(
                DatabaseOperationDispatchFailure(
                    error: DatabaseOperationError.targetKindNotAccepted(
                        request.target
                    ),
                    context: context
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
            let context = makeContext(
                request: request,
                executionContext: executionContext,
                requirement: prepared.requirement,
                wireLimits: responseLimits
            )
            return .failure(
                DatabaseOperationDispatchFailure(
                    error: RemoteOperationError(
                        category: .authorization,
                        code: denial.code,
                        message: denial.message,
                        retryability: denial.retryability,
                        details: denial.details
                    ),
                    context: context
                )
            )
        }
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
        guard container.layoutStatus == .current
                || prepared.requirement.permitsMigrationRequiredLayout else {
            let context = makeContext(
                request: request,
                executionContext: executionContext,
                requirement: prepared.requirement,
                wireLimits: responseLimits
            )
            return .failure(
                DatabaseOperationDispatchFailure(
                    error: DatabaseOperationError.migrationRequired,
                    context: context
                )
            )
        }
        #endif

        return try await execute(
            prepared,
            request: request,
            executionContext: executionContext,
            wireLimits: responseLimits
        )
    }

    private func execute(
        _ prepared: PreparedDatabaseOperation,
        request: DatabaseWireRequestEnvelope,
        executionContext: DatabaseRequestExecutionContext,
        wireLimits: DatabaseWireLimits
    ) async throws -> DatabaseOperationDispatchOutcome {
        switch request.target {
        case .database:
            let context = makeContext(
                request: request,
                executionContext: executionContext,
                requirement: prepared.requirement,
                wireLimits: wireLimits
            )
            return try await container.withDatabaseDataRoot {
                try await self.invoke(
                    prepared,
                    request: request,
                    context: context
                )
            }
        case .base(let baseID):
            #if DATABASE_OPERATIONS_MULTIPLE_BASES
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
                        requirement: prepared.requirement,
                        wireLimits: wireLimits
                    )
                    return try await self.invoke(
                        prepared,
                        request: request,
                        context: context
                    )
                }
                let context = self.makeContext(
                    request: request,
                    executionContext: executionContext,
                    requirement: prepared.requirement,
                    wireLimits: wireLimits
                )
                return .failure(
                    DatabaseOperationDispatchFailure(
                        error: Self.baseUnavailableError(),
                        context: context
                    )
                )
            }
            return try await container.withBaseLease(lease) {
                let baseContext = self.container.session(
                    authorization: executionContext.authorization
                ).base(baseID).newContext()
                let context = self.makeBaseContext(
                    request: request,
                    executionContext: executionContext,
                    baseContext: baseContext,
                    requirement: prepared.requirement,
                    wireLimits: wireLimits
                )
                return try await self.invoke(
                    prepared,
                    request: request,
                    context: context
                )
            }
            #else
            _ = baseID
            throw DatabaseOperationError.targetKindNotAccepted(request.target)
            #endif
        case .composition(let compositionID):
            #if DATABASE_OPERATIONS_MULTIPLE_BASES
            let source = container.session(
                authorization: executionContext.authorization
            ).composition(compositionID)
            let context = makeCompositionContext(
                request: request,
                executionContext: executionContext,
                composition: source,
                requirement: prepared.requirement,
                wireLimits: wireLimits
            )
            return try await invoke(
                prepared,
                request: request,
                context: context
            )
            #else
            _ = compositionID
            throw DatabaseOperationError.targetKindNotAccepted(request.target)
            #endif
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
        requirement: DatabaseOperationRequirement,
        wireLimits: DatabaseWireLimits
    ) -> DatabaseOperationContext {
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
        return DatabaseOperationContext(
            container: container,
            target: request.target,
            baseContext: nil,
            composition: nil,
            requirement: requirement,
            requestID: request.requestID,
            metadata: request.metadata,
            authorization: executionContext.authorization,
            jobAuthorizationReference:
                executionContext.jobAuthorizationReference,
            requestPayload: request.payload,
            wireLimits: wireLimits
        )
        #else
        DatabaseOperationContext(
            container: container,
            target: request.target,
            requirement: requirement,
            requestID: request.requestID,
            metadata: request.metadata,
            authorization: executionContext.authorization,
            jobAuthorizationReference:
                executionContext.jobAuthorizationReference,
            requestPayload: request.payload,
            wireLimits: wireLimits
        )
        #endif
    }

    #if DATABASE_OPERATIONS_MULTIPLE_BASES
    private func makeBaseContext(
        request: DatabaseWireRequestEnvelope,
        executionContext: DatabaseRequestExecutionContext,
        baseContext: DatabaseContext,
        requirement: DatabaseOperationRequirement,
        wireLimits: DatabaseWireLimits
    ) -> DatabaseOperationContext {
        DatabaseOperationContext(
            container: container,
            target: request.target,
            baseContext: baseContext,
            composition: nil,
            requirement: requirement,
            requestID: request.requestID,
            metadata: request.metadata,
            authorization: executionContext.authorization,
            jobAuthorizationReference:
                executionContext.jobAuthorizationReference,
            requestPayload: request.payload,
            wireLimits: wireLimits
        )
    }

    private func makeCompositionContext(
        request: DatabaseWireRequestEnvelope,
        executionContext: DatabaseRequestExecutionContext,
        composition: CompositionDataSource,
        requirement: DatabaseOperationRequirement,
        wireLimits: DatabaseWireLimits
    ) -> DatabaseOperationContext {
        DatabaseOperationContext(
            container: container,
            target: request.target,
            baseContext: nil,
            composition: composition,
            requirement: requirement,
            requestID: request.requestID,
            metadata: request.metadata,
            authorization: executionContext.authorization,
            jobAuthorizationReference:
                executionContext.jobAuthorizationReference,
            requestPayload: request.payload,
            wireLimits: wireLimits
        )
    }
    #endif

    private func invoke(
        _ prepared: PreparedDatabaseOperation,
        request: DatabaseWireRequestEnvelope,
        context: DatabaseOperationContext
    ) async throws -> DatabaseOperationDispatchOutcome {
        let result: DatabaseOperationResult
        do {
            result = try await handlerChain(prepared: prepared)(request, context)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            return .failure(
                DatabaseOperationDispatchFailure(
                    error: error,
                    context: context
                )
            )
        }

        guard result.operation == request.operation else {
            return .failure(
                DatabaseOperationDispatchFailure(
                    error: DatabaseOperationError.responseOperationMismatch(
                        expected: request.operation,
                        actual: result.operation
                    ),
                    context: context
                )
            )
        }
        return .success(result, context: context)
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

}
