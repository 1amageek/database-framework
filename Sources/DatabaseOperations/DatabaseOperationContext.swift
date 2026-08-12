import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

public struct DatabaseOperationContext: Sendable {
    package let executor: DatabaseOperationExecutor
    public let target: DatabaseOperationTarget
    package let requirement: DatabaseOperationRequirement
    public let requestID: UInt64
    public let metadata: OperationRequestMetadata
    public let authorization: AuthorizationContext
    public let jobAuthorizationReference: DatabaseJobAuthorizationReference?
    public let requestPayload: ByteString
    public let requestDigest: ByteString?
    public let wireLimits: DatabaseWireLimits

    #if DATABASE_OPERATIONS_MULTIPLE_BASES
    package init(
        container: DBContainer,
        target: DatabaseOperationTarget,
        baseContext: DatabaseContext?,
        composition: CompositionDataSource?,
        requirement: DatabaseOperationRequirement,
        requestID: UInt64,
        metadata: OperationRequestMetadata,
        authorization: AuthorizationContext = .anonymous,
        jobAuthorizationReference: DatabaseJobAuthorizationReference? = nil,
        requestPayload: ByteString,
        requestDigest: ByteString? = nil,
        wireLimits: DatabaseWireLimits
    ) {
        switch target {
        case .database:
            self.executor = .control(
                DatabaseControlExecutor(
                    container: container,
                    authorization: authorization
                )
            )
        case .base(let baseID):
            #if DATABASE_OPERATIONS_MULTIPLE_BASES
            self.executor = .base(
                BaseOperationExecutor(
                    baseID: baseID,
                    container: container,
                    authorization: authorization,
                    dataContext: baseContext
                )
            )
            #else
            _ = baseID
            self.executor = .control(
                DatabaseControlExecutor(
                    container: container,
                    authorization: authorization
                )
            )
            #endif
        case .composition(let compositionID):
            #if DATABASE_OPERATIONS_MULTIPLE_BASES
            if requirement.transaction == .write {
                self.executor = .control(
                    DatabaseControlExecutor(
                        container: container,
                        authorization: authorization
                    )
                )
            } else {
                let source = composition ?? container.session(
                    authorization: authorization
                ).composition(compositionID)
                self.executor = .composition(
                    CompositionReadExecutor(
                        compositionID: compositionID,
                        container: container,
                        authorization: authorization,
                        source: source
                    )
                )
            }
            #else
            _ = compositionID
            self.executor = .control(
                DatabaseControlExecutor(
                    container: container,
                    authorization: authorization
                )
            )
            #endif
        }
        self.target = target
        self.requirement = requirement
        self.requestID = requestID
        self.metadata = metadata
        self.authorization = authorization
        self.jobAuthorizationReference = jobAuthorizationReference
        self.requestPayload = requestPayload
        self.requestDigest = requestDigest
        self.wireLimits = wireLimits
    }

    package init(
        container: DBContainer,
        target: DatabaseOperationTarget,
        requirement: DatabaseOperationRequirement,
        requestID: UInt64,
        metadata: OperationRequestMetadata,
        authorization: AuthorizationContext = .anonymous,
        jobAuthorizationReference: DatabaseJobAuthorizationReference? = nil,
        requestPayload: ByteString,
        requestDigest: ByteString? = nil,
        wireLimits: DatabaseWireLimits
    ) {
        self.init(
            container: container,
            target: target,
            baseContext: nil,
            composition: nil,
            requirement: requirement,
            requestID: requestID,
            metadata: metadata,
            authorization: authorization,
            jobAuthorizationReference: jobAuthorizationReference,
            requestPayload: requestPayload,
            requestDigest: requestDigest,
            wireLimits: wireLimits
        )
    }
    #else
    package init(
        container: DBContainer,
        target: DatabaseOperationTarget,
        requirement: DatabaseOperationRequirement,
        requestID: UInt64,
        metadata: OperationRequestMetadata,
        authorization: AuthorizationContext = .anonymous,
        jobAuthorizationReference: DatabaseJobAuthorizationReference? = nil,
        requestPayload: ByteString,
        requestDigest: ByteString? = nil,
        wireLimits: DatabaseWireLimits
    ) {
        self.executor = .control(
            DatabaseControlExecutor(
                container: container,
                authorization: authorization
            )
        )
        self.target = target
        self.requirement = requirement
        self.requestID = requestID
        self.metadata = metadata
        self.authorization = authorization
        self.jobAuthorizationReference = jobAuthorizationReference
        self.requestPayload = requestPayload
        self.requestDigest = requestDigest
        self.wireLimits = wireLimits
    }
    #endif

    package func requireBaseContext() throws -> DatabaseContext {
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
        try requireBaseExecutor().requireDataContext()
        #else
        throw DatabaseOperationError.targetKindNotAccepted(target)
        #endif
    }

    package func requireControlExecutor() throws -> DatabaseControlExecutor {
        guard case .control(let executor) = executor else {
            throw DatabaseOperationError.targetKindNotAccepted(target)
        }
        return executor
    }

    #if DATABASE_OPERATIONS_MULTIPLE_BASES
    package func requireBaseExecutor() throws -> BaseOperationExecutor {
        guard case .base(let executor) = executor else {
            throw DatabaseOperationError.targetKindNotAccepted(target)
        }
        return executor
    }

    package func requireCompositionExecutor()
        throws -> CompositionReadExecutor {
        guard case .composition(let executor) = executor else {
            throw DatabaseOperationError.targetKindNotAccepted(target)
        }
        return executor
    }
    #endif

    package func requireDataExecutor()
        throws -> DatabaseDataOperationExecutor {
        switch executor {
        case .control(let executor):
            guard let dataExecutor = executor.dataExecutor else {
                throw DatabaseOperationError.targetKindNotAccepted(target)
            }
            return dataExecutor
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
        case .base(let executor):
            return executor.dataExecutor
        case .composition:
            throw DatabaseOperationError.targetKindNotAccepted(target)
        #endif
        }
    }

    package func requireDataContext() throws -> DatabaseContext {
        try requireDataExecutor().requireDataContext()
    }
}
