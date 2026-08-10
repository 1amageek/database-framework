import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

public struct DatabaseOperationContext: Sendable {
    package let executor: DatabaseOperationExecutor
    public let target: DatabaseOperationTarget
    package let requirement: DatabaseOperationRequirement
    public let requestID: UInt64
    public let metadata: OperationRequestMetadata
    public let authorization: AuthorizationContext
    public let requestPayload: ByteString
    public let requestDigest: ByteString?
    public let wireLimits: DatabaseWireLimits

    package init(
        container: DBContainer,
        target: DatabaseOperationTarget,
        baseContext: DatabaseContext?,
        composition: CompositionDataSource?,
        requirement: DatabaseOperationRequirement,
        requestID: UInt64,
        metadata: OperationRequestMetadata,
        authorization: AuthorizationContext = .anonymous,
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
            self.executor = .base(
                BaseOperationExecutor(
                    baseID: baseID,
                    container: container,
                    authorization: authorization,
                    dataContext: baseContext
                )
            )
        case .composition(let compositionID):
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
        }
        self.target = target
        self.requirement = requirement
        self.requestID = requestID
        self.metadata = metadata
        self.authorization = authorization
        self.requestPayload = requestPayload
        self.requestDigest = requestDigest
        self.wireLimits = wireLimits
    }

    package func requireBaseContext() throws -> DatabaseContext {
        try requireBaseExecutor().requireDataContext()
    }

    package func requireControlExecutor() throws -> DatabaseControlExecutor {
        guard case .control(let executor) = executor else {
            throw DatabaseEndpointError.targetKindNotAccepted(target)
        }
        return executor
    }

    package func requireBaseExecutor() throws -> BaseOperationExecutor {
        guard case .base(let executor) = executor else {
            throw DatabaseEndpointError.targetKindNotAccepted(target)
        }
        return executor
    }

    package func requireCompositionExecutor()
        throws -> CompositionReadExecutor {
        guard case .composition(let executor) = executor else {
            throw DatabaseEndpointError.targetKindNotAccepted(target)
        }
        return executor
    }
}
