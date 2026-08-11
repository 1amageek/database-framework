import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire
import TestSupport
@testable import DatabaseWireRuntime

func testDataRootTarget() throws -> DatabaseOperationTarget {
#if MultipleBases
    .base(try TestBaseEnvironment.id())
#else
    .database
#endif
}

extension DatabaseOperationContext {
    static func testDataRoot(
        container: DBContainer,
        operation: DatabaseOperationIdentifier,
        requestID: UInt64,
        metadata: OperationRequestMetadata = OperationRequestMetadata(),
        authorization: AuthorizationContext = TestBaseEnvironment.authorization,
        jobAuthorizationReference: DatabaseJobAuthorizationReference? = nil,
        requestPayload: ByteString = [],
        requestDigest: ByteString? = nil,
        dataContext: DatabaseContext? = nil,
        wireLimits: DatabaseWireLimits = .default
    ) -> DatabaseOperationContext {
        testDataRoot(
            container: container,
            requirement: .canonical(for: operation),
            requestID: requestID,
            metadata: metadata,
            authorization: authorization,
            jobAuthorizationReference: jobAuthorizationReference,
            requestPayload: requestPayload,
            requestDigest: requestDigest,
            dataContext: dataContext,
            wireLimits: wireLimits
        )
    }

    static func testDataRoot(
        container: DBContainer,
        requirement: DatabaseOperationRequirement,
        requestID: UInt64,
        metadata: OperationRequestMetadata = OperationRequestMetadata(),
        authorization: AuthorizationContext = TestBaseEnvironment.authorization,
        jobAuthorizationReference: DatabaseJobAuthorizationReference? = nil,
        requestPayload: ByteString = [],
        requestDigest: ByteString? = nil,
        dataContext: DatabaseContext? = nil,
        wireLimits: DatabaseWireLimits = .default
    ) -> DatabaseOperationContext {
#if MultipleBases
        let context = dataContext ?? container.testBaseContext()
        return DatabaseOperationContext(
            container: container,
            target: .base(context.baseID),
            baseContext: context,
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
#else
        _ = dataContext
        return DatabaseOperationContext(
            container: container,
            target: .database,
            requirement: requirement,
            requestID: requestID,
            metadata: metadata,
            authorization: authorization,
            jobAuthorizationReference: jobAuthorizationReference,
            requestPayload: requestPayload,
            requestDigest: requestDigest,
            wireLimits: wireLimits
        )
#endif
    }
}
