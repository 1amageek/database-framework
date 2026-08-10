import DatabaseKit
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

struct DatabasePersistentJobSpecification: ServerPayloadValue, Sendable, Hashable {
    private static let formatVersion: UInt8 = 3

    let jobID: DatabaseTypes.UUID
    let operation: JobOperationIdentifier
    let target: DatabaseOperationTarget
    let requestDigest: ByteString
    let requestID: UInt64
    let traceID: String?
    let authorization: AuthorizationContext
    let maximumSliceWorkUnits: UInt64
    let sliceTimeoutMilliseconds: UInt32
    let retryPolicy: JobStartOperation.RetryPolicy
    let planDigest: ByteString
    let createdAt: Timestamp

    func validate() throws {
        guard requestDigest.count == DatabaseRequestDigest.byteCount,
              planDigest.count == DatabaseRequestDigest.byteCount,
              maximumSliceWorkUnits > 0,
              sliceTimeoutMilliseconds > 0,
              retryPolicy.maximumAttempts > 0,
              retryPolicy.initialBackoffMilliseconds
                <= retryPolicy.maximumBackoffMilliseconds,
              authorization.principal?.identifier.isEmpty != true else {
            throw DatabaseJobRuntimeError.corruptedSpecification
        }
    }

    func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.formatVersion)
        try jobID.encode(into: &writer)
        try operation.encode(into: &writer)
        try target.encode(into: &writer)
        try writer.writeBytes(requestDigest)
        writer.writeUInt64(requestID)
        try writer.writeOptionalString(traceID)
        try Self.encode(authorization, into: &writer)
        writer.writeUInt64(maximumSliceWorkUnits)
        writer.writeUInt32(sliceTimeoutMilliseconds)
        writer.writeUInt32(retryPolicy.maximumAttempts)
        writer.writeUInt32(retryPolicy.initialBackoffMilliseconds)
        writer.writeUInt32(retryPolicy.maximumBackoffMilliseconds)
        try writer.writeBytes(planDigest)
        try createdAt.encode(into: &writer)
    }

    init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        let version = try reader.readUInt8()
        guard version == Self.formatVersion else {
            throw .unsupportedProtocolVersionValue(UInt16(version))
        }
        self.init(
            jobID: try DatabaseTypes.UUID(from: &reader),
            operation: try JobOperationIdentifier(from: &reader),
            target: try DatabaseOperationTarget(from: &reader),
            requestDigest: try reader.readBytes(),
            requestID: try reader.readUInt64(),
            traceID: try reader.readOptionalString(),
            authorization: try Self.decodeAuthorization(from: &reader),
            maximumSliceWorkUnits: try reader.readUInt64(),
            sliceTimeoutMilliseconds: try reader.readUInt32(),
            retryPolicy: JobStartOperation.RetryPolicy(
                maximumAttempts: try reader.readUInt32(),
                initialBackoffMilliseconds: try reader.readUInt32(),
                maximumBackoffMilliseconds: try reader.readUInt32()
            ),
            planDigest: try reader.readBytes(),
            createdAt: try Timestamp(from: &reader)
        )
    }

    init(
        jobID: DatabaseTypes.UUID,
        operation: JobOperationIdentifier,
        target: DatabaseOperationTarget,
        requestDigest: ByteString,
        requestID: UInt64,
        traceID: String?,
        authorization: AuthorizationContext,
        maximumSliceWorkUnits: UInt64,
        sliceTimeoutMilliseconds: UInt32,
        retryPolicy: JobStartOperation.RetryPolicy,
        planDigest: ByteString,
        createdAt: Timestamp
    ) {
        self.jobID = jobID
        self.operation = operation
        self.target = target
        self.requestDigest = requestDigest
        self.requestID = requestID
        self.traceID = traceID
        self.authorization = authorization
        self.maximumSliceWorkUnits = maximumSliceWorkUnits
        self.sliceTimeoutMilliseconds = sliceTimeoutMilliseconds
        self.retryPolicy = retryPolicy
        self.planDigest = planDigest
        self.createdAt = createdAt
    }

    private static func encode(
        _ authorization: AuthorizationContext,
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        switch authorization {
        case .anonymous:
            writer.writeUInt8(0)
        case .authenticated(let principal):
            writer.writeUInt8(1)
            try writer.writeString(principal.identifier)
            let roles = principal.roles.sorted()
            try writer.writeCount(roles.count)
            for role in roles {
                try writer.writeString(role)
            }
            try principal.claims.encode(into: &writer)
        }
    }

    private static func decodeAuthorization(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) -> AuthorizationContext {
        switch try reader.readUInt8() {
        case 0:
            return .anonymous
        case 1:
            let identifier = try reader.readString()
            let roleCount = try reader.readCount()
            var roles = Set<String>()
            roles.reserveCapacity(roleCount)
            for _ in 0..<roleCount {
                let role = try reader.readString()
                guard roles.insert(role).inserted else {
                    throw .invalidFieldObject(.duplicateKey(role))
                }
            }
            return .authenticated(
                Principal(
                    identifier: identifier,
                    roles: roles,
                    claims: try FieldObject(from: &reader)
                )
            )
        case let tag:
            throw .invalidValueTag(tag)
        }
    }
}
