import DatabaseKit
import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire

struct DatabasePersistentJobSpecification: DatabaseRuntimePayloadValue, Sendable, Hashable {
    private static let formatVersion: UInt8 = 4

    let jobID: DatabaseTypes.UUID
    let operation: JobOperationIdentifier
    let target: DatabaseOperationTarget
    let requestDigest: ByteString
    let requestID: UInt64
    let traceID: String?
    let principalIdentifier: String
    let authorizationReference: DatabaseJobAuthorizationReference
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
              !principalIdentifier.isEmpty else {
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
        try writer.writeString(principalIdentifier)
        try writer.writeString(authorizationReference.value)
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
        let jobID = try DatabaseTypes.UUID(from: &reader)
        let operation = try JobOperationIdentifier(from: &reader)
        let target = try DatabaseOperationTarget(from: &reader)
        let requestDigest = try reader.readBytes()
        let requestID = try reader.readUInt64()
        let traceID = try reader.readOptionalString()
        let principalIdentifier = try reader.readString()
        let referenceValue = try reader.readString()
        let authorizationReference: DatabaseJobAuthorizationReference
        do {
            authorizationReference = try DatabaseJobAuthorizationReference(
                referenceValue
            )
        } catch {
            throw .invalidJobOperationKind
        }
        self.init(
            jobID: jobID,
            operation: operation,
            target: target,
            requestDigest: requestDigest,
            requestID: requestID,
            traceID: traceID,
            principalIdentifier: principalIdentifier,
            authorizationReference: authorizationReference,
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
        principalIdentifier: String,
        authorizationReference: DatabaseJobAuthorizationReference,
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
        self.principalIdentifier = principalIdentifier
        self.authorizationReference = authorizationReference
        self.maximumSliceWorkUnits = maximumSliceWorkUnits
        self.sliceTimeoutMilliseconds = sliceTimeoutMilliseconds
        self.retryPolicy = retryPolicy
        self.planDigest = planDigest
        self.createdAt = createdAt
    }

}
