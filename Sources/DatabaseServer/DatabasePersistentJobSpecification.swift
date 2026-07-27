import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

struct DatabasePersistentJobSpecification: ServerPayloadValue, Sendable, Hashable {
    private static let formatVersion: UInt8 = 1

    let jobID: DatabaseTypes.UUID
    let operation: JobOperationIdentifier
    let requestDigest: ByteString
    let requestID: UInt64
    let traceID: String?
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
                <= retryPolicy.maximumBackoffMilliseconds else {
            throw DatabaseJobRuntimeError.corruptedSpecification
        }
    }

    func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.formatVersion)
        try jobID.encode(into: &writer)
        try operation.encode(into: &writer)
        try writer.writeBytes(requestDigest)
        writer.writeUInt64(requestID)
        try writer.writeOptionalString(traceID)
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
            requestDigest: try reader.readBytes(),
            requestID: try reader.readUInt64(),
            traceID: try reader.readOptionalString(),
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
        requestDigest: ByteString,
        requestID: UInt64,
        traceID: String?,
        maximumSliceWorkUnits: UInt64,
        sliceTimeoutMilliseconds: UInt32,
        retryPolicy: JobStartOperation.RetryPolicy,
        planDigest: ByteString,
        createdAt: Timestamp
    ) {
        self.jobID = jobID
        self.operation = operation
        self.requestDigest = requestDigest
        self.requestID = requestID
        self.traceID = traceID
        self.maximumSliceWorkUnits = maximumSliceWorkUnits
        self.sliceTimeoutMilliseconds = sliceTimeoutMilliseconds
        self.retryPolicy = retryPolicy
        self.planDigest = planDigest
        self.createdAt = createdAt
    }
}
