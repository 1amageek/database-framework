import DatabaseValue
import DatabaseWire

struct DatabasePersistentJobSpecification: DatabaseWireValue, Sendable, Hashable {
    private static let formatVersion: UInt8 = 1

    let jobID: DatabaseUUID
    let operation: DatabaseJobOperationIdentifier
    let requestDigest: DatabaseBytes
    let requestID: UInt64
    let traceID: String?
    let maximumSliceWorkUnits: UInt64
    let sliceTimeoutMilliseconds: UInt32
    let retryPolicy: JobStartOperation.RetryPolicy
    let planDigest: DatabaseBytes
    let createdAt: DatabaseTimestamp

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
        try retryPolicy.encode(into: &writer)
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
            jobID: try DatabaseUUID(from: &reader),
            operation: try DatabaseJobOperationIdentifier(from: &reader),
            requestDigest: try reader.readBytes(),
            requestID: try reader.readUInt64(),
            traceID: try reader.readOptionalString(),
            maximumSliceWorkUnits: try reader.readUInt64(),
            sliceTimeoutMilliseconds: try reader.readUInt32(),
            retryPolicy: try JobStartOperation.RetryPolicy(from: &reader),
            planDigest: try reader.readBytes(),
            createdAt: try DatabaseTimestamp(from: &reader)
        )
    }

    init(
        jobID: DatabaseUUID,
        operation: DatabaseJobOperationIdentifier,
        requestDigest: DatabaseBytes,
        requestID: UInt64,
        traceID: String?,
        maximumSliceWorkUnits: UInt64,
        sliceTimeoutMilliseconds: UInt32,
        retryPolicy: JobStartOperation.RetryPolicy,
        planDigest: DatabaseBytes,
        createdAt: DatabaseTimestamp
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
