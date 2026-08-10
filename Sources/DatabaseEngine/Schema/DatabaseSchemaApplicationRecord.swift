import DatabaseKit
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

/// Durable identity of one accepted schema transition.
package struct DatabaseSchemaApplicationRecord:
    Sendable,
    Hashable,
    StorageFrameValue
{
    package let idempotencyKey: String
    package let expectedFingerprint: SchemaFingerprint
    package let targetFingerprint: SchemaFingerprint
    package let job: JobIdentity

    package init(
        idempotencyKey: String,
        expectedFingerprint: SchemaFingerprint,
        targetFingerprint: SchemaFingerprint,
        job: JobIdentity
    ) {
        self.idempotencyKey = idempotencyKey
        self.expectedFingerprint = expectedFingerprint
        self.targetFingerprint = targetFingerprint
        self.job = job
    }

    package func encode(
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeString(idempotencyKey)
        try encoder.writeBytes(expectedFingerprint.bytes)
        try encoder.writeBytes(targetFingerprint.bytes)
        encoder.writeUInt64(job.jobID.high)
        encoder.writeUInt64(job.jobID.low)
        encoder.writeUInt16(job.operation.family.rawValue)
        try encoder.writeString(job.operation.kind)
        guard job.target == .database else {
            throw .invalidValue
        }
    }

    package init(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) {
        do {
            let idempotencyKey = try decoder.readString()
            let expectedFingerprint = try SchemaFingerprint(
                decoder.readBytes()
            )
            let targetFingerprint = try SchemaFingerprint(
                decoder.readBytes()
            )
            guard !idempotencyKey.isEmpty else {
                throw StorageFrameError.invalidValue
            }
            let jobID = DatabaseTypes.UUID(
                high: try decoder.readUInt64(),
                low: try decoder.readUInt64()
            )
            guard let family = DatabaseOperationIdentifier(
                rawValue: try decoder.readUInt16()
            ) else {
                throw StorageFrameError.invalidValue
            }
            let operation = try JobOperationIdentifier(
                family: family,
                kind: decoder.readString()
            )
            self.init(
                idempotencyKey: idempotencyKey,
                expectedFingerprint: expectedFingerprint,
                targetFingerprint: targetFingerprint,
                job: JobIdentity(
                    jobID: jobID,
                    operation: operation,
                    target: .database
                )
            )
        } catch let error as StorageFrameError {
            throw error
        } catch {
            throw .invalidValue
        }
    }
}
