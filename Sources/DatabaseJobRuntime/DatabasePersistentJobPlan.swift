import DatabaseOperationCore
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

package struct DatabasePersistentJobPlan: DatabaseRuntimePayloadValue, Sendable, Hashable {
    private static let formatVersion: UInt8 = 1

    package let jobID: DatabaseTypes.UUID
    package let operation: JobOperationIdentifier
    package let specificationDigest: ByteString
    package let payload: ByteString

    package func validate() throws {
        guard specificationDigest.count == DatabaseRequestDigest.byteCount else {
            throw DatabaseJobRuntimeError.corruptedPlan
        }
    }

    package func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.formatVersion)
        try jobID.encode(into: &writer)
        try operation.encode(into: &writer)
        try writer.writeBytes(specificationDigest)
        try writer.writeBytes(payload)
    }

    package init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        let version = try reader.readUInt8()
        guard version == Self.formatVersion else {
            throw .unsupportedProtocolVersionValue(UInt16(version))
        }
        self.init(
            jobID: try DatabaseTypes.UUID(from: &reader),
            operation: try JobOperationIdentifier(from: &reader),
            specificationDigest: try reader.readBytes(),
            payload: try reader.readBytes()
        )
    }

    package init(
        jobID: DatabaseTypes.UUID,
        operation: JobOperationIdentifier,
        specificationDigest: ByteString,
        payload: ByteString
    ) {
        self.jobID = jobID
        self.operation = operation
        self.specificationDigest = specificationDigest
        self.payload = payload
    }
}
