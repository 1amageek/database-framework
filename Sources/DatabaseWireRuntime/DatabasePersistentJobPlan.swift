import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire

struct DatabasePersistentJobPlan: DatabaseRuntimePayloadValue, Sendable, Hashable {
    private static let formatVersion: UInt8 = 1

    let jobID: DatabaseTypes.UUID
    let operation: JobOperationIdentifier
    let specificationDigest: ByteString
    let payload: ByteString

    func validate() throws {
        guard specificationDigest.count == DatabaseRequestDigest.byteCount else {
            throw DatabaseJobRuntimeError.corruptedPlan
        }
    }

    func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.formatVersion)
        try jobID.encode(into: &writer)
        try operation.encode(into: &writer)
        try writer.writeBytes(specificationDigest)
        try writer.writeBytes(payload)
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
            specificationDigest: try reader.readBytes(),
            payload: try reader.readBytes()
        )
    }

    init(
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
