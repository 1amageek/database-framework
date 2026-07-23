import DatabaseValue
import DatabaseWire

struct DatabasePersistentJobPlan: DatabaseWireValue, Sendable, Hashable {
    private static let formatVersion: UInt8 = 1

    let jobID: DatabaseUUID
    let operation: DatabaseJobOperationIdentifier
    let specificationDigest: DatabaseBytes
    let payload: DatabaseBytes

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
            jobID: try DatabaseUUID(from: &reader),
            operation: try DatabaseJobOperationIdentifier(from: &reader),
            specificationDigest: try reader.readBytes(),
            payload: try reader.readBytes()
        )
    }

    init(
        jobID: DatabaseUUID,
        operation: DatabaseJobOperationIdentifier,
        specificationDigest: DatabaseBytes,
        payload: DatabaseBytes
    ) {
        self.jobID = jobID
        self.operation = operation
        self.specificationDigest = specificationDigest
        self.payload = payload
    }
}
