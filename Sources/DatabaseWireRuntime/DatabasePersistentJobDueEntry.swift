import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire

struct DatabasePersistentJobDueEntry: DatabaseRuntimePayloadValue, Sendable, Hashable {
    private static let formatVersion: UInt8 = 1

    let jobID: DatabaseTypes.UUID
    let stateRevision: UInt64

    func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.formatVersion)
        try jobID.encode(into: &writer)
        writer.writeUInt64(stateRevision)
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
            stateRevision: try reader.readUInt64()
        )
    }

    init(jobID: DatabaseTypes.UUID, stateRevision: UInt64) {
        self.jobID = jobID
        self.stateRevision = stateRevision
    }
}
