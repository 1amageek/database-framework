import DatabaseOperationCore
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

package struct DatabasePersistentJobDueEntry: DatabaseRuntimePayloadValue, Sendable, Hashable {
    private static let formatVersion: UInt8 = 1

    package let jobID: DatabaseTypes.UUID
    package let stateRevision: UInt64

    package func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.formatVersion)
        try jobID.encode(into: &writer)
        writer.writeUInt64(stateRevision)
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
            stateRevision: try reader.readUInt64()
        )
    }

    package init(jobID: DatabaseTypes.UUID, stateRevision: UInt64) {
        self.jobID = jobID
        self.stateRevision = stateRevision
    }
}
