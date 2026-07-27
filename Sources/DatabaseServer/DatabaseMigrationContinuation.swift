import DatabaseKit
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

struct DatabaseMigrationContinuation: ServerPayloadValue, Sendable, Hashable {
    private static let formatVersion: UInt8 = 1

    let targetVersion: SchemaVersion
    let requestFingerprint: ByteString
    let completedWorkUnits: UInt64

    func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.formatVersion)
        writer.writeUInt32(targetVersion.major)
        writer.writeUInt32(targetVersion.minor)
        writer.writeUInt32(targetVersion.patch)
        try writer.writeBytes(requestFingerprint)
        writer.writeUInt64(completedWorkUnits)
    }

    init(
        targetVersion: SchemaVersion,
        requestFingerprint: ByteString,
        completedWorkUnits: UInt64
    ) {
        self.targetVersion = targetVersion
        self.requestFingerprint = requestFingerprint
        self.completedWorkUnits = completedWorkUnits
    }

    init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        let version = try reader.readUInt8()
        guard version == Self.formatVersion else {
            throw .unsupportedProtocolVersionValue(UInt16(version))
        }
        self.init(
            targetVersion: SchemaVersion(
                try reader.readUInt32(),
                try reader.readUInt32(),
                try reader.readUInt32()
            ),
            requestFingerprint: try reader.readBytes(),
            completedWorkUnits: try reader.readUInt64()
        )
    }
}
