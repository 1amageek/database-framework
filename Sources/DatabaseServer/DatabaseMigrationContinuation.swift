import DatabaseValue
import DatabaseWire

struct DatabaseMigrationContinuation: DatabaseWireValue, Sendable, Hashable {
    private static let formatVersion: UInt8 = 1

    let targetVersion: DatabaseSchemaVersion
    let requestFingerprint: DatabaseBytes
    let completedWorkUnits: UInt64

    func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.formatVersion)
        try targetVersion.encode(into: &writer)
        try writer.writeBytes(requestFingerprint)
        writer.writeUInt64(completedWorkUnits)
    }

    init(
        targetVersion: DatabaseSchemaVersion,
        requestFingerprint: DatabaseBytes,
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
            targetVersion: try DatabaseSchemaVersion(from: &reader),
            requestFingerprint: try reader.readBytes(),
            completedWorkUnits: try reader.readUInt64()
        )
    }
}
