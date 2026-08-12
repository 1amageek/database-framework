import DatabaseJobRuntime
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

package struct DatabaseMigrationContinuation: DatabaseRuntimePayloadValue, Sendable, Hashable {
    private static let formatVersion: UInt8 = 1

    package let targetVersion: SchemaVersion
    package let requestFingerprint: ByteString
    package let completedWorkUnits: UInt64

    package func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.formatVersion)
        writer.writeUInt32(targetVersion.major)
        writer.writeUInt32(targetVersion.minor)
        writer.writeUInt32(targetVersion.patch)
        try writer.writeBytes(requestFingerprint)
        writer.writeUInt64(completedWorkUnits)
    }

    package init(
        targetVersion: SchemaVersion,
        requestFingerprint: ByteString,
        completedWorkUnits: UInt64
    ) {
        self.targetVersion = targetVersion
        self.requestFingerprint = requestFingerprint
        self.completedWorkUnits = completedWorkUnits
    }

    package init(
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
