import DatabaseValue
import DatabaseWire

public struct DatabaseMaintenanceJobState: DatabaseWireValue, Sendable, Hashable {
    enum Value: Sendable, Hashable {
        case migrations
        case indexRebuild(started: Bool)
        case compaction(continuation: DatabaseBytes?)
    }

    private static let formatVersion: UInt8 = 1

    let value: Value

    public func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.formatVersion)
        switch value {
        case .migrations:
            writer.writeUInt8(3)
        case .indexRebuild(let started):
            writer.writeUInt8(1)
            writer.writeBool(started)
        case .compaction(let continuation):
            writer.writeUInt8(2)
            try writer.writeOptionalBytes(continuation)
        }
    }

    public init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        let version = try reader.readUInt8()
        guard version == Self.formatVersion else {
            throw .unsupportedProtocolVersionValue(UInt16(version))
        }
        switch try reader.readUInt8() {
        case 1:
            self.init(value: .indexRebuild(started: try reader.readBool()))
        case 2:
            self.init(
                value: .compaction(
                    continuation: try reader.readOptionalBytes()
                )
            )
        case 3:
            self.init(value: .migrations)
        case let tag:
            throw .invalidValueTag(tag)
        }
    }

    init(value: Value) {
        self.value = value
    }
}
