import DatabaseValue
import DatabaseWire

public struct DatabaseMaintenanceJobPlan: DatabaseWireValue, Sendable, Hashable {
    enum Invocation: Sendable, Hashable {
        case migrations(
            targetVersion: DatabaseSchemaVersion,
            totalStageCount: UInt64,
            maximumStagesPerSlice: UInt64
        )
        case indexRebuild(
            entity: String,
            index: String,
            partitions: [DatabaseObjectField],
            schemaVersion: DatabaseSchemaVersion,
            maximumWorkUnits: UInt64
        )
        case compaction(maximumWorkUnits: UInt64)
    }

    private static let formatVersion: UInt8 = 1

    let invocation: Invocation

    public func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.formatVersion)
        switch invocation {
        case .migrations(
            let targetVersion,
            let totalStageCount,
            let maximumStagesPerSlice
        ):
            writer.writeUInt8(3)
            try targetVersion.encode(into: &writer)
            writer.writeUInt64(totalStageCount)
            writer.writeUInt64(maximumStagesPerSlice)
        case .indexRebuild(
            let entity,
            let index,
            let partitions,
            let schemaVersion,
            let maximumWorkUnits
        ):
            writer.writeUInt8(1)
            try writer.writeString(entity)
            try writer.writeString(index)
            try writer.writeCount(partitions.count)
            for partition in partitions {
                try partition.encode(into: &writer)
            }
            try schemaVersion.encode(into: &writer)
            writer.writeUInt64(maximumWorkUnits)
        case .compaction(let maximumWorkUnits):
            writer.writeUInt8(2)
            writer.writeUInt64(maximumWorkUnits)
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
            let entity = try reader.readString()
            let index = try reader.readString()
            let count = try reader.readCount()
            var partitions: [DatabaseObjectField] = []
            partitions.reserveCapacity(count)
            for _ in 0..<count {
                partitions.append(try DatabaseObjectField(from: &reader))
            }
            self.init(
                invocation: .indexRebuild(
                    entity: entity,
                    index: index,
                    partitions: partitions,
                    schemaVersion: try DatabaseSchemaVersion(from: &reader),
                    maximumWorkUnits: try reader.readUInt64()
                )
            )
        case 2:
            self.init(
                invocation: .compaction(
                    maximumWorkUnits: try reader.readUInt64()
                )
            )
        case 3:
            self.init(
                invocation: .migrations(
                    targetVersion: try DatabaseSchemaVersion(from: &reader),
                    totalStageCount: try reader.readUInt64(),
                    maximumStagesPerSlice: try reader.readUInt64()
                )
            )
        case let tag:
            throw .invalidValueTag(tag)
        }
    }

    init(invocation: Invocation) {
        self.invocation = invocation
    }
}
