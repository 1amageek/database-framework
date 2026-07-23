import DatabaseValue
import DatabaseWire

struct DatabaseIndexStatusContinuation: DatabaseWireValue, Hashable {
    private static let version: UInt8 = 1

    let entityFilter: String?
    let indexFilter: String?
    let partitionFilter: [DatabaseObjectField]
    let entityPosition: UInt32
    let indexPosition: UInt32
    let partitionCatalogContinuation: DatabaseBytes?

    init(
        entityFilter: String?,
        indexFilter: String?,
        partitionFilter: [DatabaseObjectField],
        entityPosition: UInt32,
        indexPosition: UInt32,
        partitionCatalogContinuation: DatabaseBytes?
    ) {
        self.entityFilter = entityFilter
        self.indexFilter = indexFilter
        self.partitionFilter = partitionFilter
        self.entityPosition = entityPosition
        self.indexPosition = indexPosition
        self.partitionCatalogContinuation = partitionCatalogContinuation
    }

    func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.version)
        try writer.writeOptionalString(entityFilter)
        try writer.writeOptionalString(indexFilter)
        try writer.writeCount(partitionFilter.count)
        for partition in partitionFilter {
            try partition.encode(into: &writer)
        }
        writer.writeUInt32(entityPosition)
        writer.writeUInt32(indexPosition)
        try writer.writeOptionalBytes(partitionCatalogContinuation)
    }

    init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        let actualVersion = try reader.readUInt8()
        guard actualVersion == Self.version else {
            throw DatabaseWireError.invalidValueTag(actualVersion)
        }
        let entityFilter = try reader.readOptionalString()
        let indexFilter = try reader.readOptionalString()
        let partitionCount = try reader.readCount()
        var partitionFilter: [DatabaseObjectField] = []
        partitionFilter.reserveCapacity(partitionCount)
        for _ in 0..<partitionCount {
            partitionFilter.append(try DatabaseObjectField(from: &reader))
        }
        self.init(
            entityFilter: entityFilter,
            indexFilter: indexFilter,
            partitionFilter: partitionFilter,
            entityPosition: try reader.readUInt32(),
            indexPosition: try reader.readUInt32(),
            partitionCatalogContinuation: try reader.readOptionalBytes()
        )
    }
}
