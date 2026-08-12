import DatabaseJobRuntime
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

package struct DatabaseIndexStatusContinuation: DatabaseRuntimePayloadValue, Hashable {
    private static let version: UInt8 = 1

    package let entityFilter: String?
    package let indexFilter: String?
    package let partitionFilter: FieldObject
    package let entityPosition: UInt32
    package let indexPosition: UInt32
    package let partitionCatalogContinuation: ByteString?

    package init(
        entityFilter: String?,
        indexFilter: String?,
        partitionFilter: FieldObject,
        entityPosition: UInt32,
        indexPosition: UInt32,
        partitionCatalogContinuation: ByteString?
    ) {
        self.entityFilter = entityFilter
        self.indexFilter = indexFilter
        self.partitionFilter = partitionFilter
        self.entityPosition = entityPosition
        self.indexPosition = indexPosition
        self.partitionCatalogContinuation = partitionCatalogContinuation
    }

    package func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.version)
        try writer.writeOptionalString(entityFilter)
        try writer.writeOptionalString(indexFilter)
        try partitionFilter.encode(into: &writer)
        writer.writeUInt32(entityPosition)
        writer.writeUInt32(indexPosition)
        try writer.writeOptionalBytes(partitionCatalogContinuation)
    }

    package init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        let actualVersion = try reader.readUInt8()
        guard actualVersion == Self.version else {
            throw DatabaseWireError.invalidValueTag(actualVersion)
        }
        let entityFilter = try reader.readOptionalString()
        let indexFilter = try reader.readOptionalString()
        self.init(
            entityFilter: entityFilter,
            indexFilter: indexFilter,
            partitionFilter: try FieldObject(from: &reader),
            entityPosition: try reader.readUInt32(),
            indexPosition: try reader.readUInt32(),
            partitionCatalogContinuation: try reader.readOptionalBytes()
        )
    }
}
