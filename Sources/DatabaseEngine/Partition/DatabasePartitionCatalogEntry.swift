import DatabaseTypes

package struct DatabasePartitionCatalogEntry: StorageFrameValue, Hashable {
    package let entity: String
    package let partitions: FieldObject

    package init(entity: String, partitions: FieldObject) {
        self.entity = entity
        self.partitions = partitions
    }

    package func encode(
        to writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeString(entity)
        try StorageValueEncoder.write(
            .object(partitions),
            into: &writer
        )
    }

    package init(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) {
        let entity = try reader.readString()
        guard case .object(let partitions) =
                try StorageValueDecoder.read(from: &reader) else {
            throw .invalidValue
        }
        self.init(
            entity: entity,
            partitions: partitions
        )
    }
}
