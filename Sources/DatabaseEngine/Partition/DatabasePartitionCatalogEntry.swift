import DatabaseValue
import DatabaseWire

package struct DatabasePartitionCatalogEntry: DatabaseWireValue, Hashable {
    package let entity: String
    package let partitions: [DatabaseObjectField]

    package init(entity: String, partitions: [DatabaseObjectField]) {
        self.entity = entity
        self.partitions = partitions
    }

    package func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        try writer.writeString(entity)
        try writer.writeCount(partitions.count)
        for partition in partitions {
            try partition.encode(into: &writer)
        }
    }

    package init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        let entity = try reader.readString()
        let count = try reader.readCount()
        var partitions: [DatabaseObjectField] = []
        partitions.reserveCapacity(count)
        for _ in 0..<count {
            partitions.append(try DatabaseObjectField(from: &reader))
        }
        self.init(entity: entity, partitions: partitions)
    }
}
