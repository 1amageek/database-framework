import DatabaseKit
import DatabaseTypes
import DatabaseWire

extension SQLCatalogCodec {
    static func writeIndexes(_ catalog: SQLCatalog, into writer: inout StorageFrameEncoder) throws(StorageFrameError) {
        try writer.writeCount(catalog.indexes.count)
        for index in catalog.indexes { try writeIndex(index, into: &writer) }
    }

    static func writeIndex(_ index: SQLIndex, into writer: inout StorageFrameEncoder) throws(StorageFrameError) {
        try writer.writeString(index.id)
        try writer.writeString(index.namespaceID)
        try writer.writeString(index.name)
        try writer.writeString(index.relationID)
        writer.writeBool(index.unique)
        writer.writeUInt8(index.nulls == .distinct ? 1 : 2)
        try writer.writeCount(index.keys.count)
        for key in index.keys {
            try writeExpression(key.expression, into: &writer)
            writer.writeUInt8(key.direction == .ascending ? 1 : 2)
            switch key.nulls {
            case nil: writer.writeUInt8(0)
            case .first: writer.writeUInt8(1)
            case .last: writer.writeUInt8(2)
            }
        }
        writer.writeBool(index.predicate != nil)
        if let predicate = index.predicate { try writeExpression(predicate, into: &writer) }
        try writer.writeCount(index.bindings.count)
        for binding in index.bindings {
            try writer.writeString(binding.id)
            try writer.writeString(binding.name)
            do { try writer.writeBytes(QueryIRWireFormat.encodeColumnType(binding.type, limits: indexQueryLimits(writer.limits))) }
            catch { throw .invalidValue }
        }
    }

    static func readIndexes(from reader: inout StorageFrameDecoder) throws -> [SQLIndex] {
        let indexCount = try count(from: &reader, minimumBytes: 27)
        var result: [SQLIndex] = []
        result.reserveCapacity(indexCount)
        for _ in 0..<indexCount {
            let id = try reader.readString(), namespaceID = try reader.readString()
            let name = try reader.readString(), relationID = try reader.readString()
            let unique = try reader.readBool()
            let nulls: SQLNullUniqueness
            switch try reader.readUInt8() {
            case 1: nulls = .distinct
            case 2: nulls = .notDistinct
            default: throw StorageFrameError.invalidValue
            }
            let keyCount = try count(from: &reader, minimumBytes: 6)
            var keys: [SortKey] = []
            keys.reserveCapacity(keyCount)
            for _ in 0..<keyCount {
                let expression = try readIndexExpression(from: &reader)
                let direction: SortDirection
                switch try reader.readUInt8() {
                case 1: direction = .ascending
                case 2: direction = .descending
                default: throw StorageFrameError.invalidValue
                }
                let nulls: NullOrdering?
                switch try reader.readUInt8() {
                case 0: nulls = nil
                case 1: nulls = .first
                case 2: nulls = .last
                default: throw StorageFrameError.invalidValue
                }
                keys.append(SortKey(expression, direction: direction, nulls: nulls))
            }
            let predicate = try reader.readBool() ? readIndexExpression(from: &reader) : nil
            let bindingCount = try count(from: &reader, minimumBytes: 12)
            var bindings: [SQLViewColumnBinding] = []
            bindings.reserveCapacity(bindingCount)
            for _ in 0..<bindingCount {
                bindings.append(try SQLViewColumnBinding(id: reader.readString(), name: reader.readString(),
                    type: QueryIRWireFormat.decodeColumnType(reader.readBytes(), limits: indexQueryLimits(reader.limits))))
            }
            result.append(try SQLIndex(id: id, namespaceID: namespaceID, name: name, relationID: relationID,
                keys: keys, predicate: predicate, unique: unique, nulls: nulls, bindings: bindings))
        }
        return result
    }

    private static func readIndexExpression(from reader: inout StorageFrameDecoder) throws -> Expression {
        try QueryIRWireFormat.decodeExpression(reader.readBytes(), limits: indexQueryLimits(reader.limits))
    }

    private static func indexQueryLimits(_ limits: StorageFrameLimits) throws -> DatabaseWireLimits {
        try DatabaseWireLimits(maximumFrameBytes: limits.maximumFrameBytes,
            maximumStringBytes: limits.maximumStringBytes, maximumByteStringBytes: limits.maximumByteStringBytes,
            maximumCollectionCount: limits.maximumCollectionCount,
            maximumNestingDepth: min(limits.maximumNestingDepth, DatabaseWireLimits.maximumSupportedNestingDepth),
            maximumObjectCount: limits.maximumCollectionCount)
    }
}
