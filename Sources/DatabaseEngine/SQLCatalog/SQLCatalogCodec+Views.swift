import DatabaseKit
import DatabaseTypes
import DatabaseWire

extension SQLCatalogCodec {
    static func writeViews(_ catalog: SQLCatalog, into writer: inout StorageFrameEncoder) throws(StorageFrameError) {
        do {
            try writer.writeCount(catalog.views.count)
            for view in catalog.views {
                try writer.writeString(view.id); try writer.writeString(view.namespaceID); try writer.writeString(view.name)
                try writeViewDefinition(view.definition, into: &writer)
            }
            try writer.writeCount(catalog.materializedViews.count)
            for view in catalog.materializedViews {
                try writer.writeString(view.id); try writer.writeString(view.namespaceID); try writer.writeString(view.name)
                try writeViewDefinition(view.definition, into: &writer)
                try writer.writeString(view.backingRelationID); writer.writeBool(view.isPopulated)
            }
        } catch let error as StorageFrameError { throw error }
        catch { throw .invalidValue }
    }

    static func readViews(from reader: inout StorageFrameDecoder) throws
        -> (views: [SQLView], materializedViews: [SQLMaterializedView]) {
        let viewCount = try count(from: &reader, minimumBytes: 16)
        var views: [SQLView] = []; views.reserveCapacity(viewCount)
        for _ in 0..<viewCount {
            let id = try reader.readString(), namespace = try reader.readString(), name = try reader.readString()
            views.append(try SQLView(id: id, namespaceID: namespace, name: name,
                definition: readViewDefinition(from: &reader)))
        }
        let materializedCount = try count(from: &reader, minimumBytes: 21)
        var materialized: [SQLMaterializedView] = []; materialized.reserveCapacity(materializedCount)
        for _ in 0..<materializedCount {
            let id = try reader.readString(), namespace = try reader.readString(), name = try reader.readString()
            let definition = try readViewDefinition(from: &reader)
            materialized.append(try SQLMaterializedView(id: id, namespaceID: namespace, name: name,
                definition: definition, backingRelationID: reader.readString(), isPopulated: reader.readBool()))
        }
        return (views, materialized)
    }

    private static func writeViewDefinition(_ value: SQLViewDefinition,
        into writer: inout StorageFrameEncoder) throws {
        try writer.writeBytes(QueryIRWireFormat.encode(.select(value.query), limits: queryLimits(writer.limits)))
        try writer.writeCount(value.columns.count)
        for column in value.columns {
            try writer.writeString(column.id); try writer.writeString(column.name)
            try writer.writeBytes(QueryIRWireFormat.encodeColumnType(column.type, limits: queryLimits(writer.limits)))
            writer.writeBool(column.isNullable)
        }
        try writer.writeCount(value.bindings.count)
        for binding in value.bindings {
            writer.writeBool(binding.schema != nil)
            if let schema = binding.schema { try writer.writeString(schema) }
            try writer.writeString(binding.table); try writer.writeString(binding.relationID)
            try writer.writeCount(binding.columns.count)
            for column in binding.columns {
                try writer.writeString(column.id); try writer.writeString(column.name)
                try writer.writeBytes(QueryIRWireFormat.encodeColumnType(column.type, limits: queryLimits(writer.limits)))
            }
        }
    }

    private static func readViewDefinition(from reader: inout StorageFrameDecoder) throws -> SQLViewDefinition {
        guard case .select(let query) = try QueryIRWireFormat.decode(reader.readBytes(), limits: queryLimits(reader.limits)) else {
            throw StorageFrameError.invalidValue
        }
        let columnCount = try count(from: &reader, minimumBytes: 13)
        var columns: [SQLViewColumn] = []; columns.reserveCapacity(columnCount)
        for _ in 0..<columnCount {
            let id = try reader.readString(), name = try reader.readString()
            let type = try QueryIRWireFormat.decodeColumnType(reader.readBytes(), limits: queryLimits(reader.limits))
            columns.append(try SQLViewColumn(id: id, name: name, type: type, isNullable: reader.readBool()))
        }
        let bindingCount = try count(from: &reader, minimumBytes: 13)
        var bindings: [SQLViewSourceBinding] = []; bindings.reserveCapacity(bindingCount)
        for _ in 0..<bindingCount {
            let schema = try reader.readBool() ? reader.readString() : nil
            let table = try reader.readString(), relation = try reader.readString()
            let count = try count(from: &reader, minimumBytes: 8)
            var selected: [SQLViewColumnBinding] = []; selected.reserveCapacity(count)
            for _ in 0..<count {
                let id = try reader.readString(), name = try reader.readString()
                let type = try QueryIRWireFormat.decodeColumnType(reader.readBytes(), limits: queryLimits(reader.limits))
                selected.append(try SQLViewColumnBinding(id: id, name: name, type: type))
            }
            bindings.append(try SQLViewSourceBinding(schema: schema, table: table, relationID: relation, columns: selected))
        }
        return try SQLViewDefinition(query: query, columns: columns, bindings: bindings)
    }
}
