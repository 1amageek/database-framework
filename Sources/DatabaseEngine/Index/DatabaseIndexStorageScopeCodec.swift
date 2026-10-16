import DatabaseKit
import DatabaseTypes

/// Framework-owned durable source Directory contract; hosts retain opaque bytes.
@_spi(DatabaseExecution)
public enum DatabaseIndexStorageScopeCodec {
    public static func encode(
        _ scope: DatabaseIndexStorageScope,
        limits: StorageFrameLimits = .default
    ) throws -> ByteString {
        try scope.validate()
        return try StorageFrameEncoder.encode(limits: limits) { writer throws(StorageFrameError) in
            writer.writeUInt8(1)
            switch scope {
            case .entity(let name, let components, let fields):
                writer.writeUInt8(0)
                try writer.writeString(name)
                try writer.writeCount(components.count)
                for component in components {
                    switch component {
                    case .staticPath(let value): writer.writeUInt8(0); try writer.writeString(value)
                    case .dynamicField(let name): writer.writeUInt8(1); try writer.writeString(name)
                    }
                }
                try writeFields(fields, into: &writer)
            case .polymorphicGroup(let identifier, let path):
                writer.writeUInt8(1)
                try writer.writeString(identifier)
                try writer.writeCount(path.count)
                for component in path { try writer.writeString(component) }
            }
        }
    }

    public static func decode(
        _ bytes: ByteString,
        limits: StorageFrameLimits = .default
    ) throws -> DatabaseIndexStorageScope {
        var reader = try StorageFrameDecoder(bytes, limits: limits)
        guard try reader.readUInt8() == 1 else { throw StorageFrameError.invalidValue }
        let kind = try reader.readUInt8()
        let identifier = try reader.readString()
        let count = try reader.readCount()
        let scope: DatabaseIndexStorageScope
        switch kind {
        case 0:
            var components: [DirectoryPathComponent] = []
            components.reserveCapacity(count)
            for _ in 0..<count {
                let tag = try reader.readUInt8()
                let name = try reader.readString()
                switch tag {
                case 0: components.append(.staticPath(name))
                case 1: components.append(.dynamicField(fieldName: name))
                default: throw StorageFrameError.invalidValue
                }
            }
            let fieldCount = try reader.readCount()
            var fields: [DatabaseIndexStorageScope.DirectoryField] = []
            fields.reserveCapacity(fieldCount)
            for _ in 0..<fieldCount {
                let name = try reader.readString()
                guard let type = FieldSchemaType(rawValue: try reader.readString()) else { throw StorageFrameError.invalidValue }
                let queryType = try reader.readBool() ? SchemaEntityEntryCodec.readQueryType(from: &reader) : nil
                fields.append(.init(name: name, type: type, queryType: queryType))
            }
            scope = .entity(name: identifier, directoryComponents: components, directoryFields: fields)
        case 1:
            var path: [String] = []
            path.reserveCapacity(count)
            for _ in 0..<count { path.append(try reader.readString()) }
            scope = .polymorphicGroup(identifier: identifier, directoryPath: path)
        default: throw StorageFrameError.invalidValue
        }
        try reader.ensureFullyRead()
        try scope.validate()
        return scope
    }

    package static func encodeFields(_ fields: [DatabaseIndexStorageScope.DirectoryField]) throws(StorageFrameError) -> ByteString {
        try StorageFrameEncoder.encode { writer throws(StorageFrameError) in
            try writeFields(fields, into: &writer)
        }
    }

    private static func writeFields(
        _ fields: [DatabaseIndexStorageScope.DirectoryField], into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeCount(fields.count)
        for field in fields {
            try writer.writeString(field.name)
            try writer.writeString(field.type.rawValue)
            writer.writeBool(field.queryType != nil)
            if let queryType = field.queryType { try SchemaEntityEntryCodec.writeQueryType(queryType, into: &writer) }
        }
    }
}
