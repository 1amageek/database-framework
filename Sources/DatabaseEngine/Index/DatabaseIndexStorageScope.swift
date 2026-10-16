import DatabaseKit
import DatabaseTypes

/// Locates the physical store that owns one index generation.
///
/// The scope retains the source directory contract so cleanup remains
/// executable after the declaring entity or polymorphic group is removed from
/// the published schema.
@_spi(DatabaseExecution)
public enum DatabaseIndexStorageScope: Sendable, Hashable {
    case entity(
        name: String,
        directoryComponents: [DirectoryPathComponent],
        directoryFields: [DirectoryField] = []
    )
    case polymorphicGroup(
        identifier: String,
        directoryPath: [String]
    )

    public struct DirectoryField: Sendable, Hashable {
        public let name: String
        public let type: FieldSchemaType
        public let queryType: QueryColumnType?

        public init(name: String, type: FieldSchemaType, queryType: QueryColumnType? = nil) {
            self.name = name
            self.type = type
            self.queryType = queryType
        }

        package var schema: FieldSchema {
            FieldSchema(name: name, fieldNumber: 1, type: type, queryType: queryType)
        }
    }

    public static func entity(_ entity: Schema.Entity) -> Self {
        .entity(name: entity.name, directoryComponents: entity.directoryComponents,
            directoryFields: entity.dynamicFieldNames.map { name in
                let field = entity.fieldMapByName[name]!
                return DirectoryField(name: name, type: field.type, queryType: field.queryType)
            })
    }

    public var usesDynamicDirectory: Bool {
        guard case .entity(_, let components, _) = self else {
            return false
        }
        return components.contains { component in
            if case .dynamicField = component { return true }
            return false
        }
    }

    /// Whether a partition catalog entry belongs to this directory contract.
    public func accepts(partitions: FieldObject) -> Bool {
        switch self {
        case .entity(_, let components, let fields):
            let required = Set(components.compactMap { component in
                if case .dynamicField(let name) = component { return name }
                return nil
            })
            guard Set(partitions.fields.map { $0.key }) == required,
                  Set(fields.map { $0.name }) == required else { return false }
            return fields.allSatisfy { field in
                guard let value = partitions[field.name] else { return false }
                return DirectoryComponentCodec.accepts(value, schema: field.schema)
            }
        case .polymorphicGroup:
            return partitions.isEmpty
        }
    }

    /// Stable structural ordering used by transition plans and durable host
    /// checkpoints. Length framing prevents component-boundary collisions.
    public var stableOrderingKey: String {
        func frame(_ value: String) -> String {
            "\(value.utf8.count):\(value)"
        }
        switch self {
        case .entity(let name, let components, let fields):
            var key = "0\(frame(name))"
            for component in components {
                switch component {
                case .staticPath(let value):
                    key += "0\(frame(value))"
                case .dynamicField(let name):
                    key += "1\(frame(name))"
                }
            }
            do {
                let bytes = try DatabaseIndexStorageScopeCodec.encodeFields(fields)
                let digits = Array("0123456789ABCDEF".utf8)
                var encoded: [UInt8] = []
                encoded.reserveCapacity(bytes.count * 2)
                for byte in bytes { encoded.append(digits[Int(byte >> 4)]); encoded.append(digits[Int(byte & 15)]) }
                key += frame(String(decoding: encoded, as: UTF8.self))
            } catch {
                preconditionFailure("Validated index scope contains invalid Directory metadata")
            }
            return key
        case .polymorphicGroup(let identifier, let path):
            return "1\(frame(identifier))"
                + path.map { "0\(frame($0))" }.joined()
        }
    }

    package func validate() throws(DatabaseIndexStorageScopeError) {
        switch self {
        case .entity(let name, let components, let fields):
            guard !name.isEmpty else {
                throw DatabaseIndexStorageScopeError.emptyIdentifier
            }
            var dynamicNames = Set<String>()
            for component in components {
                switch component {
                case .staticPath(let value):
                    guard !value.isEmpty else {
                        throw DatabaseIndexStorageScopeError
                            .emptyDirectoryComponent
                    }
                case .dynamicField(let name):
                    guard !name.isEmpty else {
                        throw DatabaseIndexStorageScopeError
                            .emptyDirectoryComponent
                    }
                    guard dynamicNames.insert(name).inserted else {
                        throw DatabaseIndexStorageScopeError
                            .duplicateDynamicDirectoryField(name)
                    }
                }
            }
            let expected = components.compactMap { component -> String? in
                if case .dynamicField(let name) = component { return name }; return nil
            }
            guard fields.map({ $0.name }) == expected else { throw .invalidDirectoryFields }
            for field in fields {
                guard field.type.hasCanonicalDirectoryComponent else { throw .invalidDirectoryFields }
                if let descriptor = field.queryType {
                    do { try QueryFieldStorageAdapter.validateDescriptor(descriptor, fieldType: field.type, isArray: false) }
                    catch { throw .invalidDirectoryFields }
                    switch descriptor {
                    case .sql(.decimal), .sql(.json), .sql(.jsonb), .sql(.interval), .sql(.array), .sql(.time(true, _)):
                        throw .invalidDirectoryFields
                    default: break
                    }
                }
            }
        case .polymorphicGroup(let identifier, let path):
            guard !identifier.isEmpty else {
                throw DatabaseIndexStorageScopeError.emptyIdentifier
            }
            guard path.allSatisfy({ !$0.isEmpty }) else {
                throw DatabaseIndexStorageScopeError.emptyDirectoryComponent
            }
        }
    }
}

@_spi(DatabaseExecution)
public enum DatabaseIndexStorageScopeError: Error, Sendable, Equatable {
    case emptyIdentifier
    case invalidDirectoryFields
    case emptyDirectoryComponent
    case duplicateDynamicDirectoryField(String)
}
