import DatabaseKit
import DatabaseTypes

/// Holds the typed field values required to resolve a compiled directory path.
public struct DirectoryPath<T: Persistable>: Sendable {
    internal var fieldValues: [DirectoryFieldBinding] = []

    public init() {}

    public mutating func set<V: FieldValueRepresentable>(
        _ field: Field<T, V>,
        to value: V
    ) {
        fieldValues.removeAll { $0.field == field.identity }
        fieldValues.append(
            DirectoryFieldBinding(
                field: field.identity,
                value: value.fieldValue
            )
        )
    }

    public func hasValue<V>(for field: Field<T, V>) -> Bool {
        fieldValues.contains { $0.field == field.identity }
    }

    public func value<V: FieldValueDecodable>(
        for field: Field<T, V>
    ) throws(PersistableDecodingError) -> V? {
        guard let value = fieldValues.first(where: {
            $0.field == field.identity
        })?.value else {
            return nil
        }
        return try V.decodeFieldValue(value, field: field.name)
    }

    public func validate() throws(DirectoryPathError) {
        let requiredNames = T.directoryFieldNames
        guard !requiredNames.isEmpty else {
            guard fieldValues.isEmpty else {
                throw DirectoryPathError.invalidField(
                    typeName: T.persistableType,
                    field: fieldValues[0].name,
                    reason: "the entity has a static directory"
                )
            }
            return
        }

        let providedNames = Set(fieldValues.map { $0.name })
        let missingNames = Set(requiredNames).subtracting(providedNames)
        guard missingNames.isEmpty else {
            throw DirectoryPathError.missingFields(missingNames.sorted())
        }
        let unexpectedNames = providedNames.subtracting(requiredNames)
        guard unexpectedNames.isEmpty else {
            throw DirectoryPathError.invalidField(
                typeName: T.persistableType,
                field: unexpectedNames.sorted()[0],
                reason: "the field is not part of the compiled directory"
            )
        }
    }

    internal func resolve() throws -> [String] {
        let partitions = try canonicalPartitions()
        var path: [String] = []
        path.reserveCapacity(T.directoryPathComponents.count)

        for component in T.directoryPathComponents {
            switch component {
            case .staticPath(let value):
                path.append(value)
            case .dynamicField(let name):
                guard let value = partitions[name] else {
                    throw DirectoryPathError.missingFields([name])
                }
                path.append(try CanonicalDirectoryPartitionCodec.encode(value))
            }
        }
        return path
    }

    internal func canonicalPartitions() throws(DirectoryPathError) -> FieldObject {
        try validate()
        var partitions: [(key: String, value: FieldValue)] = []
        partitions.reserveCapacity(T.directoryFieldNames.count)
        var seenNames = Set<String>()

        for name in T.directoryFieldNames {
            guard seenNames.insert(name).inserted else {
                throw DirectoryPathError.invalidField(
                    typeName: T.persistableType,
                    field: name,
                    reason: "the field occurs more than once in the compiled directory"
                )
            }
            guard let binding = fieldValues.first(where: { $0.name == name }) else {
                throw DirectoryPathError.missingFields([name])
            }
            guard let schema = T.fieldSchemas.first(where: {
                $0.name == name && $0.fieldNumber == binding.field.number
            }), schema.fieldNumber > 0 else {
                throw DirectoryPathError.invalidField(
                    typeName: T.persistableType,
                    field: name,
                    reason: "the field identity does not match the compiled schema"
                )
            }
            guard !schema.isOptional, !schema.isArray, schema.type != .nested else {
                throw DirectoryPathError.invalidField(
                    typeName: T.persistableType,
                    field: name,
                    reason: "partition fields must be required scalar values"
                )
            }
            let value = binding.value
            guard value != .null else {
                throw DirectoryPathError.invalidField(
                    typeName: T.persistableType,
                    field: name,
                    reason: "partition fields cannot be null"
                )
            }
            guard FieldSchemaValueValidator.accepts(
                value,
                as: schema.type
            ) else {
                throw DirectoryPathError.invalidField(
                    typeName: T.persistableType,
                    field: name,
                    reason: "the value does not match the compiled field type"
                )
            }
            partitions.append((key: name, value: value))
        }
        do {
            return try FieldObject(partitions)
        } catch {
            switch error {
            case .duplicateKey(let key):
                throw DirectoryPathError.invalidField(
                    typeName: T.persistableType,
                    field: key,
                    reason: "the field occurs more than once in the compiled directory"
                )
            }
        }
    }

    public static func from(
        _ model: borrowing T
    ) throws(PersistableEncodingError) -> DirectoryPath<T> {
        var path = DirectoryPath<T>()
        for name in T.directoryFieldNames {
            guard let schema = T.fieldSchemas.first(where: {
                $0.name == name && $0.fieldNumber > 0
            }) else {
                throw .invalidSchema(
                    entity: T.persistableType,
                    reason: "directory field '\(name)' is missing from the compiled schema"
                )
            }
            let identity = FieldIdentity(
                name: schema.name,
                number: schema.fieldNumber
            )
            guard let value = try model.persistedFieldValue(
                for: identity
            ) else {
                throw .invalidSchema(
                    entity: T.persistableType,
                    reason: "directory field '\(name)' was not emitted by the compiled model adapter"
                )
            }
            path.fieldValues.append(
                DirectoryFieldBinding(field: identity, value: value)
            )
        }
        return path
    }

}

/// Eager, type-erased representation of a validated directory path.
public struct AnyDirectoryPath: Sendable {
    private let components: [String]
    private let partitions: FieldObject

    public init<T: Persistable>(_ path: DirectoryPath<T>) throws {
        self.components = try path.resolve()
        self.partitions = try path.canonicalPartitions()
    }

    public init(for type: any Persistable.Type) throws {
        guard !type.hasDynamicDirectory else {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: type.persistableType,
                fields: type.directoryFieldNames
            )
        }

        var resolved: [String] = []
        resolved.reserveCapacity(type.directoryPathComponents.count)
        for component in type.directoryPathComponents {
            switch component {
            case .staticPath(let value):
                resolved.append(value)
            case .dynamicField:
                throw DirectoryPathError.dynamicFieldsRequired(
                    typeName: type.persistableType,
                    fields: type.directoryFieldNames
                )
            }
        }
        self.components = resolved
        self.partitions = FieldObject()
    }

    public init(for entity: Schema.Entity) throws {
        try self.init(entity: entity, partitions: FieldObject())
    }

    package init(
        entity: Schema.Entity,
        partitions: FieldObject
    ) throws {
        let requiredNames = entity.dynamicFieldNames
        let providedNames = Set(partitions.fields.map { $0.key })
        let missingNames = Set(requiredNames).subtracting(providedNames)
        guard missingNames.isEmpty else {
            throw DirectoryPathError.missingFields(missingNames.sorted())
        }
        let unexpectedNames = providedNames.subtracting(requiredNames)
        guard unexpectedNames.isEmpty else {
            throw DirectoryPathError.invalidField(
                typeName: entity.name,
                field: unexpectedNames.sorted()[0],
                reason: "the field is not part of the compiled directory"
            )
        }

        for name in requiredNames {
            guard let schema = entity.fields.first(where: {
                $0.name == name && $0.fieldNumber > 0
            }), let value = partitions[name] else {
                throw DirectoryPathError.invalidField(
                    typeName: entity.name,
                    field: name,
                    reason: "the field is missing from the compiled schema"
                )
            }
            guard !schema.isOptional,
                  !schema.isArray,
                  schema.type != .nested,
                  value != .null,
                  FieldSchemaValueValidator.accepts(value, as: schema.type) else {
                throw DirectoryPathError.invalidField(
                    typeName: entity.name,
                    field: name,
                    reason: "partition fields must be required scalar values matching the compiled schema"
                )
            }
        }

        var resolved: [String] = []
        resolved.reserveCapacity(entity.directoryComponents.count)
        for component in entity.directoryComponents {
            switch component {
            case .staticPath(let value):
                resolved.append(value)
            case .dynamicField(let name):
                guard let value = partitions[name] else {
                    throw DirectoryPathError.missingFields([name])
                }
                resolved.append(try CanonicalDirectoryPartitionCodec.encode(value))
            }
        }
        self.components = resolved
        self.partitions = partitions
    }

    public func resolve() -> [String] {
        components
    }

    public func validate() throws {}

    public func canonicalPartitions() -> FieldObject {
        partitions
    }
}
