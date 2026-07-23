import Core
import DatabaseValue

/// Holds the typed field values required to resolve a compiled directory path.
public struct DirectoryPath<T: Persistable>: Sendable {
    internal var fieldValues: [DirectoryFieldBinding] = []

    public init() {}

    public mutating func set<V: Sendable>(_ keyPath: KeyPath<T, V>, to value: V) {
        let name = T.fieldName(for: keyPath)
        fieldValues.removeAll { $0.name == name }
        fieldValues.append(DirectoryFieldBinding(name: name, value: value))
    }

    public func hasValue(for keyPath: PartialKeyPath<T>) -> Bool {
        let name = T.fieldName(for: keyPath)
        return fieldValues.contains { $0.name == name }
    }

    public func value<V>(for keyPath: KeyPath<T, V>) -> V? {
        let name = T.fieldName(for: keyPath)
        return fieldValues.first { $0.name == name }?.value as? V
    }

    public func validate() throws {
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

        let providedNames = Set(fieldValues.map(\.name))
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
        let partitionsByName = Dictionary(
            uniqueKeysWithValues: partitions.map { ($0.name, $0.value) }
        )
        var path: [String] = []
        path.reserveCapacity(T.directoryPathComponents.count)

        for component in T.directoryPathComponents {
            switch component {
            case .staticPath(let value):
                path.append(value)
            case .dynamicField(let name):
                guard let value = partitionsByName[name] else {
                    throw DirectoryPathError.missingFields([name])
                }
                path.append(try CanonicalDirectoryPartitionCodec.encode(value))
            }
        }
        return path
    }

    internal func canonicalPartitions() throws -> [DatabaseObjectField] {
        try validate()
        var partitions: [DatabaseObjectField] = []
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
            guard let schema = T.fieldSchemas.first(where: { $0.name == name }),
                  schema.fieldNumber > 0,
                  let number = UInt32(exactly: schema.fieldNumber) else {
                throw DirectoryPathError.invalidField(
                    typeName: T.persistableType,
                    field: name,
                    reason: "the field is missing from the compiled schema"
                )
            }
            guard !schema.isOptional, !schema.isArray, schema.type != .nested else {
                throw DirectoryPathError.invalidField(
                    typeName: T.persistableType,
                    field: name,
                    reason: "partition fields must be required scalar values"
                )
            }
            let value = try DatabaseRecordEncoder.encodeValue(
                binding.value,
                schema: schema,
                entity: T.persistableType
            )
            guard value != .null else {
                throw DirectoryPathError.invalidField(
                    typeName: T.persistableType,
                    field: name,
                    reason: "partition fields cannot be null"
                )
            }
            partitions.append(
                DatabaseObjectField(number: number, name: name, value: value)
            )
        }
        return partitions
    }

    public static func from(_ model: T) -> DirectoryPath<T> {
        var path = DirectoryPath<T>()
        for name in T.directoryFieldNames {
            if let value = model[dynamicMember: name] {
                path.fieldValues.append(DirectoryFieldBinding(name: name, value: value))
            }
        }
        return path
    }
}

/// Eager, type-erased representation of a validated directory path.
public struct AnyDirectoryPath: Sendable {
    private let components: [String]
    private let partitions: [DatabaseObjectField]

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
        self.partitions = []
    }

    package init(
        fieldValues: [(name: String, value: any Sendable)],
        type: any Persistable.Type
    ) throws {
        func bind<T: Persistable>(_ concreteType: T.Type) throws -> AnyDirectoryPath {
            var path = DirectoryPath<T>()
            path.fieldValues = fieldValues.map {
                DirectoryFieldBinding(name: $0.name, value: $0.value)
            }
            return try AnyDirectoryPath(path)
        }
        self = try _openExistential(type, do: bind)
    }

    public func resolve() -> [String] {
        components
    }

    public func validate() throws {}

    public func canonicalPartitions() -> [DatabaseObjectField] {
        partitions
    }
}
