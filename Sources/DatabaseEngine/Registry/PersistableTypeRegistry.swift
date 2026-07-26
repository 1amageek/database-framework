import DatabaseKit

/// Immutable container-scoped lookup for compiled model types.
public struct PersistableTypeRegistry: Sendable {
    private let typesByEntityName: [String: any Persistable.Type]

    init(
        types: [any Persistable.Type]
    ) throws(DatabaseRuntimeConfigurationError) {
        var registered: [String: any Persistable.Type] = [:]
        registered.reserveCapacity(types.count)
        for type in types {
            guard registered[type.persistableType] == nil else {
                throw .duplicatePersistableType(
                    entityName: type.persistableType
                )
            }
            registered[type.persistableType] = type
        }
        self.typesByEntityName = registered
    }

    public func type(
        named entityName: String
    ) -> (any Persistable.Type)? {
        typesByEntityName[entityName]
    }
}
