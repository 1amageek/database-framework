import DatabaseKit

/// Immutable lookup of statically bound entities in one runtime generation.
public struct EntityRuntimeRegistry: Sendable {
    private let registrationsByEntityName: [String: EntityRuntimeRegistration]

    init(
        registrations: [EntityRuntimeRegistration]
    ) throws(DatabaseRuntimeConfigurationError) {
        var registered: [String: EntityRuntimeRegistration] = [:]
        registered.reserveCapacity(registrations.count)
        for registration in registrations {
            let entityName = registration.entity.name
            guard registered[entityName] == nil else {
                throw .duplicatePersistableType(
                    entityName: entityName
                )
            }
            registered[entityName] = registration
        }
        self.registrationsByEntityName = registered
    }

    public func registration(
        named entityName: String
    ) -> EntityRuntimeRegistration? {
        registrationsByEntityName[entityName]
    }

}
