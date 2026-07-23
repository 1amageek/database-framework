import Core
import DatabaseEngine
import DatabaseValue

enum DatabaseEntityProjection {
    static func fields(
        for model: any Persistable
    ) throws -> [DatabaseObjectField] {
        do {
            return try PersistableFieldEncoder.encode(model)
        } catch PersistableEncodingError.invalidSchema(let entity, let reason) {
            throw DatabaseMutationError.invalidCompiledSchema(
                entity: entity,
                reason: reason
            )
        } catch PersistableEncodingError.fieldNotRepresentable(let entity, let field) {
            throw DatabaseMutationError.fieldNotRepresentable(
                entity: entity,
                field: field
            )
        }
    }

    static func identity(
        for model: any Persistable
    ) throws -> PersistableIdentity {
        do {
            return try PersistableIdentityEncoder.encode(model)
        } catch PersistableIdentityEncodingError.invalidCompiledSchema(
            let entity,
            let reason
        ) {
            throw DatabaseMutationError.invalidCompiledSchema(
                entity: entity,
                reason: reason
            )
        } catch PersistableIdentityEncodingError.identifierNotRepresentable(
            let entity
        ) {
            throw DatabaseMutationError.identifierNotRepresentable(entity)
        }
    }
}
