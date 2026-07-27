import DatabaseKit
import DatabaseEngine
import DatabaseTypes

enum DatabaseEntityProjection {
    static func persistedFields(
        for model: any Persistable
    ) throws -> [PersistableField] {
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

    static func fieldObject(
        for model: any Persistable
    ) throws -> FieldObject {
        let entity = type(of: model).persistableType
        do {
            return try PersistableFieldEncoder.object(
                entity: entity,
                fields: persistedFields(for: model)
            )
        } catch PersistableEncodingError.invalidSchema(
            let entity,
            let reason
        ) {
            throw DatabaseMutationError.invalidCompiledSchema(
                entity: entity,
                reason: reason
            )
        } catch PersistableEncodingError.fieldNotRepresentable(
            let entity,
            let field
        ) {
            throw DatabaseMutationError.fieldNotRepresentable(
                entity: entity,
                field: field
            )
        }
    }

    static func identity(
        for model: any Persistable
    ) throws -> EntityReference {
        do {
            return try EntityReferenceEncoder.encode(model)
        } catch EntityReferenceEncodingError.invalidCompiledSchema(
            let entity,
            let reason
        ) {
            throw DatabaseMutationError.invalidCompiledSchema(
                entity: entity,
                reason: reason
            )
        } catch EntityReferenceEncodingError.identifierNotRepresentable(
            let entity
        ) {
            throw DatabaseMutationError.identifierNotRepresentable(entity)
        }
    }
}
