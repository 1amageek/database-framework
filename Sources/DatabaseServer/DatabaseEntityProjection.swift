import DatabaseKit
import DatabaseEngine
import DatabaseTypes

enum DatabaseEntityProjection {
    static func persistedFields<Model: Persistable>(
        for model: borrowing Model
    ) throws -> [PersistableField] {
        do {
            return try PersistableFieldEncoder.encode(model)
        } catch let error {
            throw mutationError(
                for: error,
                fallbackEntity: Model.persistableType
            )
        }
    }

    static func fieldObject<Model: Persistable>(
        for model: borrowing Model
    ) throws -> FieldObject {
        let entity = Model.persistableType
        let fields = try persistedFields(for: model)
        do {
            return try PersistableFieldEncoder.object(
                entity: entity,
                fields: fields
            )
        } catch let error {
            throw mutationError(for: error, fallbackEntity: entity)
        }
    }

    static func identity<Model: Persistable>(
        for model: borrowing Model
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

    private static func mutationError(
        for error: consuming PersistableEncodingError,
        fallbackEntity: String
    ) -> DatabaseMutationError {
        switch error {
        case .missingCompiledEncoder(let entity):
            return .invalidCompiledSchema(
                entity: entity,
                reason: "the compiled field encoder is missing"
            )
        case .invalidSchema(let entity, let reason):
            return .invalidCompiledSchema(entity: entity, reason: reason)
        case .fieldNotRepresentable(let entity, let field):
            return .fieldNotRepresentable(entity: entity, field: field)
        case .invalidScalar(let type, let reason):
            return .fieldValueNotRepresentable(
                entity: fallbackEntity,
                type: type,
                reason: reason
            )
        }
    }
}
