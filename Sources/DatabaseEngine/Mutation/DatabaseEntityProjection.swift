import DatabaseKit
import DatabaseTypes

@_spi(DatabaseExecution)
public enum DatabaseEntityProjection {
    public static func persistedFields(
        for model: borrowing PersistedModel
    ) -> [PersistableField] {
        model.fields
    }

    public static func fieldObject(
        for model: borrowing PersistedModel
    ) throws -> FieldObject {
        let entity = model.entity
        let fields = persistedFields(for: model)
        do {
            return try PersistableFieldEncoder.object(
                entity: entity,
                fields: fields
            )
        } catch let error {
            throw mutationError(for: error, fallbackEntity: entity)
        }
    }

    private static func mutationError(
        for error: borrowing PersistableEncodingError,
        fallbackEntity: String
    ) -> DatabaseEntityMutationError {
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
