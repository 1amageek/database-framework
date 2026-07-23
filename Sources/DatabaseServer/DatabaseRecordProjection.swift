import Core
import DatabaseEngine
import DatabaseValue

enum DatabaseRecordProjection {
    static func fields(
        for model: any Persistable
    ) throws -> [DatabaseObjectField] {
        do {
            return try DatabaseRecordEncoder.encode(model)
        } catch DatabaseRecordEncodingError.invalidSchema(let entity, let reason) {
            throw DatabaseMutationError.invalidCompiledSchema(
                entity: entity,
                reason: reason
            )
        } catch DatabaseRecordEncodingError.fieldNotRepresentable(let entity, let field) {
            throw DatabaseMutationError.recordFieldNotRepresentable(
                entity: entity,
                field: field
            )
        }
    }

    static func identity(
        for model: any Persistable
    ) throws -> RecordIdentity {
        do {
            return try DatabaseRecordIdentityEncoder.encode(model)
        } catch DatabaseRecordIdentityEncodingError.invalidCompiledSchema(
            let entity,
            let reason
        ) {
            throw DatabaseMutationError.invalidCompiledSchema(
                entity: entity,
                reason: reason
            )
        } catch DatabaseRecordIdentityEncodingError.identifierNotRepresentable(
            let entity
        ) {
            throw DatabaseMutationError.recordIdentifierNotRepresentable(entity)
        }
    }
}
