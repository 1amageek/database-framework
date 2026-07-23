import Core
import DatabaseValue

/// Encodes a compiled model's complete storage identity into the canonical value model.
public enum DatabaseRecordIdentityEncoder {
    public static func encode(
        _ model: any Persistable
    ) throws -> RecordIdentity {
        let modelType = type(of: model)
        let identifier = model.recordIdentifierValue
        do {
            try RecordIdentifierKeyCodec.validate(
                identifier,
                expectedType: modelType.recordIdentifierType
            )
        } catch {
            throw DatabaseRecordIdentityEncodingError.identifierNotRepresentable(
                entity: modelType.persistableType
            )
        }
        let encodedFields = try DatabaseRecordEncoder.encode(model)
        var partitions: [DatabaseObjectField] = []

        for component in modelType.directoryPathComponents {
            guard case .dynamicField(let name) = component else { continue }
            guard let schema = modelType.fieldSchemas.first(where: { $0.name == name }),
                  schema.fieldNumber > 0,
                  let number = UInt32(exactly: schema.fieldNumber) else {
                throw DatabaseRecordIdentityEncodingError.invalidCompiledSchema(
                    entity: modelType.persistableType,
                    reason: "dynamic partition field '\(name)' is missing from fieldSchemas"
                )
            }
            guard let field = encodedFields.first(where: {
                $0.number == number && $0.name == name
            }) else {
                throw DatabaseRecordIdentityEncodingError.invalidCompiledSchema(
                    entity: modelType.persistableType,
                    reason: "dynamic partition field '\(name)' was not encoded"
                )
            }
            partitions.append(field)
        }

        return RecordIdentity(
            entity: modelType.persistableType,
            id: identifier,
            partitions: partitions
        )
    }
}
