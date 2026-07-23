import Core
import DatabaseValue

/// Encodes a compiled model's complete storage identity into the canonical value model.
public enum PersistableIdentityEncoder {
    public static func encode(
        _ model: any Persistable
    ) throws -> PersistableIdentity {
        let modelType = type(of: model)
        let identifier = model.persistableIdentifierValue
        do {
            try PersistableIdentifierKeyCodec.validate(
                identifier,
                expectedType: modelType.persistableIdentifierType
            )
        } catch {
            throw PersistableIdentityEncodingError.identifierNotRepresentable(
                entity: modelType.persistableType
            )
        }
        var partitions: [DatabaseObjectField] = []
        partitions.reserveCapacity(modelType.directoryFieldNames.count)

        for component in modelType.directoryPathComponents {
            guard case .dynamicField(let name) = component else { continue }
            guard let schema = modelType.fieldSchemas.first(where: { $0.name == name }),
                  schema.fieldNumber > 0,
                  let number = UInt32(exactly: schema.fieldNumber) else {
                throw PersistableIdentityEncodingError.invalidCompiledSchema(
                    entity: modelType.persistableType,
                    reason: "dynamic partition field '\(name)' is missing from fieldSchemas"
                )
            }
            guard let value = model[dynamicMember: name] else {
                throw PersistableIdentityEncodingError.invalidCompiledSchema(
                    entity: modelType.persistableType,
                    reason: "dynamic partition field '\(name)' has no value"
                )
            }
            let encodedValue = try PersistableFieldEncoder.encodeValue(
                value,
                schema: schema,
                entity: modelType.persistableType
            )
            guard encodedValue != .null else {
                throw PersistableIdentityEncodingError.invalidCompiledSchema(
                    entity: modelType.persistableType,
                    reason: "dynamic partition field '\(name)' cannot be null"
                )
            }
            partitions.append(
                DatabaseObjectField(
                    number: number,
                    name: name,
                    value: encodedValue
                )
            )
        }

        return PersistableIdentity(
            entity: modelType.persistableType,
            id: identifier,
            partitions: partitions
        )
    }
}
