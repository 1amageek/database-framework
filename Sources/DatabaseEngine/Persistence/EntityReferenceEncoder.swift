import DatabaseKit
import DatabaseTypes

/// Encodes a compiled model's complete storage identity into the canonical value model.
public enum EntityReferenceEncoder {
    public static func encode(
        _ model: borrowing any Persistable
    ) throws -> EntityReference {
        let modelType = type(of: model)
        let identifier = model.persistableIdentifierValue
        do {
            try PersistableIdentifierKeyCodec.validate(
                identifier,
                expectedType: modelType.persistableIdentifierType
            )
        } catch {
            throw EntityReferenceEncodingError.identifierNotRepresentable(
                entity: modelType.persistableType
            )
        }
        var partitions: [(key: String, value: FieldValue)] = []
        partitions.reserveCapacity(modelType.directoryFieldNames.count)

        for component in modelType.directoryPathComponents {
            guard case .dynamicField(let name) = component else { continue }
            guard let schema = modelType.fieldSchemas.first(where: { $0.name == name }),
                  schema.fieldNumber > 0 else {
                throw EntityReferenceEncodingError.invalidCompiledSchema(
                    entity: modelType.persistableType,
                    reason: "dynamic partition field '\(name)' is missing from fieldSchemas"
                )
            }
            let field = FieldIdentity(
                name: schema.name,
                number: schema.fieldNumber
            )
            guard let encodedValue = try model.persistedFieldValue(
                for: field
            ) else {
                throw EntityReferenceEncodingError.invalidCompiledSchema(
                    entity: modelType.persistableType,
                    reason: "dynamic partition field '\(name)' was not emitted by the compiled model adapter"
                )
            }
            guard encodedValue != .null else {
                throw EntityReferenceEncodingError.invalidCompiledSchema(
                    entity: modelType.persistableType,
                    reason: "dynamic partition field '\(name)' cannot be null"
                )
            }
            partitions.append((key: name, value: encodedValue))
        }

        let partitionObject: FieldObject
        do {
            partitionObject = try FieldObject(partitions)
        } catch {
            throw EntityReferenceEncodingError.invalidCompiledSchema(
                entity: modelType.persistableType,
                reason: "partition object construction failed"
            )
        }

        return try EntityReference(
            entity: modelType.persistableType,
            id: identifier,
            partitions: partitionObject
        )
    }
}
