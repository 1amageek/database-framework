import DatabaseKit
import DatabaseTypes

/// Encodes a compiled model's complete storage identity into the canonical value model.
public enum EntityReferenceEncoder {
    public static func encode<Model: Persistable>(
        _ model: borrowing Model
    ) throws -> EntityReference {
        let identifier = model.persistableIdentifierValue
        do {
            try PersistableIdentifierKeyCodec.validate(
                identifier,
                expectedType: Model.persistableIdentifierType
            )
        } catch {
            throw EntityReferenceEncodingError.identifierNotRepresentable(
                entity: Model.persistableType
            )
        }
        var partitions: [(key: String, value: FieldValue)] = []
        partitions.reserveCapacity(Model.directoryFieldNames.count)

        for component in Model.directoryPathComponents {
            guard case .dynamicField(let name) = component else { continue }
            guard let schema = Model.fieldSchemas.first(where: { $0.name == name }),
                  schema.fieldNumber > 0 else {
                throw EntityReferenceEncodingError.invalidCompiledSchema(
                    entity: Model.persistableType,
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
                    entity: Model.persistableType,
                    reason: "dynamic partition field '\(name)' was not emitted by the compiled model adapter"
                )
            }
            guard encodedValue != .null else {
                throw EntityReferenceEncodingError.invalidCompiledSchema(
                    entity: Model.persistableType,
                    reason: "dynamic partition field '\(name)' cannot be null"
                )
            }
            partitions.append((key: name, value: encodedValue))
        }

        let partitionObject: FieldObject
        do {
            partitionObject = try FieldObject(partitions)
        } catch let error {
            throw EntityReferenceEncodingError.invalidCompiledSchema(
                entity: Model.persistableType,
                reason: String(describing: error)
            )
        }

        return try EntityReference(
            entity: Model.persistableType,
            id: identifier,
            partitions: partitionObject
        )
    }
}
