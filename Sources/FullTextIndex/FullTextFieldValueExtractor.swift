import DatabaseKit
import DatabaseTypes

enum FullTextFieldValueError: Error, Sendable {
    case missingField(entity: String, field: FieldIdentity)
    case unsupportedValue(entity: String, field: FieldIdentity, value: FieldValue)
}

enum FullTextFieldValueExtractor {
    static func strings<Item: PersistedEntityValue>(
        from item: Item,
        field: FieldIdentity
    ) throws -> [String] {
        guard let value = try item.persistedValue(
            forFieldNamed: field.name
        ) else {
            throw FullTextFieldValueError.missingField(
                entity: item.persistedEntityName,
                field: field
            )
        }
        return try strings(
            from: value,
            entity: item.persistedEntityName,
            field: field
        )
    }

    static func strings(
        from item: PersistedModel,
        entity: String,
        field: FieldIdentity
    ) throws -> [String] {
        guard let value = item.value(for: field) else {
            throw FullTextFieldValueError.missingField(
                entity: entity,
                field: field
            )
        }
        return try strings(from: value, entity: entity, field: field)
    }

    static func strings(
        from value: FieldValue,
        entity: String,
        field: FieldIdentity
    ) throws -> [String] {
        switch value {
        case .null:
            return []
        case .string(let string):
            return [string]
        case .array(let values):
            var strings: [String] = []
            strings.reserveCapacity(values.count)
            for value in values {
                guard case .string(let string) = value else {
                    throw FullTextFieldValueError.unsupportedValue(
                        entity: entity,
                        field: field,
                        value: value
                    )
                }
                strings.append(string)
            }
            return strings
        default:
            throw FullTextFieldValueError.unsupportedValue(
                entity: entity,
                field: field,
                value: value
            )
        }
    }
}
