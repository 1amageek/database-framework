import DatabaseKit
import DatabaseTypes
import StorageKit

/// Strict dynamic decoder for catalog-driven tools without compiled model types.
public enum DatabaseCatalogFieldDecoder {
    public static func decode(
        _ bytes: Bytes,
        entity: Schema.Entity
    ) throws -> [PersistableField] {
        let fields = try PersistableStorageCodec.decodeFields(
            from: bytes,
            expectedEntity: entity.name
        ).fields
        _ = try PersistedFieldCollectionInput(
            entity: entity.name,
            fields: fields,
            schemas: entity.fields
        )

        var schemasByNumber: [UInt32: FieldSchema] = [:]
        for schema in entity.fields {
            guard schema.fieldNumber > 0,
                  let number = UInt32(exactly: schema.fieldNumber) else {
                throw PersistableDecodingError.invalidNestedFieldNumber(
                    UInt32(truncatingIfNeeded: schema.fieldNumber)
                )
            }
            schemasByNumber[number] = schema
        }

        var presentFields = Set<String>()
        for field in fields {
            guard let schema = schemasByNumber[field.number] else {
                throw PersistableDecodingError.unknownField(
                    number: field.number,
                    name: field.name
                )
            }
            try validate(field.value, schema: schema)
            presentFields.insert(schema.name)
        }
        for schema in entity.fields
        where !schema.isOptional && !presentFields.contains(schema.name) {
            throw PersistableDecodingError.missingRequiredField(schema.name)
        }
        return fields
    }

    private static func validate(
        _ value: FieldValue,
        schema: FieldSchema
    ) throws {
        if case .null = value {
            guard schema.isOptional else {
                throw invalidValue(schema, expected: "a non-null value")
            }
            return
        }

        if schema.isArray {
            guard case .array(let elements) = value else {
                throw invalidValue(schema, expected: "an array")
            }
            for element in elements {
                guard case .null = element else {
                    try validateScalar(element, schema: schema)
                    continue
                }
                throw invalidValue(schema, expected: "non-null array elements")
            }
            return
        }

        guard case .array = value else {
            try validateScalar(value, schema: schema)
            return
        }
        throw invalidValue(schema, expected: "a scalar")
    }

    private static func validateScalar(
        _ value: FieldValue,
        schema: FieldSchema
    ) throws {
        let valid: Bool
        switch (schema.type, value) {
        case (.bool, .bool),
             (.int8, .int8),
             (.int16, .int16),
             (.int32, .int32),
             (.int64, .int64),
             (.uint8, .uint8),
             (.uint16, .uint16),
             (.uint32, .uint32),
             (.uint64, .uint64),
             (.float32, .float32),
             (.float64, .float64),
             (.decimal, .decimal),
             (.string, .string),
             (.bytes, .bytes),
             (.date, .date),
             (.time, .time),
             (.dateTime, .dateTime),
             (.timestamp, .timestamp),
             (.timeSpan, .timeSpan),
             (.calendarPeriod, .calendarPeriod),
             (.geographicPoint, .geographicPoint),
             (.geographicPosition, .geographicPosition),
             (.vector, .vector),
             (.uuid, .uuid),
             (.rdfTerm, .rdfTerm):
            valid = true
        case (.enum, .string),
             (.enum, .int8),
             (.enum, .int16),
             (.enum, .int32),
             (.enum, .int64),
             (.enum, .uint8),
             (.enum, .uint16),
             (.enum, .uint32),
             (.enum, .uint64):
            valid = true
        case (.object, .object),
             (.nested, .object):
            valid = true
        case (.reference, .reference(let identity)):
            valid = schema.referenceTargetEntity == nil
                || schema.referenceTargetEntity == identity.entity
        default:
            valid = false
        }
        guard valid else {
            throw invalidValue(
                schema,
                expected: "the canonical \(schema.type.rawValue) representation"
            )
        }
    }

    private static func invalidValue(
        _ schema: FieldSchema,
        expected: String
    ) -> PersistableDecodingError {
        .invalidValue(field: schema.name, expected: expected)
    }
}
