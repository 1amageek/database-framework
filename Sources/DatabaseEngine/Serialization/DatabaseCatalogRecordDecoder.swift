import Core
import DatabaseValue
import StorageKit

/// Strict dynamic decoder for catalog-driven tools without compiled model types.
public enum DatabaseCatalogRecordDecoder {
    public static func decode(
        _ bytes: Bytes,
        entity: Schema.Entity
    ) throws -> [DatabaseRecordField] {
        let fields = try DatabaseRecordStorageCodec.decodeFields(
            from: bytes,
            expectedEntity: entity.name
        ).fields
        _ = try DatabaseRecordFieldDecoder(
            entity: entity.name,
            fields: fields,
            schemas: entity.fields
        )

        var schemasByNumber: [UInt32: FieldSchema] = [:]
        for schema in entity.fields {
            guard schema.fieldNumber > 0,
                  let number = UInt32(exactly: schema.fieldNumber) else {
                throw DatabaseRecordDecodingError.invalidNestedFieldNumber(
                    UInt32(truncatingIfNeeded: schema.fieldNumber)
                )
            }
            schemasByNumber[number] = schema
        }

        var presentFields = Set<String>()
        for field in fields {
            guard let schema = schemasByNumber[field.number] else {
                throw DatabaseRecordDecodingError.unknownField(
                    number: field.number,
                    name: field.name
                )
            }
            try validate(field.value, schema: schema)
            presentFields.insert(schema.name)
        }
        for schema in entity.fields
        where !schema.isOptional && !presentFields.contains(schema.name) {
            throw DatabaseRecordDecodingError.missingRequiredField(schema.name)
        }
        return fields
    }

    private static func validate(
        _ value: DatabaseValue,
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
        _ value: DatabaseValue,
        schema: FieldSchema
    ) throws {
        let valid: Bool
        switch (schema.type, value) {
        case (.bool, .bool),
             (.int, .int64),
             (.int8, .int64),
             (.int16, .int64),
             (.int32, .int64),
             (.int64, .int64),
             (.uint, .uint64),
             (.uint8, .uint64),
             (.uint16, .uint64),
             (.uint32, .uint64),
             (.uint64, .uint64),
             (.double, .double),
             (.float, .double),
             (.date, .timestamp),
             (.string, .string),
             (.uuid, .uuid),
             (.data, .bytes),
             (.rdfTerm, .rdfTerm):
            valid = true
        case (.enum, .string),
             (.enum, .int64),
             (.enum, .uint64):
            valid = true
        case (.nested, .object(let fields)):
            try validateNested(fields)
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

    private static func validateNested(
        _ fields: [DatabaseObjectField]
    ) throws {
        var numbers = Set<UInt32>()
        var names = Set<String>()
        for field in fields {
            guard field.number > 0 else {
                throw DatabaseRecordDecodingError.invalidNestedFieldNumber(
                    field.number
                )
            }
            guard numbers.insert(field.number).inserted else {
                throw DatabaseRecordDecodingError.duplicateFieldNumber(
                    field.number
                )
            }
            guard names.insert(field.name).inserted else {
                throw DatabaseRecordDecodingError.duplicateFieldName(field.name)
            }
        }
    }

    private static func invalidValue(
        _ schema: FieldSchema,
        expected: String
    ) -> DatabaseRecordDecodingError {
        .invalidValue(field: schema.name, expected: expected)
    }
}
