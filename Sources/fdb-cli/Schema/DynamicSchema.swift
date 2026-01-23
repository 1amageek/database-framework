import Foundation

/// Definition of a single field in a dynamic schema
public struct FieldDefinition: Codable, Sendable, Equatable {
    public let name: String
    public let type: FieldType
    public let optional: Bool

    public init(name: String, type: FieldType, optional: Bool) {
        self.name = name
        self.type = type
        self.optional = optional
    }

    /// Display string for CLI output
    public var displayString: String {
        let optionalMarker = optional ? "?" : ""
        return "\(name): \(type.rawValue)\(optionalMarker)"
    }
}

/// A dynamically defined schema for CLI operations
public struct DynamicSchema: Codable, Sendable, Equatable {
    public let name: String
    public let fields: [FieldDefinition]
    public let indexes: [IndexDefinition]

    public init(name: String, fields: [FieldDefinition], indexes: [IndexDefinition] = []) {
        self.name = name
        self.fields = fields
        self.indexes = indexes
    }

    /// Parse a schema definition from CLI input using the new parser
    /// Format: `admin schema define <Name> <field>:<type>[#modifier][@relationship] ... [--options]`
    public static func parse(name: String, fieldDefinitions: [String]) throws -> DynamicSchema {
        let result = try SchemaParser.parse(schemaName: name, args: fieldDefinitions)

        guard !result.fields.isEmpty else {
            throw SchemaParseError.noFields
        }

        // Ensure 'id' field exists
        guard result.fields.contains(where: { $0.name == "id" }) else {
            throw SchemaParseError.missingIdField
        }

        return DynamicSchema(name: name, fields: result.fields, indexes: result.indexes)
    }

    /// Get a field definition by name
    public func field(named name: String) -> FieldDefinition? {
        fields.first { $0.name == name }
    }

    /// Get an index definition by name
    public func index(named name: String) -> IndexDefinition? {
        indexes.first { $0.name == name }
    }

    /// Get indexes for a specific field
    public func indexes(forField fieldName: String) -> [IndexDefinition] {
        indexes.filter { $0.fields.contains(fieldName) }
    }

    /// Get indexes of a specific kind
    public func indexes(ofKind kind: IndexKind) -> [IndexDefinition] {
        indexes.filter { $0.kind == kind }
    }

    /// Check if a field has an index
    public func hasIndex(forField fieldName: String) -> Bool {
        !indexes(forField: fieldName).isEmpty
    }

    /// Check if a field has a scalar index
    public func hasScalarIndex(forField fieldName: String) -> Bool {
        indexes.contains { $0.kind == .scalar && $0.fields.contains(fieldName) }
    }

    /// Check if a field has a unique constraint
    public func isUnique(field fieldName: String) -> Bool {
        indexes.contains { $0.fields.contains(fieldName) && $0.unique }
    }

    /// Validate a record against this schema
    public func validate(_ record: [String: Any]) throws {
        for field in fields {
            let value = record[field.name]

            if value == nil || (value is NSNull) {
                if !field.optional {
                    throw SchemaValidationError.missingRequiredField(field.name)
                }
                continue
            }

            if !field.type.validate(value) {
                throw SchemaValidationError.typeMismatch(
                    field: field.name,
                    expected: field.type.rawValue,
                    got: "\(type(of: value!))"
                )
            }
        }
    }

    /// Coerce a record's values to the appropriate types
    public func coerce(_ record: [String: Any]) throws -> [String: Any] {
        var result: [String: Any] = [:]

        for field in fields {
            let value = record[field.name]

            if value == nil || (value is NSNull) {
                if !field.optional {
                    throw SchemaValidationError.missingRequiredField(field.name)
                }
                continue
            }

            if let coerced = try field.type.coerce(value) {
                result[field.name] = coerced
            }
        }

        return result
    }

    /// Display summary for CLI output
    public var summary: String {
        var parts: [String] = []
        parts.append("\(fields.count) field(s)")
        if !indexes.isEmpty {
            parts.append("\(indexes.count) index(es)")
        }
        return parts.joined(separator: ", ")
    }

    /// Detailed display for CLI output
    public var detailedDisplay: String {
        var lines: [String] = []

        lines.append("Fields:")
        for field in fields {
            lines.append("  \(field.displayString)")
        }

        if !indexes.isEmpty {
            lines.append("")
            lines.append("Indexes:")
            for idx in indexes {
                lines.append("  \(idx.displayString)")
            }
        }

        return lines.joined(separator: "\n")
    }
}

public enum SchemaParseError: Error, CustomStringConvertible {
    case invalidFieldFormat(String)
    case unknownType(String)
    case noFields
    case missingIdField

    public var description: String {
        switch self {
        case .invalidFieldFormat(let field):
            return "Invalid field format: '\(field)'. Expected 'name:type'."
        case .unknownType(let type):
            return "Unknown type: '\(type)'. Supported: string, int, double, bool, date, [string], [double]"
        case .noFields:
            return "Schema must have at least one field."
        case .missingIdField:
            return "Schema must have an 'id' field."
        }
    }
}

public enum SchemaValidationError: Error, CustomStringConvertible {
    case missingRequiredField(String)
    case typeMismatch(field: String, expected: String, got: String)

    public var description: String {
        switch self {
        case .missingRequiredField(let field):
            return "Missing required field: '\(field)'"
        case .typeMismatch(let field, let expected, let got):
            return "Type mismatch for field '\(field)': expected \(expected), got \(got)"
        }
    }
}
