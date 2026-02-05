/// TypeCatalog - Codable schema metadata for a Persistable type
///
/// Analogous to PostgreSQL's `pg_class` + `pg_attribute` + `pg_index`.
/// Persisted in FDB under `/_catalog/[typeName]` as JSON.
/// Enables CLI and dynamic tools to access data without compiled types.

import Foundation
import Core

// MARK: - DirectoryComponentCatalog

/// Codable representation of a DirectoryPath component.
///
/// Captures both static path segments and dynamic field references,
/// enabling CLI tools to understand and resolve multi-tenant directory structures.
public enum DirectoryComponentCatalog: Sendable, Codable, Equatable {
    /// Static path segment (e.g., "app", "data")
    case staticPath(String)
    /// Dynamic field reference requiring runtime partition value (e.g., "tenantId")
    case dynamicField(fieldName: String)
}

// MARK: - TypeCatalog

/// Codable catalog entry for a single Persistable type
public struct TypeCatalog: Sendable, Codable, Equatable {
    /// Type name (e.g., "User", "Order")
    public let typeName: String

    /// Field metadata (name, type, field number, optionality, array)
    public let fields: [FieldSchema]

    /// Directory path components (static paths and dynamic field references)
    public let directoryComponents: [DirectoryComponentCatalog]

    /// Index definitions
    public let indexes: [AnyIndexDescriptor]

    /// Enum metadata: fieldName → case names
    public let enumMetadata: [String: [String]]

    public init(
        typeName: String,
        fields: [FieldSchema],
        directoryComponents: [DirectoryComponentCatalog],
        indexes: [AnyIndexDescriptor],
        enumMetadata: [String: [String]] = [:]
    ) {
        self.typeName = typeName
        self.fields = fields
        self.directoryComponents = directoryComponents
        self.indexes = indexes
        self.enumMetadata = enumMetadata
    }

    // MARK: - Field Lookup (for Encoder/Decoder optimization)

    /// Build field name → FieldSchema map (for Encoder)
    ///
    /// Call once and cache for batch operations to avoid O(fields) per encode.
    public var fieldMapByName: [String: FieldSchema] {
        Dictionary(uniqueKeysWithValues: fields.map { ($0.name, $0) })
    }

    /// Build field number → FieldSchema map (for Decoder)
    ///
    /// Call once and cache for batch operations to avoid O(fields) per decode.
    public var fieldMapByNumber: [Int: FieldSchema] {
        Dictionary(uniqueKeysWithValues: fields.map { ($0.fieldNumber, $0) })
    }
}

// MARK: - Directory Resolution

extension TypeCatalog {
    /// Resolve directoryComponents to a concrete [String] path.
    ///
    /// Static components pass through; dynamic field components are resolved
    /// from the provided `partitionValues` dictionary.
    ///
    /// - Parameter partitionValues: Mapping of field names to partition values
    /// - Throws: If a dynamic field has no corresponding partition value
    /// - Returns: Resolved directory path as string array
    public func resolvedDirectoryPath(partitionValues: [String: String] = [:]) throws -> [String] {
        try directoryComponents.map { component in
            switch component {
            case .staticPath(let value):
                return value
            case .dynamicField(let fieldName):
                guard let value = partitionValues[fieldName] else {
                    throw DirectoryPathError.missingFields([fieldName])
                }
                return value
            }
        }
    }

    /// Whether this type has dynamic directory components requiring partition values
    public var hasDynamicDirectory: Bool {
        directoryComponents.contains {
            if case .dynamicField = $0 { return true }
            return false
        }
    }

    /// Field names of dynamic directory components
    public var dynamicFieldNames: [String] {
        directoryComponents.compactMap {
            if case .dynamicField(let name) = $0 { return name }
            return nil
        }
    }
}

// MARK: - Conversion from Schema.Entity

extension TypeCatalog {
    /// Create a TypeCatalog from a Schema.Entity
    ///
    /// Extracts all Codable metadata from the entity and its underlying Persistable type.
    /// Both static `Path` and dynamic `Field` components are preserved in `directoryComponents`.
    ///
    /// - Parameter entity: The schema entity to convert
    public init(from entity: Schema.Entity) {
        self.typeName = entity.name

        // Get field schemas from the Persistable type
        self.fields = entity.persistableType.fieldSchemas

        // Extract directory components (both static paths and dynamic fields)
        let type = entity.persistableType
        let components = type.directoryPathComponents
        let fieldNames = type.directoryFieldNames
        var fieldNameIndex = 0
        self.directoryComponents = components.map { component -> DirectoryComponentCatalog in
            if let path = component as? Path {
                return .staticPath(path.value)
            } else if component is any DynamicDirectoryElement {
                let name = fieldNameIndex < fieldNames.count ? fieldNames[fieldNameIndex] : "unknown"
                fieldNameIndex += 1
                return .dynamicField(fieldName: name)
            } else {
                return .staticPath("_unknown")
            }
        }

        // Convert IndexDescriptors to AnyIndexDescriptor
        self.indexes = entity.indexDescriptors.map { AnyIndexDescriptor($0) }

        // Convert EnumMetadata
        var enumMeta: [String: [String]] = [:]
        for (fieldName, meta) in entity.enumMetadata {
            enumMeta[fieldName] = meta.cases
        }
        self.enumMetadata = enumMeta
    }
}
