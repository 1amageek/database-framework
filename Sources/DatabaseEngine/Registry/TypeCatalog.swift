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
    public let indexes: [IndexCatalog]

    /// Enum metadata: fieldName → case names
    public let enumMetadata: [String: [String]]

    public init(
        typeName: String,
        fields: [FieldSchema],
        directoryComponents: [DirectoryComponentCatalog],
        indexes: [IndexCatalog],
        enumMetadata: [String: [String]] = [:]
    ) {
        self.typeName = typeName
        self.fields = fields
        self.directoryComponents = directoryComponents
        self.indexes = indexes
        self.enumMetadata = enumMetadata
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

// MARK: - IndexCatalog

/// Codable catalog entry for a single index
public struct IndexCatalog: Sendable, Codable, Equatable {
    /// Index name (e.g., "User_email")
    public let name: String

    /// IndexKind identifier (e.g., "scalar", "graph", "vector")
    public let kindIdentifier: String

    /// Field names covered by the index
    public let fieldNames: [String]

    /// Whether the index enforces uniqueness
    public let unique: Bool

    /// Whether the index is sparse (skips null values)
    public let sparse: Bool

    public init(
        name: String,
        kindIdentifier: String,
        fieldNames: [String],
        unique: Bool = false,
        sparse: Bool = false
    ) {
        self.name = name
        self.kindIdentifier = kindIdentifier
        self.fieldNames = fieldNames
        self.unique = unique
        self.sparse = sparse
    }
}

// MARK: - Conversion from Schema.Entity

extension TypeCatalog {
    /// Create a TypeCatalog from a Schema.Entity
    ///
    /// Extracts all Codable metadata from the entity and its underlying Persistable type.
    /// Both static `Path` and dynamic `Field` components are preserved in `directoryComponents`.
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

        // Convert IndexDescriptors to IndexCatalogs
        self.indexes = entity.indexDescriptors.map { descriptor in
            IndexCatalog(
                name: descriptor.name,
                kindIdentifier: Swift.type(of: descriptor.kind).identifier,
                fieldNames: descriptor.kind.fieldNames,
                unique: descriptor.commonOptions.unique,
                sparse: descriptor.commonOptions.sparse
            )
        }

        // Convert EnumMetadata
        var enumMeta: [String: [String]] = [:]
        for (fieldName, meta) in entity.enumMetadata {
            enumMeta[fieldName] = meta.cases
        }
        self.enumMetadata = enumMeta
    }
}
