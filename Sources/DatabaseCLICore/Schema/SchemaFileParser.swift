// SchemaFileParser.swift
// Parse YAML schema files to Schema.Entity

import Foundation
import DatabaseEngine
import Core
import Yams

public enum SchemaFileParser {

    // MARK: - Public API

    /// Parse YAML schema file to Schema.Entity
    public static func parseYAML(from fileURL: URL) throws -> Schema.Entity {
        let yamlString = try String(contentsOf: fileURL, encoding: .utf8)
        return try parseYAML(yamlString)
    }

    /// Parse YAML schema string to Schema.Entity
    public static func parseYAML(_ yamlString: String) throws -> Schema.Entity {
        // Use compose to preserve key order
        guard let node = try Yams.compose(yaml: yamlString) else {
            throw SchemaFileError.invalidFormat("YAML must be a valid document")
        }

        guard case .mapping(let rootMapping) = node else {
            throw SchemaFileError.invalidFormat("YAML must be a dictionary")
        }

        guard let (typeNameNode, typeDefNode) = rootMapping.first else {
            throw SchemaFileError.invalidFormat("YAML must contain a type name")
        }

        guard case .scalar(let typeNameScalar) = typeNameNode else {
            throw SchemaFileError.invalidFormat("Type name must be a string")
        }
        let typeName = typeNameScalar.string

        guard case .mapping(let typeDefMapping) = typeDefNode else {
            throw SchemaFileError.invalidFormat("Type definition must be a dictionary")
        }

        return try parseTypeDefinition(typeName: typeName, mapping: typeDefMapping)
    }

    // MARK: - Type Definition Parsing

    private static func parseTypeDefinition(typeName: String, mapping: Node.Mapping) throws -> Schema.Entity {
        var fields: [FieldSchema] = []
        var indexes: [AnyIndexDescriptor] = []
        var directoryComponents: [DirectoryComponentCatalog] = []
        var fieldNumber = 1

        // Iterate in order (Node.Mapping preserves order)
        for (keyNode, valueNode) in mapping {
            guard case .scalar(let keyScalar) = keyNode else {
                continue
            }
            let key = keyScalar.string

            switch key {
            case "#Directory":
                directoryComponents = try parseDirectoryNode(valueNode)

            case "#Index":
                if case .sequence(let indexNodes) = valueNode {
                    for indexNode in indexNodes {
                        if case .mapping(let indexMapping) = indexNode {
                            indexes.append(try parseIndexNode(indexMapping))
                        }
                    }
                }

            case "#Relationship":
                if case .sequence(let relNodes) = valueNode {
                    for relNode in relNodes {
                        if case .mapping(let relMapping) = relNode {
                            indexes.append(try parseRelationshipNode(relMapping))
                        }
                    }
                }

            case let fieldName where !key.hasPrefix("#") && !key.hasPrefix("_"):
                guard case .scalar(let valueScalar) = valueNode else {
                    throw SchemaFileError.invalidField(fieldName, "Field definition must be a string")
                }
                let typeString = valueScalar.string
                let (field, fieldIndexes) = try parseField(name: fieldName, fieldNumber: fieldNumber, definition: typeString)
                fields.append(field)
                indexes.append(contentsOf: fieldIndexes)
                fieldNumber += 1

            default:
                break
            }
        }

        return Schema.Entity(
            name: typeName,
            fields: fields,
            directoryComponents: directoryComponents,
            indexes: indexes
        )
    }

    // MARK: - Directory Parsing

    private static func parseDirectoryNode(_ node: Node) throws -> [DirectoryComponentCatalog] {
        // Handle array of strings: [app, users]
        if case .sequence(let nodes) = node {
            var components: [DirectoryComponentCatalog] = []
            for itemNode in nodes {
                if case .scalar(let scalar) = itemNode {
                    components.append(.staticPath(scalar.string))
                } else if case .mapping(let mapping) = itemNode {
                    // Handle {field: tenantId} or {staticPath: app}
                    for (keyNode, valueNode) in mapping {
                        guard case .scalar(let keyScalar) = keyNode,
                              case .scalar(let valueScalar) = valueNode else {
                            continue
                        }
                        let key = keyScalar.string
                        let value = valueScalar.string
                        if key == "field" {
                            components.append(.dynamicField(fieldName: value))
                        } else {
                            components.append(.staticPath(value))
                        }
                    }
                }
            }
            return components
        }

        throw SchemaFileError.invalidDirectory("Directory must be array of strings or component definitions")
    }

    private static func parseDirectory(_ value: Any) throws -> [DirectoryComponentCatalog] {
        if let staticPaths = value as? [String] {
            return staticPaths.map { .staticPath($0) }
        }

        if let components = value as? [[String: Any]] {
            return try components.map { component in
                if let staticPath = component["staticPath"] as? String {
                    return .staticPath(staticPath)
                } else if let field = component["field"] as? String {
                    return .dynamicField(fieldName: field)
                } else if let path = component.values.first as? String {
                    // Handle single-value dictionary like ["field": "tenantId"]
                    if component.keys.first == "field" {
                        return .dynamicField(fieldName: path)
                    }
                    return .staticPath(path)
                } else {
                    throw SchemaFileError.invalidDirectory("Component must have 'staticPath' or 'field'")
                }
            }
        }

        throw SchemaFileError.invalidDirectory("Directory must be array of strings or component definitions")
    }

    // MARK: - Field Parsing

    private static func parseField(name: String, fieldNumber: Int, definition: Any) throws -> (FieldSchema, [AnyIndexDescriptor]) {
        guard let typeString = definition as? String else {
            throw SchemaFileError.invalidField(name, "Field definition must be a string")
        }

        // Parse "type#indexKind(options)" format
        let parts = typeString.components(separatedBy: "#")
        let typeStr = parts[0]
        let indexSpec = parts.count > 1 ? parts[1] : nil

        let (scalarType, isOptional, isArray) = try parseFieldType(typeStr)
        let field = FieldSchema(
            name: name,
            fieldNumber: fieldNumber,
            type: scalarType,
            isOptional: isOptional,
            isArray: isArray
        )

        var indexes: [AnyIndexDescriptor] = []
        if let indexSpec = indexSpec {
            let index = try parseFieldIndex(fieldName: name, indexSpec: indexSpec)
            indexes.append(index)
        }

        return (field, indexes)
    }

    private static func parseFieldType(_ typeString: String) throws -> (FieldSchemaType, Bool, Bool) {
        let trimmed = typeString.trimmingCharacters(in: .whitespaces)
        var isOptional = false
        var isArray = false
        var coreType = trimmed

        // Handle optional<T>
        if coreType.hasPrefix("optional<") && coreType.hasSuffix(">") {
            isOptional = true
            coreType = String(coreType.dropFirst(9).dropLast())
        }

        // Handle array<T>
        if coreType.hasPrefix("array<") && coreType.hasSuffix(">") {
            isArray = true
            coreType = String(coreType.dropFirst(6).dropLast())
        }

        // Primitive types
        let scalarType: FieldSchemaType
        switch coreType.lowercased() {
        case "string": scalarType = .string
        case "int", "int64": scalarType = .int64
        case "double": scalarType = .double
        case "float": scalarType = .float
        case "bool": scalarType = .bool
        case "date": scalarType = .date
        case "uuid": scalarType = .uuid
        case "data": scalarType = .data
        default:
            throw SchemaFileError.invalidType(coreType)
        }

        return (scalarType, isOptional, isArray)
    }

    // MARK: - Field Index Parsing

    private static func parseFieldIndex(fieldName: String, indexSpec: String) throws -> AnyIndexDescriptor {
        // Parse "indexKind(option:value, option:value)" or "indexKind(option:value1,value2,value3)"
        let parts = indexSpec.components(separatedBy: "(")
        let kind = parts[0].trimmingCharacters(in: .whitespaces)

        var options: [String: String] = [:]
        if parts.count > 1 {
            let optionsStr = parts[1].replacingOccurrences(of: ")", with: "")

            // Split by comma, but keep values that don't have a colon together
            var currentKey: String? = nil
            var currentValue: String = ""

            let components = optionsStr.components(separatedBy: ",")
            for component in components {
                let trimmed = component.trimmingCharacters(in: .whitespaces)
                if trimmed.contains(":") {
                    // Save previous key-value if exists
                    if let key = currentKey, !currentValue.isEmpty {
                        options[key] = currentValue
                    }

                    // Start new key-value
                    let kv = trimmed.components(separatedBy: ":")
                    if kv.count == 2 {
                        currentKey = kv[0].trimmingCharacters(in: .whitespaces)
                        currentValue = kv[1].trimmingCharacters(in: .whitespaces)
                    }
                } else {
                    // This is a continuation of the current value (e.g., "avg" in "functions:sum,avg,min,max")
                    if !currentValue.isEmpty {
                        currentValue += ","
                    }
                    currentValue += trimmed
                }
            }

            // Save last key-value
            if let key = currentKey, !currentValue.isEmpty {
                options[key] = currentValue
            }
        }

        return try buildFieldIndexDescriptor(kind: kind, fieldName: fieldName, options: options)
    }

    private static func buildFieldIndexDescriptor(kind: String, fieldName: String, options: [String: String]) throws -> AnyIndexDescriptor {
        let indexName = "\(fieldName)_\(kind)_idx"

        switch kind.lowercased() {
        case "scalar":
            let unique = options["unique"] == "true"
            return makeIndexDescriptor(
                name: indexName,
                kindIdentifier: "scalar",
                fieldNames: [fieldName],
                unique: unique,
                kindMetadata: [:]
            )

        case "vector":
            guard let dimensionsStr = options["dimensions"],
                  let dimensions = Int(dimensionsStr) else {
                throw SchemaFileError.invalidIndex("Vector index requires 'dimensions' parameter")
            }

            let metric = options["metric"] ?? "cosine"
            let algorithm = options["algorithm"] ?? "hnsw"

            return makeIndexDescriptor(
                name: indexName,
                kindIdentifier: "vector",
                fieldNames: [fieldName],
                unique: false,
                kindMetadata: [
                    "dimensions": .int(dimensions),
                    "metric": .string(metric),
                    "algorithm": .string(algorithm)
                ]
            )

        case "fulltext":
            let language = options["language"] ?? "english"
            let tokenizer = options["tokenizer"] ?? "standard"

            return makeIndexDescriptor(
                name: indexName,
                kindIdentifier: "fulltext",
                fieldNames: [fieldName],
                unique: false,
                kindMetadata: [
                    "language": .string(language),
                    "tokenizer": .string(tokenizer)
                ]
            )

        case "spatial":
            let strategy = options["strategy"] ?? "geohash"

            return makeIndexDescriptor(
                name: indexName,
                kindIdentifier: "spatial",
                fieldNames: [fieldName],
                unique: false,
                kindMetadata: ["strategy": .string(strategy)]
            )

        case "rank":
            return makeIndexDescriptor(
                name: indexName,
                kindIdentifier: "rank",
                fieldNames: [fieldName],
                unique: false,
                kindMetadata: [:]
            )

        case "bitmap":
            return makeIndexDescriptor(
                name: indexName,
                kindIdentifier: "bitmap",
                fieldNames: [fieldName],
                unique: false,
                kindMetadata: [:]
            )

        case "leaderboard":
            guard let leaderboardName = options["name"] else {
                throw SchemaFileError.invalidIndex("Leaderboard index requires 'name' parameter")
            }

            return makeIndexDescriptor(
                name: indexName,
                kindIdentifier: "leaderboard",
                fieldNames: [fieldName],
                unique: false,
                kindMetadata: ["leaderboardName": .string(leaderboardName)]
            )

        case "aggregation":
            guard let functionsStr = options["functions"] else {
                throw SchemaFileError.invalidIndex("Aggregation index requires 'functions' parameter")
            }

            return makeIndexDescriptor(
                name: indexName,
                kindIdentifier: "aggregation",
                fieldNames: [fieldName],
                unique: false,
                kindMetadata: ["functions": .string(functionsStr)]
            )

        case "version":
            return makeIndexDescriptor(
                name: indexName,
                kindIdentifier: "version",
                fieldNames: [fieldName],
                unique: false,
                kindMetadata: [:]
            )

        default:
            throw SchemaFileError.unsupportedIndexKind(kind)
        }
    }

    // MARK: - Complex Index Parsing

    private static func parseIndex(_ definition: [String: Any]) throws -> AnyIndexDescriptor {
        guard let kind = definition["kind"] as? String else {
            throw SchemaFileError.invalidIndex("Index must have 'kind' field")
        }

        switch kind.lowercased() {
        case "scalar":
            return try parseScalarIndex(definition)
        case "graph":
            return try parseGraphIndex(definition)
        case "permuted":
            return try parsePermutedIndex(definition)
        case "relationship":
            return try parseRelationshipIndex(definition)
        default:
            throw SchemaFileError.unsupportedIndexKind(kind)
        }
    }

    private static func parseScalarIndex(_ definition: [String: Any]) throws -> AnyIndexDescriptor {
        guard let fields = definition["fields"] as? [String] else {
            throw SchemaFileError.invalidIndex("Scalar index requires 'fields' array")
        }

        let unique = definition["unique"] as? Bool ?? false
        let name = definition["name"] as? String ?? "\(fields.joined(separator: "_"))_idx"

        return makeIndexDescriptor(
            name: name,
            kindIdentifier: "scalar",
            fieldNames: fields,
            unique: unique,
            kindMetadata: [:]
        )
    }

    private static func parseGraphIndex(_ definition: [String: Any]) throws -> AnyIndexDescriptor {
        guard let from = definition["from"] as? String,
              let edge = definition["edge"] as? String,
              let to = definition["to"] as? String else {
            throw SchemaFileError.invalidIndex("Graph index requires 'from', 'edge', 'to' fields")
        }

        let graph = definition["graph"] as? String
        let strategy = definition["strategy"] as? String ?? "tripleStore"
        let _ = definition["storedFields"] as? [String] ?? []  // Reserved for future use
        let name = definition["name"] as? String ?? "graph_idx"

        var kindMetadata: [String: IndexMetadataValue] = [
            "fromField": .string(from),
            "edgeField": .string(edge),
            "toField": .string(to),
            "strategy": .string(strategy)
        ]

        if let graph = graph {
            kindMetadata["graphField"] = .string(graph)
        }

        return makeIndexDescriptor(
            name: name,
            kindIdentifier: "graph",
            fieldNames: [from, edge, to],
            unique: false,
            kindMetadata: kindMetadata
        )
    }

    private static func parsePermutedIndex(_ definition: [String: Any]) throws -> AnyIndexDescriptor {
        guard let fields = definition["fields"] as? [String] else {
            throw SchemaFileError.invalidIndex("Permuted index requires 'fields' array")
        }

        let name = definition["name"] as? String ?? "\(fields.joined(separator: "_"))_permuted_idx"

        return makeIndexDescriptor(
            name: name,
            kindIdentifier: "permuted",
            fieldNames: fields,
            unique: false,
            kindMetadata: [:]
        )
    }

    private static func parseRelationshipIndex(_ definition: [String: Any]) throws -> AnyIndexDescriptor {
        guard let from = definition["from"] as? String,
              let to = definition["to"] as? String else {
            throw SchemaFileError.invalidIndex("Relationship index requires 'from' and 'to' fields")
        }

        let name = definition["name"] as? String ?? "\(from)_\(to)_rel_idx"

        return makeIndexDescriptor(
            name: name,
            kindIdentifier: "relationship",
            fieldNames: [from, to],
            unique: false,
            kindMetadata: ["from": .string(from), "to": .string(to)]
        )
    }

    // MARK: - Relationship as Index

    private static func parseRelationshipAsIndex(_ definition: [String: Any]) throws -> AnyIndexDescriptor {
        guard let type = definition["type"] as? String else {
            throw SchemaFileError.invalidIndex("Relationship must have 'type' field")
        }

        let name = definition["name"] as? String ?? "relationship_idx"
        var kindMetadata: [String: IndexMetadataValue] = ["relationshipType": .string(type)]

        if let target = definition["target"] as? String {
            kindMetadata["target"] = .string(target)
        }
        if let foreignKey = definition["foreignKey"] as? String {
            kindMetadata["foreignKey"] = .string(foreignKey)
        }
        if let through = definition["through"] as? String {
            kindMetadata["through"] = .string(through)
        }
        if let partition = definition["partition"] as? String {
            kindMetadata["partition"] = .string(partition)
        }

        return makeIndexDescriptor(
            name: name,
            kindIdentifier: "relationship_meta",
            fieldNames: [],
            unique: false,
            kindMetadata: kindMetadata
        )
    }

    // MARK: - Node-based Index Parsing

    private static func parseIndexNode(_ mapping: Node.Mapping) throws -> AnyIndexDescriptor {
        var dict: [String: Any] = [:]
        for (keyNode, valueNode) in mapping {
            guard case .scalar(let keyScalar) = keyNode else {
                continue
            }
            let key = keyScalar.string

            if case .scalar(let valueScalar) = valueNode {
                dict[key] = valueScalar.string
            } else if case .sequence(let nodes) = valueNode {
                var array: [String] = []
                for node in nodes {
                    if case .scalar(let scalar) = node {
                        array.append(scalar.string)
                    }
                }
                dict[key] = array
            }
        }

        return try parseIndex(dict)
    }

    private static func parseRelationshipNode(_ mapping: Node.Mapping) throws -> AnyIndexDescriptor {
        var dict: [String: String] = [:]
        for (keyNode, valueNode) in mapping {
            guard case .scalar(let keyScalar) = keyNode,
                  case .scalar(let valueScalar) = valueNode else {
                continue
            }
            dict[keyScalar.string] = valueScalar.string
        }

        return try parseRelationshipAsIndex(dict)
    }

    // MARK: - Helper

    /// Create an AnyIndexDescriptor from parsed components
    private static func makeIndexDescriptor(
        name: String,
        kindIdentifier: String,
        fieldNames: [String],
        unique: Bool,
        sparse: Bool = false,
        kindMetadata: [String: IndexMetadataValue],
        storedFieldNames: [String] = []
    ) -> AnyIndexDescriptor {
        let kind = AnyIndexKind(
            identifier: kindIdentifier,
            subspaceStructure: subspaceStructure(for: kindIdentifier),
            fieldNames: fieldNames,
            metadata: kindMetadata
        )

        var commonMetadata: [String: IndexMetadataValue] = [
            "unique": .bool(unique),
            "sparse": .bool(sparse)
        ]

        if !storedFieldNames.isEmpty {
            commonMetadata["storedFieldNames"] = .stringArray(storedFieldNames)
        }

        return AnyIndexDescriptor(
            name: name,
            kind: kind,
            commonMetadata: commonMetadata
        )
    }

    /// Determine subspace structure for known index kinds
    private static func subspaceStructure(for kindIdentifier: String) -> SubspaceStructure {
        switch kindIdentifier {
        case "scalar", "permuted", "bitmap":
            return .flat
        case "vector", "graph", "fulltext", "spatial", "rank", "leaderboard", "aggregation", "version", "relationship", "relationship_meta":
            return .hierarchical
        default:
            return .flat
        }
    }
}

// MARK: - Errors

public enum SchemaFileError: Error, CustomStringConvertible {
    case invalidFormat(String)
    case invalidDirectory(String)
    case invalidField(String, String)
    case invalidType(String)
    case invalidIndex(String)
    case unsupportedIndexKind(String)

    public var description: String {
        switch self {
        case .invalidFormat(let msg):
            return "Invalid schema format: \(msg)"
        case .invalidDirectory(let msg):
            return "Invalid directory definition: \(msg)"
        case .invalidField(let name, let msg):
            return "Invalid field '\(name)': \(msg)"
        case .invalidType(let type):
            return "Invalid type: '\(type)'"
        case .invalidIndex(let msg):
            return "Invalid index definition: \(msg)"
        case .unsupportedIndexKind(let kind):
            return "Unsupported index kind: '\(kind)'"
        }
    }
}
