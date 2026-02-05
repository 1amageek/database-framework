// SchemaFileExporter.swift
// Export TypeCatalog to YAML schema files

import Foundation
import DatabaseEngine
import Core

public enum SchemaFileExporter {

    /// Export TypeCatalog to YAML string
    public static func toYAML(_ catalog: TypeCatalog) throws -> String {
        var lines: [String] = []

        // Type name
        lines.append("\(catalog.typeName):")

        // Directory
        if !catalog.directoryComponents.isEmpty {
            lines.append("  \"#Directory\":")
            for component in catalog.directoryComponents {
                switch component {
                case .staticPath(let path):
                    lines.append("    - \(path)")
                case .dynamicField(let fieldName):
                    lines.append("    - field: \(fieldName)")
                }
            }
        }

        // Fields
        for field in catalog.fields {
            let typeStr = fieldTypeToString(field)
            let indexAnnotation = findFieldIndex(fieldName: field.name, in: catalog.indexes)
            if let indexAnnotation = indexAnnotation {
                lines.append("  \(field.name): \(typeStr)#\(indexAnnotation)")
            } else {
                lines.append("  \(field.name): \(typeStr)")
            }
        }

        // Complex indexes (multi-field, graph, permuted, etc.)
        let complexIndexes = catalog.indexes.filter { index in
            // Skip indexes already annotated on fields
            if index.fieldNames.count == 1 {
                let fieldName = index.fieldNames[0]
                if catalog.fields.contains(where: { $0.name == fieldName }) {
                    // This is a field index, skip it here
                    return false
                }
            }
            return true
        }

        if !complexIndexes.isEmpty {
            lines.append("  \"#Index\":")
            for index in complexIndexes {
                lines.append(contentsOf: indexToYAML(index))
            }
        }

        return lines.joined(separator: "\n") + "\n"
    }

    // MARK: - Field Type String

    private static func fieldTypeToString(_ field: FieldSchema) -> String {
        var typeStr = scalarTypeToString(field.type)

        if field.isArray {
            typeStr = "array<\(typeStr)>"
        }

        if field.isOptional {
            typeStr = "optional<\(typeStr)>"
        }

        return typeStr
    }

    private static func scalarTypeToString(_ type: FieldSchemaType) -> String {
        switch type {
        case .string: return "string"
        case .int64: return "int64"
        case .double: return "double"
        case .float: return "float"
        case .bool: return "bool"
        case .date: return "date"
        case .uuid: return "uuid"
        case .data: return "data"
        case .int: return "int"
        case .int8: return "int8"
        case .int16: return "int16"
        case .int32: return "int32"
        case .uint: return "uint"
        case .uint8: return "uint8"
        case .uint16: return "uint16"
        case .uint32: return "uint32"
        case .uint64: return "uint64"
        case .nested: return "nested"
        case .enum: return "enum"
        }
    }

    // MARK: - Find Field Index

    private static func findFieldIndex(fieldName: String, in indexes: [IndexCatalog]) -> String? {
        guard let index = indexes.first(where: { $0.fieldNames == [fieldName] }) else {
            return nil
        }

        switch index.kindIdentifier {
        case "scalar":
            if index.unique {
                return "scalar(unique:true)"
            }
            return "scalar"

        case "vector":
            var options: [String] = []
            if let dimensions = index.metadata["dimensions"] {
                options.append("dimensions:\(dimensions)")
            }
            if let metric = index.metadata["metric"] {
                options.append("metric:\(metric)")
            }
            if let algorithm = index.metadata["algorithm"] {
                options.append("algorithm:\(algorithm)")
            }
            return "vector(\(options.joined(separator: ", ")))"

        case "fulltext":
            var options: [String] = []
            if let language = index.metadata["language"] {
                options.append("language:\(language)")
            }
            if let tokenizer = index.metadata["tokenizer"] {
                options.append("tokenizer:\(tokenizer)")
            }
            return "fulltext(\(options.joined(separator: ", ")))"

        case "spatial":
            if let strategy = index.metadata["strategy"] {
                return "spatial(strategy:\(strategy))"
            }
            return "spatial"

        case "rank":
            return "rank"

        case "bitmap":
            return "bitmap"

        case "leaderboard":
            if let name = index.metadata["leaderboardName"] {
                return "leaderboard(name:\(name))"
            }
            return "leaderboard"

        case "aggregation":
            if let functions = index.metadata["functions"] {
                return "aggregation(functions:\(functions))"
            }
            return "aggregation"

        case "version":
            return "version"

        default:
            return nil
        }
    }

    // MARK: - Index to YAML

    private static func indexToYAML(_ index: IndexCatalog) -> [String] {
        var lines: [String] = []

        switch index.kindIdentifier {
        case "scalar":
            lines.append("    - kind: scalar")
            lines.append("      name: \(index.name)")
            lines.append("      fields: [\(index.fieldNames.joined(separator: ", "))]")
            if index.unique {
                lines.append("      unique: true")
            }

        case "graph":
            lines.append("    - kind: graph")
            lines.append("      name: \(index.name)")
            if let from = index.metadata["fromField"] {
                lines.append("      from: \(from)")
            }
            if let edge = index.metadata["edgeField"] {
                lines.append("      edge: \(edge)")
            }
            if let to = index.metadata["toField"] {
                lines.append("      to: \(to)")
            }
            if let graph = index.metadata["graphField"] {
                lines.append("      graph: \(graph)")
            }
            if let strategy = index.metadata["strategy"] {
                lines.append("      strategy: \(strategy)")
            }

        case "permuted":
            lines.append("    - kind: permuted")
            lines.append("      name: \(index.name)")
            lines.append("      fields: [\(index.fieldNames.joined(separator: ", "))]")

        case "relationship":
            lines.append("    - kind: relationship")
            lines.append("      name: \(index.name)")
            if let from = index.metadata["from"] {
                lines.append("      from: \(from)")
            }
            if let to = index.metadata["to"] {
                lines.append("      to: \(to)")
            }

        case "relationship_meta":
            // This is #Relationship, not #Index
            lines.append("  \"#Relationship\":")
            if let type = index.metadata["relationshipType"] {
                lines.append("    - type: \(type)")
            }
            if let target = index.metadata["target"] {
                lines.append("      target: \(target)")
            }
            if let foreignKey = index.metadata["foreignKey"] {
                lines.append("      foreignKey: \(foreignKey)")
            }
            if let through = index.metadata["through"] {
                lines.append("      through: \(through)")
            }
            if let partition = index.metadata["partition"] {
                lines.append("      partition: \(partition)")
            }
            if let name = index.metadata["name"] {
                lines.append("      name: \(name)")
            }

        default:
            lines.append("    - kind: \(index.kindIdentifier)")
            lines.append("      name: \(index.name)")
            lines.append("      fields: [\(index.fieldNames.joined(separator: ", "))]")
        }

        return lines
    }
}
