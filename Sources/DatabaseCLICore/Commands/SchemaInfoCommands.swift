/// SchemaInfoCommands - Schema introspection from Schema.Entity entries

import Foundation
import DatabaseEngine
import Core

public struct SchemaInfoCommands {

    private let entities: [Schema.Entity]
    private let output: OutputFormatter

    public init(entities: [Schema.Entity], output: OutputFormatter) {
        self.entities = entities
        self.output = output
    }

    public func execute(_ args: [String]) throws {
        guard let subCommand = args.first?.lowercased() else {
            throw CLIError.invalidArguments("Usage: schema <list|show> [name]")
        }

        switch subCommand {
        case "list":
            list()
        case "show":
            guard args.count >= 2 else {
                throw CLIError.invalidArguments("Usage: schema show <TypeName>")
            }
            try show(name: args[1])
        default:
            throw CLIError.invalidArguments("Usage: schema <list|show> [name]")
        }
    }

    // MARK: - List

    private func list() {
        let sorted = entities.sorted { $0.name < $1.name }
        if sorted.isEmpty {
            output.info("(no types registered)")
            return
        }
        output.info("Registered types:")
        for entity in sorted {
            output.line("  \(entity.name)  (\(entity.fields.count) fields, \(entity.indexes.count) indexes)")
        }
    }

    // MARK: - Show

    private func show(name: String) throws {
        guard let entity = entities.first(where: { $0.name == name }) else {
            throw CLIError.entityNotFound(name)
        }

        output.header(entity.name)

        output.line("  Fields:")
        for field in entity.fields {
            var info = "    \(field.name): \(field.type.rawValue)"
            if field.isOptional { info += "?" }
            if field.isArray { info = "    \(field.name): [\(field.type.rawValue)]" }
            if let cases = entity.enumMetadata[field.name] {
                info += " (enum: \(cases.joined(separator: ", ")))"
            }
            output.line(info)
        }

        if entity.indexes.isEmpty {
            output.line("  Indexes: (none)")
        } else {
            output.line("  Indexes:")
            for idx in entity.indexes {
                let unique = idx.unique ? " [unique]" : ""
                let fields = idx.fieldNames.joined(separator: ", ")
                output.line("    \(idx.name) (\(idx.kindIdentifier)) [\(fields)]\(unique)")
            }
        }

        if !entity.directoryComponents.isEmpty {
            let pathDisplay = entity.directoryComponents.map { component -> String in
                switch component {
                case .staticPath(let value):
                    return "\"\(value)\""
                case .dynamicField(let fieldName):
                    return "<\(fieldName)>"
                }
            }.joined(separator: ", ")
            output.line("  Directory: [\(pathDisplay)]")

            if entity.hasDynamicDirectory {
                for component in entity.directoryComponents {
                    switch component {
                    case .staticPath(let value):
                        output.line("    - Static: \"\(value)\"")
                    case .dynamicField(let fieldName):
                        output.line("    - Dynamic: \(fieldName) (use --partition \(fieldName)=<value>)")
                    }
                }
            }
        }
    }
}

// MARK: - Help

extension SchemaInfoCommands {
    static var helpText: String {
        """
        Schema Commands:
          schema list                  List all registered types
          schema show <TypeName>       Show type fields, types, and indexes

        Type information is loaded from the FDB schema registry (_schema).
        """
    }
}
