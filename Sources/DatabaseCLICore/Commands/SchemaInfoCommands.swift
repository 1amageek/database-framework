/// SchemaInfoCommands - Schema introspection from TypeCatalog entries

import Foundation
import DatabaseEngine

public struct SchemaInfoCommands {

    private let catalogs: [TypeCatalog]
    private let output: OutputFormatter

    public init(catalogs: [TypeCatalog], output: OutputFormatter) {
        self.catalogs = catalogs
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
        let sorted = catalogs.sorted { $0.typeName < $1.typeName }
        if sorted.isEmpty {
            output.info("(no types registered)")
            return
        }
        output.info("Registered types:")
        for catalog in sorted {
            output.line("  \(catalog.typeName)  (\(catalog.fields.count) fields, \(catalog.indexes.count) indexes)")
        }
    }

    // MARK: - Show

    private func show(name: String) throws {
        guard let catalog = catalogs.first(where: { $0.typeName == name }) else {
            throw CLIError.entityNotFound(name)
        }

        output.header(catalog.typeName)

        output.line("  Fields:")
        for field in catalog.fields {
            var info = "    \(field.name): \(field.type.rawValue)"
            if field.isOptional { info += "?" }
            if field.isArray { info = "    \(field.name): [\(field.type.rawValue)]" }
            if let cases = catalog.enumMetadata[field.name] {
                info += " (enum: \(cases.joined(separator: ", ")))"
            }
            output.line(info)
        }

        if catalog.indexes.isEmpty {
            output.line("  Indexes: (none)")
        } else {
            output.line("  Indexes:")
            for idx in catalog.indexes {
                let unique = idx.unique ? " [unique]" : ""
                let fields = idx.fieldNames.joined(separator: ", ")
                output.line("    \(idx.name) (\(idx.kindIdentifier)) [\(fields)]\(unique)")
            }
        }

        if !catalog.directoryComponents.isEmpty {
            let pathDisplay = catalog.directoryComponents.map { component -> String in
                switch component {
                case .staticPath(let value):
                    return "\"\(value)\""
                case .dynamicField(let fieldName):
                    return "<\(fieldName)>"
                }
            }.joined(separator: ", ")
            output.line("  Directory: [\(pathDisplay)]")

            if catalog.hasDynamicDirectory {
                for component in catalog.directoryComponents {
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

        Type information is loaded from the FDB catalog (_catalog).
        """
    }
}
