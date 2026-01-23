import Foundation

/// Handler for schema-related commands
public struct SchemaCommands {
    private let storage: SchemaStorage
    private let output: OutputFormatter

    public init(storage: SchemaStorage, output: OutputFormatter) {
        self.storage = storage
        self.output = output
    }

    /// Execute a schema command
    public func execute(_ command: String, args: [String]) async throws {
        switch command {
        case "define":
            try await define(args: args)
        case "list":
            try await list()
        case "show":
            try await show(args: args)
        case "drop":
            try await drop(args: args)
        default:
            throw CLIError.unknownCommand("schema \(command)")
        }
    }

    // MARK: - Commands

    /// Define a new schema
    /// Usage: schema define <Name> <field>:<type> [<field>:<type>...]
    private func define(args: [String]) async throws {
        guard args.count >= 2 else {
            throw CLIError.invalidArguments("Usage: schema define <Name> <field>:<type> [<field>:<type>...]")
        }

        let name = args[0]
        let fieldDefs = Array(args.dropFirst())

        // Check if schema already exists
        if let _ = try await storage.getSchema(name: name) {
            throw CLIError.schemaExists(name)
        }

        // Parse and create schema
        let schema = try DynamicSchema.parse(name: name, fieldDefinitions: fieldDefs)

        // Save to FDB
        try await storage.saveSchema(schema)

        output.success("Schema '\(name)' defined with \(schema.fields.count) fields")
    }

    /// List all schemas
    private func list() async throws {
        let schemas = try await storage.listSchemas()

        if schemas.isEmpty {
            output.info("No schemas defined")
            return
        }

        output.header("Schemas")
        for schema in schemas.sorted(by: { $0.name < $1.name }) {
            output.line("  \(schema.name) (\(schema.fields.count) fields)")
        }
    }

    /// Show schema details
    /// Usage: schema show <Name>
    private func show(args: [String]) async throws {
        guard let name = args.first else {
            throw CLIError.invalidArguments("Usage: schema show <Name>")
        }

        guard let schema = try await storage.getSchema(name: name) else {
            throw CLIError.schemaNotFound(name)
        }

        output.header(schema.name)
        for field in schema.fields {
            output.line("  \(field.displayString)")
        }
    }

    /// Drop a schema
    /// Usage: schema drop <Name>
    private func drop(args: [String]) async throws {
        guard let name = args.first else {
            throw CLIError.invalidArguments("Usage: schema drop <Name>")
        }

        guard let _ = try await storage.getSchema(name: name) else {
            throw CLIError.schemaNotFound(name)
        }

        try await storage.dropSchema(name: name)
        output.success("Schema '\(name)' dropped")
    }
}

// MARK: - Help

extension SchemaCommands {
    public static var helpText: String {
        """
        Schema Commands:
          schema define <Name> <field>:<type>...  Define a new schema
          schema list                              List all schemas
          schema show <Name>                       Show schema details
          schema drop <Name>                       Drop a schema and its data

        Field Types:
          string, int, double, bool, date, [string]
          Add '?' for optional fields (e.g., email:string?)

        Examples:
          schema define User id:string name:string age:int email:string?
          schema define Product id:string name:string price:double tags:[string]
        """
    }
}
