import Foundation

/// Handler for admin-related commands (schema and index management)
public struct AdminCommands {
    private let storage: SchemaStorage
    private let output: OutputFormatter

    public init(storage: SchemaStorage, output: OutputFormatter) {
        self.storage = storage
        self.output = output
    }

    /// Execute an admin command
    /// Usage: admin <schema|index> <subcommand> ...
    public func execute(_ args: [String]) async throws {
        guard let category = args.first?.lowercased() else {
            throw CLIError.invalidArguments("Usage: admin <schema|index> <subcommand> ...")
        }

        let subArgs = Array(args.dropFirst())

        switch category {
        case "schema":
            try await executeSchemaCommand(subArgs)
        case "index":
            try await executeIndexCommand(subArgs)
        default:
            throw CLIError.invalidArguments("Unknown admin category: '\(category)'. Use 'schema' or 'index'.")
        }
    }

    // MARK: - Schema Commands

    private func executeSchemaCommand(_ args: [String]) async throws {
        guard let subCommand = args.first?.lowercased() else {
            throw CLIError.invalidArguments("Usage: admin schema <define|list|show|drop> ...")
        }

        let subArgs = Array(args.dropFirst())

        switch subCommand {
        case "define":
            try await defineSchema(args: subArgs)
        case "list":
            try await listSchemas()
        case "show":
            try await showSchema(args: subArgs)
        case "drop":
            try await dropSchema(args: subArgs)
        default:
            throw CLIError.unknownCommand("admin schema \(subCommand)")
        }
    }

    /// Define a new schema with optional indexes
    /// Usage: admin schema define <Name> <field>:<type>[#modifier][@relationship] ... [--options]
    private func defineSchema(args: [String]) async throws {
        guard args.count >= 2 else {
            throw CLIError.invalidArguments(
                "Usage: admin schema define <Name> <field>:<type>[#modifier][@relationship] ... [--options]"
            )
        }

        let name = args[0]
        let fieldDefs = Array(args.dropFirst())

        // Check if schema already exists
        if let _ = try await storage.getSchema(name: name) {
            throw CLIError.schemaExists(name)
        }

        // Parse and create schema with indexes
        let schema = try DynamicSchema.parse(name: name, fieldDefinitions: fieldDefs)

        // Save to FDB
        try await storage.saveSchema(schema)

        // Output result
        output.success("Schema '\(name)' defined with \(schema.fields.count) field(s)")

        if !schema.indexes.isEmpty {
            output.info("Indexes:")
            for idx in schema.indexes {
                output.line("  \(idx.displayString)")
            }
        }
    }

    /// List all schemas
    private func listSchemas() async throws {
        let schemas = try await storage.listSchemas()

        if schemas.isEmpty {
            output.info("No schemas defined")
            return
        }

        output.header("Schemas")
        for schema in schemas.sorted(by: { $0.name < $1.name }) {
            output.line("  \(schema.name) (\(schema.summary))")
        }
    }

    /// Show schema details
    /// Usage: admin schema show <Name>
    private func showSchema(args: [String]) async throws {
        guard let name = args.first else {
            throw CLIError.invalidArguments("Usage: admin schema show <Name>")
        }

        guard let schema = try await storage.getSchema(name: name) else {
            throw CLIError.schemaNotFound(name)
        }

        output.header(schema.name)
        output.info(schema.detailedDisplay)
    }

    /// Drop a schema
    /// Usage: admin schema drop <Name>
    private func dropSchema(args: [String]) async throws {
        guard let name = args.first else {
            throw CLIError.invalidArguments("Usage: admin schema drop <Name>")
        }

        guard let _ = try await storage.getSchema(name: name) else {
            throw CLIError.schemaNotFound(name)
        }

        try await storage.dropSchema(name: name)
        output.success("Schema '\(name)' dropped")
    }

    // MARK: - Index Commands

    private func executeIndexCommand(_ args: [String]) async throws {
        guard let subCommand = args.first?.lowercased() else {
            throw CLIError.invalidArguments("Usage: admin index <add|list|drop|rebuild> ...")
        }

        let subArgs = Array(args.dropFirst())

        switch subCommand {
        case "add":
            try await addIndex(args: subArgs)
        case "list":
            try await listIndexes(args: subArgs)
        case "drop":
            try await dropIndex(args: subArgs)
        case "rebuild":
            try await rebuildIndex(args: subArgs)
        default:
            throw CLIError.unknownCommand("admin index \(subCommand)")
        }
    }

    /// Add an index to an existing schema
    /// Usage: admin index add <Schema> <field> <type> [options]
    /// Or: admin index add <Schema> --<option> name(params)
    private func addIndex(args: [String]) async throws {
        guard args.count >= 2 else {
            throw CLIError.invalidArguments(
                "Usage: admin index add <Schema> <field>#<type> or admin index add <Schema> --<option> name(params)"
            )
        }

        let schemaName = args[0]
        let indexArgs = Array(args.dropFirst())

        // Get existing schema
        guard let schema = try await storage.getSchema(name: schemaName) else {
            throw CLIError.schemaNotFound(schemaName)
        }

        // Parse the index definition using SchemaParser
        let result = try SchemaParser.parse(schemaName: schemaName, args: indexArgs)

        guard !result.indexes.isEmpty else {
            throw CLIError.invalidArguments("No valid index definition found")
        }

        // Check for duplicate index names
        for newIndex in result.indexes {
            if schema.indexes.contains(where: { $0.name == newIndex.name }) {
                throw CLIError.validationError("Index '\(newIndex.name)' already exists in schema '\(schemaName)'")
            }
        }

        // Create updated schema with new indexes
        let updatedSchema = DynamicSchema(
            name: schema.name,
            fields: schema.fields,
            indexes: schema.indexes + result.indexes
        )

        // Save updated schema
        try await storage.saveSchema(updatedSchema)

        output.success("Added \(result.indexes.count) index(es) to '\(schemaName)':")
        for idx in result.indexes {
            output.line("  \(idx.displayString)")
        }
    }

    /// List indexes for a schema
    /// Usage: admin index list <Schema>
    private func listIndexes(args: [String]) async throws {
        guard let schemaName = args.first else {
            throw CLIError.invalidArguments("Usage: admin index list <Schema>")
        }

        guard let schema = try await storage.getSchema(name: schemaName) else {
            throw CLIError.schemaNotFound(schemaName)
        }

        if schema.indexes.isEmpty {
            output.info("No indexes defined for schema '\(schemaName)'")
            return
        }

        output.header("Indexes for '\(schemaName)'")
        for idx in schema.indexes {
            output.line("  \(idx.displayString)")
        }
    }

    /// Drop an index from a schema
    /// Usage: admin index drop <Schema> <indexName>
    private func dropIndex(args: [String]) async throws {
        guard args.count >= 2 else {
            throw CLIError.invalidArguments("Usage: admin index drop <Schema> <indexName>")
        }

        let schemaName = args[0]
        let indexName = args[1]

        guard let schema = try await storage.getSchema(name: schemaName) else {
            throw CLIError.schemaNotFound(schemaName)
        }

        guard schema.indexes.contains(where: { $0.name == indexName }) else {
            throw CLIError.validationError("Index '\(indexName)' not found in schema '\(schemaName)'")
        }

        // Remove the index
        let updatedIndexes = schema.indexes.filter { $0.name != indexName }
        let updatedSchema = DynamicSchema(
            name: schema.name,
            fields: schema.fields,
            indexes: updatedIndexes
        )

        // Save updated schema
        try await storage.saveSchema(updatedSchema)

        // Clear index data
        let droppedIndex = schema.indexes.first { $0.name == indexName }!
        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: droppedIndex.kind,
            indexName: indexName
        )
        let (begin, end) = indexSubspace.range()

        try await storage.databaseRef.withTransaction { transaction in
            transaction.clearRange(beginKey: begin, endKey: end)
        }

        output.success("Dropped index '\(indexName)' from '\(schemaName)'")
    }

    /// Rebuild an index
    /// Usage: admin index rebuild <Schema> <indexName>
    private func rebuildIndex(args: [String]) async throws {
        guard args.count >= 2 else {
            throw CLIError.invalidArguments("Usage: admin index rebuild <Schema> <indexName>")
        }

        let schemaName = args[0]
        let indexName = args[1]

        guard let schema = try await storage.getSchema(name: schemaName) else {
            throw CLIError.schemaNotFound(schemaName)
        }

        guard let indexDef = schema.indexes.first(where: { $0.name == indexName }) else {
            throw CLIError.validationError("Index '\(indexName)' not found in schema '\(schemaName)'")
        }

        output.info("Rebuilding index '\(indexName)' on '\(schemaName)'...")

        // Clear existing index data
        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: indexDef.kind,
            indexName: indexName
        )
        let (begin, end) = indexSubspace.range()

        try await storage.databaseRef.withTransaction { transaction in
            transaction.clearRange(beginKey: begin, endKey: end)
        }

        // Fetch all records and rebuild index
        let records = try await storage.query(schemaName: schemaName, limit: 1_000_000)

        // Create index handler
        let handler = try createIndexHandler(for: indexDef, schemaName: schemaName)

        var count = 0
        for (id, values) in records {
            try await storage.databaseRef.withTransaction { transaction in
                try await handler.updateIndex(
                    oldItem: nil,
                    newItem: values,
                    id: id,
                    transaction: transaction,
                    storage: storage
                )
            }
            count += 1
            if count % 100 == 0 {
                output.info("  Processed \(count) records...")
            }
        }

        output.success("Rebuilt index '\(indexName)' with \(count) record(s)")
    }

    // MARK: - Index Handler Factory

    /// Create an index handler for a given index definition
    private func createIndexHandler(for indexDef: IndexDefinition, schemaName: String) throws -> any IndexHandler {
        switch indexDef.kind {
        case .scalar:
            return ScalarIndexHandler(indexDefinition: indexDef, schemaName: schemaName)
        case .bitmap:
            return BitmapIndexHandler(indexDefinition: indexDef, schemaName: schemaName)
        case .rank:
            return RankIndexHandler(indexDefinition: indexDef, schemaName: schemaName)
        case .vector:
            return VectorIndexHandler(indexDefinition: indexDef, schemaName: schemaName)
        case .fulltext:
            return FullTextIndexHandler(indexDefinition: indexDef, schemaName: schemaName)
        case .spatial:
            return SpatialIndexHandler(indexDefinition: indexDef, schemaName: schemaName)
        case .graph:
            return GraphIndexHandler(indexDefinition: indexDef, schemaName: schemaName)
        case .aggregation:
            return AggregationIndexHandler(indexDefinition: indexDef, schemaName: schemaName)
        case .version:
            return VersionIndexHandler(indexDefinition: indexDef, schemaName: schemaName)
        case .leaderboard:
            return LeaderboardIndexHandler(indexDefinition: indexDef, schemaName: schemaName)
        case .relationship:
            return RelationshipIndexHandler(indexDefinition: indexDef, schemaName: schemaName)
        case .permuted:
            return PermutedIndexHandler(indexDefinition: indexDef, schemaName: schemaName)
        }
    }
}

// MARK: - Help

extension AdminCommands {
    public static var helpText: String {
        """
        Admin Commands:

        Schema Management:
          admin schema define <Name> <fields...>  Define a new schema with indexes
          admin schema list                        List all schemas
          admin schema show <Name>                 Show schema details
          admin schema drop <Name>                 Drop a schema and its data

        Index Management:
          admin index add <Schema> <field>#<type>  Add an index to a schema
          admin index add <Schema> --<option>      Add a compound index
          admin index list <Schema>                List indexes for a schema
          admin index drop <Schema> <indexName>    Drop an index
          admin index rebuild <Schema> <indexName> Rebuild an index

        Field Modifiers (#):
          #indexed          Scalar index (equality, range queries)
          #unique           Unique constraint with scalar index
          #bitmap           Bitmap index (low cardinality fields)
          #rank             Rank index (leaderboards)
          #vector(dim=N,metric=M,algorithm=A)  Vector index
          #fulltext(tokenizer=T,positions=B)   Full-text search index
          #leaderboard(window=W,count=N)       Time-windowed leaderboard

        Relationship Modifier (@):
          @relationship(Target,rule)  Foreign key with delete rule
                                      Rules: cascade, nullify, deny, noAction

        Compound Options (--):
          --spatial name(fields=lat,lon,encoding=E,level=L)
          --graph name(from=F,to=T,label=L,strategy=S)
          --aggregate name(type=T,field=F,by=B)
          --version name(retention=R)
          --composite name(fields=F1,F2,...)
          --permuted name(source=S,order=0,1,2)

        Examples:
          admin schema define User id:string name:string#indexed email:string#unique age:int#indexed status:string#bitmap
          admin schema define Document id:string content:string#fulltext(tokenizer=stem) embedding:[double]#vector(dim=384)
          admin schema define Store id:string lat:double lon:double --spatial location(fields=lat,lon)
          admin schema define Follow id:string follower:string followee:string --graph edges(from=follower,to=followee)
          admin schema define Order id:string customerId:string@relationship(Customer,cascade) amount:double
        """
    }
}
