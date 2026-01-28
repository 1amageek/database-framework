import Foundation

/// Handler for data-related commands
public struct DataCommands {
    private let storage: SchemaStorage
    private let output: OutputFormatter

    public init(storage: SchemaStorage, output: OutputFormatter) {
        self.storage = storage
        self.output = output
    }

    // MARK: - Insert

    /// Insert a record
    /// Usage: insert <Schema> <json>
    public func insert(args: [String]) async throws {
        guard args.count >= 2 else {
            throw CLIError.invalidArguments("Usage: insert <Schema> <json>")
        }

        let schemaName = args[0]
        let jsonString = args.dropFirst().joined(separator: " ")

        // Get schema
        guard let schema = try await storage.getSchema(name: schemaName) else {
            throw CLIError.schemaNotFound(schemaName)
        }

        // Parse JSON
        let values = try JSONParser.parse(jsonString)

        // Validate against schema
        try schema.validate(values)

        // Coerce values to correct types
        let coercedValues = try schema.coerce(values)

        // Extract ID
        guard let id = coercedValues["id"] as? String else {
            throw CLIError.validationError("Missing required 'id' field")
        }

        // Check if record already exists
        if let _ = try await storage.get(schemaName: schemaName, id: id) {
            throw CLIError.validationError("Record with id '\(id)' already exists. Use 'update' to modify.")
        }

        // Check unique constraints
        try await checkUniqueConstraints(schema: schema, values: coercedValues, excludeId: nil)

        // Validate relationship constraints (FK must reference existing records)
        try await validateRelationshipConstraints(schema: schema, values: coercedValues)

        // Create index handlers for this schema
        let indexHandlers = IndexHandlerRegistry.createHandlers(for: schema)

        // Insert with index maintenance
        try await storage.insert(
            schemaName: schemaName,
            id: id,
            values: coercedValues,
            schema: schema,
            indexHandlers: indexHandlers
        )

        output.success("Inserted record '\(id)' into '\(schemaName)'")
    }

    // MARK: - Get

    /// Get a record by ID
    /// Usage: get <Schema> <id>
    public func get(args: [String]) async throws {
        guard args.count >= 2 else {
            throw CLIError.invalidArguments("Usage: get <Schema> <id>")
        }

        let schemaName = args[0]
        let id = args[1]

        // Verify schema exists
        guard let _ = try await storage.getSchema(name: schemaName) else {
            throw CLIError.schemaNotFound(schemaName)
        }

        // Get record
        guard let values = try await storage.get(schemaName: schemaName, id: id) else {
            throw CLIError.recordNotFound(schema: schemaName, id: id)
        }

        var displayValues = values
        displayValues["id"] = id
        output.json(displayValues)
    }

    // MARK: - Query

    /// Query records
    /// Usage: query <Schema> [where <field> <op> <value>] [limit <N>]
    public func query(args: [String]) async throws {
        guard !args.isEmpty else {
            throw CLIError.invalidArguments("Usage: query <Schema> [where <field> <op> <value>] [limit <N>]")
        }

        let schemaName = args[0]
        var limit = 100
        var whereField: String? = nil
        var whereFilter: (@Sendable (Any?) -> Bool)? = nil

        // Parse options
        var i = 1
        while i < args.count {
            let arg = args[i].lowercased()

            if arg == "where" && i + 1 < args.count {
                // Collect the where clause (may span multiple args)
                var clauseParts: [String] = []
                i += 1
                while i < args.count {
                    let next = args[i].lowercased()
                    if next == "limit" { break }
                    clauseParts.append(args[i])
                    i += 1
                }
                let clause = clauseParts.joined(separator: " ")
                let (field, filter) = try parseWhereClause(clause)
                whereField = field
                whereFilter = filter
            } else if arg == "limit" && i + 1 < args.count {
                i += 1
                if let n = Int(args[i]) {
                    limit = n
                }
                i += 1
            } else {
                i += 1
            }
        }

        // Verify schema exists
        guard let _ = try await storage.getSchema(name: schemaName) else {
            throw CLIError.schemaNotFound(schemaName)
        }

        // Build filter function
        let filter: (@Sendable (String, [String: Any]) -> Bool)? = {
            guard let field = whereField, let filterFn = whereFilter else {
                return nil
            }
            // Capture field value for Sendable closure
            let capturedField = field
            let capturedFilter = filterFn
            return { @Sendable _, values in
                capturedFilter(values[capturedField])
            }
        }()

        // Execute query
        let results = try await storage.query(schemaName: schemaName, filter: filter, limit: limit)

        if results.isEmpty {
            output.info("(no results)")
        } else {
            output.info("Found \(results.count) record(s):")
            output.table(results)
        }
    }

    // MARK: - Update

    /// Update a record
    /// Usage: update <Schema> <id> <json>
    public func update(args: [String]) async throws {
        guard args.count >= 3 else {
            throw CLIError.invalidArguments("Usage: update <Schema> <id> <json>")
        }

        let schemaName = args[0]
        let id = args[1]
        let jsonString = args.dropFirst(2).joined(separator: " ")

        // Get schema
        guard let schema = try await storage.getSchema(name: schemaName) else {
            throw CLIError.schemaNotFound(schemaName)
        }

        // Get existing record
        guard let existing = try await storage.get(schemaName: schemaName, id: id) else {
            throw CLIError.recordNotFound(schema: schemaName, id: id)
        }

        // Parse new values
        let newValues = try JSONParser.parse(jsonString)

        // Merge with existing
        var merged = existing
        for (key, value) in newValues {
            // Don't allow changing ID
            if key == "id" {
                continue
            }
            merged[key] = value
        }
        merged["id"] = id

        // Validate merged result
        try schema.validate(merged)

        // Coerce values
        let coercedValues = try schema.coerce(merged)

        // Check unique constraints (exclude current record)
        try await checkUniqueConstraints(schema: schema, values: coercedValues, excludeId: id)

        // Validate relationship constraints (FK must reference existing records)
        try await validateRelationshipConstraints(schema: schema, values: coercedValues)

        // Create index handlers for this schema
        let indexHandlers = IndexHandlerRegistry.createHandlers(for: schema)

        // Update with index maintenance
        try await storage.update(
            schemaName: schemaName,
            id: id,
            oldValues: existing,
            newValues: coercedValues,
            schema: schema,
            indexHandlers: indexHandlers
        )

        output.success("Updated record '\(id)' in '\(schemaName)'")
    }

    // MARK: - Delete

    /// Delete a record
    /// Usage: delete <Schema> <id>
    public func delete(args: [String]) async throws {
        guard args.count >= 2 else {
            throw CLIError.invalidArguments("Usage: delete <Schema> <id>")
        }

        let schemaName = args[0]
        let id = args[1]

        // Get schema
        guard let schema = try await storage.getSchema(name: schemaName) else {
            throw CLIError.schemaNotFound(schemaName)
        }

        // Get existing record
        guard let existing = try await storage.get(schemaName: schemaName, id: id) else {
            throw CLIError.recordNotFound(schema: schemaName, id: id)
        }

        // Create index handlers for this schema
        let indexHandlers = IndexHandlerRegistry.createHandlers(for: schema)

        // Handle relationship delete rules (for schemas that reference this one)
        let cascadeDeletes = try await handleRelationshipDeleteRules(
            schemaName: schemaName,
            id: id
        )

        // Delete with index maintenance
        try await storage.delete(
            schemaName: schemaName,
            id: id,
            oldValues: existing,
            schema: schema,
            indexHandlers: indexHandlers
        )

        // Process cascade deletes after main delete
        for cascadeDelete in cascadeDeletes {
            try await performCascadeDelete(cascadeDelete)
        }

        output.success("Deleted record '\(id)' from '\(schemaName)'")
    }

    // MARK: - Constraint Checking

    /// Check unique constraints for all unique indexes
    private func checkUniqueConstraints(
        schema: DynamicSchema,
        values: [String: Any],
        excludeId: String?
    ) async throws {
        for indexDef in schema.indexes where indexDef.unique {
            for field in indexDef.fields {
                guard let value = values[field] else { continue }

                let isDuplicate = try await storage.checkUniqueConstraint(
                    schemaName: schema.name,
                    indexName: indexDef.name,
                    value: value,
                    excludeId: excludeId
                )

                if isDuplicate {
                    throw CLIError.validationError(
                        "Unique constraint violation: field '\(field)' with value '\(value)' already exists"
                    )
                }
            }
        }
    }

    /// Validate that FK references point to existing records
    private func validateRelationshipConstraints(
        schema: DynamicSchema,
        values: [String: Any]
    ) async throws {
        for indexDef in schema.indexes {
            guard let config = indexDef.config,
                  case .relationship(let relConfig) = config else {
                continue
            }

            // Get the FK value
            guard let fkValue = values[relConfig.foreignKeyField] as? String else {
                continue // FK is null, which is allowed
            }

            // Check if target record exists
            guard let _ = try await storage.get(schemaName: relConfig.targetSchema, id: fkValue) else {
                throw CLIError.validationError(
                    "Relationship constraint violation: referenced record '\(fkValue)' does not exist in schema '\(relConfig.targetSchema)'"
                )
            }
        }
    }

    // MARK: - Delete Rule Handling

    /// Represents a cascade delete action
    private struct CascadeDeleteAction {
        let schemaName: String
        let ids: [String]
    }

    /// Handle relationship delete rules for schemas that reference this one
    private func handleRelationshipDeleteRules(
        schemaName: String,
        id: String
    ) async throws -> [CascadeDeleteAction] {
        var cascadeDeletes: [CascadeDeleteAction] = []

        // Find all schemas that have relationships pointing to this schema
        let allSchemas = try await storage.listSchemas()

        for childSchema in allSchemas {
            for indexDef in childSchema.indexes {
                guard let config = indexDef.config,
                      case .relationship(let relConfig) = config,
                      relConfig.targetSchema == schemaName else {
                    continue
                }

                // Create the handler to check for references
                let handler = RelationshipIndexHandler(
                    indexDefinition: indexDef,
                    schemaName: childSchema.name
                )

                // Check what action is needed
                let result = try await storage.databaseRef.withTransaction { transaction in
                    try await handler.handleParentDelete(
                        parentId: id,
                        transaction: transaction,
                        storage: self.storage
                    )
                }

                switch result {
                case .allowed:
                    // No child records reference this parent
                    break

                case .cascade(let childIds):
                    cascadeDeletes.append(CascadeDeleteAction(
                        schemaName: childSchema.name,
                        ids: childIds
                    ))

                case .nullify(let childIds, let field):
                    // Update child records to nullify the FK
                    for childId in childIds {
                        try await nullifyForeignKey(
                            schemaName: childSchema.name,
                            id: childId,
                            field: field
                        )
                    }

                case .denied(let reason):
                    throw CLIError.validationError(reason)
                }
            }
        }

        return cascadeDeletes
    }

    /// Nullify a foreign key field in a child record
    private func nullifyForeignKey(
        schemaName: String,
        id: String,
        field: String
    ) async throws {
        guard let schema = try await storage.getSchema(name: schemaName) else {
            return
        }

        guard var existing = try await storage.get(schemaName: schemaName, id: id) else {
            return
        }

        // Set the FK field to null (remove it from the dictionary)
        existing.removeValue(forKey: field)
        existing["id"] = id

        let indexHandlers = IndexHandlerRegistry.createHandlers(for: schema)

        let oldValues = try await storage.get(schemaName: schemaName, id: id) ?? [:]

        try await storage.update(
            schemaName: schemaName,
            id: id,
            oldValues: oldValues,
            newValues: existing,
            schema: schema,
            indexHandlers: indexHandlers
        )
    }

    /// Perform a cascade delete recursively
    private func performCascadeDelete(_ action: CascadeDeleteAction) async throws {
        guard let schema = try await storage.getSchema(name: action.schemaName) else {
            return
        }

        let indexHandlers = IndexHandlerRegistry.createHandlers(for: schema)

        for childId in action.ids {
            guard let existing = try await storage.get(schemaName: action.schemaName, id: childId) else {
                continue
            }

            // Recursively handle delete rules for this child's children
            let nestedCascades = try await handleRelationshipDeleteRules(
                schemaName: action.schemaName,
                id: childId
            )

            // Delete the child record
            try await storage.delete(
                schemaName: action.schemaName,
                id: childId,
                oldValues: existing,
                schema: schema,
                indexHandlers: indexHandlers
            )

            // Process nested cascade deletes
            for nestedCascade in nestedCascades {
                try await performCascadeDelete(nestedCascade)
            }
        }
    }
}

// MARK: - Help

extension DataCommands {
    public static var helpText: String {
        """
        Data Commands:
          insert <Schema> <json>                          Insert a record
          get <Schema> <id>                               Get a record by ID
          query <Schema> [where field op value] [limit N] Query records
          update <Schema> <id> <json>                     Update a record
          delete <Schema> <id>                            Delete a record

        Where Operators:
          =, !=, >, <, >=, <=

        Examples:
          insert User {"id": "u1", "name": "Alice", "age": 25}
          get User u1
          query User where age > 20 limit 10
          update User u1 {"age": 26}
          delete User u1
        """
    }
}
