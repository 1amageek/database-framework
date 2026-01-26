import Foundation
import FoundationDB
import QueryAST

/// Handler for unified search commands
public struct FindCommands {
    private let storage: SchemaStorage
    private let output: OutputFormatter

    public init(storage: SchemaStorage, output: OutputFormatter) {
        self.storage = storage
        self.output = output
    }

    /// Execute a find command
    /// Usage: find <Schema> [conditions] [options]
    public func execute(_ args: [String]) async throws {
        guard !args.isEmpty else {
            throw CLIError.invalidArguments("Usage: find <Schema> [where ...] [--vector ...] [--text ...] [options]")
        }

        let schemaName = args[0]
        let queryArgs = Array(args.dropFirst())

        // Get schema
        guard let schema = try await storage.getSchema(name: schemaName) else {
            throw CLIError.schemaNotFound(schemaName)
        }

        // Parse query type
        if let vectorIdx = queryArgs.firstIndex(of: "--vector") {
            try await executeVectorQuery(schema: schema, args: queryArgs, startIdx: vectorIdx)
        } else if let textIdx = queryArgs.firstIndex(of: "--text") {
            try await executeFullTextQuery(schema: schema, args: queryArgs, startIdx: textIdx)
        } else if let nearIdx = queryArgs.firstIndex(of: "--near") {
            try await executeSpatialNearQuery(schema: schema, args: queryArgs, startIdx: nearIdx)
        } else if let bboxIdx = queryArgs.firstIndex(of: "--bbox") {
            try await executeSpatialBBoxQuery(schema: schema, args: queryArgs, startIdx: bboxIdx)
        } else if let bitmapIdx = queryArgs.firstIndex(of: "--bitmap") {
            try await executeBitmapQuery(schema: schema, args: queryArgs, startIdx: bitmapIdx)
        } else if let rankIdx = queryArgs.firstIndex(of: "--rank") {
            try await executeRankQuery(schema: schema, args: queryArgs, startIdx: rankIdx)
        } else if let lbIdx = queryArgs.firstIndex(of: "--leaderboard") {
            try await executeLeaderboardQuery(schema: schema, args: queryArgs, startIdx: lbIdx)
        } else if let aggIdx = queryArgs.firstIndex(of: "--aggregate") {
            try await executeAggregateQuery(schema: schema, args: queryArgs, startIdx: aggIdx)
        } else if let joinIdx = queryArgs.firstIndex(of: "--join") {
            try await executeJoinQuery(schema: schema, args: queryArgs, startIdx: joinIdx)
        } else if let whereIdx = queryArgs.firstIndex(where: { $0.lowercased() == "where" }) {
            try await executeScalarQuery(schema: schema, args: queryArgs, startIdx: whereIdx)
        } else {
            // Default: return all records
            try await executeAllQuery(schema: schema, args: queryArgs)
        }
    }

    // MARK: - Scalar Query

    private func executeScalarQuery(schema: DynamicSchema, args: [String], startIdx: Int) async throws {
        var limit = 100
        var orderBy: String? = nil
        var orderDesc = false

        // Parse where clause
        var whereArgs: [String] = []
        var i = startIdx + 1
        while i < args.count {
            let arg = args[i].lowercased()
            if arg == "limit" && i + 1 < args.count {
                limit = Int(args[i + 1]) ?? 100
                i += 2
            } else if arg == "order-by" && i + 1 < args.count {
                orderBy = args[i + 1]
                i += 2
                if i < args.count && args[i].lowercased() == "desc" {
                    orderDesc = true
                    i += 1
                }
            } else {
                whereArgs.append(args[i])
                i += 1
            }
        }

        // Build sort descriptor if ordering requested
        let sortDescriptor: DynamicSortDescriptor? = orderBy.map {
            DynamicSortDescriptor(field: $0, direction: orderDesc ? .descending : .ascending)
        }

        // Parse the where clause
        let clauseString = whereArgs.joined(separator: " ")
        let (field, filterFn) = try parseWhereClause(clauseString)

        // Find the scalar index for this field (need index name, not field name)
        let scalarIndex = schema.indexes.first { $0.kind == .scalar && $0.fields.contains(field) }

        // Check if field has a scalar index AND we're sorting by the same field (or no sort)
        let canUseIndexOrdering = scalarIndex != nil &&
                                  (orderBy == nil || orderBy == field)

        if canUseIndexOrdering, let indexDef = scalarIndex {
            // Get field type for proper value coercion
            let fieldType = schema.field(named: field)?.type

            // Use index-backed query with native ordering
            let operation = try parseScalarOperation(clauseString, fieldType: fieldType)
            let results = try await storage.queryByScalarIndexOrdered(
                schemaName: schema.name,
                indexName: indexDef.name,  // Use index name, not field name
                operation: operation,
                sortDescriptor: sortDescriptor,
                limit: limit
            )
            outputResults(results)
        } else if let indexDef = scalarIndex {
            // Use index for filtering, but need in-memory sort for different field
            let fieldType = schema.field(named: field)?.type
            let operation = try parseScalarOperation(clauseString, fieldType: fieldType)
            var results = try await storage.queryByScalarIndex(
                schemaName: schema.name,
                indexName: indexDef.name,  // Use index name, not field name
                operation: operation,
                limit: limit * 2  // Fetch extra for post-sort limiting
            )

            // Apply in-memory sort if ordering by different field
            if let sort = sortDescriptor {
                results = storage.sortResults(results, by: sort)
                if results.count > limit {
                    results = Array(results.prefix(limit))
                }
            }

            outputResults(results)
        } else {
            // Fall back to scan with filtering and sorting
            let capturedField = field
            let capturedFilter = filterFn
            let filter: @Sendable (String, [String: Any]) -> Bool = { _, values in
                capturedFilter(values[capturedField])
            }

            let results = try await storage.queryWithSort(
                schemaName: schema.name,
                filter: filter,
                sortDescriptor: sortDescriptor,
                limit: limit
            )
            outputResults(results)
        }
    }

    /// Parse a scalar operation from a WHERE clause, with optional type coercion
    ///
    /// - Parameters:
    ///   - clause: The WHERE clause string (e.g., "age > 25")
    ///   - fieldType: Optional field type for proper value coercion
    /// - Returns: A ScalarOperation with properly typed values
    private func parseScalarOperation(_ clause: String, fieldType: FieldType? = nil) throws -> ScalarOperation {
        let operators = [">=", "<=", "!=", "=", ">", "<"]

        for op in operators {
            if let range = clause.range(of: op) {
                var valueStr = String(clause[range.upperBound...]).trimmingCharacters(in: .whitespaces)

                // Remove quotes
                if valueStr.hasPrefix("\"") && valueStr.hasSuffix("\"") {
                    valueStr = String(valueStr.dropFirst().dropLast())
                }

                // Coerce value to appropriate type based on field definition
                let value: Any = coerceValue(valueStr, to: fieldType)

                switch op {
                case "=":
                    return .equals(value)
                case "!=":
                    // Not equals requires scan
                    throw CLIError.validationError("!= operator requires full scan (no index support)")
                case ">":
                    return .greaterThan(value)
                case "<":
                    return .lessThan(value)
                case ">=":
                    return .range(lower: value, upper: nil)
                case "<=":
                    return .range(lower: nil, upper: value)
                default:
                    break
                }
            }
        }

        throw CLIError.invalidArguments("Could not parse where clause: \(clause)")
    }

    /// Coerce a string value to the appropriate type
    private func coerceValue(_ valueStr: String, to fieldType: FieldType?) -> Any {
        guard let fieldType = fieldType else {
            // No type info - try to infer
            if let intValue = Int(valueStr) {
                return intValue
            } else if let doubleValue = Double(valueStr) {
                return doubleValue
            } else if valueStr == "true" || valueStr == "false" {
                return valueStr == "true"
            }
            return valueStr
        }

        switch fieldType {
        case .int:
            return Int(valueStr) ?? 0
        case .double:
            return Double(valueStr) ?? 0.0
        case .bool:
            return valueStr.lowercased() == "true" || valueStr == "1"
        case .string, .date:
            return valueStr
        case .stringArray, .doubleArray:
            return valueStr  // Arrays aren't typically used in scalar operations
        }
    }

    // MARK: - Vector Query

    private func executeVectorQuery(schema: DynamicSchema, args: [String], startIdx: Int) async throws {
        // Parse: --vector <field> <vector> --k <N> [--metric <M>]
        guard startIdx + 2 < args.count else {
            throw CLIError.invalidArguments("Usage: find <Schema> --vector <field> <vector> --k <N>")
        }

        let field = args[startIdx + 1]
        let vectorString = args[startIdx + 2]
        var k = 10
        var metric = VectorMetric.cosine

        // Parse options
        var i = startIdx + 3
        while i < args.count {
            if args[i] == "--k" && i + 1 < args.count {
                k = Int(args[i + 1]) ?? 10
                i += 2
            } else if args[i] == "--metric" && i + 1 < args.count {
                metric = VectorMetric(rawValue: args[i + 1]) ?? .cosine
                i += 2
            } else {
                i += 1
            }
        }

        // Parse vector
        let queryVector = try parseVector(vectorString)

        // Find vector index
        guard let indexDef = schema.indexes.first(where: { $0.kind == .vector && $0.fields.contains(field) }) else {
            throw CLIError.validationError("No vector index found for field '\(field)'")
        }

        // Execute query
        let handler = try IndexHandlerRegistry.createHandler(
            for: .vector,
            definition: indexDef,
            schemaName: schema.name
        )
        let query = VectorQuery(vector: queryVector, k: k, metric: metric)

        let ids = try await storage.databaseRef.withTransaction { transaction in
            try await handler.scan(query: query, limit: k, transaction: transaction, storage: self.storage)
        }

        // Fetch full records
        let results = try await fetchRecords(schemaName: schema.name, ids: ids)
        outputResults(results)
    }

    private func parseVector(_ str: String) throws -> [Float] {
        var cleanStr = str.trimmingCharacters(in: .whitespaces)
        if cleanStr.hasPrefix("[") { cleanStr = String(cleanStr.dropFirst()) }
        if cleanStr.hasSuffix("]") { cleanStr = String(cleanStr.dropLast()) }

        let parts = cleanStr.split(separator: ",")
        var result: [Float] = []
        for part in parts {
            if let f = Float(part.trimmingCharacters(in: .whitespaces)) {
                result.append(f)
            }
        }

        guard !result.isEmpty else {
            throw CLIError.invalidArguments("Could not parse vector: \(str)")
        }

        return result
    }

    // MARK: - Full-Text Query

    private func executeFullTextQuery(schema: DynamicSchema, args: [String], startIdx: Int) async throws {
        // Parse: --text <field> "<query>" [--phrase] [--fuzzy N]
        guard startIdx + 2 < args.count else {
            throw CLIError.invalidArguments("Usage: find <Schema> --text <field> \"<query>\"")
        }

        let field = args[startIdx + 1]
        var queryText = args[startIdx + 2]
        var phrase = false
        var fuzzy: Int? = nil
        var limit = 100

        // Remove quotes
        if queryText.hasPrefix("\"") && queryText.hasSuffix("\"") {
            queryText = String(queryText.dropFirst().dropLast())
        }

        // Parse options
        var i = startIdx + 3
        while i < args.count {
            if args[i] == "--phrase" {
                phrase = true
                i += 1
            } else if args[i] == "--fuzzy" && i + 1 < args.count {
                fuzzy = Int(args[i + 1])
                i += 2
            } else if args[i] == "limit" && i + 1 < args.count {
                limit = Int(args[i + 1]) ?? 100
                i += 2
            } else {
                i += 1
            }
        }

        // Find fulltext index
        guard let indexDef = schema.indexes.first(where: { $0.kind == .fulltext && $0.fields.contains(field) }) else {
            throw CLIError.validationError("No fulltext index found for field '\(field)'")
        }

        // Execute query
        let handler = try IndexHandlerRegistry.createHandler(
            for: .fulltext,
            definition: indexDef,
            schemaName: schema.name
        )
        let query = FullTextQuery(text: queryText, phrase: phrase, fuzzy: fuzzy)

        let ids = try await storage.databaseRef.withTransaction { transaction in
            try await handler.scan(query: query, limit: limit, transaction: transaction, storage: self.storage)
        }

        // Fetch full records
        let results = try await fetchRecords(schemaName: schema.name, ids: ids)
        outputResults(results)
    }

    // MARK: - Spatial Query

    private func executeSpatialNearQuery(schema: DynamicSchema, args: [String], startIdx: Int) async throws {
        // Parse: --near <lat> <lon> --radius <distance>
        guard startIdx + 2 < args.count else {
            throw CLIError.invalidArguments("Usage: find <Schema> --near <lat> <lon> --radius <distance>")
        }

        guard let lat = Double(args[startIdx + 1]),
              let lon = Double(args[startIdx + 2]) else {
            throw CLIError.invalidArguments("Invalid latitude/longitude")
        }

        var radiusMeters = 1000.0
        var limit = 100

        // Parse options
        var i = startIdx + 3
        while i < args.count {
            if args[i] == "--radius" && i + 1 < args.count {
                radiusMeters = parseDistance(args[i + 1])
                i += 2
            } else if args[i] == "limit" && i + 1 < args.count {
                limit = Int(args[i + 1]) ?? 100
                i += 2
            } else {
                i += 1
            }
        }

        // Find spatial index
        guard let indexDef = schema.indexes.first(where: { $0.kind == .spatial }) else {
            throw CLIError.validationError("No spatial index found")
        }

        // Execute query
        let handler = try IndexHandlerRegistry.createHandler(
            for: .spatial,
            definition: indexDef,
            schemaName: schema.name
        )
        let query = SpatialQuery.near(lat: lat, lon: lon, radiusMeters: radiusMeters)

        let ids = try await storage.databaseRef.withTransaction { transaction in
            try await handler.scan(query: query, limit: limit, transaction: transaction, storage: self.storage)
        }

        let results = try await fetchRecords(schemaName: schema.name, ids: ids)
        outputResults(results)
    }

    private func executeSpatialBBoxQuery(schema: DynamicSchema, args: [String], startIdx: Int) async throws {
        // Parse: --bbox <minLat> <minLon> <maxLat> <maxLon>
        guard startIdx + 4 < args.count else {
            throw CLIError.invalidArguments("Usage: find <Schema> --bbox <minLat> <minLon> <maxLat> <maxLon>")
        }

        guard let minLat = Double(args[startIdx + 1]),
              let minLon = Double(args[startIdx + 2]),
              let maxLat = Double(args[startIdx + 3]),
              let maxLon = Double(args[startIdx + 4]) else {
            throw CLIError.invalidArguments("Invalid bounding box coordinates")
        }

        var limit = 100
        var i = startIdx + 5
        while i < args.count {
            if args[i] == "limit" && i + 1 < args.count {
                limit = Int(args[i + 1]) ?? 100
                i += 2
            } else {
                i += 1
            }
        }

        guard let indexDef = schema.indexes.first(where: { $0.kind == .spatial }) else {
            throw CLIError.validationError("No spatial index found")
        }

        let handler = try IndexHandlerRegistry.createHandler(
            for: .spatial,
            definition: indexDef,
            schemaName: schema.name
        )
        let query = SpatialQuery.bbox(minLat: minLat, minLon: minLon, maxLat: maxLat, maxLon: maxLon)

        let ids = try await storage.databaseRef.withTransaction { transaction in
            try await handler.scan(query: query, limit: limit, transaction: transaction, storage: self.storage)
        }

        let results = try await fetchRecords(schemaName: schema.name, ids: ids)
        outputResults(results)
    }

    private func parseDistance(_ str: String) -> Double {
        let lowercased = str.lowercased()
        if lowercased.hasSuffix("km") {
            return (Double(lowercased.dropLast(2)) ?? 1) * 1000
        } else if lowercased.hasSuffix("m") {
            return Double(lowercased.dropLast(1)) ?? 1000
        } else {
            return Double(str) ?? 1000
        }
    }

    // MARK: - Bitmap Query

    private func executeBitmapQuery(schema: DynamicSchema, args: [String], startIdx: Int) async throws {
        // Parse: --bitmap <field> = <value> [AND/OR ...]
        guard startIdx + 3 < args.count else {
            throw CLIError.invalidArguments("Usage: find <Schema> --bitmap <field> = <value>")
        }

        let field = args[startIdx + 1]
        let op = args[startIdx + 2]
        let value = args[startIdx + 3]

        var limit = 100
        var countOnly = false

        var i = startIdx + 4
        while i < args.count {
            if args[i] == "limit" && i + 1 < args.count {
                limit = Int(args[i + 1]) ?? 100
                i += 2
            } else if args[i] == "--count" {
                countOnly = true
                i += 1
            } else {
                i += 1
            }
        }

        guard let indexDef = schema.indexes.first(where: { $0.kind == .bitmap && $0.fields.contains(field) }) else {
            throw CLIError.validationError("No bitmap index found for field '\(field)'")
        }

        let handler = try IndexHandlerRegistry.createHandler(
            for: .bitmap,
            definition: indexDef,
            schemaName: schema.name
        )

        let query: BitmapQuery
        if countOnly {
            query = .count
        } else if op == "in" {
            // Parse array: [val1, val2, ...]
            var values: [String] = []
            var valueStr = value
            if valueStr.hasPrefix("[") { valueStr = String(valueStr.dropFirst()) }
            if valueStr.hasSuffix("]") { valueStr = String(valueStr.dropLast()) }
            values = valueStr.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }
            query = .inSet(values)
        } else {
            query = .equals(value)
        }

        let ids = try await storage.databaseRef.withTransaction { transaction in
            try await handler.scan(query: query, limit: limit, transaction: transaction, storage: self.storage)
        }

        if countOnly {
            output.info("Count: \(ids.first ?? "0")")
        } else {
            let results = try await fetchRecords(schemaName: schema.name, ids: ids)
            outputResults(results)
        }
    }

    // MARK: - Rank Query

    private func executeRankQuery(schema: DynamicSchema, args: [String], startIdx: Int) async throws {
        guard startIdx + 2 < args.count else {
            throw CLIError.invalidArguments("Usage: find <Schema> --rank <field> --top N | --of <id> | --count")
        }

        let field = args[startIdx + 1]
        let operation = args[startIdx + 2]

        guard let indexDef = schema.indexes.first(where: { $0.kind == .rank && $0.fields.contains(field) }) else {
            throw CLIError.validationError("No rank index found for field '\(field)'")
        }

        let handler = try IndexHandlerRegistry.createHandler(
            for: .rank,
            definition: indexDef,
            schemaName: schema.name
        )

        let query: RankQuery
        switch operation {
        case "--top":
            let count = startIdx + 3 < args.count ? Int(args[startIdx + 3]) ?? 100 : 100
            query = .top(count)
        case "--of":
            guard startIdx + 3 < args.count else {
                throw CLIError.invalidArguments("--of requires an ID")
            }
            query = .of(args[startIdx + 3])
        case "--count":
            query = .count
        case "--range":
            guard startIdx + 4 < args.count else {
                throw CLIError.invalidArguments("--range requires start-end (e.g., 10-20)")
            }
            let range = args[startIdx + 3].split(separator: "-")
            let start = Int(range[0]) ?? 1
            let end = Int(range.count > 1 ? range[1] : range[0]) ?? 10
            query = .range(start, end)
        default:
            query = .top(100)
        }

        let result = try await storage.databaseRef.withTransaction { transaction in
            try await handler.scan(query: query, limit: 1000, transaction: transaction, storage: self.storage)
        }

        if case .count = query {
            output.info("Total: \(result.first ?? "0")")
        } else if case .of = query {
            output.info("Rank: \(result.first ?? "Not found")")
        } else {
            let records = try await fetchRecords(schemaName: schema.name, ids: result)
            outputResults(records)
        }
    }

    // MARK: - Leaderboard Query

    private func executeLeaderboardQuery(schema: DynamicSchema, args: [String], startIdx: Int) async throws {
        guard startIdx + 2 < args.count else {
            throw CLIError.invalidArguments("Usage: find <Schema> --leaderboard <indexName> --top N | --of <id>")
        }

        let indexName = args[startIdx + 1]
        let operation = args[startIdx + 2]

        guard let indexDef = schema.indexes.first(where: { $0.kind == .leaderboard && $0.name == indexName }) else {
            throw CLIError.validationError("No leaderboard index '\(indexName)' found")
        }

        let handler = try IndexHandlerRegistry.createHandler(
            for: .leaderboard,
            definition: indexDef,
            schemaName: schema.name
        )

        let query: LeaderboardQuery
        switch operation {
        case "--top":
            let count = startIdx + 3 < args.count ? Int(args[startIdx + 3]) ?? 100 : 100
            query = .top(count: count, window: nil, groupKey: nil)
        case "--of":
            guard startIdx + 3 < args.count else {
                throw CLIError.invalidArguments("--of requires an ID")
            }
            query = .rank(id: args[startIdx + 3], window: nil, groupKey: nil)
        case "--windows":
            query = .listWindows
        default:
            query = .top(count: 100, window: nil, groupKey: nil)
        }

        let result = try await storage.databaseRef.withTransaction { transaction in
            try await handler.scan(query: query, limit: 1000, transaction: transaction, storage: self.storage)
        }

        if case .listWindows = query {
            output.info("Active windows:")
            for window in result {
                output.line("  \(window)")
            }
        } else if case .rank = query {
            output.info("Rank: \(result.first ?? "Not found")")
        } else {
            let records = try await fetchRecords(schemaName: schema.name, ids: result)
            outputResults(records)
        }
    }

    // MARK: - Aggregate Query

    private func executeAggregateQuery(schema: DynamicSchema, args: [String], startIdx: Int) async throws {
        guard startIdx + 1 < args.count else {
            throw CLIError.invalidArguments("Usage: find <Schema> --aggregate <indexName>")
        }

        let indexName = args[startIdx + 1]

        guard let indexDef = schema.indexes.first(where: { $0.kind == .aggregation && $0.name == indexName }) else {
            throw CLIError.validationError("No aggregation index '\(indexName)' found")
        }

        let handler = try IndexHandlerRegistry.createHandler(
            for: .aggregation,
            definition: indexDef,
            schemaName: schema.name
        ) as! AggregationIndexHandler
        let query = AggregationQuery.all

        let result = try await storage.databaseRef.withTransaction { transaction in
            try await handler.scan(query: query, limit: 1000, transaction: transaction, storage: self.storage)
        }

        output.info("Aggregation results:")
        for line in result {
            output.line("  \(line)")
        }
    }

    // MARK: - Join Query

    private func executeJoinQuery(schema: DynamicSchema, args: [String], startIdx: Int) async throws {
        guard startIdx + 1 < args.count else {
            throw CLIError.invalidArguments("Usage: find <Schema> --join <relationName>")
        }

        let relationName = args[startIdx + 1]

        // Find relationship index
        guard let indexDef = schema.indexes.first(where: {
            $0.kind == .relationship && $0.name.contains(relationName)
        }) else {
            throw CLIError.validationError("No relationship index for '\(relationName)' found")
        }

        guard let config = indexDef.config,
              case .relationship(let relConfig) = config else {
            throw CLIError.validationError("Invalid relationship configuration")
        }

        // Fetch records with joined data
        let records = try await storage.query(schemaName: schema.name, limit: 100)

        var joinedResults: [(id: String, values: [String: Any])] = []
        for (id, values) in records {
            var joinedValues = values
            joinedValues["id"] = id

            if let foreignKey = values[relConfig.foreignKeyField] as? String {
                if let targetRecord = try await storage.get(schemaName: relConfig.targetSchema, id: foreignKey) {
                    joinedValues["_\(relationName)"] = targetRecord
                }
            }

            joinedResults.append((id: id, values: joinedValues))
        }

        outputResults(joinedResults)
    }

    // MARK: - All Query (default)

    private func executeAllQuery(schema: DynamicSchema, args: [String]) async throws {
        var limit = 100
        var orderBy: String? = nil
        var orderDesc = false

        var i = 0
        while i < args.count {
            let arg = args[i].lowercased()
            if arg == "limit" && i + 1 < args.count {
                limit = Int(args[i + 1]) ?? 100
                i += 2
            } else if arg == "order-by" && i + 1 < args.count {
                orderBy = args[i + 1]
                i += 2
                if i < args.count && args[i].lowercased() == "desc" {
                    orderDesc = true
                    i += 1
                }
            } else {
                i += 1
            }
        }

        // Build sort descriptor if ordering requested
        let sortDescriptor: DynamicSortDescriptor? = orderBy.map {
            DynamicSortDescriptor(field: $0, direction: orderDesc ? .descending : .ascending)
        }

        // Find scalar index for the sort field
        let sortIndex: IndexDefinition? = orderBy.flatMap { sortField in
            schema.indexes.first { $0.kind == .scalar && $0.fields.contains(sortField) }
        }

        // Check if we can use an index for ordering
        if let indexDef = sortIndex {
            // Use index for ordering (full range scan with ordering)
            let results = try await storage.queryByScalarIndexOrdered(
                schemaName: schema.name,
                indexName: indexDef.name,  // Use actual index name
                operation: .fullRange,     // Full scan of the index
                sortDescriptor: sortDescriptor,
                limit: limit
            )
            outputResults(results)
        } else {
            // Use scan with in-memory sort
            let results = try await storage.queryWithSort(
                schemaName: schema.name,
                filter: nil,
                sortDescriptor: sortDescriptor,
                limit: limit
            )
            outputResults(results)
        }
    }

    // MARK: - Helpers

    private func fetchRecords(schemaName: String, ids: [String]) async throws -> [(id: String, values: [String: Any])] {
        var results: [(id: String, values: [String: Any])] = []

        for id in ids {
            if let values = try await storage.get(schemaName: schemaName, id: id) {
                results.append((id: id, values: values))
            }
        }

        return results
    }

    private func outputResults(_ results: [(id: String, values: [String: Any])]) {
        if results.isEmpty {
            output.info("(no results)")
        } else {
            output.info("Found \(results.count) record(s):")
            output.table(results)
        }
    }
}

// MARK: - Help

extension FindCommands {
    public static var helpText: String {
        """
        Find Commands:
          find <Schema> [where <field> <op> <value>]     Scalar query
          find <Schema> --vector <field> <vector> --k N  Vector similarity search
          find <Schema> --text <field> "<query>"         Full-text search
          find <Schema> --near <lat> <lon> --radius <d>  Spatial near query
          find <Schema> --bbox <coords...>               Spatial bounding box
          find <Schema> --bitmap <field> = <value>       Bitmap filter
          find <Schema> --rank <field> --top N           Rank query
          find <Schema> --leaderboard <name> --top N     Leaderboard query
          find <Schema> --aggregate <name>               Aggregation query
          find <Schema> --join <relation>                Join with related schema

        Options:
          limit N                                        Limit results
          order-by <field> [desc]                        Sort results

        Where Operators:
          =, !=, >, <, >=, <=

        Examples:
          find User where age > 20 limit 10
          find Document --vector embedding [0.1, 0.2, ...] --k 10
          find Article --text body "machine learning"
          find Store --near 35.68 139.73 --radius 5km
          find User --bitmap status = active
          find Player --rank score --top 100
        """
    }
}
