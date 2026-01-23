import Foundation
import FoundationDB

/// Handler for graph-related commands
public struct GraphCommands {
    private let storage: SchemaStorage
    private let output: OutputFormatter

    public init(storage: SchemaStorage, output: OutputFormatter) {
        self.storage = storage
        self.output = output
    }

    /// Execute a graph command
    /// Usage: graph <Schema> [from=<node>] [to=<node>] [--depth N] [--path from=<a> to=<b>] [--pagerank --top N]
    public func execute(_ args: [String]) async throws {
        guard !args.isEmpty else {
            throw CLIError.invalidArguments("Usage: graph <Schema> from=<node> | to=<node> | --path | --pagerank")
        }

        let schemaName = args[0]
        let queryArgs = Array(args.dropFirst())

        // Get schema
        guard let schema = try await storage.getSchema(name: schemaName) else {
            throw CLIError.schemaNotFound(schemaName)
        }

        // Find graph index
        guard let indexDef = schema.indexes.first(where: { $0.kind == .graph }) else {
            throw CLIError.validationError("No graph index found in schema '\(schemaName)'")
        }

        let handler = GraphIndexHandler(indexDefinition: indexDef, schemaName: schemaName)

        // Parse query type
        if let pathIdx = queryArgs.firstIndex(of: "--path") {
            try await executePathQuery(handler: handler, args: queryArgs, startIdx: pathIdx)
        } else if queryArgs.contains("--pagerank") {
            try await executePageRankQuery(handler: handler, schema: schema, args: queryArgs)
        } else if queryArgs.contains("--cycles") {
            try await executeCycleDetection(handler: handler, schema: schema)
        } else if queryArgs.contains("--communities") {
            try await executeCommunityDetection(handler: handler, schema: schema)
        } else if let sparqlIdx = queryArgs.firstIndex(of: "--sparql") {
            try await executeSPARQL(handler: handler, schema: schema, args: queryArgs, startIdx: sparqlIdx)
        } else {
            // Parse from= or to=
            var fromNode: String? = nil
            var toNode: String? = nil
            var depth = 1

            for arg in queryArgs {
                if arg.hasPrefix("from=") {
                    fromNode = String(arg.dropFirst(5))
                } else if arg.hasPrefix("to=") {
                    toNode = String(arg.dropFirst(3))
                } else if arg == "--depth" {
                    // Get next arg
                    if let idx = queryArgs.firstIndex(of: arg),
                       idx + 1 < queryArgs.count {
                        depth = Int(queryArgs[idx + 1]) ?? 1
                    }
                }
            }

            if let from = fromNode {
                if depth > 1 {
                    try await executeTraversalQuery(handler: handler, start: from, depth: depth)
                } else {
                    try await executeOutgoingQuery(handler: handler, node: from)
                }
            } else if let to = toNode {
                try await executeIncomingQuery(handler: handler, node: to)
            } else {
                throw CLIError.invalidArguments("Specify from=<node> or to=<node>")
            }
        }
    }

    // MARK: - Query Implementations

    private func executeOutgoingQuery(handler: GraphIndexHandler, node: String) async throws {
        let query = GraphQuery.outgoing(node)

        let neighbors = try await storage.databaseRef.withTransaction { transaction in
            try await handler.scan(query: query, limit: 100, transaction: transaction, storage: self.storage)
        }

        output.info("Outgoing edges from '\(node)':")
        if neighbors.isEmpty {
            output.line("  (none)")
        } else {
            for neighbor in neighbors {
                output.line("  -> \(neighbor)")
            }
        }
    }

    private func executeIncomingQuery(handler: GraphIndexHandler, node: String) async throws {
        let query = GraphQuery.incoming(node)

        let neighbors = try await storage.databaseRef.withTransaction { transaction in
            try await handler.scan(query: query, limit: 100, transaction: transaction, storage: self.storage)
        }

        output.info("Incoming edges to '\(node)':")
        if neighbors.isEmpty {
            output.line("  (none)")
        } else {
            for neighbor in neighbors {
                output.line("  <- \(neighbor)")
            }
        }
    }

    private func executeTraversalQuery(handler: GraphIndexHandler, start: String, depth: Int) async throws {
        let query = GraphQuery.traverse(start: start, depth: depth)

        let reachable = try await storage.databaseRef.withTransaction { transaction in
            try await handler.scan(query: query, limit: 1000, transaction: transaction, storage: self.storage)
        }

        output.info("Nodes reachable from '\(start)' within \(depth) hop(s):")
        if reachable.isEmpty {
            output.line("  (none)")
        } else {
            for node in reachable {
                output.line("  \(node)")
            }
        }
    }

    private func executePathQuery(handler: GraphIndexHandler, args: [String], startIdx: Int) async throws {
        // Parse: --path from=<a> to=<b>
        var fromNode: String? = nil
        var toNode: String? = nil

        for i in startIdx..<args.count {
            let arg = args[i]
            if arg.hasPrefix("from=") {
                fromNode = String(arg.dropFirst(5))
            } else if arg.hasPrefix("to=") {
                toNode = String(arg.dropFirst(3))
            }
        }

        guard let from = fromNode, let to = toNode else {
            throw CLIError.invalidArguments("--path requires from=<node> and to=<node>")
        }

        let query = GraphQuery.shortestPath(from: from, to: to)

        let path = try await storage.databaseRef.withTransaction { transaction in
            try await handler.scan(query: query, limit: 1000, transaction: transaction, storage: self.storage)
        }

        if path.isEmpty {
            output.info("No path found from '\(from)' to '\(to)'")
        } else {
            output.info("Shortest path from '\(from)' to '\(to)':")
            output.line("  \(path.joined(separator: " -> "))")
        }
    }

    private func executePageRankQuery(handler: GraphIndexHandler, schema: DynamicSchema, args: [String]) async throws {
        var topN = 10

        for (i, arg) in args.enumerated() {
            if arg == "--top" && i + 1 < args.count {
                topN = Int(args[i + 1]) ?? 10
            }
        }

        output.info("Computing PageRank (simplified implementation)...")

        // Get all edges
        var outDegree: [String: Int] = [:]
        var inEdges: [String: [String]] = [:]
        var allNodes = Set<String>()

        // Collect graph structure from index
        let indexSubspace = storage.indexSubspace(
            schema: schema.name,
            kind: .graph,
            indexName: handler.indexDefinition.name
        )
        let outSubspace = indexSubspace.subspace(Tuple(["out"]))

        try await storage.databaseRef.withTransaction { transaction in
            let (begin, end) = outSubspace.range()
            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

            for try await (key, _) in sequence {
                if let tuple = try? outSubspace.unpack(key),
                   tuple.count >= 2,
                   let from = tuple[0] as? String,
                   let to = tuple[1] as? String {
                    allNodes.insert(from)
                    allNodes.insert(to)
                    outDegree[from, default: 0] += 1
                    inEdges[to, default: []].append(from)
                }
            }
        }

        // Simple PageRank iteration
        let damping = 0.85
        var scores = Dictionary(uniqueKeysWithValues: allNodes.map { ($0, 1.0 / Double(allNodes.count)) })
        let baseScore = (1 - damping) / Double(allNodes.count)

        for _ in 0..<20 { // 20 iterations
            var newScores: [String: Double] = [:]

            for node in allNodes {
                var score = baseScore

                if let incoming = inEdges[node] {
                    for source in incoming {
                        if let sourceScore = scores[source], let sourceOut = outDegree[source], sourceOut > 0 {
                            score += damping * sourceScore / Double(sourceOut)
                        }
                    }
                }

                newScores[node] = score
            }

            scores = newScores
        }

        // Sort and output top N
        let ranked = scores.sorted { $0.value > $1.value }.prefix(topN)

        output.info("Top \(topN) nodes by PageRank:")
        for (i, (node, score)) in ranked.enumerated() {
            output.line("  \(i + 1). \(node) (score: \(String(format: "%.4f", score)))")
        }
    }

    private func executeCycleDetection(handler: GraphIndexHandler, schema: DynamicSchema) async throws {
        output.info("Cycle detection not yet implemented")
        // Would implement using DFS-based cycle detection
    }

    private func executeCommunityDetection(handler: GraphIndexHandler, schema: DynamicSchema) async throws {
        output.info("Community detection not yet implemented")
        // Would implement using Louvain or similar algorithm
    }

    private func executeSPARQL(handler: GraphIndexHandler, schema: DynamicSchema, args: [String], startIdx: Int) async throws {
        guard startIdx + 1 < args.count else {
            throw CLIError.invalidArguments("--sparql requires a query string")
        }

        var queryString = args[startIdx + 1]
        if queryString.hasPrefix("\"") && queryString.hasSuffix("\"") {
            queryString = String(queryString.dropFirst().dropLast())
        }

        output.info("SPARQL query execution not yet implemented")
        output.info("Query: \(queryString)")
        // Would implement SPARQL parser and executor
    }
}

// MARK: - Help

extension GraphCommands {
    public static var helpText: String {
        """
        Graph Commands:
          graph <Schema> from=<node>              Get outgoing edges from a node
          graph <Schema> to=<node>                Get incoming edges to a node
          graph <Schema> from=<node> --depth N    Traverse N hops from a node
          graph <Schema> --path from=<a> to=<b>   Find shortest path
          graph <Schema> --pagerank --top N       Compute PageRank
          graph <Schema> --cycles                 Detect cycles
          graph <Schema> --communities            Detect communities
          graph <Schema> --sparql "<query>"       Execute SPARQL query

        Examples:
          graph Follow from=alice
          graph Follow to=alice
          graph Follow from=alice --depth 3
          graph Follow --path from=alice to=frank
          graph Follow --pagerank --top 10
        """
    }
}
