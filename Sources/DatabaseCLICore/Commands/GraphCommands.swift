/// GraphCommands - Graph traversal and SPARQL queries using dynamic execution
///
/// Executes graph and SPARQL queries without compiled @Persistable types.
/// Uses TypeCatalog/IndexCatalog metadata to resolve index subspaces,
/// then delegates to non-generic GraphQueryExecutor / executeSPARQLString.

import Foundation
import GraphIndex
import Database
import Graph

struct GraphCommands {

    private let dataAccess: CatalogDataAccess
    private let output: OutputFormatter

    init(dataAccess: CatalogDataAccess, output: OutputFormatter) {
        self.dataAccess = dataAccess
        self.output = output
    }

    // MARK: - Graph Query

    /// Execute a graph traversal query
    ///
    /// Usage: `graph <TypeName> [from=<node>] [edge=<rel>] [to=<node>] [--limit N]`
    func execute(_ args: [String]) async throws {
        guard let typeName = args.first else {
            throw CLIError.invalidArguments("Usage: graph <TypeName> [from=<value>] [edge=<value>] [to=<value>] [--limit N]")
        }

        let catalog = try dataAccess.catalog(for: typeName)
        let meta = try dataAccess.graphIndexMetadata(for: catalog)

        // Parse arguments
        var fromValue: String?
        var edgeValue: String?
        var toValue: String?
        var limit: Int?

        let restArgs = Array(args.dropFirst())
        var i = 0
        while i < restArgs.count {
            let arg = restArgs[i]
            if arg.hasPrefix("from=") {
                fromValue = String(arg.dropFirst("from=".count))
            } else if arg.hasPrefix("edge=") {
                edgeValue = String(arg.dropFirst("edge=".count))
            } else if arg.hasPrefix("to=") {
                toValue = String(arg.dropFirst("to=".count))
            } else if arg == "--limit", i + 1 < restArgs.count {
                limit = Int(restArgs[i + 1])
                i += 1
            }
            i += 1
        }

        // Resolve index subspace
        let indexBase = try await dataAccess.indexSubspace(for: catalog)
        let indexSubspace = indexBase.subspace(meta.indexName)

        // Build executor
        var executor = GraphQueryExecutor(
            database: dataAccess.database,
            indexSubspace: indexSubspace,
            strategy: meta.strategy,
            fromFieldName: meta.fromField,
            edgeFieldName: meta.edgeField,
            toFieldName: meta.toField
        )

        if let v = fromValue { executor = executor.from(v) }
        if let v = edgeValue { executor = executor.edge(v) }
        if let v = toValue { executor = executor.to(v) }
        if let lim = limit { executor = executor.limit(lim) }

        // Execute
        let edges = try await executor.execute()

        // Display results
        if edges.isEmpty {
            output.info("(no results)")
            return
        }

        output.header("Graph query results (\(edges.count) edges)")
        for edge in edges {
            output.line("  \(edge.from) --[\(edge.edge)]--> \(edge.to)")
        }
    }

    // MARK: - SPARQL Query

    /// Execute a SPARQL query string
    ///
    /// Usage: `sparql <TypeName> <SPARQL query string>`
    func executeSPARQL(_ args: [String]) async throws {
        guard args.count >= 2 else {
            throw CLIError.invalidArguments("Usage: sparql <TypeName> <SPARQL query>")
        }

        let typeName = args[0]
        let sparqlString = args.dropFirst().joined(separator: " ")

        let catalog = try dataAccess.catalog(for: typeName)
        let meta = try dataAccess.graphIndexMetadata(for: catalog)

        // Resolve index subspace
        let indexBase = try await dataAccess.indexSubspace(for: catalog)
        let indexSubspace = indexBase.subspace(meta.indexName)

        // Execute SPARQL
        let result = try await executeSPARQLString(
            sparqlString,
            database: dataAccess.database,
            indexSubspace: indexSubspace,
            strategy: meta.strategy,
            fromFieldName: meta.fromField,
            edgeFieldName: meta.edgeField,
            toFieldName: meta.toField
        )

        // Display results
        if result.isEmpty {
            output.info("(no results)")
            return
        }

        output.header("SPARQL results (\(result.count) bindings, \(result.projectedVariables.joined(separator: ", ")))")

        // Print as aligned table
        let vars = result.projectedVariables
        let widths: [Int] = vars.map { v in
            max(v.count, result.bindings.map { $0.string(v)?.count ?? 4 }.max() ?? 4)
        }

        // Header
        let headerLine = zip(vars, widths).map { $0.0.padding(toLength: $0.1, withPad: " ", startingAt: 0) }.joined(separator: " | ")
        output.line(headerLine)
        output.line(widths.map { String(repeating: "-", count: $0) }.joined(separator: "-+-"))

        // Rows
        for binding in result.bindings {
            let row = zip(vars, widths).map { (pair: (String, Int)) -> String in
                let val = binding.string(pair.0) ?? "null"
                return val.padding(toLength: pair.1, withPad: " ", startingAt: 0)
            }.joined(separator: " | ")
            output.line(row)
        }

        // Statistics
        let stats = result.statistics
        output.line("\nStatistics: \(stats.patternsEvaluated) patterns, \(stats.indexScans) scans, \(String(format: "%.2f", Double(stats.durationNs) / 1_000_000))ms")
    }
}

// MARK: - Help

extension GraphCommands {
    static var helpText: String {
        """
        Graph Commands:
          graph <TypeName> [from=<value>] [edge=<value>] [to=<value>] [--limit N]
            Execute a graph traversal query.
            Patterns are optional; omit to wildcard.

            Examples:
              graph RDFTriple from=ex:Toyota
              graph RDFTriple from=ex:Toyota edge=rdf:type
              graph RDFTriple edge=ex:knows to=ex:Alice --limit 10

          sparql <TypeName> <SPARQL query>
            Execute a SPARQL SELECT query.

            Examples:
              sparql RDFTriple SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10
              sparql RDFTriple SELECT ?s WHERE { ?s <rdf:type> "ex:Company" }
        """
    }
}
