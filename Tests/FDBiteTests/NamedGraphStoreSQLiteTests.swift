#if SQLITE
import Foundation
import Testing
import Database
import TestHeartbeat

@Persistable
private struct SQLiteNamedGraphStatement {
    #Directory<SQLiteNamedGraphStatement>("test", "sqlite_named_graph_store", "statements")

    var id: String = UUID().uuidString
    var subject: String = ""
    var predicate: String = ""
    var object: String = ""
    var graph: String = ""
    var confidence: Int = 0

    #Index(GraphIndexKind<SQLiteNamedGraphStatement>(
        from: \.subject,
        edge: \.predicate,
        to: \.object,
        graph: \.graph,
        strategy: .namedGraphStore
    ), storedFields: [\SQLiteNamedGraphStatement.confidence], name: "named_graph_store")
}

@Suite("NamedGraphStore SQLite Tests", .serialized, .heartbeat)
struct NamedGraphStoreSQLiteTests {
    private func makeContainer() async throws -> DBContainer {
        let schema = Schema(
            [SQLiteNamedGraphStatement.self],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.inMemory(for: schema, security: .disabled)
    }

    private func makeStatement(
        graph: String,
        subject: String,
        predicate: String,
        object: String,
        confidence: Int
    ) -> SQLiteNamedGraphStatement {
        var statement = SQLiteNamedGraphStatement()
        statement.graph = graph
        statement.subject = subject
        statement.predicate = predicate
        statement.object = object
        statement.confidence = confidence
        return statement
    }

    private func seed(_ context: FDBContext) async throws {
        let statements = [
            makeStatement(
                graph: "doc:invoice",
                subject: "ex:invoice",
                predicate: "ex:amount",
                object: "100000",
                confidence: 97
            ),
            makeStatement(
                graph: "doc:invoice",
                subject: "ex:invoice",
                predicate: "ex:issuedTo",
                object: "ex:acme",
                confidence: 95
            ),
            makeStatement(
                graph: "doc:receipt",
                subject: "ex:receipt",
                predicate: "ex:settles",
                object: "ex:invoice",
                confidence: 99
            ),
            makeStatement(
                graph: "doc:receipt",
                subject: "ex:invoice",
                predicate: "ex:amount",
                object: "100000",
                confidence: 92
            ),
            makeStatement(
                graph: "doc:mail",
                subject: "ex:mail",
                predicate: "ex:mentions",
                object: "ex:invoice",
                confidence: 70
            ),
        ]

        for statement in statements {
            context.insert(statement)
        }
        try await context.save()
    }

    @Test("SPARQL GRAPH value scans only the selected named graph")
    func sparqlGraphValueFiltersSelectedNamedGraph() async throws {
        let container = try await makeContainer()
        let context = container.newContext()
        try await seed(context)

        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: .variable("?p"),
                object: .variable("?o"),
                graph: .value(.string("doc:receipt"))
            )
        ])

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SQLiteNamedGraphStatement.self,
            projection: ["?s", "?p", "?o"]
        )

        #expect(result.count == 2)
        let predicates = Set(result.bindings.compactMap { $0["?p"]?.stringValue })
        #expect(predicates == Set(["ex:amount", "ex:settles"]))
        #expect(!result.bindings.contains { $0["?p"]?.stringValue == "ex:issuedTo" })
    }

    @Test("SPARQL GRAPH variable binds graph names from graph-first keys")
    func sparqlGraphVariableBindsGraphNames() async throws {
        let container = try await makeContainer()
        let context = container.newContext()
        try await seed(context)

        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: .value(.string("ex:amount")),
                object: .variable("?o"),
                graph: .variable("?g")
            )
        ])

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SQLiteNamedGraphStatement.self,
            projection: ["?s", "?o", "?g"]
        )

        #expect(result.count == 2)
        let graphs = Set(result.bindings.compactMap { $0["?g"]?.stringValue })
        #expect(graphs == Set(["doc:invoice", "doc:receipt"]))
    }

    @Test("GraphPropertyScanner applies graph prefix and stored property filters")
    func graphPropertyScannerFiltersByGraphAndStoredProperties() async throws {
        let container = try await makeContainer()
        let context = container.newContext()
        try await seed(context)

        try await container.engine.withTransaction { transaction in
            let subspace = try await container.resolveDirectory(for: SQLiteNamedGraphStatement.self)
            let graphIndexSubspace = subspace.subspace("I").subspace("named_graph_store")
            let scanner = GraphPropertyScanner(
                indexSubspace: graphIndexSubspace,
                strategy: .namedGraphStore,
                storedFieldNames: ["confidence"]
            )
            let filters = [
                PropertyFilter(
                    fieldName: "confidence",
                    op: .greaterThanOrEqual,
                    value: .int64(95)
                )
            ]

            var edges: [GraphEdgeWithProperties] = []
            for try await edge in scanner.scanEdges(
                from: nil,
                edge: nil,
                to: "ex:invoice",
                graph: "doc:receipt",
                propertyFilters: filters,
                transaction: transaction
            ) {
                edges.append(edge)
            }

            #expect(edges.count == 1)
            #expect(edges[0].source == "ex:receipt")
            #expect(edges[0].edgeLabel == "ex:settles")
            #expect(edges[0].target == "ex:invoice")
            #expect(edges[0].graph == "doc:receipt")
            #expect(edges[0].properties["confidence"] as? Int64 == 99)
        }
    }

    @Test("Graph query builder can read namedGraphStore through graph-agnostic scans")
    func graphQueryBuilderReadsNamedGraphStore() async throws {
        let container = try await makeContainer()
        let context = container.newContext()
        try await seed(context)

        let edges = try await context.graph(SQLiteNamedGraphStatement.self)
            .defaultIndex()
            .from("ex:receipt")
            .edge("ex:settles")
            .execute()

        #expect(edges.count == 1)
        #expect(edges[0].from == "ex:receipt")
        #expect(edges[0].edge == "ex:settles")
        #expect(edges[0].to == "ex:invoice")
    }

    @Test("GraphEdgeScanner remains usable for graph-agnostic traversals")
    func graphEdgeScannerSupportsGraphAgnosticTraversal() async throws {
        let container = try await makeContainer()
        let context = container.newContext()
        try await seed(context)

        try await container.engine.withTransaction { transaction in
            let subspace = try await container.resolveDirectory(for: SQLiteNamedGraphStatement.self)
            let graphIndexSubspace = subspace.subspace("I").subspace("named_graph_store")
            let scanner = GraphEdgeScanner(
                indexSubspace: graphIndexSubspace,
                strategy: .namedGraphStore
            )

            var outgoing: [EdgeInfo] = []
            for try await edge in scanner.scanOutgoing(
                from: "ex:receipt",
                edgeLabel: "ex:settles",
                transaction: transaction
            ) {
                outgoing.append(edge)
            }

            #expect(outgoing == [
                EdgeInfo(source: "ex:receipt", target: "ex:invoice", edgeLabel: "ex:settles")
            ])

            var incoming: [EdgeInfo] = []
            for try await edge in scanner.scanIncoming(
                to: "ex:invoice",
                edgeLabel: nil,
                transaction: transaction
            ) {
                incoming.append(edge)
            }

            let incomingLabels = Set(incoming.map(\.edgeLabel))
            #expect(incoming.count == 2)
            #expect(incomingLabels == Set(["ex:mentions", "ex:settles"]))
        }
    }
}
#endif
