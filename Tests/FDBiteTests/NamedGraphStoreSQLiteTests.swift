#if SQLITE
import Database
import DatabaseValue
import Foundation
import TestHeartbeat
import Testing

@Persistable
private struct SQLiteNamedGraphStatement {
    #Directory<SQLiteNamedGraphStatement>(
        "test",
        "sqlite_named_graph_dataset",
        "statements"
    )
    #Index(
        RDFQuadIndexKind<SQLiteNamedGraphStatement>(
            subject: \.subject,
            predicate: \.predicate,
            object: \.object,
            graph: \.graph
        ),
        name: "rdf_quad"
    )

    var id: String = UUID().uuidString
    var subject: DatabaseRDFTerm = .iri("https://example.com/resource")
    var predicate: DatabaseRDFTerm = .iri("https://example.com/predicate")
    var object: DatabaseRDFTerm = .iri("https://example.com/object")
    var graph: DatabaseRDFTerm? = nil
}

@Suite("Canonical named graph SQLite", .serialized, .heartbeat)
struct NamedGraphStoreSQLiteTests {
    private let amountPredicate = "https://example.com/amount"
    private let invoiceGraphIRI = "https://example.com/graph/invoice"
    private let mailGraphIRI = "https://example.com/graph/mail"
    private let receiptGraphIRI = "https://example.com/graph/receipt"

    @Test("SPARQL GRAPH value scans only the selected named graph")
    func sparqlGraphValueFiltersSelectedNamedGraph() async throws {
        let context = try await seededContext()
        let pattern = ExecutionPattern.graph(
            .named(try RDFGraphName(iri: receiptGraphIRI)),
            .basic([
                ExecutionTriple(
                    subject: .variable("?subject"),
                    predicate: .variable("?predicate"),
                    object: .variable("?object")
                )
            ])
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SQLiteNamedGraphStatement.self,
            projection: ["?subject", "?predicate", "?object"]
        )

        #expect(result.count == 2)
        let predicates = Set(result.bindings.compactMap { binding -> String? in
            guard case .rdfTerm(.iri(let iri)) = binding["?predicate"] else {
                return nil
            }
            return iri
        })
        #expect(predicates == Set([
            amountPredicate,
            "https://example.com/settles",
        ]))
        #expect(!predicates.contains("https://example.com/issued-to"))
    }

    @Test("SPARQL GRAPH variable binds graph names from graph-first keys")
    func sparqlGraphVariableBindsGraphNames() async throws {
        let context = try await seededContext()
        let basic = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?subject"),
                predicate: .value(.rdfTerm(.iri(amountPredicate))),
                object: .variable("?object")
            )
        ])
        let pattern = ExecutionPattern.graph(.variable("?graph"), basic)

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SQLiteNamedGraphStatement.self,
            projection: ["?subject", "?object", "?graph"]
        )

        #expect(result.count == 2)
        let graphs = Set(result.bindings.compactMap { binding -> String? in
            guard case .rdfTerm(.iri(let iri)) = binding["?graph"] else {
                return nil
            }
            return iri
        })
        #expect(graphs == Set([invoiceGraphIRI, receiptGraphIRI]))
    }

    @Test("Canonical scanner applies graph and predicate constraints")
    func canonicalScannerAppliesGraphAndPredicateConstraints() async throws {
        let context = try await seededContext()
        let scanner = try await makeScanner(context: context)

        try await context.container.engine.withTransaction { transaction in
            let meter = DatabaseWorkMeter(
                budget: DatabaseExecutionBudget(
                    maximumRows: 10_000,
                    maximumWorkUnits: 100_000,
                    timeoutMilliseconds: 30_000
                )
            )
            let result = try await scanner.scan(
                subject: nil,
                predicate: .iri(amountPredicate),
                object: nil,
                graphScope: .named(try RDFGraphName(iri: receiptGraphIRI)),
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            )

            #expect(result.physicalScanCount == 1)
            #expect(result.count == 1)
            #expect(result[0].subject == .iri("https://example.com/invoice"))
            #expect(result[0].predicate == .iri(amountPredicate))
            #expect(result[0].graph == .iri(receiptGraphIRI))
        }
    }

    @Test("Default graph patterns remain isolated from named graph quads")
    func defaultGraphPatternRemainsIsolated() async throws {
        let context = try await seededContext()
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?subject"),
                predicate: .value(.rdfTerm(.iri(amountPredicate))),
                object: .variable("?object")
            )
        ])

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SQLiteNamedGraphStatement.self,
            projection: ["?subject", "?object"]
        )

        #expect(result.count == 1)
        #expect(
            result.first?["?subject"]
                == .rdfTerm(.iri("https://example.com/default-invoice"))
        )
    }

    @Test("Canonical scanner enumerates every named graph exactly once")
    func canonicalScannerEnumeratesNamedGraphs() async throws {
        let context = try await seededContext()
        let scanner = try await makeScanner(context: context)

        try await context.container.engine.withTransaction { transaction in
            let meter = DatabaseWorkMeter(
                budget: DatabaseExecutionBudget(
                    maximumRows: 10_000,
                    maximumWorkUnits: 100_000,
                    timeoutMilliseconds: 30_000
                )
            )
            let graphs = try await scanner.namedGraphs(
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            )

            #expect(Set(graphs) == Set([
                try RDFGraphName(iri: invoiceGraphIRI),
                try RDFGraphName(iri: receiptGraphIRI),
                try RDFGraphName(iri: mailGraphIRI),
            ]))
            #expect(try await scanner.containsNamedGraph(
                try RDFGraphName(iri: invoiceGraphIRI),
                transaction: transaction,
                workMeter: meter
            ))
            #expect(try await scanner.containsNamedGraph(
                try RDFGraphName(iri: "https://example.com/graph/missing"),
                transaction: transaction,
                workMeter: meter
            ) == false)
        }
    }

    private func seededContext() async throws -> FDBContext {
        let schema = Schema(
            [SQLiteNamedGraphStatement.self],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.inMemory(
            for: schema,
            security: .disabled
        )
        let context = container.newContext()
        for statement in try statements() {
            try context.insert(statement)
        }
        try await context.save()
        return context
    }

    private func makeScanner(
        context: FDBContext
    ) async throws -> IndexedRDFDatasetScanner {
        let typeSubspace = try await context.indexQueryContext.indexSubspace(
            for: SQLiteNamedGraphStatement.self
        )
        let source = RDFDatasetSource(
            entityName: SQLiteNamedGraphStatement.persistableType,
            indexName: "rdf_quad",
            indexSubspace: typeSubspace.subspace("rdf_quad"),
            coverage: .dataset
        )
        return IndexedRDFDatasetScanner(sources: [source])
    }

    private func statements() throws -> [SQLiteNamedGraphStatement] {
        [
            statement(
                id: "invoice-amount",
                graph: try RDFGraphName(iri: invoiceGraphIRI),
                subject: .iri("https://example.com/invoice"),
                predicate: .iri(amountPredicate),
                object: integerLiteral("100000")
            ),
            statement(
                id: "invoice-issued-to",
                graph: try RDFGraphName(iri: invoiceGraphIRI),
                subject: .iri("https://example.com/invoice"),
                predicate: .iri("https://example.com/issued-to"),
                object: .iri("https://example.com/acme")
            ),
            statement(
                id: "receipt-settles",
                graph: try RDFGraphName(iri: receiptGraphIRI),
                subject: .iri("https://example.com/receipt"),
                predicate: .iri("https://example.com/settles"),
                object: .iri("https://example.com/invoice")
            ),
            statement(
                id: "receipt-amount",
                graph: try RDFGraphName(iri: receiptGraphIRI),
                subject: .iri("https://example.com/invoice"),
                predicate: .iri(amountPredicate),
                object: integerLiteral("100000")
            ),
            statement(
                id: "mail-mentions",
                graph: try RDFGraphName(iri: mailGraphIRI),
                subject: .iri("https://example.com/mail"),
                predicate: .iri("https://example.com/mentions"),
                object: .iri("https://example.com/invoice")
            ),
            statement(
                id: "default-amount",
                graph: nil,
                subject: .iri("https://example.com/default-invoice"),
                predicate: .iri(amountPredicate),
                object: integerLiteral("50000")
            ),
        ]
    }

    private func statement(
        id: String,
        graph: RDFGraphName?,
        subject: DatabaseRDFTerm,
        predicate: DatabaseRDFTerm,
        object: DatabaseRDFTerm
    ) -> SQLiteNamedGraphStatement {
        var value = SQLiteNamedGraphStatement()
        value.id = id
        value.graph = graph?.term
        value.subject = subject
        value.predicate = predicate
        value.object = object
        return value
    }

    private func integerLiteral(_ lexicalForm: String) -> DatabaseRDFTerm {
        .literal(
            DatabaseRDFLiteral(
                lexicalForm: lexicalForm,
                datatype: "http://www.w3.org/2001/XMLSchema#integer"
            )
        )
    }
}
#endif
