#if SQLITE
import Database
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import Foundation
import TestSupport
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
        .graph(
            name: "rdf_quad",
            definition: .rdf(
                subject: \SQLiteNamedGraphStatement.subject,
                predicate: \SQLiteNamedGraphStatement.predicate,
                object: \SQLiteNamedGraphStatement.object,
        graph: \SQLiteNamedGraphStatement.graph)))

    var id: String = UUID().uuidString
    var subject: RDFTerm = .iri(.xsdString)
    var predicate: RDFTerm = .iri(.xsdString)
    var object: RDFTerm = .iri(.xsdString)
    var graph: RDFTerm? = nil
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
            return iri.rawValue
        })
        #expect(predicates == Set([
            amountPredicate,
            "https://example.com/settles",
        ]))
        #expect(!predicates.contains("https://example.com/issued-to"))

        let federated = try await context
            .sparql(namedGraph: try RDFGraphName(iri: receiptGraphIRI))
            .where("?subject", "?predicate", "?object")
            .select("?predicate")
            .distinct()
            .orderBy("?predicate")
            .offset(1)
            .limit(1)
            .execute()
        #expect(federated.count == 1)
        #expect(federated.bindings.first?.count == 1)
        #expect(
            federated.bindings.first?["?predicate"]
                == .rdfTerm(
                    try .iri(validating: "https://example.com/settles")
                )
        )

        let wrapperPredicate = try RDFTerm.iri(
            validating: "https://example.com/wrapper-rank"
        )
        for (index, rank) in ["3", "1", "2", "2"].enumerated() {
            try context.insert(
                statement(
                    id: "wrapper-\(index)",
                    graph: nil,
                    subject: try .iri(
                        validating: "https://example.com/wrapper/\(index)"
                    ),
                    predicate: wrapperPredicate,
                    object: integerLiteral(rank)
                )
            )
        }
        try await context.save()

        let typed = try await context
            .sparql(SQLiteNamedGraphStatement.self)
            .defaultIndex()
            .where(
                .variable("?subject"),
                .value(.rdfTerm(wrapperPredicate)),
                .variable("?rank")
            )
            .orderBy("?rank")
            .select("?rank")
            .distinct()
            .offset(1)
            .limit(1)
            .execute()
        #expect(typed.count == 1)
        #expect(typed.bindings.first?.count == 1)
        #expect(typed.bindings.first?["?rank"] == .rdfTerm(integerLiteral("2")))

        let grouped = try await context
            .sparql(SQLiteNamedGraphStatement.self)
            .defaultIndex()
            .where(
                .variable("?subject"),
                .value(.rdfTerm(wrapperPredicate)),
                .variable("?rank")
            )
            .groupBy("?rank")
            .count("?subject", as: "count")
            .orderBy("?rank")
            .distinct()
            .offset(1)
            .limit(1)
            .execute()
        #expect(grouped.count == 1)
        #expect(grouped.bindings.first?["?rank"] == .rdfTerm(integerLiteral("2")))
        #expect(grouped.bindings.first?.int("count") == 2)

        let string = try await context.executeSPARQL(
            """
            SELECT DISTINCT ?rank
            WHERE { ?subject <https://example.com/wrapper-rank> ?rank }
            ORDER BY ?rank
            LIMIT 1
            OFFSET 1
            """,
            on: SQLiteNamedGraphStatement.self
        )
        #expect(string.count == 1)
        #expect(string.bindings.first?.count == 1)
        #expect(string.bindings.first?["?rank"] == .rdfTerm(integerLiteral("2")))
    }

    @Test("SPARQL GRAPH variable binds graph names from graph-first keys")
    func sparqlGraphVariableBindsGraphNames() async throws {
        let context = try await seededContext()
        let basic = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?subject"),
                predicate: .value(.rdfTerm(try .iri(validating: amountPredicate))),
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
            return iri.rawValue
        })
        #expect(graphs == Set([invoiceGraphIRI, receiptGraphIRI]))
    }

    @Test("Canonical scanner applies graph and predicate constraints")
    func canonicalScannerAppliesGraphAndPredicateConstraints() async throws {
        let context = try await seededContext()
        let scanner = try await makeScanner(context: context)

        try await context.container.engine.withTransaction { transaction in
            let meter = DatabaseWorkMeter(
                budget: ExecutionBudget(
                    maximumRows: 10_000,
                    maximumWorkUnits: 100_000,
                    timeoutMilliseconds: 30_000
                ),
                monotonicClock: context.container.monotonicClock
            )
            let result = try await scanner.scan(
                subject: nil,
                predicate: try .iri(validating: amountPredicate),
                object: nil,
                graphTarget: .named(try RDFGraphName(iri: receiptGraphIRI)),
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            )

            #expect(result.physicalScanCount == 1)
            #expect(result.count == 1)
            let expectedSubject = try RDFTerm.iri(
                validating: "https://example.com/invoice"
            )
            let expectedPredicate = try RDFTerm.iri(
                validating: amountPredicate
            )
            let expectedGraph = try RDFTerm.iri(
                validating: receiptGraphIRI
            )
            result.withQuad(at: 0) { quad in
                #expect(
                    quad.subject.term == expectedSubject
                )
                #expect(
                    quad.predicate.term == expectedPredicate
                )
                #expect(
                    quad.graph?.term == expectedGraph
                )
            }
        }
    }

    @Test("Default graph patterns remain isolated from named graph quads")
    func defaultGraphPatternRemainsIsolated() async throws {
        let context = try await seededContext()
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?subject"),
                predicate: .value(.rdfTerm(try .iri(validating: amountPredicate))),
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
                == .rdfTerm(try .iri(validating: "https://example.com/default-invoice"))
        )
    }

    @Test("Canonical scanner enumerates every named graph exactly once")
    func canonicalScannerEnumeratesNamedGraphs() async throws {
        let context = try await seededContext()
        let scanner = try await makeScanner(context: context)

        try await context.container.engine.withTransaction { transaction in
            let meter = DatabaseWorkMeter(
                budget: ExecutionBudget(
                    maximumRows: 10_000,
                    maximumWorkUnits: 100_000,
                    timeoutMilliseconds: 30_000
                ),
                monotonicClock: context.container.monotonicClock
            )
            let graphs = try await scanner.namedGraphs(
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            )

            var retainedGraphs = Set<RDFGraphName>()
            for index in 0..<graphs.count {
                graphs.withGraph(at: index) { graph in
                    retainedGraphs.insert(copy graph)
                }
            }
            #expect(retainedGraphs == Set([
                try RDFGraphName(iri: invoiceGraphIRI),
                try RDFGraphName(iri: receiptGraphIRI),
                try RDFGraphName(iri: mailGraphIRI),
            ]))
            #expect(try await scanner.containsNamedGraph(
                try RDFGraphName(iri: invoiceGraphIRI),
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            ))
            #expect(try await scanner.containsNamedGraph(
                try RDFGraphName(iri: "https://example.com/graph/missing"),
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            ) == false)
        }
    }

    private func seededContext() async throws -> DatabaseContext {
        let schema = try Schema(
            entities: [try SQLiteNamedGraphStatement.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteNamedGraphStatement.self)]
            ),
            security: .testingDisabled
        )
        let context = container.testBaseContext()
        for statement in try statements() {
            try context.insert(statement)
        }
        try await context.save()
        return context
    }

    private func makeScanner(
        context: DatabaseContext
    ) async throws -> IndexedRDFDatasetScanner {
        let readableIndex = try await context.indexQueryContext.withReadableIndex(
            named: "rdf_quad",
            indexType: .graph(.rdf),
            for: SQLiteNamedGraphStatement.self
        ) { index, _ in
            index
        }
        guard let readableIndex else {
            throw SPARQLQueryError.indexNotConfigured
        }
        let source = RDFDatasetSource(
            entityName: SQLiteNamedGraphStatement.persistableType,
            indexName: "rdf_quad",
            indexSubspace: readableIndex.subspace,
            coverage: .dataset
        )
        return IndexedRDFDatasetScanner(sources: [source])
    }

    private func statements() throws -> [SQLiteNamedGraphStatement] {
        [
            statement(
                id: "invoice-amount",
                graph: try RDFGraphName(iri: invoiceGraphIRI),
                subject: try .iri(validating: "https://example.com/invoice"),
                predicate: try .iri(validating: amountPredicate),
                object: integerLiteral("100000")
            ),
            statement(
                id: "invoice-issued-to",
                graph: try RDFGraphName(iri: invoiceGraphIRI),
                subject: try .iri(validating: "https://example.com/invoice"),
                predicate: try .iri(validating: "https://example.com/issued-to"),
                object: try .iri(validating: "https://example.com/acme")
            ),
            statement(
                id: "receipt-settles",
                graph: try RDFGraphName(iri: receiptGraphIRI),
                subject: try .iri(validating: "https://example.com/receipt"),
                predicate: try .iri(validating: "https://example.com/settles"),
                object: try .iri(validating: "https://example.com/invoice")
            ),
            statement(
                id: "receipt-amount",
                graph: try RDFGraphName(iri: receiptGraphIRI),
                subject: try .iri(validating: "https://example.com/invoice"),
                predicate: try .iri(validating: amountPredicate),
                object: integerLiteral("100000")
            ),
            statement(
                id: "mail-mentions",
                graph: try RDFGraphName(iri: mailGraphIRI),
                subject: try .iri(validating: "https://example.com/mail"),
                predicate: try .iri(validating: "https://example.com/mentions"),
                object: try .iri(validating: "https://example.com/invoice")
            ),
            statement(
                id: "default-amount",
                graph: nil,
                subject: try .iri(validating: "https://example.com/default-invoice"),
                predicate: try .iri(validating: amountPredicate),
                object: integerLiteral("50000")
            ),
        ]
    }

    private func statement(
        id: String,
        graph: RDFGraphName?,
        subject: RDFTerm,
        predicate: RDFTerm,
        object: RDFTerm
    ) -> SQLiteNamedGraphStatement {
        var value = SQLiteNamedGraphStatement()
        value.id = id
        value.graph = graph?.term
        value.subject = subject
        value.predicate = predicate
        value.object = object
        return value
    }

    private func integerLiteral(_ lexicalForm: String) -> RDFTerm {
        .literal(
            RDFLiteral(
                lexicalForm: lexicalForm,
                datatype: XSDDatatype.integer.typedLiteralDatatype
            )
        )
    }
}
#endif
