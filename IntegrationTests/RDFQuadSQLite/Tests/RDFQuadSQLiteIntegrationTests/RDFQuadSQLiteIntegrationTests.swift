import Core
import DatabaseEngine
import DatabaseValue
import DatabaseWire
import Graph
import GraphIndex
import QueryIR
import SQLiteStorage
import Testing

@Suite("Canonical RDF quad SQLite integration", .serialized)
struct RDFQuadSQLiteIntegrationTests {
    private let titlePredicate = "https://example.com/title"
    private let parentPredicate = "https://example.com/parent"
    private let detailPredicate = "https://example.com/detail"
    private let excludedForwardPredicate = "https://example.com/excluded-forward"
    private let excludedInversePredicate = "https://example.com/excluded-inverse"
    private let namedGraph = "https://example.com/graph/events"
    private let secondNamedGraph = "https://example.com/graph/archive"

    @Test("SQLite preserves default and named graph SPARQL scopes")
    func sqlitePreservesDefaultAndNamedGraphScopes() async throws {
        let context = try await seededContext()

        let defaultGraphResult = try await context.executeSPARQLPattern(
            titlePattern(graph: nil),
            on: RDFQuadStatement.self,
            projection: ["?subject", "?title"]
        )
        let namedGraphResult = try await context.executeSPARQLPattern(
            try titlePattern(graph: RDFGraphName(iri: namedGraph)),
            on: RDFQuadStatement.self,
            projection: ["?subject", "?title"]
        )

        #expect(defaultGraphResult.count == 1)
        #expect(
            defaultGraphResult.first?["?subject"]
                == .rdfTerm(.iri("https://example.com/event/default"))
        )
        #expect(
            defaultGraphResult.first?["?title"]
                == titleValue("Default event")
        )

        #expect(namedGraphResult.count == 1)
        #expect(
            namedGraphResult.first?["?subject"]
                == .rdfTerm(.iri("https://example.com/event/named"))
        )
        #expect(
            namedGraphResult.first?["?title"]
                == titleValue("Named event")
        )
    }

    @Test("GRAPH variable keeps property paths inside each named graph")
    func graphVariableKeepsPropertyPathsInsideNamedGraph() async throws {
        let context = try await seededContext()
        let pattern = ExecutionPattern.graph(
            .variable("?graph"),
            .propertyPath(
                subject: .value(
                    .rdfTerm(.iri("https://example.com/node/root"))
                ),
                path: .oneOrMore(
                    .iri(try RDFPredicateIRI(parentPredicate))
                ),
                object: .variable("?node")
            )
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: RDFQuadStatement.self,
            projection: ["?graph", "?node"]
        )

        #expect(result.count == 1)
        #expect(
            result.first?["?graph"]
                == .rdfTerm(.iri(namedGraph))
        )
        #expect(
            result.first?["?node"]
                == .rdfTerm(.iri("https://example.com/node/middle"))
        )
    }

    @Test("SQLite enumerates and probes named graphs through graph-first keys")
    func sqliteEnumeratesAndProbesNamedGraphs() async throws {
        let context = try await seededContext()
        let scanner = IndexedRDFDatasetScanner(sources: [
            try await source(
                for: RDFQuadStatement.self,
                indexName: "rdf_quad",
                context: context
            ),
        ])

        try await context.container.engine.withTransaction { transaction in
            let workMeter = makeWorkMeter()
            let graphs = try await scanner.namedGraphs(
                limit: nil,
                transaction: transaction,
                workMeter: workMeter
            )
            let expected = try Set([
                RDFGraphName(iri: namedGraph),
                RDFGraphName(iri: secondNamedGraph),
            ])
            #expect(Set(graphs) == expected)
            #expect(try await scanner.containsNamedGraph(
                RDFGraphName(iri: namedGraph),
                transaction: transaction,
                workMeter: workMeter
            ))
            #expect(try await scanner.containsNamedGraph(
                RDFGraphName(iri: "https://example.com/graph/missing"),
                transaction: transaction,
                workMeter: workMeter
            ) == false)
        }
    }

    @Test("One logical query joins canonical quad indexes from multiple sources")
    func logicalQueryJoinsMultipleSources() async throws {
        let context = try await federatedContext()
        let sources = try await [
            source(
                for: RDFQuadStatement.self,
                indexName: "rdf_quad",
                context: context
            ),
            source(
                for: SecondaryRDFQuadStatement.self,
                indexName: "rdf_quad_secondary",
                context: context
            ),
        ]
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?event"),
                predicate: .value(.rdfTerm(.iri(detailPredicate))),
                object: .variable("?detail")
            ),
            ExecutionTriple(
                subject: .variable("?detail"),
                predicate: .value(.rdfTerm(.iri(titlePredicate))),
                object: .variable("?title")
            ),
        ])
        let executor = SPARQLQueryExecutor(
            database: context.container.engine,
            sources: sources
        )

        let (bindings, _) = try await executor.execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: makeWorkMeter()
        )

        #expect(bindings.count == 1)
        #expect(
            bindings.first?["?event"]
                == .rdfTerm(.iri("https://example.com/event/federated"))
        )
        #expect(bindings.first?["?title"] == titleValue("Federated event"))
    }

    @Test("Bounded property paths preserve exact repetition depth")
    func boundedPropertyPathPreservesExactDepth() async throws {
        let context = try await seededContext()
        let predicate = try RDFPredicateIRI(parentPredicate)
        let bounds = try PropertyPathRange(minimum: 2, maximum: 2)
        let pattern = ExecutionPattern.propertyPath(
            subject: .value(
                .rdfTerm(.iri("https://example.com/range/root"))
            ),
            path: .range(.iri(predicate), bounds),
            object: .variable("?node")
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: RDFQuadStatement.self,
            projection: ["?node"]
        )

        #expect(result.count == 1)
        #expect(
            result.first?["?node"]
                == .rdfTerm(.iri("https://example.com/range/leaf"))
        )
    }

    @Test("Negated property sets preserve forward and inverse exclusions")
    func negatedPropertySetPreservesDirections() async throws {
        let context = try await seededContext()
        let exclusions = try PropertyPathNegatedSet(
            forward: Set([
                try RDFPredicateIRI(excludedForwardPredicate),
            ]),
            inverse: Set([
                try RDFPredicateIRI(excludedInversePredicate),
            ])
        )
        let pattern = ExecutionPattern.propertyPath(
            subject: .value(
                .rdfTerm(.iri("https://example.com/negated/root"))
            ),
            path: .negatedPropertySet(exclusions),
            object: .variable("?node")
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: RDFQuadStatement.self,
            projection: ["?node"]
        )
        let values = Set(result.compactMap { $0["?node"] })

        #expect(values == Set([
            FieldValue.rdfTerm(.iri("https://example.com/negated/forward")),
            FieldValue.rdfTerm(.iri("https://example.com/negated/inverse")),
        ]))
    }

    private func seededContext() async throws -> FDBContext {
        let schema = Schema(
            [RDFQuadStatement.self],
            version: Schema.Version(1, 0, 0)
        )
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let container = try await DBContainer.open(
            for: schema,
            configuration: DBConfiguration(backend: .custom(engine)),
            runtimeConfiguration: try runtimeConfiguration(),
            security: .disabled
        )
        let context = container.newContext()
        context.insert(
            statement(
                id: "default",
                title: "Default event",
                graph: nil
            )
        )
        context.insert(
            statement(
                id: "named",
                title: "Named event",
                graph: .iri(namedGraph)
            )
        )
        context.insert(
            statement(
                id: "named-path-first",
                subject: .iri("https://example.com/node/root"),
                predicate: .iri(parentPredicate),
                object: .iri("https://example.com/node/middle"),
                graph: .iri(namedGraph)
            )
        )
        context.insert(
            statement(
                id: "named-path-second",
                subject: .iri("https://example.com/node/middle"),
                predicate: .iri(parentPredicate),
                object: .iri("https://example.com/node/leaf"),
                graph: .iri(secondNamedGraph)
            )
        )
        context.insert(
            statement(
                id: "range-first",
                subject: .iri("https://example.com/range/root"),
                predicate: .iri(parentPredicate),
                object: .iri("https://example.com/range/middle"),
                graph: nil
            )
        )
        context.insert(
            statement(
                id: "range-second",
                subject: .iri("https://example.com/range/middle"),
                predicate: .iri(parentPredicate),
                object: .iri("https://example.com/range/leaf"),
                graph: nil
            )
        )
        context.insert(
            statement(
                id: "range-third",
                subject: .iri("https://example.com/range/leaf"),
                predicate: .iri(parentPredicate),
                object: .iri("https://example.com/range/beyond"),
                graph: nil
            )
        )
        context.insert(
            statement(
                id: "negated-forward-excluded",
                subject: .iri("https://example.com/negated/root"),
                predicate: .iri(excludedForwardPredicate),
                object: .iri("https://example.com/negated/excluded-forward"),
                graph: nil
            )
        )
        context.insert(
            statement(
                id: "negated-forward-allowed",
                subject: .iri("https://example.com/negated/root"),
                predicate: .iri("https://example.com/allowed-forward"),
                object: .iri("https://example.com/negated/forward"),
                graph: nil
            )
        )
        context.insert(
            statement(
                id: "negated-inverse-excluded",
                subject: .iri("https://example.com/negated/excluded-inverse"),
                predicate: .iri(excludedInversePredicate),
                object: .iri("https://example.com/negated/root"),
                graph: nil
            )
        )
        context.insert(
            statement(
                id: "negated-inverse-allowed",
                subject: .iri("https://example.com/negated/inverse"),
                predicate: .iri("https://example.com/allowed-inverse"),
                object: .iri("https://example.com/negated/root"),
                graph: nil
            )
        )
        try await context.save()
        return context
    }

    private func federatedContext() async throws -> FDBContext {
        let schema = Schema(
            [RDFQuadStatement.self, SecondaryRDFQuadStatement.self],
            version: Schema.Version(1, 0, 0)
        )
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let container = try await DBContainer.open(
            for: schema,
            configuration: DBConfiguration(backend: .custom(engine)),
            runtimeConfiguration: try runtimeConfiguration(),
            security: .disabled
        )
        let context = container.newContext()

        context.insert(
            statement(
                id: "federated-link",
                subject: .iri("https://example.com/event/federated"),
                predicate: .iri(detailPredicate),
                object: .iri("https://example.com/detail/federated"),
                graph: nil
            )
        )
        var titleStatement = SecondaryRDFQuadStatement(
            subject: .iri("https://example.com/detail/federated"),
            predicate: .iri(titlePredicate),
            object: titleTerm("Federated event"),
            graph: nil
        )
        titleStatement.id = "federated-title"
        context.insert(titleStatement)
        try await context.save()
        return context
    }

    private func source<T: Persistable>(
        for type: T.Type,
        indexName: String,
        context: FDBContext
    ) async throws -> RDFDatasetSource {
        let typeSubspace = try await context.indexQueryContext.indexSubspace(
            for: type
        )
        return RDFDatasetSource(
            entityName: T.persistableType,
            indexName: indexName,
            indexSubspace: typeSubspace.subspace(indexName),
            coverage: .dataset
        )
    }

    private func titlePattern(
        graph: RDFGraphName?
    ) throws -> ExecutionPattern {
        let basic = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?subject"),
                predicate: .value(.rdfTerm(.iri(titlePredicate))),
                object: .variable("?title")
            ),
        ])
        if let graph {
            return .graph(.named(graph), basic)
        }
        return basic
    }

    private func statement(
        id: String,
        title: String,
        graph: DatabaseRDFTerm?
    ) -> RDFQuadStatement {
        var statement = RDFQuadStatement(
            subject: DatabaseRDFTerm.iri(
                "https://example.com/event/\(id)"
            ),
            predicate: DatabaseRDFTerm.iri(titlePredicate),
            object: titleTerm(title),
            graph: graph
        )
        statement.id = id
        return statement
    }

    private func statement(
        id: String,
        subject: DatabaseRDFTerm,
        predicate: DatabaseRDFTerm,
        object: DatabaseRDFTerm,
        graph: DatabaseRDFTerm?
    ) -> RDFQuadStatement {
        var statement = RDFQuadStatement(
            subject: subject,
            predicate: predicate,
            object: object,
            graph: graph
        )
        statement.id = id
        return statement
    }

    private func titleTerm(_ lexicalForm: String) -> DatabaseRDFTerm {
        .literal(
            RDFLiteral(
                lexicalForm: lexicalForm,
                datatype: .xsdString
            )
        )
    }

    private func makeWorkMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: DatabaseExecutionBudget(
                maximumRows: 10_000,
                maximumWorkUnits: 100_000,
                timeoutMilliseconds: 30_000
            )
        )
    }

    private func runtimeConfiguration() throws -> DatabaseRuntimeConfiguration {
        try DatabaseRuntimeConfiguration(
            indexMaintainerProviders: [RDFQuadIndexMaintainerProvider()],
            graphTableSourceExecutor: GraphTableReadBridge.sourceExecutor,
            sparqlSourceExecutor: SPARQLReadBridge.sourceExecutor(
                functionRegistry: .empty
            )
        )
    }

    private func titleValue(_ lexicalForm: String) -> FieldValue {
        .rdfTerm(titleTerm(lexicalForm))
    }
}
