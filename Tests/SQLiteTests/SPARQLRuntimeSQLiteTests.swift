#if SQLITE
import Database
import DatabaseRuntime
import DatabaseTypes
import DatabaseWire
import Foundation
import TestHeartbeat
import Testing

@Persistable(type: "SQLiteSPARQLPrimaryStatement")
private struct SQLiteSPARQLPrimaryStatement {
    #Directory<SQLiteSPARQLPrimaryStatement>(
        "test",
        "sqlite_sparql_runtime",
        "primary"
    )
    #Index(
        .rdfDataset,
        from: \SQLiteSPARQLPrimaryStatement.subject,
        edge: \SQLiteSPARQLPrimaryStatement.predicate,
        to: \SQLiteSPARQLPrimaryStatement.object,
        graph: \SQLiteSPARQLPrimaryStatement.graph,
        name: "rdf_primary"
    )

    var id: String = UUID().uuidString
    var subject: RDFTerm = .iri(.xsdString)
    var predicate: RDFTerm = .iri(.xsdString)
    var object: RDFTerm = .iri(.xsdString)
    var graph: RDFTerm? = nil
}

@Persistable(type: "SQLiteSPARQLSecondaryStatement")
private struct SQLiteSPARQLSecondaryStatement {
    #Directory<SQLiteSPARQLSecondaryStatement>(
        "test",
        "sqlite_sparql_runtime",
        "secondary"
    )
    #Index(
        .rdfDataset,
        from: \SQLiteSPARQLSecondaryStatement.subject,
        edge: \SQLiteSPARQLSecondaryStatement.predicate,
        to: \SQLiteSPARQLSecondaryStatement.object,
        graph: \SQLiteSPARQLSecondaryStatement.graph,
        name: "rdf_secondary"
    )

    var id: String = UUID().uuidString
    var subject: RDFTerm = .iri(.xsdString)
    var predicate: RDFTerm = .iri(.xsdString)
    var object: RDFTerm = .iri(.xsdString)
    var graph: RDFTerm? = nil
}

@Suite("SQLite SPARQL runtime integration", .serialized, .heartbeat)
struct SPARQLRuntimeSQLiteTests {
    private let titlePredicate = "https://example.com/title"
    private let parentPredicate = "https://example.com/parent"
    private let detailPredicate = "https://example.com/detail"
    private let excludedForwardPredicate =
        "https://example.com/excluded-forward"
    private let excludedInversePredicate =
        "https://example.com/excluded-inverse"
    private let namedGraph = "https://example.com/graph/events"
    private let secondNamedGraph = "https://example.com/graph/archive"

    @Test("GRAPH variables keep property paths inside each named graph")
    func graphVariableKeepsPropertyPathsInsideNamedGraph() async throws {
        let context = try await seededContext()
        let pattern = ExecutionPattern.graph(
            .variable("?graph"),
            .propertyPath(
                subject: try executionValue(
                    "https://example.com/node/root"
                ),
                path: .oneOrMore(
                    .iri(try RDFPredicateIRI(parentPredicate))
                ),
                object: .variable("?node")
            )
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SQLiteSPARQLPrimaryStatement.self,
            projection: ["?graph", "?node"]
        )

        #expect(result.count == 1)
        #expect(
            result.first?["?graph"]
                == .rdfTerm(try .iri(validating: namedGraph))
        )
        #expect(
            result.first?["?node"]
                == .rdfTerm(
                    try .iri(validating: "https://example.com/node/middle")
                )
        )
    }

    @Test("One logical query joins multiple SQLite RDF dataset sources")
    func logicalQueryJoinsMultipleDatasetSources() async throws {
        let context = try await federatedContext()
        let sources = try await [
            datasetSource(
                for: SQLiteSPARQLPrimaryStatement.self,
                context: context
            ),
            datasetSource(
                for: SQLiteSPARQLSecondaryStatement.self,
                context: context
            ),
        ]
        let executor = SPARQLQueryExecutor(
            database: context.container.engine,
            wallClock: context.container.wallClock,
            sources: sources
        )
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?event"),
                predicate: try executionValue(detailPredicate),
                object: .variable("?detail")
            ),
            ExecutionTriple(
                subject: .variable("?detail"),
                predicate: try executionValue(titlePredicate),
                object: .variable("?title")
            ),
        ])

        let (bindings, _) = try await executor.execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: makeWorkMeter(for: context)
        )

        #expect(bindings.count == 1)
        #expect(
            bindings.first?["?event"]
                == .rdfTerm(
                    try .iri(validating: "https://example.com/event/federated")
                )
        )
        #expect(bindings.first?["?title"] == titleValue("Federated event"))
    }

    @Test("Bounded property paths preserve exact repetition depth on SQLite")
    func boundedPropertyPathPreservesExactDepth() async throws {
        let context = try await seededContext()
        let bounds = try PropertyPathRange(minimum: 2, maximum: 2)
        let pattern = ExecutionPattern.propertyPath(
            subject: try executionValue(
                "https://example.com/range/root"
            ),
            path: .range(
                .iri(try RDFPredicateIRI(parentPredicate)),
                bounds
            ),
            object: .variable("?node")
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SQLiteSPARQLPrimaryStatement.self,
            projection: ["?node"]
        )

        #expect(result.count == 1)
        #expect(
            result.first?["?node"]
                == .rdfTerm(
                    try .iri(validating: "https://example.com/range/leaf")
                )
        )
    }

    @Test("Negated property sets preserve forward and inverse directions")
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
            subject: try executionValue(
                "https://example.com/negated/root"
            ),
            path: .negatedPropertySet(exclusions),
            object: .variable("?node")
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SQLiteSPARQLPrimaryStatement.self,
            projection: ["?node"]
        )
        let values = Set(result.bindings.compactMap { $0["?node"] })

        #expect(values == Set([
            FieldValue.rdfTerm(
                try .iri(validating: "https://example.com/negated/forward")
            ),
            FieldValue.rdfTerm(
                try .iri(validating: "https://example.com/negated/inverse")
            ),
        ]))
    }

    private func seededContext() async throws -> DatabaseContext {
        let container = try await makeContainer(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteSPARQLPrimaryStatement.self)],
            entities: [try SQLiteSPARQLPrimaryStatement.schemaEntity]
        )
        let context = container.newContext()
        for statement in try primaryStatements() {
            try context.insert(statement)
        }
        try await context.save()
        return context
    }

    private func federatedContext() async throws -> DatabaseContext {
        let container = try await makeContainer(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteSPARQLPrimaryStatement.self), try DatabaseFrameworkRuntime.entity(SQLiteSPARQLSecondaryStatement.self)],
            entities: [
                try SQLiteSPARQLPrimaryStatement.schemaEntity,
                try SQLiteSPARQLSecondaryStatement.schemaEntity,
            ]
        )
        let context = container.newContext()
        try context.insert(
            try primaryStatement(
                id: "federated-link",
                subject: "https://example.com/event/federated",
                predicate: detailPredicate,
                object: "https://example.com/detail/federated"
            )
        )
        var titleStatement = SQLiteSPARQLSecondaryStatement()
        titleStatement.id = "federated-title"
        titleStatement.subject = try .iri(
            validating: "https://example.com/detail/federated"
        )
        titleStatement.predicate = try .iri(validating: titlePredicate)
        titleStatement.object = titleTerm("Federated event")
        try context.insert(titleStatement)
        try await context.save()
        return context
    }

    private func makeContainer(
        entityRuntimes: [EntityRuntimeRegistration],
        entities: [Schema.Entity]
    ) async throws -> DBContainer {
        let schema = try Schema(
            entities: entities,
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: entityRuntimes
            ),
            security: .disabled
        )
    }

    private func datasetSource<T: Persistable>(
        for type: T.Type,
        context: DatabaseContext
    ) async throws -> RDFDatasetSource {
        let selections = try T.indexDescriptors.compactMap(
            RDFDatasetIndexSelection.init(descriptor:)
        )
        guard selections.count == 1 else {
            throw SPARQLQueryError.indexNotConfigured
        }
        let selection = selections[0]
        let typeSubspace = try await context.indexQueryContext.indexSubspace(
            for: type
        )
        return try RDFDatasetSource(
            entityName: T.persistableType,
            selection: selection,
            indexSubspace: typeSubspace.subspace(selection.indexName)
        )
    }

    private func primaryStatements() throws -> [SQLiteSPARQLPrimaryStatement] {
        [
            try primaryStatement(
                id: "named-path-first",
                subject: "https://example.com/node/root",
                predicate: parentPredicate,
                object: "https://example.com/node/middle",
                graph: namedGraph
            ),
            try primaryStatement(
                id: "named-path-second",
                subject: "https://example.com/node/middle",
                predicate: parentPredicate,
                object: "https://example.com/node/leaf",
                graph: secondNamedGraph
            ),
            try primaryStatement(
                id: "range-first",
                subject: "https://example.com/range/root",
                predicate: parentPredicate,
                object: "https://example.com/range/middle"
            ),
            try primaryStatement(
                id: "range-second",
                subject: "https://example.com/range/middle",
                predicate: parentPredicate,
                object: "https://example.com/range/leaf"
            ),
            try primaryStatement(
                id: "range-third",
                subject: "https://example.com/range/leaf",
                predicate: parentPredicate,
                object: "https://example.com/range/beyond"
            ),
            try primaryStatement(
                id: "negated-forward-excluded",
                subject: "https://example.com/negated/root",
                predicate: excludedForwardPredicate,
                object: "https://example.com/negated/excluded-forward"
            ),
            try primaryStatement(
                id: "negated-forward-allowed",
                subject: "https://example.com/negated/root",
                predicate: "https://example.com/allowed-forward",
                object: "https://example.com/negated/forward"
            ),
            try primaryStatement(
                id: "negated-inverse-excluded",
                subject: "https://example.com/negated/excluded-inverse",
                predicate: excludedInversePredicate,
                object: "https://example.com/negated/root"
            ),
            try primaryStatement(
                id: "negated-inverse-allowed",
                subject: "https://example.com/negated/inverse",
                predicate: "https://example.com/allowed-inverse",
                object: "https://example.com/negated/root"
            ),
        ]
    }

    private func primaryStatement(
        id: String,
        subject: String,
        predicate: String,
        object: String,
        graph: String? = nil
    ) throws -> SQLiteSPARQLPrimaryStatement {
        var statement = SQLiteSPARQLPrimaryStatement()
        statement.id = id
        statement.subject = try .iri(validating: subject)
        statement.predicate = try .iri(validating: predicate)
        statement.object = try .iri(validating: object)
        if let graph {
            statement.graph = try .iri(validating: graph)
        }
        return statement
    }

    private func executionValue(_ iri: String) throws -> ExecutionTerm {
        .value(.rdfTerm(try .iri(validating: iri)))
    }

    private func titleTerm(_ lexicalForm: String) -> RDFTerm {
        .literal(
            RDFLiteral(
                lexicalForm: lexicalForm,
                datatype: .xsdString
            )
        )
    }

    private func titleValue(_ lexicalForm: String) -> FieldValue {
        .rdfTerm(titleTerm(lexicalForm))
    }

    private func makeWorkMeter(for context: DatabaseContext) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10_000,
                maximumWorkUnits: 100_000,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: context.container.monotonicClock
        )
    }
}
#endif
