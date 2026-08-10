#if SQLITE
import Database
import DatabaseRuntime
import DatabaseTypes
import Foundation
import TestSupport
import TestHeartbeat
import Testing

@Persistable
private struct SQLiteRDFQuadStatement {
    #Directory<SQLiteRDFQuadStatement>("test", "rdf_quad", "statements")
    #Index(
        .rdfDataset,
        from: \SQLiteRDFQuadStatement.subject,
        edge: \SQLiteRDFQuadStatement.predicate,
        to: \SQLiteRDFQuadStatement.object,
        graph: \SQLiteRDFQuadStatement.graph,
        name: "rdf_quad"
    )

    var id: String = UUID().uuidString
    var subject: RDFTerm = .iri(.xsdString)
    var predicate: RDFTerm = .iri(.xsdString)
    var object: RDFTerm = .iri(.xsdString)
    var graph: RDFTerm? = nil
}

@Suite("Canonical RDF quad SQLite", .serialized, .heartbeat)
struct RDFQuadSQLiteTests {
    private let titlePredicate = "https://example.com/title"
    private let firstGraph = "https://example.com/graph/first"
    private let secondGraph = "https://example.com/graph/second"

    @Test("Named graph scans use graph-first canonical keys")
    func namedGraphScanUsesGraphFirstKeys() async throws {
        let context = try await seededContext()
        let basic = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?subject"),
                predicate: .value(.rdfTerm(try .iri(validating: titlePredicate))),
                object: .variable("?title")
            )
        ])
        let pattern = ExecutionPattern.graph(
            .named(try RDFGraphName(iri: firstGraph)),
            basic
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SQLiteRDFQuadStatement.self,
            projection: ["?subject", "?title"]
        )

        #expect(result.count == 1)
        #expect(
            result.first?["?subject"]
                == .rdfTerm(try .iri(validating: "https://example.com/event/first"))
        )
        #expect(
            result.first?["?title"]
                == .rdfTerm(
                    .literal(
                        RDFLiteral(
                            lexicalForm: "First",
                            datatype: .xsdString
                        )
                    )
                )
        )
    }

    @Test("Default graph patterns exclude named graph quads")
    func defaultGraphPatternExcludesNamedGraphs() async throws {
        let context = try await seededContext()
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?subject"),
                predicate: .value(.rdfTerm(try .iri(validating: titlePredicate))),
                object: .variable("?title")
            )
        ])

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SQLiteRDFQuadStatement.self,
            projection: ["?subject", "?title"]
        )

        #expect(result.count == 1)
        #expect(
            result.first?["?subject"]
                == .rdfTerm(try .iri(validating: "https://example.com/event/default"))
        )
    }

    @Test("Graph variables bind named graphs and exclude the default graph")
    func graphVariableBindsNamedGraphs() async throws {
        let context = try await seededContext()
        let basic = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?subject"),
                predicate: .value(.rdfTerm(try .iri(validating: titlePredicate))),
                object: .variable("?title")
            )
        ])
        let pattern = ExecutionPattern.graph(.variable("?graph"), basic)

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SQLiteRDFQuadStatement.self,
            projection: ["?graph"]
        )
        let graphs = Set(result.bindings.compactMap { binding -> String? in
            guard case .rdfTerm(.iri(let iri)) = binding["?graph"] else {
                return nil
            }
            return iri.rawValue
        })

        #expect(graphs == Set([firstGraph, secondGraph]))
    }

    private func seededContext() async throws -> DatabaseContext {
        let schema = try Schema(
            entities: [try SQLiteRDFQuadStatement.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteRDFQuadStatement.self)]
            ),
            security: .testingDisabled
        )
        let context = container.testBaseContext()
        try context.insert(
            statement(
                event: "first",
                title: "First",
                graph: try RDFGraphName(iri: firstGraph)
            )
        )
        try context.insert(
            statement(
                event: "second",
                title: "Second",
                graph: try RDFGraphName(iri: secondGraph)
            )
        )
        try context.insert(statement(event: "default", title: "Default", graph: nil))
        try await context.save()
        return context
    }

    private func statement(
        event: String,
        title: String,
        graph: RDFGraphName?
    ) throws -> SQLiteRDFQuadStatement {
        var statement = SQLiteRDFQuadStatement()
        statement.subject = try .iri(validating: "https://example.com/event/\(event)")
        statement.predicate = try .iri(validating: titlePredicate)
        statement.object = .literal(
            RDFLiteral(
                lexicalForm: title,
                datatype: .xsdString
            )
        )
        statement.graph = graph?.term
        return statement
    }
}
#endif
