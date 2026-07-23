#if FOUNDATION_DB
import Core
import DatabaseRuntime
import DatabaseValue
import DatabaseValueCodable
import FDBStorage
import Foundation
import Graph
import StorageKit
import TestSupport
import Testing
@testable import DatabaseEngine
@testable import GraphIndex

@Persistable
private struct SPARQLQuadStatement {
    #Directory<SPARQLQuadStatement>("named_graph_sparql_tests")
    #Index(
        RDFQuadIndexKind<SPARQLQuadStatement>(
            subject: \.subject,
            predicate: \.predicate,
            object: \.object,
            graph: \.graph
        ),
        name: "rdf_quad"
    )

    var id: String = ULID().ulidString
    var subject: DatabaseRDFTerm = .iri("https://example.com/resource")
    var predicate: DatabaseRDFTerm = .iri("https://example.com/predicate")
    var object: DatabaseRDFTerm = .iri("https://example.com/object")
    var graph: DatabaseRDFTerm? = nil
}

@Suite("Canonical Named Graph SPARQL Integration", .serialized, .heartbeat)
struct NamedGraphSPARQLTests {
    private let alice = "https://example.com/person/alice"
    private let bob = "https://example.com/person/bob"
    private let carol = "https://example.com/person/carol"
    private let acme = "https://example.com/organization/acme"
    private let beta = "https://example.com/organization/beta"
    private let knows = "https://example.com/vocabulary/knows"
    private let worksAt = "https://example.com/vocabulary/worksAt"
    private let socialGraph = "https://example.com/graph/social"
    private let workGraph = "https://example.com/graph/work"

    @Test("Default graph excludes named graph quads")
    func defaultGraphExcludesNamedGraphQuads() async throws {
        let context = try await seededContext()
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?subject"),
                predicate: .variable("?predicate"),
                object: .variable("?object")
            )
        ])

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SPARQLQuadStatement.self,
            projection: ["?subject", "?predicate", "?object"]
        )

        #expect(result.count == 1)
        #expect(result.first?["?subject"] == .rdfTerm(.iri(carol)))
    }

    @Test("Named graph selector isolates one active graph")
    func namedGraphSelectorIsolatesOneActiveGraph() async throws {
        let context = try await seededContext()
        let basic = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?subject"),
                predicate: .value(.rdfTerm(.iri(knows))),
                object: .variable("?object")
            )
        ])
        let pattern = ExecutionPattern.graph(
            .named(try RDFGraphName(iri: socialGraph)),
            basic
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SPARQLQuadStatement.self,
            projection: ["?subject", "?object"]
        )

        #expect(result.count == 3)
        #expect(
            Set(result.bindings.compactMap { iri($0["?object"]) })
                == Set([bob, carol])
        )
    }

    @Test("Graph variable binds only named graphs")
    func graphVariableBindsOnlyNamedGraphs() async throws {
        let context = try await seededContext()
        let basic = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?subject"),
                predicate: .variable("?predicate"),
                object: .variable("?object")
            )
        ])
        let pattern = ExecutionPattern.graph(.variable("?graph"), basic)

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SPARQLQuadStatement.self,
            projection: ["?graph"]
        )

        #expect(result.count == 5)
        #expect(
            Set(result.bindings.compactMap { iri($0["?graph"]) })
                == Set([socialGraph, workGraph])
        )
    }

    @Test("Join evaluates both triples in one named active graph")
    func joinEvaluatesBothTriplesInOneNamedActiveGraph() async throws {
        let context = try await seededContext()
        let left = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .value(.rdfTerm(.iri(alice))),
                predicate: .value(.rdfTerm(.iri(knows))),
                object: .variable("?middle")
            )
        ])
        let right = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?middle"),
                predicate: .value(.rdfTerm(.iri(knows))),
                object: .variable("?friend")
            )
        ])
        let pattern = ExecutionPattern.graph(
            .named(try RDFGraphName(iri: socialGraph)),
            .join(left, right)
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SPARQLQuadStatement.self,
            projection: ["?middle", "?friend"]
        )

        #expect(result.count == 1)
        #expect(result.first?["?middle"] == .rdfTerm(.iri(bob)))
        #expect(result.first?["?friend"] == .rdfTerm(.iri(carol)))
    }

    @Test("Optional joins across explicit named graph scopes")
    func optionalJoinsAcrossExplicitNamedGraphScopes() async throws {
        let context = try await seededContext()
        let social = ExecutionPattern.graph(
            .named(try RDFGraphName(iri: socialGraph)),
            .basic([
                ExecutionTriple(
                    subject: .variable("?subject"),
                    predicate: .value(.rdfTerm(.iri(knows))),
                    object: .variable("?friend")
                )
            ])
        )
        let work = ExecutionPattern.graph(
            .named(try RDFGraphName(iri: workGraph)),
            .basic([
                ExecutionTriple(
                    subject: .variable("?subject"),
                    predicate: .value(.rdfTerm(.iri(worksAt))),
                    object: .variable("?company")
                )
            ])
        )

        let result = try await context.executeSPARQLPattern(
            .optional(social, work),
            on: SPARQLQuadStatement.self,
            projection: ["?subject", "?friend", "?company"]
        )

        #expect(result.count == 3)
        let aliceRows = result.bindings.filter {
            $0["?subject"] == .rdfTerm(.iri(alice))
        }
        #expect(aliceRows.allSatisfy { $0["?company"] == .rdfTerm(.iri(acme)) })
        let bobRows = result.bindings.filter {
            $0["?subject"] == .rdfTerm(.iri(bob))
        }
        #expect(bobRows.allSatisfy { $0["?company"] == .rdfTerm(.iri(beta)) })
    }

    private func seededContext() async throws -> FDBContext {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        if try await database.directoryExists(
            path: ["named_graph_sparql_tests"]
        ) {
            try await database.removeDirectory(
                path: ["named_graph_sparql_tests"]
            )
        }
        let schema = Schema(
            [SPARQLQuadStatement.self],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer(
            testing: schema,
            configuration: .init(backend: .custom(database)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
        try await container.ensureIndexesReady()
        let context = container.newContext()

        let quads = [
            statement(alice, knows, bob, graph: socialGraph),
            statement(alice, knows, carol, graph: socialGraph),
            statement(bob, knows, carol, graph: socialGraph),
            statement(alice, worksAt, acme, graph: workGraph),
            statement(bob, worksAt, beta, graph: workGraph),
            statement(carol, worksAt, acme, graph: nil),
        ]
        for quad in quads {
            context.insert(quad)
        }
        try await context.save()
        return context
    }

    private func statement(
        _ subject: String,
        _ predicate: String,
        _ object: String,
        graph: String?
    ) -> SPARQLQuadStatement {
        var statement = SPARQLQuadStatement()
        statement.subject = .iri(subject)
        statement.predicate = .iri(predicate)
        statement.object = .iri(object)
        statement.graph = graph.map(DatabaseRDFTerm.iri)
        return statement
    }

    private func iri(_ value: FieldValue?) -> String? {
        guard case .rdfTerm(.iri(let iri)) = value else {
            return nil
        }
        return iri
    }
}
#endif
