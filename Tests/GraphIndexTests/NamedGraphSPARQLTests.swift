#if FOUNDATION_DB
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import DatabaseKitFoundation
import FDBStorage
import Foundation
import StorageKit
import TestSupport
import Testing
@testable import DatabaseEngine
@testable import GraphIndex

@Persistable
private struct SPARQLQuadStatement {
    #Directory<SPARQLQuadStatement>("named_graph_sparql_tests")
    #Index(
        .graph(
            name: "rdf_quad",
            definition: .rdf(
                subject: \SPARQLQuadStatement.subject,
                predicate: \SPARQLQuadStatement.predicate,
                object: \SPARQLQuadStatement.object,
        graph: \SPARQLQuadStatement.graph)))

    var id: String = UUID().uuidString
    var subject: RDFTerm = .iri(.xsdString)
    var predicate: RDFTerm = .iri(.xsdString)
    var object: RDFTerm = .iri(.xsdString)
    var graph: RDFTerm? = nil
}

@Suite("Canonical Named Graph SPARQL Integration", .serialized, .foundationDBScenario, .heartbeat)
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
        let expectedSubject = FieldValue.rdfTerm(
            try .iri(validating: carol)
        )
        #expect(result.first?["?subject"] == expectedSubject)
    }

    @Test("Named graph selector isolates one active graph")
    func namedGraphSelectorIsolatesOneActiveGraph() async throws {
        let context = try await seededContext()
        let basic = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?subject"),
                predicate: .value(
                    .rdfTerm(try .iri(validating: knows))
                ),
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
                subject: .value(
                    .rdfTerm(try .iri(validating: alice))
                ),
                predicate: .value(
                    .rdfTerm(try .iri(validating: knows))
                ),
                object: .variable("?middle")
            )
        ])
        let right = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?middle"),
                predicate: .value(
                    .rdfTerm(try .iri(validating: knows))
                ),
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
        let expectedMiddle = FieldValue.rdfTerm(
            try .iri(validating: bob)
        )
        let expectedFriend = FieldValue.rdfTerm(
            try .iri(validating: carol)
        )
        #expect(result.first?["?middle"] == expectedMiddle)
        #expect(result.first?["?friend"] == expectedFriend)
    }

    @Test("Optional joins across explicit named graph scopes")
    func optionalJoinsAcrossExplicitNamedGraphScopes() async throws {
        let context = try await seededContext()
        let social = ExecutionPattern.graph(
            .named(try RDFGraphName(iri: socialGraph)),
            .basic([
                ExecutionTriple(
                    subject: .variable("?subject"),
                    predicate: .value(
                        .rdfTerm(try .iri(validating: knows))
                    ),
                    object: .variable("?friend")
                )
            ])
        )
        let work = ExecutionPattern.graph(
            .named(try RDFGraphName(iri: workGraph)),
            .basic([
                ExecutionTriple(
                    subject: .variable("?subject"),
                    predicate: .value(
                        .rdfTerm(try .iri(validating: worksAt))
                    ),
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
        let aliceValue = FieldValue.rdfTerm(
            try .iri(validating: alice)
        )
        let bobValue = FieldValue.rdfTerm(
            try .iri(validating: bob)
        )
        let acmeValue = FieldValue.rdfTerm(
            try .iri(validating: acme)
        )
        let betaValue = FieldValue.rdfTerm(
            try .iri(validating: beta)
        )
        let aliceRows = result.bindings.filter {
            $0["?subject"] == aliceValue
        }
        #expect(aliceRows.allSatisfy { $0["?company"] == acmeValue })
        let bobRows = result.bindings.filter {
            $0["?subject"] == bobValue
        }
        #expect(bobRows.allSatisfy { $0["?company"] == betaValue })
    }

    private func seededContext() async throws -> DatabaseContext {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [try SPARQLQuadStatement.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SPARQLQuadStatement.self)]),
            security: .testingDisabled
        )
        try await container.resetTestBaseData()
        let context = container.testBaseContext()

        let quads = [
            try statement(alice, knows, bob, graph: socialGraph),
            try statement(alice, knows, carol, graph: socialGraph),
            try statement(bob, knows, carol, graph: socialGraph),
            try statement(alice, worksAt, acme, graph: workGraph),
            try statement(bob, worksAt, beta, graph: workGraph),
            try statement(carol, worksAt, acme, graph: nil),
        ]
        for quad in quads {
            try context.insert(quad)
        }
        try await context.save()
        return context
    }

    private func statement(
        _ subject: String,
        _ predicate: String,
        _ object: String,
        graph: String?
    ) throws -> SPARQLQuadStatement {
        var statement = SPARQLQuadStatement()
        statement.subject = try .iri(validating: subject)
        statement.predicate = try .iri(validating: predicate)
        statement.object = try .iri(validating: object)
        if let graph {
            statement.graph = try .iri(validating: graph)
        }
        return statement
    }

    private func iri(_ value: FieldValue?) -> String? {
        guard case .rdfTerm(.iri(let iri)) = value else {
            return nil
        }
        return iri.rawValue
    }
}
#endif
