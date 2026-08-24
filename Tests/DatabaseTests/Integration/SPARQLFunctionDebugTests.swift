#if FOUNDATION_DB
// SPARQLFunctionDebugTests.swift
// Debug test for SPARQL function integration

import Testing
import Foundation
@testable import Database
@testable import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import DatabaseKitFoundation
import StorageKit
import FDBStorage
import TestSupport

@Suite("SPARQL Function Debug", .serialized, .foundationDBScenario, .heartbeat)
struct SPARQLFunctionDebugTests {

    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    @Persistable
    struct SPARQLDebugUser {
        #Directory<SPARQLDebugUser>("sparql_debug_functions", "users")
        var id: String = UUID().uuidString
        var name: String = ""
    }

    @Persistable
    struct SPARQLDebugTriple {
        #Directory<SPARQLDebugTriple>("sparql_debug_functions", "rdf")
        var id: String
        var subject: RDFTerm
        var predicate: RDFTerm
        var object: RDFTerm

        #Index(
            .graph(
                name: "SPARQLDebugTriple_rdf_quad_subject_predicate_object",
                definition: .rdf(
                    subject: \SPARQLDebugTriple.subject,
                    predicate: \SPARQLDebugTriple.predicate,
                    object: \SPARQLDebugTriple.object, graph: nil)))

        init(
            subject: String,
            predicate: String,
            object: String
        ) throws {
            self.id = UUID().uuidString
            self.subject = try .iri(validating: subject)
            self.predicate = try .iri(validating: predicate)
            self.object = try .iri(validating: object)
        }
    }

    private func makeContainer() async throws -> DBContainer {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [
                try SPARQLDebugUser.schemaEntity,
                try SPARQLDebugTriple.schemaEntity,
            ],
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
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SPARQLDebugUser.self), try DatabaseFrameworkRuntime.entity(SPARQLDebugTriple.self),
                ]),
            security: .testingDisabled,
        )
        try await container.resetTestBaseData()
        return container
    }

    @Test("Debug: Check data insertion and graph index")
    func testDataInsertionAndIndex() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()

        // Insert user
        var alice = SPARQLDebugUser(name: "Alice")
        alice.id = "alice-001"

        try context.insert(alice)
        try await context.save()

        // Insert triple
        var triple = try SPARQLDebugTriple(
            subject: "urn:user:alice-001",
            predicate: "urn:predicate:knows",
            object: "urn:user:bob-001"
        )
        triple.id = "triple-001"
        try context.insert(triple)
        try await context.save()

        // Verify triple was saved
        let triples = try await context.fetch(Query<SPARQLDebugTriple>())
        print("DEBUG: Triples count = \(triples.count)")
        print("DEBUG: Triple subject = \(triples.first.map { String(describing: $0.subject) } ?? "nil")")
        print("DEBUG: Triple predicate = \(triples.first.map { String(describing: $0.predicate) } ?? "nil")")
        print("DEBUG: Triple object = \(triples.first.map { String(describing: $0.object) } ?? "nil")")

        // Verify data was saved
        let users = try await context.fetch(Query<SPARQLDebugUser>())
        print("DEBUG: Users count = \(users.count)")
        #expect(users.count == 1)

        // Check available indexes
        let entity = container.schema.entity(for: SPARQLDebugTriple.self)!
        print("DEBUG: Available indexes: \(entity.indexDescriptors.map { $0.name })")

        // Try direct SPARQL query using QueryBuilder with KeyPath
        let result = try await context.sparql(SPARQLDebugTriple.self)
            .index(
                SPARQLDebugTriple.fields.subject,
                SPARQLDebugTriple.fields.predicate,
                SPARQLDebugTriple.fields.object
            )
            .where(
                .var("s"),
                .value(
                    .rdfTerm(
                        try .iri(validating: "urn:predicate:knows")
                    )
                ),
                .value(
                    .rdfTerm(
                        try .iri(validating: "urn:user:bob-001")
                    )
                )
            )
            .execute()

        print("DEBUG: SPARQL result count = \(result.count)")
        print("DEBUG: SPARQL bindings = \(result.bindings)")
        #expect(result.count == 1)
    }

    @Test("Debug: Check executeSPARQL string method")
    func testExecuteSPARQLString() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()

        // Insert triple
        var triple = try SPARQLDebugTriple(
            subject: "urn:user:alice-002",
            predicate: "urn:predicate:knows",
            object: "urn:user:bob-002"
        )
        triple.id = "triple-002"
        try context.insert(triple)
        try await context.save()

        // Verify triple was saved
        let triples = try await context.fetch(Query<SPARQLDebugTriple>())
        print("DEBUG: Saved triples count = \(triples.count)")
        if let t = triples.first {
            print("DEBUG: Triple: \(t.subject) \(t.predicate) \(t.object)")
        }

        // Try executeSPARQL with string (no angle brackets)
        let result = try await context.executeSPARQL(
            "SELECT ?s WHERE { ?s <urn:predicate:knows> <urn:user:bob-002> }",
            on: SPARQLDebugTriple.self
        )

        print("DEBUG: executeSPARQL result count = \(result.count)")
        print("DEBUG: executeSPARQL bindings = \(result.bindings)")
        print("DEBUG: executeSPARQL projected variables = \(result.projectedVariables)")
        #expect(result.count == 1)
    }
}
#endif
