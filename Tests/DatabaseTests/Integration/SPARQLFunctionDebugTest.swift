#if FOUNDATION_DB
// SPARQLFunctionDebugTest.swift
// Debug test for SPARQL function integration

import Testing
import Foundation
@testable import Database
@testable import DatabaseEngine
import Core
import Graph
import StorageKit
import FDBStorage
import TestSupport

@Suite("SPARQL Function Debug", .serialized, .heartbeat)
struct SPARQLFunctionDebugTest {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
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
        var id: String = UUID().uuidString
        var subject: String = ""
        var predicate: String = ""
        var object: String = ""

        #Index(GraphIndexKind<SPARQLDebugTriple>(
            from: \.subject,
            edge: \.predicate,
            to: \.object,
            strategy: .tripleStore
        ))
    }

    private func makeContainer() async throws -> DBContainer {
        let database = try await FDBTestSetup.shared.makeEngine()
        if try await database.directoryService.exists(path: ["sparql_debug_functions"]) {
            try await database.directoryService.remove(path: ["sparql_debug_functions"])
        }

        let schema = Schema([SPARQLDebugUser.self, SPARQLDebugTriple.self], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer(
            testing: schema,
            configuration: .init(backend: .custom(database)),
            security: .disabled,
        )
        try await container.ensureIndexesReady()
        return container
    }

    @Test("Debug: Check data insertion and graph index")
    func testDataInsertionAndIndex() async throws {
        let container = try await makeContainer()
        let context = container.newContext()

        // Insert user
        var alice = SPARQLDebugUser(name: "Alice")
        alice.id = "alice-001"

        context.insert(alice)
        try await context.save()

        // Insert triple
        var triple = SPARQLDebugTriple(subject: "alice-001", predicate: "knows", object: "bob-001")
        triple.id = "triple-001"
        context.insert(triple)
        try await context.save()

        // Verify triple was saved
        let triples = try await context.fetch(Query<SPARQLDebugTriple>())
        print("DEBUG: Triples count = \(triples.count)")
        print("DEBUG: Triple subject = \(triples.first?.subject ?? "nil")")
        print("DEBUG: Triple predicate = \(triples.first?.predicate ?? "nil")")
        print("DEBUG: Triple object = \(triples.first?.object ?? "nil")")

        // Verify data was saved
        let users = try await context.fetch(Query<SPARQLDebugUser>())
        print("DEBUG: Users count = \(users.count)")
        #expect(users.count == 1)

        // Check available indexes
        let entity = container.schema.entity(for: SPARQLDebugTriple.self)!
        print("DEBUG: Available indexes: \(entity.indexDescriptors.map { $0.name })")

        // Try direct SPARQL query using QueryBuilder with KeyPath
        let result = try await context.sparql(SPARQLDebugTriple.self)
            .index(\.subject, \.predicate, \.object)
            .where("?s", "knows", "bob-001")
            .execute()

        print("DEBUG: SPARQL result count = \(result.count)")
        print("DEBUG: SPARQL bindings = \(result.bindings)")
        #expect(result.count == 1)
    }

    @Test("Debug: Check executeSPARQL string method")
    func testExecuteSPARQLString() async throws {
        let container = try await makeContainer()
        let context = container.newContext()

        // Insert triple
        var triple = SPARQLDebugTriple(subject: "alice-002", predicate: "knows", object: "bob-002")
        triple.id = "triple-002"
        context.insert(triple)
        try await context.save()

        // Verify triple was saved
        let triples = try await context.fetch(Query<SPARQLDebugTriple>())
        print("DEBUG: Saved triples count = \(triples.count)")
        if let t = triples.first {
            print("DEBUG: Triple: \(t.subject) \(t.predicate) \(t.object)")
        }

        // Try executeSPARQL with string (no angle brackets)
        let result = try await context.executeSPARQL(
            "SELECT ?s WHERE { ?s \"knows\" \"bob-002\" }",
            on: SPARQLDebugTriple.self
        )

        print("DEBUG: executeSPARQL result count = \(result.count)")
        print("DEBUG: executeSPARQL bindings = \(result.bindings)")
        print("DEBUG: executeSPARQL projected variables = \(result.projectedVariables)")
        #expect(result.count == 1)
    }
}
#endif
