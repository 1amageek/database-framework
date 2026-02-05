// SPARQLFunctionDebugTest.swift
// Debug test for SPARQL function integration

import Testing
import Foundation
@testable import Database
@testable import DatabaseEngine
import Core
import Graph
import FoundationDB
import TestSupport

@Suite("SPARQL Function Debug", .serialized)
struct SPARQLFunctionDebugTest {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    @Persistable
    struct TestUser {
        #Directory<TestUser>("test", "sparql_debug", "users")
        var id: String = UUID().uuidString
        var name: String = ""
    }

    @Persistable
    struct TestTriple {
        #Directory<TestTriple>("test", "sparql_debug", "rdf")
        var id: String = UUID().uuidString
        var subject: String = ""
        var predicate: String = ""
        var object: String = ""

        #Index(GraphIndexKind<TestTriple>(
            from: \.subject,
            edge: \.predicate,
            to: \.object,
            strategy: .tripleStore
        ))
    }

    @Test("Debug: Check data insertion and graph index")
    func testDataInsertionAndIndex() async throws {
        let database = try FDBClient.openDatabase()
        let schema = Schema([TestUser.self, TestTriple.self], version: Schema.Version(1, 0, 0))

        // Clean up directory BEFORE creating container to avoid stale state
        let directoryLayer = DirectoryLayer(database: database)
        try? await directoryLayer.remove(path: ["test", "sparql_debug"])

        // Create container and ensure indexes are ready
        let container = FDBContainer(database: database, schema: schema, security: .disabled)
        try await container.ensureIndexesReady()

        let context = container.newContext()

        // Insert user
        var alice = TestUser(name: "Alice")
        alice.id = "alice-001"

        context.insert(alice)
        try await context.save()

        // Insert triple
        var triple = TestTriple(subject: "alice-001", predicate: "knows", object: "bob-001")
        triple.id = "triple-001"
        context.insert(triple)
        try await context.save()

        // Verify triple was saved
        let triples = try await context.fetch(Query<TestTriple>())
        print("DEBUG: Triples count = \(triples.count)")
        print("DEBUG: Triple subject = \(triples.first?.subject ?? "nil")")
        print("DEBUG: Triple predicate = \(triples.first?.predicate ?? "nil")")
        print("DEBUG: Triple object = \(triples.first?.object ?? "nil")")

        // Verify data was saved
        let users = try await context.fetch(Query<TestUser>())
        print("DEBUG: Users count = \(users.count)")
        #expect(users.count == 1)

        // Check available indexes
        let entity = schema.entity(for: TestTriple.self)!
        print("DEBUG: Available indexes: \(entity.indexDescriptors.map { $0.name })")

        // Try direct SPARQL query using QueryBuilder with KeyPath
        let result = try await context.sparql(TestTriple.self)
            .index(\.subject, \.predicate, \.object)
            .where("?s", "knows", "bob-001")
            .execute()

        print("DEBUG: SPARQL result count = \(result.count)")
        print("DEBUG: SPARQL bindings = \(result.bindings)")
        #expect(result.count == 1)
    }

    @Test("Debug: Check executeSPARQL string method")
    func testExecuteSPARQLString() async throws {
        let database = try FDBClient.openDatabase()
        let schema = Schema([TestUser.self, TestTriple.self], version: Schema.Version(1, 0, 0))

        // Clean up directory BEFORE creating container to avoid stale state
        let directoryLayer = DirectoryLayer(database: database)
        try? await directoryLayer.remove(path: ["test", "sparql_debug"])

        // Create container and ensure indexes are ready (handles all index state management)
        let container = FDBContainer(database: database, schema: schema, security: .disabled)
        try await container.ensureIndexesReady()

        let context = container.newContext()

        // Insert triple
        var triple = TestTriple(subject: "alice-002", predicate: "knows", object: "bob-002")
        triple.id = "triple-002"
        context.insert(triple)
        try await context.save()

        // Verify triple was saved
        let triples = try await context.fetch(Query<TestTriple>())
        print("DEBUG: Saved triples count = \(triples.count)")
        if let t = triples.first {
            print("DEBUG: Triple: \(t.subject) \(t.predicate) \(t.object)")
        }

        // Try executeSPARQL with string (no angle brackets)
        let result = try await context.executeSPARQL(
            "SELECT ?s WHERE { ?s \"knows\" \"bob-002\" }",
            on: TestTriple.self
        )

        print("DEBUG: executeSPARQL result count = \(result.count)")
        print("DEBUG: executeSPARQL bindings = \(result.bindings)")
        print("DEBUG: executeSPARQL projected variables = \(result.projectedVariables)")
        #expect(result.count == 1)
    }
}
