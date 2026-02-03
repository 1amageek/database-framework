// SPARQLFunctionIntegrationTests.swift
// DatabaseTests - Integration tests for SPARQL() SQL function

import Testing
import Foundation
@testable import Database
@testable import DatabaseEngine
import Core
import Graph
import FoundationDB
import TestSupport

@Suite("SPARQL() Function Integration Tests", .serialized)
struct SPARQLFunctionIntegrationTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Test Models

    @Persistable
    struct User {
        #Directory<User>("test", "sparql_func", "users")

        var id: String = UUID().uuidString
        var name: String = ""
        var age: Int = 0
    }

    @Persistable
    struct RDFTriple {
        #Directory<RDFTriple>("test", "sparql_func", "rdf")

        var id: String = UUID().uuidString
        var subject: String = ""
        var predicate: String = ""
        var object: String = ""

        #Index(GraphIndexKind<RDFTriple>(
            from: \.subject,
            edge: \.predicate,
            to: \.object,
            strategy: .tripleStore
        ))
    }

    // MARK: - Helper Methods

    private func setupContainer() async throws -> FDBContainer {
        let database = try FDBClient.openDatabase()
        let schema = Schema([User.self, RDFTriple.self], version: Schema.Version(1, 0, 0))
        let container = FDBContainer(database: database, schema: schema, security: .disabled)

        // Clean up previous test data
        let directoryLayer = DirectoryLayer(database: database)
        try? await directoryLayer.remove(path: ["test", "sparql_func"])

        // Set index to readable (required for SPARQL queries)
        let subspace = try await container.resolveDirectory(for: RDFTriple.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in RDFTriple.indexDescriptors {
            let currentState = try await indexStateManager.state(of: descriptor.name)
            if currentState == .disabled {
                try await indexStateManager.enable(descriptor.name)
                try await indexStateManager.makeReadable(descriptor.name)
            } else if currentState == .writeOnly {
                try await indexStateManager.makeReadable(descriptor.name)
            }
        }

        return container
    }

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    // MARK: - Test 1: Basic IN Predicate with SPARQL()

    @Test("Basic IN predicate with SPARQL()")
    func testBasicINPredicate() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // Setup: Create users
        var alice = User(name: "Alice", age: 25)
        var bob = User(name: "Bob", age: 30)
        var carol = User(name: "Carol", age: 35)

        alice.id = uniqueID("user")
        bob.id = uniqueID("user")
        carol.id = uniqueID("user")

        context.insert(alice)
        context.insert(bob)
        context.insert(carol)
        try await context.save()

        // Setup: Create RDF triples (Alice and Bob know each other)
        context.insert(RDFTriple(subject: alice.id, predicate: "knows", object: bob.id))
        context.insert(RDFTriple(subject: bob.id, predicate: "knows", object: alice.id))
        try await context.save()

        // Execute: SQL with SPARQL() function
        let sql = """
        SELECT * FROM User
        WHERE id IN (SPARQL(RDFTriple, 'SELECT ?s WHERE { ?s \"knows\" \"\(bob.id)\" }'))
        """

        let users = try await context.executeSQL(sql, as: User.self)

        // Verify: Only Alice should be returned
        #expect(users.count == 1)
        #expect(users[0].id == alice.id)
        #expect(users[0].name == "Alice")
    }

    // MARK: - Test 2: SPARQL() with Complex WHERE Clause

    @Test("SPARQL() with complex WHERE clause")
    func testComplexWhereClause() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // Setup: Create users
        var user1 = User(name: "User1", age: 20)
        var user2 = User(name: "User2", age: 30)
        var user3 = User(name: "User3", age: 40)

        user1.id = uniqueID("user")
        user2.id = uniqueID("user")
        user3.id = uniqueID("user")

        context.insert(user1)
        context.insert(user2)
        context.insert(user3)
        try await context.save()

        // Setup: Create RDF triples
        context.insert(RDFTriple(subject: user1.id, predicate: "role", object: "admin"))
        context.insert(RDFTriple(subject: user2.id, predicate: "role", object: "admin"))
        context.insert(RDFTriple(subject: user3.id, predicate: "role", object: "user"))
        try await context.save()

        // Execute: SQL with SPARQL() + age filter
        let sql = """
        SELECT * FROM User
        WHERE age > 25
          AND id IN (SPARQL(RDFTriple, 'SELECT ?s WHERE { ?s \"role\" "admin" }'))
        """

        let users = try await context.executeSQL(sql, as: User.self)

        // Verify: Only User2 (age=30, role=admin)
        #expect(users.count == 1)
        #expect(users[0].id == user2.id)
        #expect(users[0].age == 30)
    }

    // MARK: - Test 3: Multiple SPARQL() Calls

    @Test("Multiple SPARQL() calls in same query")
    func testMultipleSPARQLCalls() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // Setup: Create users
        var admin = User(name: "Admin", age: 30)
        var developer = User(name: "Developer", age: 25)
        var both = User(name: "Both", age: 35)
        var none = User(name: "None", age: 20)

        admin.id = uniqueID("user")
        developer.id = uniqueID("user")
        both.id = uniqueID("user")
        none.id = uniqueID("user")

        context.insert(admin)
        context.insert(developer)
        context.insert(both)
        context.insert(none)
        try await context.save()

        // Setup: Create RDF triples
        context.insert(RDFTriple(subject: admin.id, predicate: "role", object: "admin"))
        context.insert(RDFTriple(subject: both.id, predicate: "role", object: "admin"))

        context.insert(RDFTriple(subject: developer.id, predicate: "skill", object: "swift"))
        context.insert(RDFTriple(subject: both.id, predicate: "skill", object: "swift"))
        try await context.save()

        // Execute: Find users who are admins AND have swift skill
        let sql = """
        SELECT * FROM User
        WHERE id IN (SPARQL(RDFTriple, 'SELECT ?s WHERE { ?s \"role\" "admin" }'))
          AND id IN (SPARQL(RDFTriple, 'SELECT ?s WHERE { ?s "skill" "swift" }'))
        """

        let users = try await context.executeSQL(sql, as: User.self)

        // Verify: Only 'Both' user
        #expect(users.count == 1)
        #expect(users[0].id == both.id)
        #expect(users[0].name == "Both")
    }

    // MARK: - Test 4: Error - Type Not Found

    @Test("Error: Type not found")
    func testErrorTypeNotFound() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let sql = """
        SELECT * FROM User
        WHERE id IN (SPARQL(NonExistentType, 'SELECT ?s WHERE { ?s "p" "o" }'))
        """

        await #expect(throws: SPARQLFunctionError.self) {
            try await context.executeSQL(sql, as: User.self)
        }
    }

    // MARK: - Test 5: Error - No Graph Index

    @Test("Error: No graph index")
    func testErrorNoGraphIndex() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // User type has no graph index
        let sql = """
        SELECT * FROM User
        WHERE id IN (SPARQL(User, 'SELECT ?s WHERE { ?s "p" "o" }'))
        """

        await #expect(throws: SPARQLFunctionError.self) {
            try await context.executeSQL(sql, as: User.self)
        }
    }

    // MARK: - Test 6: Error - Multiple Variables (No Explicit Selection)

    @Test("Error: Multiple variables without explicit selection")
    func testErrorMultipleVariables() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // Setup: Create triple
        var user = User(name: "Test", age: 25)
        user.id = uniqueID("user")

        context.insert(user)
        context.insert(RDFTriple(subject: user.id, predicate: "knows", object: "someone"))
        try await context.save()

        // Execute: Query returns multiple variables (?s and ?o)
        let sql = """
        SELECT * FROM User
        WHERE id IN (SPARQL(RDFTriple, 'SELECT ?s ?o WHERE { ?s "knows" ?o }'))
        """

        await #expect(throws: SPARQLFunctionError.self) {
            try await context.executeSQL(sql, as: User.self)
        }
    }

    // MARK: - Test 7: Explicit Variable Selection

    @Test("Explicit variable selection")
    func testExplicitVariableSelection() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // Setup
        var person1 = User(name: "Person1", age: 25)
        var person2 = User(name: "Person2", age: 30)

        person1.id = uniqueID("person")
        person2.id = uniqueID("person")

        context.insert(person1)
        context.insert(person2)
        try await context.save()

        context.insert(RDFTriple(subject: person1.id, predicate: "knows", object: person2.id))
        try await context.save()

        // Execute: Query returns ?s and ?o, but we explicitly select ?s
        let sql = """
        SELECT * FROM User
        WHERE id IN (SPARQL(RDFTriple, 'SELECT ?s ?o WHERE { ?s "knows" ?o }', '?s'))
        """

        let users = try await context.executeSQL(sql, as: User.self)

        // Verify: person1 is returned
        #expect(users.count == 1)
        #expect(users[0].id == person1.id)
    }

    // MARK: - Test 8: Empty Result Set

    @Test("Empty result set from SPARQL()")
    func testEmptyResultSet() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // Setup: Create users but no matching triples
        var user = User(name: "Test", age: 25)
        user.id = uniqueID("user")

        context.insert(user)
        try await context.save()

        // Execute: SPARQL returns no results
        let sql = """
        SELECT * FROM User
        WHERE id IN (SPARQL(RDFTriple, 'SELECT ?s WHERE { ?s "nonexistent" "value" }'))
        """

        let users = try await context.executeSQL(sql, as: User.self)

        // Verify: No users returned
        #expect(users.isEmpty)
    }

    // MARK: - Test 9: Performance - Large Result Set

    @Test("Performance: Large result set")
    func testPerformanceLargeResultSet() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // Setup: Create 100 users and triples
        var users: [User] = []
        for i in 0..<100 {
            var user = User(name: "User\(i)", age: 20 + (i % 50))
            user.id = uniqueID("user-\(i)")
            users.append(user)
            context.insert(user)
        }
        try await context.save()

        // Create triples for all users
        for user in users {
            context.insert(RDFTriple(subject: user.id, predicate: "status", object: "active"))
        }
        try await context.save()

        // Execute: Should return all users
        let sql = """
        SELECT * FROM User
        WHERE id IN (SPARQL(RDFTriple, 'SELECT ?s WHERE { ?s "status" "active" }'))
        LIMIT 100
        """

        let startTime = Date()
        let results = try await context.executeSQL(sql, as: User.self)
        let duration = Date().timeIntervalSince(startTime)

        // Verify
        #expect(results.count == 100)
        print("Performance: \(results.count) users fetched in \(String(format: "%.3f", duration))s")
    }

    // MARK: - Test 10: Integration with ORDER BY and LIMIT

    // TODO: Enable this test when QueryBridge supports ORDER BY
    // @Test("Integration with ORDER BY and LIMIT")
    func testOrderByAndLimit() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // Setup
        var user1 = User(name: "Alice", age: 30)
        var user2 = User(name: "Bob", age: 25)
        var user3 = User(name: "Carol", age: 35)

        user1.id = uniqueID("user")
        user2.id = uniqueID("user")
        user3.id = uniqueID("user")

        context.insert(user1)
        context.insert(user2)
        context.insert(user3)
        try await context.save()

        context.insert(RDFTriple(subject: user1.id, predicate: "verified", object: "true"))
        context.insert(RDFTriple(subject: user2.id, predicate: "verified", object: "true"))
        context.insert(RDFTriple(subject: user3.id, predicate: "verified", object: "true"))
        try await context.save()

        // Execute: SPARQL + ORDER BY + LIMIT
        let sql = """
        SELECT * FROM User
        WHERE id IN (SPARQL(RDFTriple, 'SELECT ?s WHERE { ?s "verified" "true" }'))
        ORDER BY age ASC
        LIMIT 2
        """

        let users = try await context.executeSQL(sql, as: User.self)

        // Verify: Bob (25) and Alice (30)
        #expect(users.count == 2)
        #expect(users[0].age == 25)
        #expect(users[0].name == "Bob")
        #expect(users[1].age == 30)
        #expect(users[1].name == "Alice")
    }
}
