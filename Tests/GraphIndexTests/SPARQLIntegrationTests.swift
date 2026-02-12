// SPARQLIntegrationTests.swift
// End-to-end tests for SPARQL-like query functionality against FoundationDB
//
// These tests validate the complete query execution path:
//   User Code → FDBContext.sparql() → SPARQLQueryBuilder → SPARQLQueryExecutor → FDB

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Model

/// RDF-like statement for SPARQL testing
@Persistable
struct SPARQLTestStatement {
    #Directory<SPARQLTestStatement>("test", "sparql", "statements")

    var id: String = ULID().ulidString
    var subject: String = ""
    var predicate: String = ""
    var object: String = ""

    #Index(GraphIndexKind<SPARQLTestStatement>(
        from: \.subject,
        edge: \.predicate,
        to: \.object,
        strategy: .hexastore
    ))
}

// MARK: - Test Suite

@Suite("SPARQL Integration Tests", .serialized)
struct SPARQLIntegrationTests {

    // MARK: - Setup Helpers

    private func setupContainer() async throws -> FDBContainer {
        let database = try FDBClient.openDatabase()
        let schema = Schema([SPARQLTestStatement.self], version: Schema.Version(1, 0, 0))
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    private func cleanup(container: FDBContainer) async throws {
        let directoryLayer = DirectoryLayer(database: container.database)
        try? await directoryLayer.remove(path: ["test", "sparql", "statements"])
    }

    private func setIndexStatesToReadable(container: FDBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: SPARQLTestStatement.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in SPARQLTestStatement.indexDescriptors {
            // Use retry loop to handle concurrent state transitions from parallel tests
            let maxAttempts = 3
            for attempt in 1...maxAttempts {
                let currentState = try await indexStateManager.state(of: descriptor.name)

                switch currentState {
                case .disabled:
                    do {
                        try await indexStateManager.enable(descriptor.name)
                        try await indexStateManager.makeReadable(descriptor.name)
                        break  // Success
                    } catch let error as IndexStateError {
                        // Another test may have enabled it concurrently
                        if case .invalidTransition = error, attempt < maxAttempts {
                            continue  // Retry
                        }
                        throw error
                    }
                case .writeOnly:
                    do {
                        try await indexStateManager.makeReadable(descriptor.name)
                        break  // Success
                    } catch let error as IndexStateError {
                        if case .invalidTransition = error, attempt < maxAttempts {
                            continue
                        }
                        throw error
                    }
                case .readable:
                    break  // Already readable, success
                }
            }
        }
    }

    private func insertStatements(_ statements: [SPARQLTestStatement], context: FDBContext) async throws {
        for statement in statements {
            context.insert(statement)
        }
        try await context.save()
    }

    private func makeStatement(subject: String, predicate: String, object: String) -> SPARQLTestStatement {
        var stmt = SPARQLTestStatement()
        stmt.subject = subject
        stmt.predicate = predicate
        stmt.object = object
        return stmt
    }

    // MARK: - Basic Pattern Tests

    @Test("Single pattern: subject bound")
    func testSinglePatternSubjectBound() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            // Insert test data: Alice knows Bob, Carol, Dave
            try await insertStatements([
                makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                makeStatement(subject: "Alice", predicate: "knows", object: "Carol"),
                makeStatement(subject: "Alice", predicate: "knows", object: "Dave"),
                makeStatement(subject: "Bob", predicate: "knows", object: "Alice")
            ], context: context)

            // Query: SELECT ?friend WHERE { "Alice" "knows" ?friend }
            let results = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("Alice", "knows", "?friend")
                .select("?friend")
                .execute()

            #expect(results.count == 3)
            let friends = results.nonNilValues(for: "?friend")
            #expect(friends.contains("Bob"))
            #expect(friends.contains("Carol"))
            #expect(friends.contains("Dave"))

            try await cleanup(container: container)
        }
    }

    @Test("Single pattern: object bound")
    func testSinglePatternObjectBound() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            try await insertStatements([
                makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                makeStatement(subject: "Carol", predicate: "knows", object: "Bob"),
                makeStatement(subject: "Dave", predicate: "follows", object: "Bob")
            ], context: context)

            // Query: SELECT ?person WHERE { ?person "knows" "Bob" }
            let results = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("?person", "knows", "Bob")
                .select("?person")
                .execute()

            #expect(results.count == 2)
            let people = results.nonNilValues(for: "?person")
            #expect(people.contains("Alice"))
            #expect(people.contains("Carol"))
            #expect(!people.contains("Dave"))  // Dave follows, not knows

            try await cleanup(container: container)
        }
    }

    @Test("Single pattern: no results")
    func testSinglePatternNoResults() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            try await insertStatements([
                makeStatement(subject: "Alice", predicate: "knows", object: "Bob")
            ], context: context)

            // Query: SELECT ?person WHERE { ?person "knows" "NonExistent" }
            let results = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("?person", "knows", "NonExistent")
                .execute()

            #expect(results.isEmpty)
            #expect(results.count == 0)

            try await cleanup(container: container)
        }
    }

    @Test("exists() helper")
    func testExistsHelper() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            try await insertStatements([
                makeStatement(subject: "Alice", predicate: "knows", object: "Bob")
            ], context: context)

            // First verify with a variable pattern (this works in other tests)
            let checkResults = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("Alice", "knows", "?obj")
                .execute()

            #expect(checkResults.count == 1, "Should find one object for Alice knows")
            #expect(checkResults.first?["?obj"] == "Bob", "The object should be Bob")

            // Now test fully bound pattern
            // ASK { "Alice" "knows" "Bob" }
            let aliceKnowsBobResults = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("Alice", "knows", "Bob")
                .execute()

            let aliceKnowsBob = !aliceKnowsBobResults.isEmpty

            // ASK { "Alice" "knows" "Carol" }
            let aliceKnowsCarolResults = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("Alice", "knows", "Carol")
                .execute()

            let aliceKnowsCarol = !aliceKnowsCarolResults.isEmpty

            #expect(aliceKnowsBob == true, "Alice should know Bob (found \(aliceKnowsBobResults.count) results)")
            #expect(aliceKnowsCarol == false, "Alice should not know Carol (found \(aliceKnowsCarolResults.count) results)")

            try await cleanup(container: container)
        }
    }

    // MARK: - JOIN Tests

    @Test("JOIN: two patterns with shared variable")
    func testJoinSharedVariable() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            try await insertStatements([
                makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                makeStatement(subject: "Alice", predicate: "lives", object: "Tokyo"),
                makeStatement(subject: "Bob", predicate: "knows", object: "Carol"),
                makeStatement(subject: "Bob", predicate: "lives", object: "NYC"),
                makeStatement(subject: "Carol", predicate: "knows", object: "Dave")
                // Carol has no "lives" triple
            ], context: context)

            // Query: SELECT ?person ?city WHERE { ?person "knows" "Bob" . ?person "lives" ?city }
            let results = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("?person", "knows", "Bob")
                .where("?person", "lives", "?city")
                .select("?person", "?city")
                .execute()

            // Only Alice knows Bob AND has a lives triple
            #expect(results.count == 1)
            #expect(results.first?["?person"] == "Alice")
            #expect(results.first?["?city"] == "Tokyo")

            try await cleanup(container: container)
        }
    }

    @Test("JOIN: friends of friends")
    func testFriendsOfFriends() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            // Graph: Alice -> Bob -> Carol -> Dave
            //              \-> Eve
            try await insertStatements([
                makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                makeStatement(subject: "Bob", predicate: "knows", object: "Carol"),
                makeStatement(subject: "Bob", predicate: "knows", object: "Eve"),
                makeStatement(subject: "Carol", predicate: "knows", object: "Dave")
            ], context: context)

            // Query: SELECT ?fof WHERE { "Alice" "knows" ?friend . ?friend "knows" ?fof }
            let results = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("Alice", "knows", "?friend")
                .where("?friend", "knows", "?fof")
                .select("?fof")
                .execute()

            // Alice's friends: Bob
            // Bob's friends: Carol, Eve
            // So friends-of-friends: Carol, Eve
            #expect(results.count == 2)
            let fofs = results.nonNilValues(for: "?fof")
            #expect(fofs.contains("Carol"))
            #expect(fofs.contains("Eve"))

            try await cleanup(container: container)
        }
    }

    // MARK: - OPTIONAL Tests

    @Test("OPTIONAL: some match, some don't")
    func testOptionalPattern() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            try await insertStatements([
                makeStatement(subject: "Alice", predicate: "type", object: "User"),
                makeStatement(subject: "Alice", predicate: "email", object: "alice@example.com"),
                makeStatement(subject: "Bob", predicate: "type", object: "User"),
                // Bob has no email
                makeStatement(subject: "Carol", predicate: "type", object: "User"),
                makeStatement(subject: "Carol", predicate: "email", object: "carol@example.com")
            ], context: context)

            // Query: SELECT ?person ?email WHERE { ?person "type" "User" } OPTIONAL { ?person "email" ?email }
            let results = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("?person", "type", "User")
                .optional { $0.where("?person", "email", "?email") }
                .select("?person", "?email")
                .execute()

            #expect(results.count == 3)

            // Check each person - sort by person for deterministic ordering
            let sorted = results.bindings.sorted { ($0["?person"] ?? "") < ($1["?person"] ?? "") }

            // Alice should have email
            #expect(sorted[0]["?person"] == "Alice")
            #expect(sorted[0]["?email"] == "alice@example.com")

            // Bob should NOT have email (OPTIONAL didn't match)
            #expect(sorted[1]["?person"] == "Bob")
            #expect(sorted[1]["?email"] == nil)

            // Carol should have email
            #expect(sorted[2]["?person"] == "Carol")
            #expect(sorted[2]["?email"] == "carol@example.com")

            try await cleanup(container: container)
        }
    }

    // MARK: - UNION Tests

    @Test("UNION: alternative patterns")
    func testUnionPattern() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            try await insertStatements([
                makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                makeStatement(subject: "Carol", predicate: "follows", object: "Bob"),
                makeStatement(subject: "Dave", predicate: "likes", object: "Bob")
            ], context: context)

            // Query: SELECT ?person WHERE { { ?person "knows" "Bob" } UNION { ?person "follows" "Bob" } }
            let results = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("?person", "knows", "Bob")
                .union { $0.where("?person", "follows", "Bob") }
                .select("?person")
                .execute()

            #expect(results.count == 2)
            let people = results.nonNilValues(for: "?person")
            #expect(people.contains("Alice"))
            #expect(people.contains("Carol"))
            #expect(!people.contains("Dave"))  // Dave "likes", not knows or follows

            try await cleanup(container: container)
        }
    }

    // MARK: - FILTER Tests

    @Test("FILTER: exclude specific value")
    func testFilterNotEquals() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            // Graph with cycle: Alice -> Bob -> Alice (and others)
            try await insertStatements([
                makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                makeStatement(subject: "Bob", predicate: "knows", object: "Alice"),
                makeStatement(subject: "Bob", predicate: "knows", object: "Carol")
            ], context: context)

            // Query: SELECT ?fof WHERE { "Alice" "knows" ?friend . ?friend "knows" ?fof . FILTER(?fof != "Alice") }
            let results = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("Alice", "knows", "?friend")
                .where("?friend", "knows", "?fof")
                .filter("?fof", notEquals: "Alice")
                .select("?fof")
                .execute()

            #expect(results.count == 1)
            #expect(results.first?["?fof"] == "Carol")

            try await cleanup(container: container)
        }
    }

    @Test("FILTER: regex pattern")
    func testFilterRegex() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            try await insertStatements([
                makeStatement(subject: "Alice", predicate: "name", object: "Alice Smith"),
                makeStatement(subject: "Bob", predicate: "name", object: "Bob Jones"),
                makeStatement(subject: "Anna", predicate: "name", object: "Anna Lee")
            ], context: context)

            // Query: SELECT ?person ?name WHERE { ?person "name" ?name . FILTER(REGEX(?name, "^A")) }
            let results = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("?person", "name", "?name")
                .filter("?name", matches: "^A")
                .select("?person", "?name")
                .execute()

            #expect(results.count == 2)
            let names = results.nonNilValues(for: "?name")
            #expect(names.contains("Alice Smith"))
            #expect(names.contains("Anna Lee"))
            #expect(!names.contains("Bob Jones"))

            try await cleanup(container: container)
        }
    }

    @Test("FILTER: bound check with OPTIONAL")
    func testFilterBoundWithOptional() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            try await insertStatements([
                makeStatement(subject: "Alice", predicate: "type", object: "User"),
                makeStatement(subject: "Alice", predicate: "email", object: "alice@example.com"),
                makeStatement(subject: "Bob", predicate: "type", object: "User")
                // Bob has no email
            ], context: context)

            // Query: SELECT ?person WHERE { ?person "type" "User" } OPTIONAL { ?person "email" ?email } FILTER(BOUND(?email))
            let results = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("?person", "type", "User")
                .optional { $0.where("?person", "email", "?email") }
                .filter(.bound("?email"))
                .select("?person")
                .execute()

            // Only Alice has email bound
            #expect(results.count == 1)
            #expect(results.first?["?person"] == "Alice")

            try await cleanup(container: container)
        }
    }

    // MARK: - Modifier Tests

    @Test("DISTINCT: removes duplicates after projection")
    func testDistinct() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            // Multiple triples with same predicate
            try await insertStatements([
                makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                makeStatement(subject: "Alice", predicate: "knows", object: "Carol"),
                makeStatement(subject: "Bob", predicate: "knows", object: "Carol")
            ], context: context)

            // Query: SELECT DISTINCT ?pred WHERE { ?s ?pred ?o }
            let results = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("?s", "knows", "?o")
                .select("?s")
                .distinct()
                .execute()

            // Without distinct: Alice, Alice, Bob (3 results)
            // With distinct: Alice, Bob (2 results)
            #expect(results.count == 2)

            try await cleanup(container: container)
        }
    }

    @Test("LIMIT and OFFSET")
    func testLimitOffset() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            // Insert 10 triples
            var statements: [SPARQLTestStatement] = []
            for i in 1...10 {
                statements.append(makeStatement(subject: "Person\(i)", predicate: "type", object: "User"))
            }
            try await insertStatements(statements, context: context)

            // Query with LIMIT 3
            let limitResults = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("?person", "type", "User")
                .select("?person")
                .limit(3)
                .execute()

            #expect(limitResults.count == 3)
            #expect(!limitResults.isComplete)

            // Query with LIMIT 3 OFFSET 5
            let offsetResults = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("?person", "type", "User")
                .select("?person")
                .limit(3)
                .offset(5)
                .execute()

            #expect(offsetResults.count == 3)

            try await cleanup(container: container)
        }
    }

    @Test("SELECT projection")
    func testSelectProjection() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            try await insertStatements([
                makeStatement(subject: "Alice", predicate: "knows", object: "Bob")
            ], context: context)

            // Query: SELECT ?s WHERE { ?s ?p ?o }
            let results = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("?s", "?p", "?o")
                .select("?s")
                .execute()

            #expect(results.count == 1)
            #expect(results.projectedVariables == ["?s"])
            #expect(results.first?["?s"] == "Alice")
            // Non-projected variables should not be in result
            #expect(results.first?["?p"] == nil)
            #expect(results.first?["?o"] == nil)

            try await cleanup(container: container)
        }
    }

    // MARK: - Edge Cases

    @Test("Empty database")
    func testEmptyDatabase() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            let results = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("?s", "?p", "?o")
                .execute()

            #expect(results.isEmpty)
            #expect(results.count == 0)
            #expect(results.isComplete)

            try await cleanup(container: container)
        }
    }

    @Test("Large dataset (100 edges)")
    func testLargeDataset() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            // Insert 100 edges: Person1 -> Person2 -> ... -> Person100
            var statements: [SPARQLTestStatement] = []
            for i in 1..<100 {
                statements.append(makeStatement(
                    subject: "Person\(i)",
                    predicate: "knows",
                    object: "Person\(i + 1)"
                ))
            }
            try await insertStatements(statements, context: context)

            // Query all edges
            let allResults = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("?s", "knows", "?o")
                .execute()

            #expect(allResults.count == 99)

            // Query specific person's friends
            let person50Friends = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("Person50", "knows", "?friend")
                .execute()

            #expect(person50Friends.count == 1)
            #expect(person50Friends.first?["?friend"] == "Person51")

            try await cleanup(container: container)
        }
    }

    @Test("Cyclic graph")
    func testCyclicGraph() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)
            try await setIndexStatesToReadable(container: container)

            let context = container.newContext()

            // Create a cycle: A -> B -> C -> A
            try await insertStatements([
                makeStatement(subject: "A", predicate: "knows", object: "B"),
                makeStatement(subject: "B", predicate: "knows", object: "C"),
                makeStatement(subject: "C", predicate: "knows", object: "A")
            ], context: context)

            // 2-hop query from A
            let results = try await context.sparql(SPARQLTestStatement.self)
                .defaultIndex()
                .where("A", "knows", "?x")
                .where("?x", "knows", "?y")
                .select("?y")
                .execute()

            #expect(results.count == 1)
            #expect(results.first?["?y"] == "C")

            try await cleanup(container: container)
        }
    }
}
