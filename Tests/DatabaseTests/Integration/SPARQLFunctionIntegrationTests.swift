#if FOUNDATION_DB
// SPARQLFunctionIntegrationTests.swift
// DatabaseTests - Integration tests for SPARQL() SQL function

import Testing
import Foundation
@_spi(DatabaseExecution) @testable import Database
@testable import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import DatabaseKitFoundation
import DatabaseWire
import StorageKit
import FDBStorage
import TestSupport

@Suite("SPARQL() Function Integration Tests", .serialized, .heartbeat)
struct SPARQLFunctionIntegrationTests {

    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    // MARK: - Test Models

    @Persistable
    struct SPARQLFunctionUser {
        #Directory<SPARQLFunctionUser>("sparql_function_test_users")

        var id: String = UUID().uuidString
        var resource: RDFTerm = .iri(.xsdString)
        var name: String = ""
        var age: Int64 = 0

        mutating func assignIdentity(_ value: String) throws {
            id = value
            resource = try .iri(validating: value)
        }
    }

    @Persistable
    struct SPARQLFunctionTriple {
        #Directory<SPARQLFunctionTriple>("sparql_function_test_rdf")

        var id: String = UUID().uuidString
        var subject: RDFTerm
        var predicate: RDFTerm
        var object: RDFTerm

        #Index(
            .graph(
                name: "SPARQLFunctionTriple_rdf_quad_subject_predicate_object",
                definition: .rdf(
                    subject: \SPARQLFunctionTriple.subject,
                    predicate: \SPARQLFunctionTriple.predicate,
                    object: \SPARQLFunctionTriple.object, graph: nil)))

        init(
            subject: String,
            predicate: String,
            object: String
        ) throws {
            self.subject = try .iri(validating: subject)
            self.predicate = try .iri(
                validating: "urn:predicate:\(predicate)"
            )
            do {
                self.object = .iri(try RDFIRI(object))
            } catch {
                // This test model interprets non-IRI object input as a string
                // literal; RDFIRI validation remains the classification rule.
                self.object = .literal(
                    RDFLiteral(
                        lexicalForm: object,
                        datatype: .xsdString
                    )
                )
            }
        }
    }

    // MARK: - Helper Methods

    private func setupContainer() async throws -> DBContainer {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [
                try SPARQLFunctionUser.schemaEntity,
                try SPARQLFunctionTriple.schemaEntity,
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
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SPARQLFunctionUser.self), try DatabaseFrameworkRuntime.entity(SPARQLFunctionTriple.self),
                ]),
            security: .testingDisabled,
        )

        try await container.resetTestBaseData()

        return container
    }

    private func uniqueID(_ prefix: String) -> String {
        "urn:\(prefix):\(UUID().uuidString.prefix(8))"
    }

    // MARK: - Test 1: Basic IN Predicate with SPARQL()

    @Test("Basic IN predicate with SPARQL()")
    func testBasicINPredicate() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        // Setup: Create users
        var alice = SPARQLFunctionUser(name: "Alice", age: 25)
        var bob = SPARQLFunctionUser(name: "Bob", age: 30)
        var carol = SPARQLFunctionUser(name: "Carol", age: 35)

        try alice.assignIdentity(uniqueID("user"))
        try bob.assignIdentity(uniqueID("user"))
        try carol.assignIdentity(uniqueID("user"))

        try context.insert(alice)
        try context.insert(bob)
        try context.insert(carol)
        try await context.save()

        // Setup: Create RDF triples (Alice and Bob know each other)
        try context.insert(SPARQLFunctionTriple(subject: alice.id, predicate: "knows", object: bob.id))
        try context.insert(SPARQLFunctionTriple(subject: bob.id, predicate: "knows", object: alice.id))
        try await context.save()

        // Execute: SQL with SPARQL() function
        let sql = """
            SELECT * FROM SPARQLFunctionUser
            WHERE resource IN (SPARQL(SPARQLFunctionTriple, 'SELECT ?s WHERE { ?s <urn:predicate:knows> <\(bob.id)> }'))
            """

        let users = try await context.executeSQL(sql, as: SPARQLFunctionUser.self)

        // Verify: Only Alice should be returned
        let user = try #require(users.first)
        #expect(users.count == 1)
        #expect(user.id == alice.id)
        #expect(user.name == "Alice")
    }

    // MARK: - Test 2: SPARQL() with Complex WHERE Clause

    @Test("SPARQL() with complex WHERE clause")
    func testComplexWhereClause() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        // Setup: Create users
        var user1 = SPARQLFunctionUser(name: "User1", age: 20)
        var user2 = SPARQLFunctionUser(name: "User2", age: 30)
        var user3 = SPARQLFunctionUser(name: "User3", age: 40)

        try user1.assignIdentity(uniqueID("user"))
        try user2.assignIdentity(uniqueID("user"))
        try user3.assignIdentity(uniqueID("user"))

        try context.insert(user1)
        try context.insert(user2)
        try context.insert(user3)
        try await context.save()

        // Setup: Create RDF triples
        try context.insert(SPARQLFunctionTriple(subject: user1.id, predicate: "role", object: "admin"))
        try context.insert(SPARQLFunctionTriple(subject: user2.id, predicate: "role", object: "admin"))
        try context.insert(SPARQLFunctionTriple(subject: user3.id, predicate: "role", object: "user"))
        try await context.save()

        // Execute: SQL with SPARQL() + age filter
        let sql = """
            SELECT * FROM SPARQLFunctionUser
            WHERE age > 25
              AND resource IN (SPARQL(SPARQLFunctionTriple, 'SELECT ?s WHERE { ?s <urn:predicate:role> "admin" }'))
            """

        let users = try await context.executeSQL(sql, as: SPARQLFunctionUser.self)

        // Verify: Only User2 (age=30, role=admin)
        let user = try #require(users.first)
        #expect(users.count == 1)
        #expect(user.id == user2.id)
        #expect(user.age == 30)
    }

    // MARK: - Test 3: Multiple SPARQL() Calls

    @Test("Multiple SPARQL() calls in same query")
    func testMultipleSPARQLCalls() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        // Setup: Create users
        var admin = SPARQLFunctionUser(name: "Admin", age: 30)
        var developer = SPARQLFunctionUser(name: "Developer", age: 25)
        var both = SPARQLFunctionUser(name: "Both", age: 35)
        var none = SPARQLFunctionUser(name: "None", age: 20)

        try admin.assignIdentity(uniqueID("user"))
        try developer.assignIdentity(uniqueID("user"))
        try both.assignIdentity(uniqueID("user"))
        try none.assignIdentity(uniqueID("user"))

        try context.insert(admin)
        try context.insert(developer)
        try context.insert(both)
        try context.insert(none)
        try await context.save()

        // Setup: Create RDF triples
        try context.insert(SPARQLFunctionTriple(subject: admin.id, predicate: "role", object: "admin"))
        try context.insert(SPARQLFunctionTriple(subject: both.id, predicate: "role", object: "admin"))

        try context.insert(SPARQLFunctionTriple(subject: developer.id, predicate: "skill", object: "swift"))
        try context.insert(SPARQLFunctionTriple(subject: both.id, predicate: "skill", object: "swift"))
        try await context.save()

        // Execute: Find users who are admins AND have swift skill
        let sql = """
            SELECT * FROM SPARQLFunctionUser
            WHERE resource IN (SPARQL(SPARQLFunctionTriple, 'SELECT ?s WHERE { ?s <urn:predicate:role> "admin" }'))
              AND resource IN (SPARQL(SPARQLFunctionTriple, 'SELECT ?s WHERE { ?s <urn:predicate:skill> "swift" }'))
            """

        let users = try await context.executeSQL(sql, as: SPARQLFunctionUser.self)

        // Verify: Only 'Both' user
        let user = try #require(users.first)
        #expect(users.count == 1)
        #expect(user.id == both.id)
        #expect(user.name == "Both")
    }

    // MARK: - Test 4: Error - Type Not Found

    @Test("Error: Type not found")
    func testErrorTypeNotFound() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        let sql = """
            SELECT * FROM SPARQLFunctionUser
            WHERE id IN (SPARQL(NonExistentType, 'SELECT ?s WHERE { ?s <urn:predicate:p> "o" }'))
            """

        await #expect(throws: SPARQLFunctionError.self) {
            try await context.executeSQL(sql, as: SPARQLFunctionUser.self)
        }
    }

    // MARK: - Test 5: Error - No Graph Index

    @Test("Error: No graph index")
    func testErrorNoGraphIndex() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        // User type has no graph index
        let sql = """
            SELECT * FROM SPARQLFunctionUser
            WHERE id IN (SPARQL(SPARQLFunctionUser, 'SELECT ?s WHERE { ?s <urn:predicate:p> "o" }'))
            """

        await #expect(throws: SPARQLFunctionError.self) {
            try await context.executeSQL(sql, as: SPARQLFunctionUser.self)
        }
    }

    // MARK: - Test 6: Error - Multiple Variables (No Explicit Selection)

    @Test("Error: Multiple variables without explicit selection")
    func testErrorMultipleVariables() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        // Setup: Create triple
        var user = SPARQLFunctionUser(name: "Test", age: 25)
        try user.assignIdentity(uniqueID("user"))

        try context.insert(user)
        try context.insert(SPARQLFunctionTriple(subject: user.id, predicate: "knows", object: "someone"))
        try await context.save()

        // Execute: Query returns multiple variables (?s and ?o)
        let sql = """
            SELECT * FROM SPARQLFunctionUser
            WHERE id IN (SPARQL(SPARQLFunctionTriple, 'SELECT ?s ?o WHERE { ?s <urn:predicate:knows> ?o }'))
            """

        await #expect(throws: SPARQLFunctionError.self) {
            try await context.executeSQL(sql, as: SPARQLFunctionUser.self)
        }
    }

    // MARK: - Test 7: Explicit Variable Selection

    @Test("Explicit variable selection")
    func testExplicitVariableSelection() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        // Setup
        var person1 = SPARQLFunctionUser(name: "Person1", age: 25)
        var person2 = SPARQLFunctionUser(name: "Person2", age: 30)

        try person1.assignIdentity(uniqueID("person"))
        try person2.assignIdentity(uniqueID("person"))

        try context.insert(person1)
        try context.insert(person2)
        try await context.save()

        try context.insert(SPARQLFunctionTriple(subject: person1.id, predicate: "knows", object: person2.id))
        try await context.save()

        // Execute: Query returns ?s and ?o, but we explicitly select ?s
        let sql = """
            SELECT * FROM SPARQLFunctionUser
            WHERE resource IN (SPARQL(SPARQLFunctionTriple, 'SELECT ?s ?o WHERE { ?s <urn:predicate:knows> ?o }', '?s'))
            """

        let users = try await context.executeSQL(sql, as: SPARQLFunctionUser.self)

        // Verify: person1 is returned
        let user = try #require(users.first)
        #expect(users.count == 1)
        #expect(user.id == person1.id)
    }

    // MARK: - Test 8: Empty Result Set

    @Test("Empty result set from SPARQL()")
    func testEmptyResultSet() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        // Setup: Create users but no matching triples
        var user = SPARQLFunctionUser(name: "Test", age: 25)
        try user.assignIdentity(uniqueID("user"))

        try context.insert(user)
        try await context.save()

        // Execute: SPARQL returns no results
        let sql = """
            SELECT * FROM SPARQLFunctionUser
            WHERE id IN (SPARQL(SPARQLFunctionTriple, 'SELECT ?s WHERE { ?s <urn:predicate:nonexistent> "value" }'))
            """

        let users = try await context.executeSQL(sql, as: SPARQLFunctionUser.self)

        // Verify: No users returned
        #expect(users.isEmpty)
    }

    // MARK: - Test 9: Bounded Multi-Row Result

    @Test("SPARQL returns a bounded multi-row result set")
    func boundedMultiRowResultSet() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        // A small multi-row fixture verifies result mapping without measuring
        // throughput in the correctness suite.
        var users: [SPARQLFunctionUser] = []
        for i in 0..<10 {
            var user = SPARQLFunctionUser(
                name: "User\(i)",
                age: Int64(20 + (i % 50))
            )
            try user.assignIdentity(uniqueID("user-\(i)"))
            users.append(user)
            try context.insert(user)
        }
        try await context.save()

        // Create triples for all users
        for user in users {
            try context.insert(SPARQLFunctionTriple(subject: user.id, predicate: "status", object: "active"))
        }
        try await context.save()

        // Execute: Should return all users
        let sql = """
            SELECT * FROM SPARQLFunctionUser
            WHERE resource IN (SPARQL(SPARQLFunctionTriple, 'SELECT ?s WHERE { ?s <urn:predicate:status> "active" }'))
            LIMIT 10
            """

        let results = try await context.executeSQL(sql, as: SPARQLFunctionUser.self)

        #expect(results.count == 10)
    }

    // MARK: - Test 10: SPARQLFunctionRewriter preserves from/fromNamed

    @Test("SPARQLFunctionRewriter preserves dataset failure and executable semantics")
    func testRewriterPreservesDatasetClauses() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()
        var user = SPARQLFunctionUser(name: "Alice", age: 30)
        try user.assignIdentity(uniqueID("user"))
        try context.insert(user)
        try await context.save()

        func executeRewritten(_ query: SelectQuery) async throws -> QueryResponse {
            let options = ReadExecutionOptions()
            let workMeter = DatabaseWorkMeter(
                budget: options.budget,
                monotonicClock: container.monotonicClock
            )
            let execution = ReadExecutionContext(
                options: options,
                monotonicClock: container.monotonicClock,
                workMeter: workMeter
            )
            return try await context.indexQueryContext.withTransaction {
                transaction in
                let retainedStorage = try DatabasePreparedSQLSelectStorage(
                    workMeter: workMeter
                )
                let rewriter = SPARQLFunctionRewriter(
                    context: context,
                    workMeter: workMeter,
                    transaction: transaction,
                    retainedStorage: retainedStorage
                )
                let prepared = try await rewriter.rewritePrepared(query)
                return try await prepared.execute(
                    in: context,
                    execution: execution,
                    transaction: transaction
                )
            }
        }

        let explicitDatasetQuery = SelectQuery(
            projection: .all,
            source: .table(TableRef("SPARQLFunctionUser")),
            filter: .greaterThan(
                .column(ColumnRef(column: "age")),
                .literal(.int(25))
            ),
            dataset: .explicit(
                defaultGraphs: ["http://example.org/graph1"],
                namedGraphs: [
                    "http://example.org/named1",
                    "http://example.org/named2",
                ]
            )
        )

        do {
            _ = try await executeRewritten(explicitDatasetQuery)
            Issue.record("Rewriting silently removed the explicit dataset")
        } catch let error as CanonicalReadError {
            guard case .unsupportedSelectQuery(let reason) = error else {
                Issue.record("Unexpected canonical read error: \(error)")
                return
            }
            #expect(
                reason == "Canonical relational execution does not support SPARQL dataset clauses"
            )
        }

        let executableQuery = SelectQuery(
            projection: .all,
            source: .table(TableRef("SPARQLFunctionUser")),
            filter: .greaterThan(
                .column(ColumnRef(column: "age")),
                .literal(.int(25))
            )
        )
        let response = try await executeRewritten(executableQuery)
        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields["name"] == .string("Alice"))
    }

    // MARK: - Test 11: Integration with ORDER BY and LIMIT

    @Test("Integration with ORDER BY and LIMIT")
    func testOrderByAndLimit() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        // Setup
        var user1 = SPARQLFunctionUser(name: "Alice", age: 30)
        var user2 = SPARQLFunctionUser(name: "Bob", age: 25)
        var user3 = SPARQLFunctionUser(name: "Carol", age: 35)

        try user1.assignIdentity(uniqueID("user"))
        try user2.assignIdentity(uniqueID("user"))
        try user3.assignIdentity(uniqueID("user"))

        try context.insert(user1)
        try context.insert(user2)
        try context.insert(user3)
        try await context.save()

        try context.insert(SPARQLFunctionTriple(subject: user1.id, predicate: "verified", object: "true"))
        try context.insert(SPARQLFunctionTriple(subject: user2.id, predicate: "verified", object: "true"))
        try context.insert(SPARQLFunctionTriple(subject: user3.id, predicate: "verified", object: "true"))
        try await context.save()

        // Execute: SPARQL + ORDER BY + LIMIT
        let sql = """
            SELECT * FROM SPARQLFunctionUser
            WHERE resource IN (SPARQL(SPARQLFunctionTriple, 'SELECT ?s WHERE { ?s <urn:predicate:verified> "true" }'))
            ORDER BY age ASC
            LIMIT 2
            """

        let users = try await context.executeSQL(sql, as: SPARQLFunctionUser.self)

        // Verify: Bob (25) and Alice (30)
        let first = try #require(users.first)
        let second = try #require(users.dropFirst().first)
        #expect(users.count == 2)
        #expect(first.age == 25)
        #expect(first.name == "Bob")
        #expect(second.age == 30)
        #expect(second.name == "Alice")
    }
}
#endif
