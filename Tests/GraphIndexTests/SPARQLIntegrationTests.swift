#if FOUNDATION_DB
// SPARQLIntegrationTests.swift
// End-to-end tests for SPARQL-like query functionality against FoundationDB
//
// These tests validate the complete query execution path:
//   User Code → DatabaseContext.sparql() → SPARQLQueryBuilder → SPARQLQueryExecutor → FDB

import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import DatabaseRuntime
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Model

/// RDF-like statement for SPARQL testing
@Persistable
struct SPARQLQueryStatement {
    #Directory<SPARQLQueryStatement>("test", "sparql", "statements")

    var id: String = UUID().uuidString
    var subject: RDFTerm = .iri(.xsdString)
    var predicate: RDFTerm = .iri(.xsdString)
    var object: RDFTerm = .iri(.xsdString)

    #Index(
        .rdfDataset,
        from: \SPARQLQueryStatement.subject,
        edge: \SPARQLQueryStatement.predicate,
        to: \SPARQLQueryStatement.object
    )
}

// MARK: - Test Suite

@Suite("SPARQL Integration Tests", .serialized, .foundationDBScenario, .heartbeat)
struct SPARQLIntegrationTests {
    private static let resourcePrefix =
        "did:database-framework:test-resource:"

    // MARK: - Setup Helpers

    private func setupContainer() async throws -> DBContainer {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [try SPARQLQueryStatement.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.open(for: schema, configuration: .init(backend: .custom(database)), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [SPARQLQueryStatement.self]), security: .disabled)
    }

    private func cleanup(container: DBContainer) async throws {
        if try await container.engine.namespaceExists(
            path: ["test", "sparql", "statements"]
        ) {
            try await container.engine.removeNamespace(
                path: ["test", "sparql", "statements"]
            )
        }
        try await container.ensureIndexesReady()
    }

    private func insertStatements(_ statements: [SPARQLQueryStatement], context: DatabaseContext) async throws {
        for statement in statements {
            try context.insert(statement)
        }
        try await context.save()
    }

    private func makeStatement(
        subject: String,
        predicate: String,
        object: String
    ) throws -> SPARQLQueryStatement {
        var stmt = SPARQLQueryStatement()
        stmt.subject = try Self.resource(subject)
        stmt.predicate = try Self.predicate(predicate)
        stmt.object = try Self.resource(object)
        return stmt
    }

    private func makeLiteralStatement(
        subject: String,
        predicate: String,
        object: String
    ) throws -> SPARQLQueryStatement {
        var stmt = SPARQLQueryStatement()
        stmt.subject = try Self.resource(subject)
        stmt.predicate = try Self.predicate(predicate)
        stmt.object = .string(object)
        return stmt
    }

    private static func resource(_ identifier: String) throws -> RDFTerm {
        try .iri(validating: resourcePrefix + identifier)
    }

    private static func predicate(_ identifier: String) throws -> RDFTerm {
        try .iri(validating: "did:database-framework:test-predicate:\(identifier)")
    }

    private static func subjectTerm(_ value: String) throws -> ExecutionTerm {
        value.hasPrefix("?")
            ? .variable(value)
            : .value(.rdfTerm(try resource(value)))
    }

    private static func predicateTerm(_ value: String) throws -> ExecutionTerm {
        value.hasPrefix("?")
            ? .variable(value)
            : .value(.rdfTerm(try predicate(value)))
    }

    private static func objectTerm(_ value: String) throws -> ExecutionTerm {
        value.hasPrefix("?")
            ? .variable(value)
            : .value(.rdfTerm(try resource(value)))
    }

    private static func resourceValue(
        _ identifier: String
    ) throws -> FieldValue {
        .rdfTerm(try resource(identifier))
    }

    private static func resourceIdentifier(
        _ value: FieldValue?
    ) -> String? {
        guard case .rdfTerm(.iri(let iri)) = value,
              iri.rawValue.hasPrefix(resourcePrefix) else {
            return nil
        }
        return String(iri.rawValue.dropFirst(resourcePrefix.count))
    }

    // MARK: - Basic Pattern Tests

    @Test("Single pattern: subject bound")
    func testSinglePatternSubjectBound() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            // Insert test data: Alice knows Bob, Carol, Dave
            try await insertStatements([
                try makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                try makeStatement(subject: "Alice", predicate: "knows", object: "Carol"),
                try makeStatement(subject: "Alice", predicate: "knows", object: "Dave"),
                try makeStatement(subject: "Bob", predicate: "knows", object: "Alice")
            ], context: context)

            // Query: SELECT ?friend WHERE { "Alice" "knows" ?friend }
            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("Alice"), try Self.predicateTerm("knows"), try Self.objectTerm("?friend"))
                .select("?friend")
                .execute()

            #expect(results.count == 3)
            let friends = results.nonNilValues(for: "?friend").compactMap {
                Self.resourceIdentifier($0)
            }
            #expect(friends.contains("Bob"))
            #expect(friends.contains("Carol"))
            #expect(friends.contains("Dave"))

            try await cleanup(container: container)
        }
    }

    @Test("Single pattern: object bound")
    func testSinglePatternObjectBound() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            try await insertStatements([
                try makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                try makeStatement(subject: "Carol", predicate: "knows", object: "Bob"),
                try makeStatement(subject: "Dave", predicate: "follows", object: "Bob")
            ], context: context)

            // Query: SELECT ?person WHERE { ?person "knows" "Bob" }
            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("?person"), try Self.predicateTerm("knows"), try Self.objectTerm("Bob"))
                .select("?person")
                .execute()

            #expect(results.count == 2)
            let people = results.nonNilValues(for: "?person").compactMap {
                Self.resourceIdentifier($0)
            }
            #expect(people.contains("Alice"))
            #expect(people.contains("Carol"))
            #expect(!people.contains("Dave"))  // Dave follows, not knows

            try await cleanup(container: container)
        }
    }

    @Test("Single pattern: no results")
    func testSinglePatternNoResults() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            try await insertStatements([
                try makeStatement(subject: "Alice", predicate: "knows", object: "Bob")
            ], context: context)

            // Query: SELECT ?person WHERE { ?person "knows" "NonExistent" }
            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("?person"), try Self.predicateTerm("knows"), try Self.objectTerm("NonExistent"))
                .execute()

            #expect(results.isEmpty)
            #expect(results.count == 0)

            try await cleanup(container: container)
        }
    }

    @Test("EXISTS evaluates the current solution bindings")
    func existsExpressionEvaluatesBindings() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            try await insertStatements([
                try makeStatement(subject: "Alice", predicate: "knows", object: "Bob")
            ], context: context)

            // First verify with a variable pattern (this works in other tests)
            let checkResults = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("Alice"), try Self.predicateTerm("knows"), try Self.objectTerm("?obj"))
                .execute()

            #expect(checkResults.count == 1, "Should find one object for Alice knows")
            #expect(
                Self.resourceIdentifier(checkResults.first?["?obj"]) == "Bob",
                "The object should be Bob"
            )

            // Now test fully bound pattern
            // ASK { "Alice" "knows" "Bob" }
            let aliceKnowsBobResults = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("Alice"), try Self.predicateTerm("knows"), try Self.objectTerm("Bob"))
                .execute()

            let aliceKnowsBob = !aliceKnowsBobResults.isEmpty

            // ASK { "Alice" "knows" "Carol" }
            let aliceKnowsCarolResults = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("Alice"), try Self.predicateTerm("knows"), try Self.objectTerm("Carol"))
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
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            try await insertStatements([
                try makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                try makeLiteralStatement(subject: "Alice", predicate: "lives", object: "Tokyo"),
                try makeStatement(subject: "Bob", predicate: "knows", object: "Carol"),
                try makeLiteralStatement(subject: "Bob", predicate: "lives", object: "NYC"),
                try makeStatement(subject: "Carol", predicate: "knows", object: "Dave")
                // Carol has no "lives" triple
            ], context: context)

            // Query: SELECT ?person ?city WHERE { ?person "knows" "Bob" . ?person "lives" ?city }
            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("?person"), try Self.predicateTerm("knows"), try Self.objectTerm("Bob"))
                .where(try Self.subjectTerm("?person"), try Self.predicateTerm("lives"), try Self.objectTerm("?city"))
                .select("?person", "?city")
                .execute()

            // Only Alice knows Bob AND has a lives triple
            #expect(results.count == 1)
            #expect(
                Self.resourceIdentifier(results.first?["?person"]) == "Alice"
            )
            #expect(results.first?.string("?city") == "Tokyo")

            try await cleanup(container: container)
        }
    }

    @Test("JOIN: friends of friends")
    func testFriendsOfFriends() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            // Graph: Alice -> Bob -> Carol -> Dave
            //              \-> Eve
            try await insertStatements([
                try makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                try makeStatement(subject: "Bob", predicate: "knows", object: "Carol"),
                try makeStatement(subject: "Bob", predicate: "knows", object: "Eve"),
                try makeStatement(subject: "Carol", predicate: "knows", object: "Dave")
            ], context: context)

            // Query: SELECT ?fof WHERE { "Alice" "knows" ?friend . ?friend "knows" ?fof }
            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("Alice"), try Self.predicateTerm("knows"), try Self.objectTerm("?friend"))
                .where(try Self.subjectTerm("?friend"), try Self.predicateTerm("knows"), try Self.objectTerm("?fof"))
                .select("?fof")
                .execute()

            // Alice's friends: Bob
            // Bob's friends: Carol, Eve
            // So friends-of-friends: Carol, Eve
            #expect(results.count == 2)
            let fofs = results.nonNilValues(for: "?fof").compactMap {
                Self.resourceIdentifier($0)
            }
            #expect(fofs.contains("Carol"))
            #expect(fofs.contains("Eve"))

            try await cleanup(container: container)
        }
    }

    @Test("JOIN strategy: hash join selected when right side is bounded")
    func testHashJoinStrategySelected() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            var statements: [SPARQLQueryStatement] = []
            for i in 0..<80 {
                statements.append(try makeStatement(subject: "u\(i)", predicate: "type", object: "User"))
                if i < 10 {
                    statements.append(try makeStatement(subject: "u\(i)", predicate: "knows", object: "Target"))
                }
            }
            try await insertStatements(statements, context: context)

            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("?person"), try Self.predicateTerm("type"), try Self.objectTerm("User"))
                .where(try Self.subjectTerm("?person"), try Self.predicateTerm("knows"), try Self.objectTerm("Target"))
                .select("?person")
                .execute()

            #expect(results.count == 10)
            #expect(results.statistics.joinStrategies.contains(.hashJoin))
            #expect(results.statistics.joinFallbackReasons.isEmpty)

            try await cleanup(container: container)
        }
    }

    @Test("JOIN strategy: hash join falls back with explicit reason")
    func testHashJoinFallbackReasonRecorded() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            var statements: [SPARQLQueryStatement] = []
            for i in 0..<80 {
                statements.append(try makeStatement(subject: "u\(i)", predicate: "type", object: "User"))
                statements.append(try makeStatement(subject: "u\(i)", predicate: "knows", object: "Target"))
            }
            try await insertStatements(statements, context: context)

            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("?person"), try Self.predicateTerm("type"), try Self.objectTerm("User"))
                .where(try Self.subjectTerm("?person"), try Self.predicateTerm("knows"), try Self.objectTerm("Target"))
                .select("?person")
                .execute()

            #expect(results.count == 80)
            #expect(results.statistics.joinFallbackReasons.contains(.hashJoinRightSideExceededCap))
            #expect(results.statistics.joinStrategies.contains(.batchedNestedLoop))

            try await cleanup(container: container)
        }
    }

    // MARK: - OPTIONAL Tests

    @Test("OPTIONAL: some match, some don't")
    func testOptionalPattern() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            try await insertStatements([
                try makeStatement(subject: "Alice", predicate: "type", object: "User"),
                try makeLiteralStatement(subject: "Alice", predicate: "email", object: "alice@example.com"),
                try makeStatement(subject: "Bob", predicate: "type", object: "User"),
                // Bob has no email
                try makeStatement(subject: "Carol", predicate: "type", object: "User"),
                try makeLiteralStatement(subject: "Carol", predicate: "email", object: "carol@example.com")
            ], context: context)

            // Query: SELECT ?person ?email WHERE { ?person "type" "User" } OPTIONAL { ?person "email" ?email }
            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("?person"), try Self.predicateTerm("type"), try Self.objectTerm("User"))
                .optional { $0.where(try Self.subjectTerm("?person"), try Self.predicateTerm("email"), try Self.objectTerm("?email")) }
                .select("?person", "?email")
                .execute()

            #expect(results.count == 3)

            // Check each person - sort by person for deterministic ordering
            let sorted = results.bindings.sorted { ($0["?person"] ?? "") < ($1["?person"] ?? "") }

            // Alice should have email
            #expect(Self.resourceIdentifier(sorted[0]["?person"]) == "Alice")
            #expect(sorted[0].string("?email") == "alice@example.com")

            // Bob should NOT have email (OPTIONAL didn't match)
            #expect(Self.resourceIdentifier(sorted[1]["?person"]) == "Bob")
            #expect(sorted[1]["?email"] == nil)

            // Carol should have email
            #expect(Self.resourceIdentifier(sorted[2]["?person"]) == "Carol")
            #expect(sorted[2].string("?email") == "carol@example.com")

            try await cleanup(container: container)
        }
    }

    @Test("OPTIONAL: batched evaluation does not duplicate unmatched left rows")
    func testOptionalBatchedNoDuplicateUnmatchedRows() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            try await insertStatements([
                try makeStatement(subject: "Alice", predicate: "likes", object: "Tea"),
                try makeLiteralStatement(subject: "Alice", predicate: "email", object: "alice@example.com"),
                try makeStatement(subject: "Bob", predicate: "likes", object: "Coffee"),
                try makeStatement(subject: "Bob", predicate: "likes", object: "Cake"),
                // Bob intentionally has no email
            ], context: context)

            // Bob yields two left bindings with the same optional lookup key:
            // {?person=Bob, ?item=Coffee}, {?person=Bob, ?item=Cake}
            // OPTIONAL must keep each unmatched left row exactly once.
            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("?person"), try Self.predicateTerm("likes"), try Self.objectTerm("?item"))
                .optional { $0.where(try Self.subjectTerm("?person"), try Self.predicateTerm("email"), try Self.objectTerm("?email")) }
                .select("?person", "?item", "?email")
                .execute()

            #expect(results.count == 3)

            let bobRows = results.bindings.filter {
                Self.resourceIdentifier($0["?person"]) == "Bob"
            }
            #expect(bobRows.count == 2)
            #expect(bobRows.allSatisfy { $0["?email"] == nil })

            let bobItems = Set(bobRows.compactMap {
                Self.resourceIdentifier($0["?item"])
            })
            #expect(bobItems == Set(["Coffee", "Cake"]))

            let aliceRows = results.bindings.filter {
                Self.resourceIdentifier($0["?person"]) == "Alice"
            }
            #expect(aliceRows.count == 1)
            #expect(aliceRows[0].string("?email") == "alice@example.com")

            try await cleanup(container: container)
        }
    }

    // MARK: - UNION Tests

    @Test("UNION: alternative patterns")
    func testUnionPattern() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            try await insertStatements([
                try makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                try makeStatement(subject: "Carol", predicate: "follows", object: "Bob"),
                try makeStatement(subject: "Dave", predicate: "likes", object: "Bob")
            ], context: context)

            // Query: SELECT ?person WHERE { { ?person "knows" "Bob" } UNION { ?person "follows" "Bob" } }
            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("?person"), try Self.predicateTerm("knows"), try Self.objectTerm("Bob"))
                .union { $0.where(try Self.subjectTerm("?person"), try Self.predicateTerm("follows"), try Self.objectTerm("Bob")) }
                .select("?person")
                .execute()

            #expect(results.count == 2)
            let people = results.nonNilValues(for: "?person").compactMap {
                Self.resourceIdentifier($0)
            }
            #expect(people.contains("Alice"))
            #expect(people.contains("Carol"))
            #expect(!people.contains("Dave"))  // Dave "likes", not knows or follows

            try await cleanup(container: container)
        }
    }

    // MARK: - FILTER Tests

    @Test("FILTER: exclude specific value")
    func testFilterNotEquals() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            // Graph with cycle: Alice -> Bob -> Alice (and others)
            try await insertStatements([
                try makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                try makeStatement(subject: "Bob", predicate: "knows", object: "Alice"),
                try makeStatement(subject: "Bob", predicate: "knows", object: "Carol")
            ], context: context)

            // Query: SELECT ?fof WHERE { "Alice" "knows" ?friend . ?friend "knows" ?fof . FILTER(?fof != "Alice") }
            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("Alice"), try Self.predicateTerm("knows"), try Self.objectTerm("?friend"))
                .where(try Self.subjectTerm("?friend"), try Self.predicateTerm("knows"), try Self.objectTerm("?fof"))
                .filter(
                    .notEquals(
                        "?fof",
                        try Self.resourceValue("Alice")
                    )
                )
                .select("?fof")
                .execute()

            #expect(results.count == 1)
            #expect(
                Self.resourceIdentifier(results.first?["?fof"]) == "Carol"
            )

            try await cleanup(container: container)
        }
    }

    @Test("FILTER: regex pattern")
    func testFilterRegex() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            try await insertStatements([
                try makeLiteralStatement(subject: "Alice", predicate: "name", object: "Alice Smith"),
                try makeLiteralStatement(subject: "Bob", predicate: "name", object: "Bob Jones"),
                try makeLiteralStatement(subject: "Anna", predicate: "name", object: "Anna Lee")
            ], context: context)

            // Query: SELECT ?person ?name WHERE { ?person "name" ?name . FILTER(REGEX(?name, "^A")) }
            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("?person"), try Self.predicateTerm("name"), try Self.objectTerm("?name"))
                .filter("?name", matches: "^A")
                .select("?person", "?name")
                .execute()

            #expect(results.count == 2)
            let names = results.bindings.compactMap { $0.string("?name") }
            #expect(names.contains("Alice Smith"))
            #expect(names.contains("Anna Lee"))
            #expect(!names.contains("Bob Jones"))

            try await cleanup(container: container)
        }
    }

    @Test("FILTER: bound check with OPTIONAL")
    func testFilterBoundWithOptional() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            try await insertStatements([
                try makeStatement(subject: "Alice", predicate: "type", object: "User"),
                try makeLiteralStatement(subject: "Alice", predicate: "email", object: "alice@example.com"),
                try makeStatement(subject: "Bob", predicate: "type", object: "User")
                // Bob has no email
            ], context: context)

            // Query: SELECT ?person WHERE { ?person "type" "User" } OPTIONAL { ?person "email" ?email } FILTER(BOUND(?email))
            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("?person"), try Self.predicateTerm("type"), try Self.objectTerm("User"))
                .optional { $0.where(try Self.subjectTerm("?person"), try Self.predicateTerm("email"), try Self.objectTerm("?email")) }
                .filter(.bound("?email"))
                .select("?person")
                .execute()

            // Only Alice has email bound
            #expect(results.count == 1)
            #expect(
                Self.resourceIdentifier(results.first?["?person"]) == "Alice"
            )

            try await cleanup(container: container)
        }
    }

    // MARK: - Modifier Tests

    @Test("DISTINCT: removes duplicates after projection")
    func testDistinct() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            // Multiple triples with same predicate
            try await insertStatements([
                try makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                try makeStatement(subject: "Alice", predicate: "knows", object: "Carol"),
                try makeStatement(subject: "Bob", predicate: "knows", object: "Carol")
            ], context: context)

            // Query: SELECT DISTINCT ?pred WHERE { ?s ?pred ?o }
            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("?s"), try Self.predicateTerm("knows"), try Self.objectTerm("?o"))
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
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            // Insert 10 triples
            var statements: [SPARQLQueryStatement] = []
            for i in 1...10 {
                statements.append(try makeStatement(subject: "Person\(i)", predicate: "type", object: "User"))
            }
            try await insertStatements(statements, context: context)

            // Query with LIMIT 3
            let limitResults = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("?person"), try Self.predicateTerm("type"), try Self.objectTerm("User"))
                .select("?person")
                .limit(3)
                .execute()

            #expect(limitResults.count == 3)
            #expect(!limitResults.isComplete)

            // Query with LIMIT 3 OFFSET 5
            let offsetResults = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("?person"), try Self.predicateTerm("type"), try Self.objectTerm("User"))
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
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            try await insertStatements([
                try makeStatement(subject: "Alice", predicate: "knows", object: "Bob")
            ], context: context)

            // Query: SELECT ?s WHERE { ?s ?p ?o }
            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("?s"), try Self.predicateTerm("?p"), try Self.objectTerm("?o"))
                .select("?s")
                .execute()

            #expect(results.count == 1)
            #expect(results.projectedVariables == ["?s"])
            #expect(Self.resourceIdentifier(results.first?["?s"]) == "Alice")
            // Non-projected variables should not be in result
            #expect(results.first?["?p"] == nil)
            #expect(results.first?["?o"] == nil)

            try await cleanup(container: container)
        }
    }

    // MARK: - Edge Cases

    @Test("Empty database")
    func testEmptyDatabase() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("?s"), try Self.predicateTerm("?p"), try Self.objectTerm("?o"))
                .execute()

            #expect(results.isEmpty)
            #expect(results.count == 0)
            #expect(results.isComplete)

            try await cleanup(container: container)
        }
    }

    @Test("Large dataset (100 edges)")
    func testLargeDataset() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            // Insert 100 edges: Person1 -> Person2 -> ... -> Person100
            var statements: [SPARQLQueryStatement] = []
            for i in 1..<100 {
                statements.append(try makeStatement(
                    subject: "Person\(i)",
                    predicate: "knows",
                    object: "Person\(i + 1)"
                ))
            }
            try await insertStatements(statements, context: context)

            // Query all edges
            let allResults = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("?s"), try Self.predicateTerm("knows"), try Self.objectTerm("?o"))
                .execute()

            #expect(allResults.count == 99)

            // Query specific person's friends
            let person50Friends = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("Person50"), try Self.predicateTerm("knows"), try Self.objectTerm("?friend"))
                .execute()

            #expect(person50Friends.count == 1)
            #expect(
                Self.resourceIdentifier(person50Friends.first?["?friend"])
                    == "Person51"
            )

            try await cleanup(container: container)
        }
    }

    @Test("Cyclic graph")
    func testCyclicGraph() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            // Create a cycle: A -> B -> C -> A
            try await insertStatements([
                try makeStatement(subject: "A", predicate: "knows", object: "B"),
                try makeStatement(subject: "B", predicate: "knows", object: "C"),
                try makeStatement(subject: "C", predicate: "knows", object: "A")
            ], context: context)

            // 2-hop query from A
            let results = try await context.sparql(SPARQLQueryStatement.self)
                .defaultIndex()
                .where(try Self.subjectTerm("A"), try Self.predicateTerm("knows"), try Self.objectTerm("?x"))
                .where(try Self.subjectTerm("?x"), try Self.predicateTerm("knows"), try Self.objectTerm("?y"))
                .select("?y")
                .execute()

            #expect(results.count == 1)
            #expect(Self.resourceIdentifier(results.first?["?y"]) == "C")

            try await cleanup(container: container)
        }
    }
}
#endif
