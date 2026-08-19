#if POSTGRESQL
// PostgreSQLSPARQLTests.swift
// SPARQL query tests against PostgreSQL backend
//
// Validates graph index + SPARQL query execution with StaticDirectoryService.

import Testing
import DatabaseRuntime
import Foundation
import StorageKit
import PostgreSQLStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Model

@Persistable
struct PGStatement {
    #Directory<PGStatement>("test", "pg", "sparql", "statements")

    var id: String = UUID().uuidString
    var subject: RDFTerm = .iri(.xsdString)
    var predicate: RDFTerm = .iri(.xsdString)
    var object: RDFTerm = .iri(.xsdString)

    #Index(
        .graph(
            name: "PGStatement_rdf_quad_subject_predicate_object",
            definition: .rdf(
                subject: \PGStatement.subject, predicate: \PGStatement.predicate, object: \PGStatement.object,
                graph: nil)))
}

@Suite("PostgreSQL SPARQL Tests", .serialized, .heartbeat, .enabled(if: PostgreSQLScenarioCoordinator.isConfigured))
struct PostgreSQLSPARQLTests {
    private static let resourcePrefix =
        "did:database-framework:postgresql-test-resource:"
    private static let predicatePrefix =
        "did:database-framework:postgresql-test-predicate:"

    // MARK: - Setup

    private func setupContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [try PGStatement.schemaEntity], version: Schema.Version(1, 0, 0))
        return try await PostgreSQLScenarioCoordinator.shared.makeContainer(schema: schema, entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGStatement.self)])
    }

    private func makeScenario() async throws -> (DBContainer, DatabaseContext) {
        let container = try await setupContainer()
        let context = container.testBaseContext()
        return (container, context)
    }

    private func makeStatement(
        subject: String,
        predicate: String,
        object: String
    ) throws -> PGStatement {
        var stmt = PGStatement()
        stmt.subject = try Self.resource(subject)
        stmt.predicate = try Self.predicate(predicate)
        stmt.object = try Self.resource(object)
        return stmt
    }

    private static func resource(_ identifier: String) throws -> RDFTerm {
        try .iri(validating: resourcePrefix + identifier)
    }

    private static func predicate(_ identifier: String) throws -> RDFTerm {
        try .iri(validating: predicatePrefix + identifier)
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

    private static func resourceIdentifier(
        _ value: FieldValue?
    ) -> String? {
        guard case .rdfTerm(.iri(let iri)) = value,
              iri.rawValue.hasPrefix(resourcePrefix) else {
            return nil
        }
        return String(iri.rawValue.dropFirst(resourcePrefix.count))
    }

    private func insertStatements(_ statements: [PGStatement], context: DatabaseContext) async throws {
        for statement in statements {
            try context.insert(statement)
        }
        try await context.save()
    }

    // MARK: - Basic Pattern Tests

    @Test("Single pattern: subject bound")
    func singlePatternSubjectBound() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let (_, context) = try await makeScenario()

            let stmts = [
                try makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                try makeStatement(subject: "Alice", predicate: "likes", object: "Coffee"),
                try makeStatement(subject: "Bob", predicate: "knows", object: "Charlie"),
            ]
            try await insertStatements(stmts, context: context)

            // SPARQL: SELECT ?p ?o WHERE { "Alice" ?p ?o }
            let result = try await context.sparql(PGStatement.self)
                .defaultIndex()
                .where(
                    try Self.subjectTerm("Alice"),
                    try Self.predicateTerm("?p"),
                    try Self.objectTerm("?o")
                )
                .select("?p", "?o")
                .execute()

            #expect(result.count == 2)

            let predicates = result.nonNilValues(for: "?p")
            #expect(predicates.contains(.rdfTerm(try Self.predicate("knows"))))
            #expect(predicates.contains(.rdfTerm(try Self.predicate("likes"))))
        }
    }

    @Test("Single pattern: object bound")
    func singlePatternObjectBound() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let (_, context) = try await makeScenario()

            let stmts = [
                try makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                try makeStatement(subject: "Charlie", predicate: "knows", object: "Bob"),
                try makeStatement(subject: "Dave", predicate: "likes", object: "Bob"),
            ]
            try await insertStatements(stmts, context: context)

            // Find who knows Bob
            let result = try await context.sparql(PGStatement.self)
                .defaultIndex()
                .where(
                    try Self.subjectTerm("?s"),
                    try Self.predicateTerm("knows"),
                    try Self.objectTerm("Bob")
                )
                .select("?s")
                .execute()

            let subjects = result.nonNilValues(for: "?s").compactMap {
                Self.resourceIdentifier($0)
            }
            #expect(subjects.contains("Alice"))
            #expect(subjects.contains("Charlie"))
            #expect(!subjects.contains("Dave"))
        }
    }

    // MARK: - Multi-Pattern (Join) Tests

    @Test("Two-pattern join: friend of a friend")
    func twoPatternJoin() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let (_, context) = try await makeScenario()

            let stmts = [
                try makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                try makeStatement(subject: "Bob", predicate: "knows", object: "Charlie"),
                try makeStatement(subject: "Bob", predicate: "knows", object: "Dave"),
            ]
            try await insertStatements(stmts, context: context)

            // Friends of friends of Alice
            let result = try await context.sparql(PGStatement.self)
                .defaultIndex()
                .where(
                    try Self.subjectTerm("Alice"),
                    try Self.predicateTerm("knows"),
                    try Self.objectTerm("?friend")
                )
                .where(
                    try Self.subjectTerm("?friend"),
                    try Self.predicateTerm("knows"),
                    try Self.objectTerm("?foaf")
                )
                .select("?foaf")
                .execute()

            let foafs = result.nonNilValues(for: "?foaf").compactMap {
                Self.resourceIdentifier($0)
            }
            #expect(foafs.contains("Charlie"))
            #expect(foafs.contains("Dave"))
        }
    }

    // MARK: - Empty Results

    @Test("Query with no matches returns empty")
    func noMatches() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let (_, context) = try await makeScenario()

            let stmts = [
                try makeStatement(subject: "Alice", predicate: "knows", object: "Bob")
            ]
            try await insertStatements(stmts, context: context)

            // Query for non-existent predicate
            let result = try await context.sparql(PGStatement.self)
                .defaultIndex()
                .where(
                    try Self.subjectTerm("Alice"),
                    try Self.predicateTerm("hates"),
                    try Self.objectTerm("?o")
                )
                .select("?o")
                .execute()

            #expect(result.count == 0)
        }
    }

    // MARK: - All Variables (Full Scan)

    @Test("All variables returns all triples")
    func allVariables() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let (_, context) = try await makeScenario()

            let stmts = [
                try makeStatement(subject: "A", predicate: "r1", object: "B"),
                try makeStatement(subject: "C", predicate: "r2", object: "D"),
            ]
            try await insertStatements(stmts, context: context)

            let result = try await context.sparql(PGStatement.self)
                .defaultIndex()
                .where(
                    try Self.subjectTerm("?s"),
                    try Self.predicateTerm("?p"),
                    try Self.objectTerm("?o")
                )
                .select("?s", "?p", "?o")
                .execute()

            #expect(result.count >= 2)
        }
    }

    // MARK: - Graph Traversal

    @Test("Traverse graph: two-hop path")
    func graphTraversal() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let (_, context) = try await makeScenario()

            // A -> B -> C -> D
            let stmts = [
                try makeStatement(subject: "A", predicate: "next", object: "B"),
                try makeStatement(subject: "B", predicate: "next", object: "C"),
                try makeStatement(subject: "C", predicate: "next", object: "D"),
            ]
            try await insertStatements(stmts, context: context)

            // Find direct next of A
            let direct = try await context.sparql(PGStatement.self)
                .defaultIndex()
                .where(
                    try Self.subjectTerm("A"),
                    try Self.predicateTerm("next"),
                    try Self.objectTerm("?next")
                )
                .select("?next")
                .execute()

            #expect(direct.count == 1)
            #expect(
                direct.nonNilValues(for: "?next").compactMap {
                    Self.resourceIdentifier($0)
                }.contains("B")
            )

            // Find two-hop path: A -> ?mid -> ?end
            let twoHop = try await context.sparql(PGStatement.self)
                .defaultIndex()
                .where(
                    try Self.subjectTerm("A"),
                    try Self.predicateTerm("next"),
                    try Self.objectTerm("?mid")
                )
                .where(
                    try Self.subjectTerm("?mid"),
                    try Self.predicateTerm("next"),
                    try Self.objectTerm("?end")
                )
                .select("?mid", "?end")
                .execute()

            #expect(twoHop.count == 1)
            #expect(
                twoHop.nonNilValues(for: "?mid").compactMap {
                    Self.resourceIdentifier($0)
                }.contains("B")
            )
            #expect(
                twoHop.nonNilValues(for: "?end").compactMap {
                    Self.resourceIdentifier($0)
                }.contains("C")
            )
        }
    }
}
#endif
