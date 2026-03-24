#if POSTGRESQL
// PostgreSQLSPARQLTests.swift
// SPARQL query tests against PostgreSQL backend
//
// Validates graph index + SPARQL query execution with StaticDirectoryService.

import Testing
import Foundation
import StorageKit
import PostgreSQLStorage
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Model

@Persistable
struct PGStatement {
    #Directory<PGStatement>("test", "pg", "sparql", "statements")

    var id: String = ULID().ulidString
    var subject: String = ""
    var predicate: String = ""
    var object: String = ""

    #Index(GraphIndexKind<PGStatement>(
        from: \.subject,
        edge: \.predicate,
        to: \.object,
        strategy: .hexastore
    ))
}

@Suite("PostgreSQL SPARQL Tests", .serialized)
struct PostgreSQLSPARQLTests {

    // MARK: - Setup

    private func setupContainer() async throws -> DBContainer {
        let schema = Schema([PGStatement.self], version: Schema.Version(1, 0, 0))
        return try await PostgreSQLTestSetup.shared.makeContainer(schema: schema)
    }

    private func cleanupAndSetup() async throws -> (DBContainer, FDBContext) {
        let container = try await setupContainer()
        try? await container.engine.directoryService.remove(path: ["test", "pg", "sparql", "statements"])
        let container2 = try await setupContainer()
        try await setIndexStatesToReadable(container: container2)
        let context = container2.newContext()
        return (container2, context)
    }

    private func setIndexStatesToReadable(container: DBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: PGStatement.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in PGStatement.indexDescriptors {
            let maxAttempts = 3
            for attempt in 1...maxAttempts {
                let currentState = try await indexStateManager.state(of: descriptor.name)

                switch currentState {
                case .disabled:
                    do {
                        try await indexStateManager.enable(descriptor.name)
                        try await indexStateManager.makeReadable(descriptor.name)
                        break
                    } catch let error as IndexStateError {
                        if case .invalidTransition = error, attempt < maxAttempts {
                            continue
                        }
                        throw error
                    }
                case .writeOnly:
                    do {
                        try await indexStateManager.makeReadable(descriptor.name)
                        break
                    } catch let error as IndexStateError {
                        if case .invalidTransition = error, attempt < maxAttempts {
                            continue
                        }
                        throw error
                    }
                case .readable:
                    break
                }
            }
        }
    }

    private func makeStatement(subject: String, predicate: String, object: String) -> PGStatement {
        var stmt = PGStatement()
        stmt.subject = subject
        stmt.predicate = predicate
        stmt.object = object
        return stmt
    }

    private func insertStatements(_ statements: [PGStatement], context: FDBContext) async throws {
        for statement in statements {
            context.insert(statement)
        }
        try await context.save()
    }

    // MARK: - Basic Pattern Tests

    @Test("Single pattern: subject bound")
    func singlePatternSubjectBound() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let (_, context) = try await cleanupAndSetup()

            let stmts = [
                makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                makeStatement(subject: "Alice", predicate: "likes", object: "Coffee"),
                makeStatement(subject: "Bob", predicate: "knows", object: "Charlie"),
            ]
            try await insertStatements(stmts, context: context)

            // SPARQL: SELECT ?p ?o WHERE { "Alice" ?p ?o }
            let result = try await context.sparql(PGStatement.self)
                .defaultIndex()
                .where("Alice", "?p", "?o")
                .select("?p", "?o")
                .execute()

            #expect(result.count == 2)

            let predicates = result.nonNilValues(for: "?p")
            #expect(predicates.contains(.string("knows")))
            #expect(predicates.contains(.string("likes")))
        }
    }

    @Test("Single pattern: object bound")
    func singlePatternObjectBound() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let (_, context) = try await cleanupAndSetup()

            let stmts = [
                makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                makeStatement(subject: "Charlie", predicate: "knows", object: "Bob"),
                makeStatement(subject: "Dave", predicate: "likes", object: "Bob"),
            ]
            try await insertStatements(stmts, context: context)

            // Find who knows Bob
            let result = try await context.sparql(PGStatement.self)
                .defaultIndex()
                .where("?s", "knows", "Bob")
                .select("?s")
                .execute()

            let subjects = result.nonNilValues(for: "?s")
            #expect(subjects.contains(.string("Alice")))
            #expect(subjects.contains(.string("Charlie")))
            #expect(!subjects.contains(.string("Dave"))) // Dave "likes" Bob, not "knows"
        }
    }

    // MARK: - Multi-Pattern (Join) Tests

    @Test("Two-pattern join: friend of a friend")
    func twoPatternJoin() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let (_, context) = try await cleanupAndSetup()

            let stmts = [
                makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
                makeStatement(subject: "Bob", predicate: "knows", object: "Charlie"),
                makeStatement(subject: "Bob", predicate: "knows", object: "Dave"),
            ]
            try await insertStatements(stmts, context: context)

            // Friends of friends of Alice
            let result = try await context.sparql(PGStatement.self)
                .defaultIndex()
                .where("Alice", "knows", "?friend")
                .where("?friend", "knows", "?foaf")
                .select("?foaf")
                .execute()

            let foafs = result.nonNilValues(for: "?foaf")
            #expect(foafs.contains(.string("Charlie")))
            #expect(foafs.contains(.string("Dave")))
        }
    }

    // MARK: - Empty Results

    @Test("Query with no matches returns empty")
    func noMatches() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let (_, context) = try await cleanupAndSetup()

            let stmts = [
                makeStatement(subject: "Alice", predicate: "knows", object: "Bob"),
            ]
            try await insertStatements(stmts, context: context)

            // Query for non-existent predicate
            let result = try await context.sparql(PGStatement.self)
                .defaultIndex()
                .where("Alice", "hates", "?o")
                .select("?o")
                .execute()

            #expect(result.count == 0)
        }
    }

    // MARK: - All Variables (Full Scan)

    @Test("All variables returns all triples")
    func allVariables() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let (_, context) = try await cleanupAndSetup()

            let stmts = [
                makeStatement(subject: "A", predicate: "r1", object: "B"),
                makeStatement(subject: "C", predicate: "r2", object: "D"),
            ]
            try await insertStatements(stmts, context: context)

            let result = try await context.sparql(PGStatement.self)
                .defaultIndex()
                .where("?s", "?p", "?o")
                .select("?s", "?p", "?o")
                .execute()

            #expect(result.count >= 2)
        }
    }

    // MARK: - Graph Traversal

    @Test("Traverse graph: two-hop path")
    func graphTraversal() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let (_, context) = try await cleanupAndSetup()

            // A -> B -> C -> D
            let stmts = [
                makeStatement(subject: "A", predicate: "next", object: "B"),
                makeStatement(subject: "B", predicate: "next", object: "C"),
                makeStatement(subject: "C", predicate: "next", object: "D"),
            ]
            try await insertStatements(stmts, context: context)

            // Find direct next of A
            let direct = try await context.sparql(PGStatement.self)
                .defaultIndex()
                .where("A", "next", "?next")
                .select("?next")
                .execute()

            #expect(direct.count == 1)
            #expect(direct.nonNilValues(for: "?next").contains(.string("B")))

            // Find two-hop path: A -> ?mid -> ?end
            let twoHop = try await context.sparql(PGStatement.self)
                .defaultIndex()
                .where("A", "next", "?mid")
                .where("?mid", "next", "?end")
                .select("?mid", "?end")
                .execute()

            #expect(twoHop.count == 1)
            #expect(twoHop.nonNilValues(for: "?mid").contains(.string("B")))
            #expect(twoHop.nonNilValues(for: "?end").contains(.string("C")))
        }
    }
}
#endif
