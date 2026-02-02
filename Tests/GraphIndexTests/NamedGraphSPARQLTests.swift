// NamedGraphSPARQLTests.swift
// SPARQL integration tests for Named Graph (Quad) support
//
// Layer 3: End-to-end SPARQL execution with graph field (FDB required)
// Uses executeSPARQLPattern with ExecutionPattern.withGraph() to verify
// graph filtering and variable binding at the query execution level.

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Model (Quad Statement)

@Persistable
struct SPARQLQuadStatement {
    #Directory<SPARQLQuadStatement>("test", "sparql", "quads")

    var id: String = ULID().ulidString
    var subject: String = ""
    var predicate: String = ""
    var object: String = ""
    var graph: String = ""

    #Index(GraphIndexKind<SPARQLQuadStatement>(
        from: \.subject,
        edge: \.predicate,
        to: \.object,
        graph: \.graph,
        strategy: .hexastore
    ))
}

// MARK: - Test Suite

@Suite("NamedGraph SPARQL Integration Tests", .serialized)
struct NamedGraphSPARQLTests {

    // MARK: - Setup Helpers

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let schema = Schema([SPARQLQuadStatement.self], version: Schema.Version(1, 0, 0))
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    private func cleanup(container: FDBContainer) async throws {
        let directoryLayer = DirectoryLayer(database: container.database)
        try? await directoryLayer.remove(path: ["test", "sparql", "quads"])
    }

    private func setIndexStatesToReadable(container: FDBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: SPARQLQuadStatement.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in SPARQLQuadStatement.indexDescriptors {
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

    private func makeQuad(
        subject: String,
        predicate: String,
        object: String,
        graph: String
    ) -> SPARQLQuadStatement {
        var stmt = SPARQLQuadStatement()
        stmt.subject = subject
        stmt.predicate = predicate
        stmt.object = object
        stmt.graph = graph
        return stmt
    }

    /// Standard test data:
    /// g1 (Social): Alice knows Bob, Alice knows Carol, Bob knows Carol
    /// g2 (Work): Alice worksAt Acme, Bob worksAt Beta
    private func insertTestData(context: FDBContext) async throws {
        let quads = [
            makeQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1"),
            makeQuad(subject: "Alice", predicate: "knows", object: "Carol", graph: "g1"),
            makeQuad(subject: "Bob", predicate: "knows", object: "Carol", graph: "g1"),
            makeQuad(subject: "Alice", predicate: "worksAt", object: "Acme", graph: "g2"),
            makeQuad(subject: "Bob", predicate: "worksAt", object: "Beta", graph: "g2"),
        ]
        for quad in quads {
            context.insert(quad)
        }
        try await context.save()
    }

    // MARK: - Basic Named Graph Query Tests

    @Test("Query without graph returns all graphs")
    func testQueryWithoutGraphReturnsAllGraphs() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()
        try await insertTestData(context: context)

        // No graph constraint: should return all 5 triples
        let results = try await context.sparql(SPARQLQuadStatement.self)
            .defaultIndex()
            .where("?s", "?p", "?o")
            .select("?s", "?p", "?o")
            .execute()

        #expect(results.count == 5)

        try await cleanup(container: container)
    }

    @Test("Query with graph value filters single graph")
    func testQueryWithGraphValueFiltersSingleGraph() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()
        try await insertTestData(context: context)

        // Build pattern with graph constraint: only g1
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: .variable("?p"),
                object: .variable("?o"),
                graph: .value(.string("g1"))
            )
        ])

        let results = try await context.executeSPARQLPattern(
            pattern,
            on: SPARQLQuadStatement.self,
            projection: ["?s", "?p", "?o"]
        )

        // g1 has 3 triples (Alice knows Bob, Alice knows Carol, Bob knows Carol)
        #expect(results.count == 3)
        let predicates = Set(results.bindings.compactMap { $0["?p"]?.stringValue })
        #expect(predicates == Set(["knows"]))

        try await cleanup(container: container)
    }

    @Test("Query with graph variable binds graph name")
    func testQueryWithGraphVariableBindsGraphName() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()
        try await insertTestData(context: context)

        // Pattern with graph variable: bind graph names
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: .variable("?p"),
                object: .variable("?o"),
                graph: .variable("?g")
            )
        ])

        let results = try await context.executeSPARQLPattern(
            pattern,
            on: SPARQLQuadStatement.self,
            projection: ["?s", "?p", "?o", "?g"]
        )

        #expect(results.count == 5)

        // Every binding should have ?g set
        for binding in results.bindings {
            #expect(binding["?g"] != nil, "Each binding should have ?g")
        }

        // Distinct graph values
        let graphs = Set(results.bindings.compactMap { $0["?g"]?.stringValue })
        #expect(graphs == Set(["g1", "g2"]))

        try await cleanup(container: container)
    }

    @Test("Query non-existent graph returns empty")
    func testQueryNonExistentGraphReturnsEmpty() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()
        try await insertTestData(context: context)

        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: .variable("?p"),
                object: .variable("?o"),
                graph: .value(.string("nonexistent"))
            )
        ])

        let results = try await context.executeSPARQLPattern(
            pattern,
            on: SPARQLQuadStatement.self,
            projection: ["?s", "?p", "?o"]
        )

        #expect(results.count == 0)

        try await cleanup(container: container)
    }

    // MARK: - Join Tests

    @Test("Join within same graph")
    func testJoinWithinSameGraph() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()
        try await insertTestData(context: context)

        // Find friends-of-friends within g1:
        // ?s knows ?mid AND ?mid knows ?o, both in g1
        let graphTerm: ExecutionTerm = .value(.string("g1"))
        let left = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .value(.string("Alice")),
                predicate: .value(.string("knows")),
                object: .variable("?mid"),
                graph: graphTerm
            )
        ])
        let right = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?mid"),
                predicate: .value(.string("knows")),
                object: .variable("?fof"),
                graph: graphTerm
            )
        ])
        let pattern = ExecutionPattern.join(left, right)

        let results = try await context.executeSPARQLPattern(
            pattern,
            on: SPARQLQuadStatement.self,
            projection: ["?mid", "?fof"]
        )

        // Alice knows Bob and Carol. Bob knows Carol. Carol knows nobody.
        // So: mid=Bob, fof=Carol
        #expect(results.count == 1)
        #expect(results.bindings[0]["?mid"]?.stringValue == "Bob")
        #expect(results.bindings[0]["?fof"]?.stringValue == "Carol")

        try await cleanup(container: container)
    }

    // MARK: - OPTIONAL Test

    @Test("OPTIONAL with graph constraint")
    func testOptionalWithGraphConstraint() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()
        try await insertTestData(context: context)

        // All subjects in g1, optionally their workplace in g2
        let mandatory = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: .value(.string("knows")),
                object: .variable("?o"),
                graph: .value(.string("g1"))
            )
        ])
        let optional = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: .value(.string("worksAt")),
                object: .variable("?company"),
                graph: .value(.string("g2"))
            )
        ])
        let pattern = ExecutionPattern.optional(mandatory, optional)

        let results = try await context.executeSPARQLPattern(
            pattern,
            on: SPARQLQuadStatement.self,
            projection: ["?s", "?o", "?company"]
        )

        // 3 knows triples in g1, each optionally joined with worksAt in g2
        #expect(results.count == 3)

        // Alice has a company (Acme), Bob has a company (Beta)
        let aliceBindings = results.bindings.filter { $0["?s"]?.stringValue == "Alice" }
        #expect(aliceBindings.allSatisfy { $0["?company"]?.stringValue == "Acme" })

        let bobBindings = results.bindings.filter { $0["?s"]?.stringValue == "Bob" }
        #expect(bobBindings.allSatisfy { $0["?company"]?.stringValue == "Beta" })

        try await cleanup(container: container)
    }

    // MARK: - Backward Compatibility Test

    @Test("Triple model without graph field still works")
    func testTripleModelWithoutGraphStillWorks() async throws {
        let container: FDBContainer
        do {
            try await FDBTestSetup.shared.initialize()
            let database = try FDBClient.openDatabase()
            let schema = Schema(
                [SPARQLTestStatement.self],
                version: Schema.Version(1, 0, 0)
            )
            container = FDBContainer(database: database, schema: schema, security: .disabled)
        }

        let directoryLayer = DirectoryLayer(database: container.database)
        try? await directoryLayer.remove(path: ["test", "sparql", "statements"])

        let subspace = try await container.resolveDirectory(for: SPARQLTestStatement.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)
        for descriptor in SPARQLTestStatement.indexDescriptors {
            let state = try await indexStateManager.state(of: descriptor.name)
            if state == .disabled {
                try await indexStateManager.enable(descriptor.name)
                try await indexStateManager.makeReadable(descriptor.name)
            } else if state == .writeOnly {
                try await indexStateManager.makeReadable(descriptor.name)
            }
        }

        let context = container.newContext()
        var stmt = SPARQLTestStatement()
        stmt.subject = "Alice"
        stmt.predicate = "knows"
        stmt.object = "Bob"
        context.insert(stmt)
        try await context.save()

        let results = try await context.sparql(SPARQLTestStatement.self)
            .defaultIndex()
            .where("Alice", "knows", "?friend")
            .select("?friend")
            .execute()

        #expect(results.count == 1)
        #expect(results.bindings[0]["?friend"]?.stringValue == "Bob")

        try? await directoryLayer.remove(path: ["test", "sparql", "statements"])
    }
}
