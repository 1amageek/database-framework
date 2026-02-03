/// SPARQLPropertyFilterIntegrationTests.swift
/// Integration tests for SPARQL property filter pushdown optimization

import Testing
import Foundation
import Core
import Graph
import DatabaseEngine
import FoundationDB
import TestSupport
@testable import GraphIndex

@Persistable
fileprivate struct SocialConnection {
    #Directory<SocialConnection>("test", "sparql_property")

    var id: String = UUID().uuidString
    var from: String = ""
    var target: String = ""
    var relation: String = ""
    var since: Int = 0
    var strength: Double = 0.0
    var status: String = "active"

    #Index(GraphIndexKind<SocialConnection>(
        from: \.from,
        edge: \.relation,
        to: \.target,
        graph: nil,
        strategy: .tripleStore
    ), storedFields: [
        \SocialConnection.since,
        \SocialConnection.strength,
        \SocialConnection.status
    ], name: "social_graph")
}

// Test model without storedFields (for backward compatibility testing)
@Persistable
fileprivate struct BasicEdge {
    #Directory<BasicEdge>("test", "basic_edge")
    var id: String = UUID().uuidString
    var from: String = ""
    var target: String = ""
    var label: String = ""

    #Index(GraphIndexKind<BasicEdge>(
        from: \.from,
        edge: \.label,
        to: \.target,
        graph: nil,
        strategy: .tripleStore
    ), name: "basic_graph")
}

@Suite("SPARQL Property Filter Integration Tests", .serialized)
struct SPARQLPropertyFilterIntegrationTests {

    // MARK: - Setup

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func makeConnection(
        from: String,
        to: String,
        relation: String,
        since: Int,
        strength: Double,
        status: String = "active"
    ) -> SocialConnection {
        SocialConnection(
            from: from,
            target: to,
            relation: relation,
            since: since,
            strength: strength,
            status: status
        )
    }

    private func setupContainer() async throws -> FDBContainer {
        let database = try FDBClient.openDatabase()
        let schema = Schema([SocialConnection.self], version: Schema.Version(1, 0, 0))
        let container = FDBContainer(database: database, schema: schema, security: .disabled)

        let directoryLayer = DirectoryLayer(database: database)
        try? await directoryLayer.remove(path: ["test", "sparql_property"])

        // Set index states to readable
        try await setIndexStatesToReadable(container: container)

        return container
    }

    private func setIndexStatesToReadable(container: FDBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: SocialConnection.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in SocialConnection.indexDescriptors {
            let currentState = try await indexStateManager.state(of: descriptor.name)

            switch currentState {
            case .disabled:
                try await indexStateManager.enable(descriptor.name)
                try await indexStateManager.makeReadable(descriptor.name)
            case .writeOnly:
                try await indexStateManager.makeReadable(descriptor.name)
            case .readable:
                break
            }
        }
    }

    // MARK: - Property Filter Pushdown Tests

    @Test("Property filter pushdown - equality")
    func testPropertyFilterPushdownEquality() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        // Insert 100 connections (only 1 with since = 2020)
        for year in 2010..<2020 {
            context.insert(makeConnection(
                from: alice,
                to: uniqueID("user-\(year)"),
                relation: "knows",
                since: year,
                strength: 0.5
            ))
        }
        context.insert(makeConnection(
            from: alice,
            to: uniqueID("user-2020"),
            relation: "knows",
            since: 2020,
            strength: 0.9,
            status: "active"
        ))
        try await context.save()

        // SPARQL query with property filter
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
                )
            ]),
            .equals("?since", .int64(2020))
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SocialConnection.self
        )

        #expect(result.bindings.count == 1)
        #expect(result.bindings[0]["?since"] == .int64(2020))
    }

    @Test("Property filter pushdown - range comparison")
    func testPropertyFilterPushdownRange() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        // Insert connections from 2015-2024
        for year in 2015...2024 {
            context.insert(makeConnection(
                from: alice,
                to: uniqueID("user-\(year)"),
                relation: "knows",
                since: year,
                strength: Double(year) / 100.0
            ))
        }
        try await context.save()

        // Filter: since >= 2020
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
                )
            ]),
            .greaterThanOrEqual("?since", .int64(2020))
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SocialConnection.self
        )

        #expect(result.bindings.count == 5)  // 2020-2024
        #expect(result.bindings.allSatisfy { binding in
            if case .int64(let year) = binding["?since"] {
                return year >= 2020
            }
            return false
        })
    }

    @Test("Property filter pushdown - string contains")
    func testPropertyFilterPushdownStringContains() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        context.insert(makeConnection(from: alice, to: uniqueID("bob"), relation: "knows", since: 2020, strength: 0.5, status: "active-premium"))
        context.insert(makeConnection(from: alice, to: uniqueID("carol"), relation: "knows", since: 2021, strength: 0.6, status: "disabled"))
        context.insert(makeConnection(from: alice, to: uniqueID("dave"), relation: "knows", since: 2022, strength: 0.7, status: "active"))
        try await context.save()

        // Filter: status CONTAINS "active"
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
                )
            ]),
            .contains("?status", "active")
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SocialConnection.self
        )

        #expect(result.bindings.count == 2)  // "active-premium" and "active"
    }

    // MARK: - AND Decomposition Tests

    @Test("AND decomposition - multiple pushable filters")
    func testAndMultiplePushable() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        context.insert(makeConnection(from: alice, to: uniqueID("bob"), relation: "knows", since: 2020, strength: 0.9, status: "active"))
        context.insert(makeConnection(from: alice, to: uniqueID("carol"), relation: "knows", since: 2020, strength: 0.3, status: "inactive"))
        context.insert(makeConnection(from: alice, to: uniqueID("dave"), relation: "knows", since: 2021, strength: 0.9, status: "active"))
        try await context.save()

        // Filter: since = 2020 AND strength >= 0.5
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
                )
            ]),
            .and(
                .equals("?since", .int64(2020)),
                .greaterThanOrEqual("?strength", .double(0.5))
            )
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SocialConnection.self
        )

        #expect(result.bindings.count == 1)  // Only Bob (2020 + strength 0.9)
    }

    @Test("AND decomposition - pushable + complex filter")
    func testAndPushableAndComplex() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        context.insert(makeConnection(from: alice, to: uniqueID("bob"), relation: "knows", since: 2020, strength: 0.9, status: "active"))
        context.insert(makeConnection(from: alice, to: uniqueID("carol"), relation: "knows", since: 2020, strength: 0.5, status: "inactive"))
        context.insert(makeConnection(from: alice, to: uniqueID("dave"), relation: "knows", since: 2021, strength: 0.9, status: "active"))
        try await context.save()

        // Filter: since = 2020 AND status =~ /^active/
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
                )
            ]),
            .and(
                .equals("?since", .int64(2020)),  // Pushable
                .regex("?status", "^active")      // Complex (post-scan)
            )
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SocialConnection.self
        )

        #expect(result.bindings.count == 1)  // Only Bob
        #expect(result.bindings[0]["?target"] != nil)
    }

    // MARK: - Backward Compatibility Tests

    @Test("Backward compatibility - no stored fields")
    func testBackwardCompatibilityNoStoredFields() async throws {
        let database = try FDBClient.openDatabase()
        let schema = Schema([BasicEdge.self], version: Schema.Version(1, 0, 0))
        let container = FDBContainer(database: database, schema: schema, security: .disabled)

        let directoryLayer = DirectoryLayer(database: database)
        try? await directoryLayer.remove(path: ["test", "basic_edge"])

        // Set index to readable
        let subspace = try await container.resolveDirectory(for: BasicEdge.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in BasicEdge.indexDescriptors {
            let currentState = try await indexStateManager.state(of: descriptor.name)
            if currentState == .disabled {
                try await indexStateManager.enable(descriptor.name)
                try await indexStateManager.makeReadable(descriptor.name)
            } else if currentState == .writeOnly {
                try await indexStateManager.makeReadable(descriptor.name)
            }
        }

        let context = FDBContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")

        let edge = BasicEdge(from: alice, target: bob, label: "knows")
        context.insert(edge)
        try await context.save()

        // This should use legacy path (no storedFieldNames)
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .value(.string(alice)),
                predicate: .value(.string("knows")),
                object: .variable("?target"),
                graph: nil
            )
        ])

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: BasicEdge.self
        )

        #expect(result.bindings.count == 1)
        #expect(result.bindings[0]["?target"] == .string(bob))
    }

    // MARK: - Performance Tests

    @Test("Performance - property filter reduces scan")
    func testPerformancePropertyFilterReducesScan() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        // Insert 200 connections (only 2 with since = 2025)
        for i in 0..<198 {
            let year = 2010 + (i % 10)  // Years 2010-2019
            context.insert(makeConnection(
                from: alice,
                to: uniqueID("old-\(i)"),
                relation: "knows",
                since: year,
                strength: 0.5
            ))
        }
        // Add 2 recent connections
        context.insert(makeConnection(from: alice, to: uniqueID("recent-1"), relation: "knows", since: 2025, strength: 0.9))
        context.insert(makeConnection(from: alice, to: uniqueID("recent-2"), relation: "knows", since: 2025, strength: 0.95))
        try await context.save()

        // Filter to only 2025 (1% selectivity)
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
                )
            ]),
            .equals("?since", .int64(2025))
        )

        let startTime = Date()
        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SocialConnection.self
        )
        let duration = Date().timeIntervalSince(startTime)

        #expect(result.bindings.count == 2)
        #expect(duration < 1.0)  // Should be fast with early filtering
    }

    @Test("Property variables are bound in results")
    func testPropertyVariablesAreBound() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")

        context.insert(makeConnection(from: alice, to: bob, relation: "knows", since: 2020, strength: 0.9, status: "active"))
        try await context.save()

        // No filter - just check property variables are bound
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .value(.string(alice)),
                predicate: .value(.string("knows")),
                object: .variable("?target"),
                graph: nil
            )
        ])

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SocialConnection.self
        )

        #expect(result.bindings.count == 1)
        let binding = result.bindings[0]

        // Check all property variables are bound
        #expect(binding["?since"] == .int64(2020))
        #expect(binding["?strength"] == .double(0.9))
        #expect(binding["?status"] == .string("active"))
        #expect(binding["?target"] == .string(bob))
    }
}
