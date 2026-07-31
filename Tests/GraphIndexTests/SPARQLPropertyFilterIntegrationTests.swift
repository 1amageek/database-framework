#if FOUNDATION_DB
/// SPARQLPropertyFilterIntegrationTests.swift
/// Integration tests for SPARQL property filter pushdown optimization

import Testing
import Foundation
import DatabaseKit
import DatabaseTypes
import DatabaseRuntime
import DatabaseEngine
import StorageKit
import FDBStorage
import TestSupport
@testable import GraphIndex

@Persistable
fileprivate struct SocialConnection {
    #Directory<SocialConnection>("test", "sparql_property")

    var id: String = UUID().uuidString
    var from: RDFTerm = .iri(.xsdString)
    var target: RDFTerm = .iri(.xsdString)
    var relation: RDFTerm = .iri(.xsdString)
    var since: Int64 = 0
    var strength: Double = 0.0
    var status: String = "active"

    #Index(
        .rdfDataset,
        from: \SocialConnection.from,
        edge: \SocialConnection.relation,
        to: \SocialConnection.target,
        storedFields: [
            \SocialConnection.since,
            \SocialConnection.strength,
            \SocialConnection.status,
        ],
        name: "social_graph"
    )
}

// RDF dataset model without stored fields.
@Persistable
fileprivate struct BasicEdge {
    #Directory<BasicEdge>("test", "basic_edge")
    var id: String = UUID().uuidString
    var from: RDFTerm = .iri(.xsdString)
    var target: RDFTerm = .iri(.xsdString)
    var label: RDFTerm = .iri(.xsdString)

    #Index(
        .rdfDataset,
        from: \BasicEdge.from,
        edge: \BasicEdge.label,
        to: \BasicEdge.target,
        name: "basic_graph"
    )
}

@Suite("SPARQL Property Filter Integration Tests", .serialized, .foundationDBScenario, .heartbeat)
struct SPARQLPropertyFilterIntegrationTests {

    // MARK: - Setup

    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func makeConnection(
        from: String,
        to: String,
        relation: String,
        since: Int64,
        strength: Double,
        status: String = "active"
    ) throws -> SocialConnection {
        var connection = SocialConnection()
        connection.from = try Self.resource(from)
        connection.target = try Self.resource(to)
        connection.relation = try Self.predicate(relation)
        connection.since = since
        connection.strength = strength
        connection.status = status
        return connection
    }

    private func makeBasicEdge(
        from: String,
        target: String,
        label: String
    ) throws -> BasicEdge {
        var edge = BasicEdge()
        edge.from = try Self.resource(from)
        edge.target = try Self.resource(target)
        edge.label = try Self.predicate(label)
        return edge
    }

    private static func resource(_ identifier: String) throws -> RDFTerm {
        try .iri(
            validating: "did:database-framework:test-resource:\(identifier)"
        )
    }

    private static func predicate(_ identifier: String) throws -> RDFTerm {
        try .iri(
            validating: "did:database-framework:test-predicate:\(identifier)"
        )
    }

    private func setupContainer() async throws -> DBContainer {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [try SocialConnection.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: database), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SocialConnection.self), try DatabaseFrameworkRuntime.entity(BasicEdge.self)]), security: .disabled)


        if try await database.namespaceExists(
            path: ["test", "sparql_property"]
        ) {
            try await database.removeNamespace(
                path: ["test", "sparql_property"]
            )
        }
        try await container.ensureIndexesReady()

        return container
    }

    // MARK: - Property Filter Pushdown Tests

    @Test("Property filter pushdown - equality")
    func testPropertyFilterPushdownEquality() async throws {
        let container = try await setupContainer()
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")

        // Insert 100 connections (only 1 with since = 2020)
        for year in 2010..<2020 {
            try context.insert(try makeConnection(
                from: alice,
                to: uniqueID("user-\(year)"),
                relation: "knows",
                since: Int64(year),
                strength: 0.5
            ))
        }
        try context.insert(try makeConnection(
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
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
                )
            ]),
            .equals("?since", .int64(2020))
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SocialConnection.self
        )

        #expect(result.bindings.count == 1)
        let binding = try #require(result.bindings.first)
        #expect(binding["?since"] == .int64(2020))
    }

    @Test("Property filter pushdown - range comparison")
    func testPropertyFilterPushdownRange() async throws {
        let container = try await setupContainer()
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")

        // Insert connections from 2015-2024
        for year in 2015...2024 {
            try context.insert(try makeConnection(
                from: alice,
                to: uniqueID("user-\(year)"),
                relation: "knows",
                since: Int64(year),
                strength: Double(year) / 100.0
            ))
        }
        try await context.save()

        // Filter: since >= 2020
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
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
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")

        try context.insert(try makeConnection(from: alice, to: uniqueID("bob"), relation: "knows", since: 2020, strength: 0.5, status: "active-premium"))
        try context.insert(try makeConnection(from: alice, to: uniqueID("carol"), relation: "knows", since: 2021, strength: 0.6, status: "disabled"))
        try context.insert(try makeConnection(from: alice, to: uniqueID("dave"), relation: "knows", since: 2022, strength: 0.7, status: "active"))
        try await context.save()

        // Filter: status CONTAINS "active"
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
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
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")

        try context.insert(try makeConnection(from: alice, to: uniqueID("bob"), relation: "knows", since: 2020, strength: 0.9, status: "active"))
        try context.insert(try makeConnection(from: alice, to: uniqueID("carol"), relation: "knows", since: 2020, strength: 0.3, status: "inactive"))
        try context.insert(try makeConnection(from: alice, to: uniqueID("dave"), relation: "knows", since: 2021, strength: 0.9, status: "active"))
        try await context.save()

        // Filter: since = 2020 AND strength >= 0.5
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
                )
            ]),
            .and(
                .equals("?since", .int64(2020)),
                .greaterThanOrEqual("?strength", .float64(0.5))
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
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")

        try context.insert(try makeConnection(from: alice, to: uniqueID("bob"), relation: "knows", since: 2020, strength: 0.9, status: "active"))
        try context.insert(try makeConnection(from: alice, to: uniqueID("carol"), relation: "knows", since: 2020, strength: 0.5, status: "inactive"))
        try context.insert(try makeConnection(from: alice, to: uniqueID("dave"), relation: "knows", since: 2021, strength: 0.9, status: "active"))
        try await context.save()

        // Filter: since = 2020 AND status =~ /^active/
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
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
        let binding = try #require(result.bindings.first)
        #expect(binding["?target"] != nil)
    }

    // MARK: - Dataset Without Stored Fields

    @Test("RDF dataset without stored fields")
    func testDatasetWithoutStoredFields() async throws {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [try BasicEdge.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: database), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(SocialConnection.self), try DatabaseFrameworkRuntime.entity(BasicEdge.self)]), security: .disabled)


        if try await database.namespaceExists(
            path: ["test", "basic_edge"]
        ) {
            try await database.removeNamespace(
                path: ["test", "basic_edge"]
            )
        }

        // Set index to readable
        let subspace = try await container.resolveDirectory(for: BasicEdge.self)
        let indexLifecycleStore = IndexLifecycleStore(container: container, subspace: subspace)

        for descriptor in try BasicEdge.indexDescriptors {
            let currentState = try await indexLifecycleStore.state(of: descriptor.name)
            if currentState == .disabled {
                try await indexLifecycleStore.enable(descriptor.name)
                try await indexLifecycleStore.makeReadable(descriptor.name)
            } else if currentState == .writeOnly {
                try await indexLifecycleStore.makeReadable(descriptor.name)
            }
        }

        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")

        let edge = try makeBasicEdge(from: alice, target: bob, label: "knows")
        try context.insert(edge)
        try await context.save()

        // The RDF scan remains valid when no covering properties are configured.
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .value(.rdfTerm(try Self.resource(alice))),
                predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                object: .variable("?target")
            )
        ])

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: BasicEdge.self
        )

        #expect(result.bindings.count == 1)
        let binding = try #require(result.bindings.first)
        let expectedTarget = FieldValue.rdfTerm(try Self.resource(bob))
        #expect(binding["?target"] == expectedTarget)
    }

    // MARK: - Performance Tests

    @Test("Performance - property filter reduces scan")
    func testPerformancePropertyFilterReducesScan() async throws {
        let container = try await setupContainer()
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")

        // Insert 200 connections (only 2 with since = 2025)
        for i in 0..<198 {
            let year = 2010 + (i % 10)  // Years 2010-2019
            try context.insert(try makeConnection(
                from: alice,
                to: uniqueID("old-\(i)"),
                relation: "knows",
                since: Int64(year),
                strength: 0.5
            ))
        }
        // Add 2 recent connections
        try context.insert(try makeConnection(from: alice, to: uniqueID("recent-1"), relation: "knows", since: 2025, strength: 0.9))
        try context.insert(try makeConnection(from: alice, to: uniqueID("recent-2"), relation: "knows", since: 2025, strength: 0.95))
        try await context.save()

        // Filter to only 2025 (1% selectivity)
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
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

    @Test("Explicit projection excludes property variables")
    func testExplicitProjectionExcludesPropertyVariables() async throws {
        let container = try await setupContainer()
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")

        try context.insert(try makeConnection(from: alice, to: bob, relation: "knows", since: 2020, strength: 0.9, status: "active"))
        try await context.save()

        // Explicit projection: only ?target
        let result = try await context.executeSPARQLPattern(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
                )
            ]),
            on: SocialConnection.self,
            projection: ["?target"]  // Explicit projection
        )

        #expect(result.bindings.count == 1)
        let binding = try #require(result.bindings.first)
        let expectedTarget = FieldValue.rdfTerm(try Self.resource(bob))

        // Only ?target should be in result (property variables excluded)
        #expect(binding["?target"] == expectedTarget)
        #expect(binding["?since"] == nil)
        #expect(binding["?strength"] == nil)
        #expect(binding["?status"] == nil)

        // projectedVariables should only contain ?target
        #expect(result.projectedVariables == ["?target"])
    }

    @Test("Property variables are bound in results")
    func testPropertyVariablesAreBound() async throws {
        let container = try await setupContainer()
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")

        try context.insert(try makeConnection(from: alice, to: bob, relation: "knows", since: 2020, strength: 0.9, status: "active"))
        try await context.save()

        // No filter - just check property variables are bound
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .value(.rdfTerm(try Self.resource(alice))),
                predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                object: .variable("?target")
            )
        ])

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: SocialConnection.self
        )

        #expect(result.bindings.count == 1)
        let binding = try #require(result.bindings.first)
        let expectedTarget = FieldValue.rdfTerm(try Self.resource(bob))

        // Check all property variables are bound
        #expect(binding["?since"] == .int64(2020))
        #expect(binding["?strength"] == .float64(0.9))
        #expect(binding["?status"] == .string("active"))
        #expect(binding["?target"] == expectedTarget)
    }
}
#endif
