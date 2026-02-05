/// SPARQLDebugTest.swift
/// Debug test to trace SPARQL property binding

import Testing
import Foundation
import Core
import Graph
import DatabaseEngine
import FoundationDB
import TestSupport
@testable import GraphIndex

@Persistable
fileprivate struct DebugEdge {
    #Directory<DebugEdge>("test", "debug_edge")
    var id: String = UUID().uuidString
    var from: String = ""
    var target: String = ""
    var label: String = ""
    var score: Int = 0

    #Index(GraphIndexKind<DebugEdge>(
        from: \.from,
        edge: \.label,
        to: \.target,
        graph: nil,
        strategy: .tripleStore
    ), storedFields: [\DebugEdge.score], name: "debug_graph")
}

@Suite("SPARQL Debug Test", .serialized)
struct SPARQLDebugTest {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    @Test("Debug: Check storedFieldNames propagation")
    func testStoredFieldNamesPropagation() async throws {
        let database = try FDBClient.openDatabase()
        let schema = Schema([DebugEdge.self], version: Schema.Version(1, 0, 0))

        // Clean up directory BEFORE creating container to avoid stale state
        let directoryLayer = DirectoryLayer(database: database)
        try? await directoryLayer.remove(path: ["test", "debug_edge"])

        // Create container and ensure indexes are ready AFTER cleanup
        let container = FDBContainer(database: database, schema: schema, security: .disabled)
        try await container.ensureIndexesReady()

        // Set index to readable
        let subspace = try await container.resolveDirectory(for: DebugEdge.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)
        let indexName = "debug_graph"

        let currentState = try await indexStateManager.state(of: indexName)
        if currentState == .disabled {
            try await indexStateManager.enable(indexName)
            try await indexStateManager.makeReadable(indexName)
        } else if currentState == .writeOnly {
            try await indexStateManager.makeReadable(indexName)
        }

        // Insert test data
        let context = FDBContext(container: container)
        let alice = "alice-debug"
        let bob = "bob-debug"

        let edge = DebugEdge(from: alice, target: bob, label: "knows", score: 100)
        context.insert(edge)
        try await context.save()

        // Check index descriptor
        guard let indexDescriptor = DebugEdge.indexDescriptors.first else {
            Issue.record("No index descriptor found")
            return
        }

        print("✓ Index descriptor storedFieldNames: \(indexDescriptor.storedFieldNames)")
        #expect(!indexDescriptor.storedFieldNames.isEmpty)
        #expect(indexDescriptor.storedFieldNames.contains("score"))

        // Execute simple pattern (no filter)
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
            on: DebugEdge.self
        )

        print("✓ Result bindings count: \(result.bindings.count)")
        #expect(result.bindings.count == 1)

        let binding = result.bindings[0]
        print("✓ Binding ?target: \(binding["?target"] ?? .null)")
        print("✓ Binding ?score: \(binding["?score"] ?? .null)")

        // Check if property variable is bound
        if binding["?score"] == nil {
            Issue.record("Property variable ?score is not bound! Full binding: \(binding)")
        }

        #expect(binding["?target"] == .string(bob))
        #expect(binding["?score"] == .int64(100))
    }

    @Test("Debug: Direct GraphPropertyScanner test")
    func testDirectGraphPropertyScanner() async throws {
        let database = try FDBClient.openDatabase()
        let schema = Schema([DebugEdge.self], version: Schema.Version(1, 0, 0))

        // Clean up directory BEFORE creating container to avoid stale state
        let directoryLayer = DirectoryLayer(database: database)
        try? await directoryLayer.remove(path: ["test", "debug_edge"])

        // Create container and ensure indexes are ready AFTER cleanup
        let container = FDBContainer(database: database, schema: schema, security: .disabled)
        try await container.ensureIndexesReady()

        // Set index to readable
        let subspace = try await container.resolveDirectory(for: DebugEdge.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)
        let indexName = "debug_graph"

        let currentState = try await indexStateManager.state(of: indexName)
        if currentState == .disabled {
            try await indexStateManager.enable(indexName)
            try await indexStateManager.makeReadable(indexName)
        } else if currentState == .writeOnly {
            try await indexStateManager.makeReadable(indexName)
        }

        // Insert test data
        let context = FDBContext(container: container)
        let alice = "alice-direct"
        let bob = "bob-direct"

        let edge = DebugEdge(from: alice, target: bob, label: "knows", score: 200)
        context.insert(edge)
        try await context.save()

        // Get index descriptor
        guard let indexDescriptor = DebugEdge.indexDescriptors.first else {
            Issue.record("No index descriptor found")
            return
        }

        // Direct GraphPropertyScanner test
        // Index entries are stored at [typeSubspace]/I/[indexName], not [typeSubspace]/[indexName]
        let typeSubspace = try await container.resolveDirectory(for: DebugEdge.self)
        let indexSubspace = typeSubspace.subspace(SubspaceKey.indexes).subspace(indexName)

        guard let kind = indexDescriptor.kind as? GraphIndexKind<DebugEdge> else {
            Issue.record("Failed to cast index kind")
            return
        }

        let scanner = GraphPropertyScanner(
            indexSubspace: indexSubspace,
            strategy: kind.strategy,
            storedFieldNames: indexDescriptor.storedFieldNames
        )

        print("✓ GraphPropertyScanner storedFieldNames: \(indexDescriptor.storedFieldNames)")

        var edgeCount = 0
        var propertiesFound = false

        try await database.withTransaction { transaction in
            let stream = scanner.scanEdges(
                from: alice,
                edge: "knows",
                to: nil,
                graph: nil,
                propertyFilters: nil,
                transaction: transaction
            )

            for try await scannedEdge in stream {
                edgeCount += 1
                print("✓ Scanned edge: from=\(scannedEdge.source), to=\(scannedEdge.target)")
                print("✓ Properties: \(scannedEdge.properties)")

                if !scannedEdge.properties.isEmpty {
                    propertiesFound = true
                }

                if let scoreValue = scannedEdge.properties["score"] {
                    print("✓ Found score property: \(scoreValue)")
                } else {
                    print("✗ Score property not found in: \(scannedEdge.properties.keys)")
                }
            }
        }

        print("✓ Total edges scanned: \(edgeCount)")
        #expect(edgeCount == 1)
        #expect(propertiesFound)
    }
}
