#if FOUNDATION_DB
/// SPARQLDebugTests.swift
/// Debug test to trace SPARQL property binding

import Testing
import Foundation
import Core
import DatabaseRuntime
import DatabaseValue
import DatabaseValueCodable
import Graph
import DatabaseEngine
import StorageKit
import FDBStorage
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

@Persistable
fileprivate struct DebugRDFStatement {
    #Directory<DebugRDFStatement>("test", "debug_rdf")
    #Index(
        RDFQuadIndexKind<DebugRDFStatement>(
            subject: \.subject,
            predicate: \.predicate,
            object: \.object,
            graph: \.graph
        ),
        storedFields: [\DebugRDFStatement.score],
        name: "debug_rdf"
    )

    var id: String = UUID().uuidString
    var subject: DatabaseRDFTerm = .iri("https://example.com/resource")
    var predicate: DatabaseRDFTerm = .iri("https://example.com/predicate")
    var object: DatabaseRDFTerm = .iri("https://example.com/object")
    var graph: DatabaseRDFTerm? = nil
    var score: Int = 0
}

@Suite("SPARQL Debug Test", .serialized, .heartbeat)
struct SPARQLDebugTests {

    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    @Test("Debug: Check storedFieldNames propagation")
    func testStoredFieldNamesPropagation() async throws {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = Schema(
            [DebugRDFStatement.self],
            version: Schema.Version(1, 0, 0)
        )

        // Clean up directory BEFORE creating container to avoid stale state
        
        if try await database.directoryExists(path: ["test", "debug_rdf"]) {
            try await database.removeDirectory(path: ["test", "debug_rdf"])
        }

        // Create container and ensure indexes are ready AFTER cleanup
        let container = try await DBContainer(for: schema, configuration: .init(backend: .custom(database)), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(), security: .disabled)
        try await container.ensureIndexesReady()

        // Set index to readable
        let subspace = try await container.resolveDirectory(for: DebugRDFStatement.self)
        let indexLifecycleStore = IndexLifecycleStore(container: container, subspace: subspace)
        let indexName = "debug_rdf"

        let currentState = try await indexLifecycleStore.state(of: indexName)
        if currentState == .disabled {
            try await indexLifecycleStore.enable(indexName)
            try await indexLifecycleStore.makeReadable(indexName)
        } else if currentState == .writeOnly {
            try await indexLifecycleStore.makeReadable(indexName)
        }

        // Insert test data
        let context = FDBContext(container: container)
        let alice = "https://example.com/person/alice-debug"
        let bob = "https://example.com/person/bob-debug"
        let knows = "https://example.com/vocabulary/knows"

        var statement = DebugRDFStatement()
        statement.subject = .iri(alice)
        statement.predicate = .iri(knows)
        statement.object = .iri(bob)
        statement.score = 100
        context.insert(statement)
        try await context.save()

        // Check index descriptor
        guard let indexDescriptor = DebugRDFStatement.indexDescriptors.first else {
            Issue.record("No index descriptor found")
            return
        }

        print("✓ Index descriptor storedFieldNames: \(indexDescriptor.storedFieldNames)")
        #expect(!indexDescriptor.storedFieldNames.isEmpty)
        #expect(indexDescriptor.storedFieldNames.contains("score"))

        // Execute simple pattern (no filter)
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .value(.rdfTerm(.iri(alice))),
                predicate: .value(.rdfTerm(.iri(knows))),
                object: .variable("?target")
            )
        ])

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: DebugRDFStatement.self
        )

        print("✓ Result bindings count: \(result.bindings.count)")
        #expect(result.bindings.count == 1)

        let binding = result.bindings[0]
        print("✓ Binding ?target: \(String(describing: binding["?target"]))")
        print("✓ Binding ?score: \(String(describing: binding["?score"]))")

        // Check if property variable is bound
        if binding["?score"] == nil {
            Issue.record("Property variable ?score is not bound! Full binding: \(binding)")
        }

        #expect(binding["?target"] == .rdfTerm(.iri(bob)))
        #expect(binding["?score"] == .int64(100))
    }

    @Test("Debug: Direct GraphPropertyScanner test")
    func testDirectGraphPropertyScanner() async throws {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = Schema([DebugEdge.self], version: Schema.Version(1, 0, 0))

        // Clean up directory BEFORE creating container to avoid stale state
        
        if try await database.directoryExists(path: ["test", "debug_edge"]) {
            try await database.removeDirectory(path: ["test", "debug_edge"])
        }

        // Create container and ensure indexes are ready AFTER cleanup
        let container = try await DBContainer(for: schema, configuration: .init(backend: .custom(database)), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(), security: .disabled)
        try await container.ensureIndexesReady()

        // Set index to readable
        let subspace = try await container.resolveDirectory(for: DebugEdge.self)
        let indexLifecycleStore = IndexLifecycleStore(container: container, subspace: subspace)
        let indexName = "debug_graph"

        let currentState = try await indexLifecycleStore.state(of: indexName)
        if currentState == .disabled {
            try await indexLifecycleStore.enable(indexName)
            try await indexLifecycleStore.makeReadable(indexName)
        } else if currentState == .writeOnly {
            try await indexLifecycleStore.makeReadable(indexName)
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

        let metadata = try PropertyGraphIndexMetadata(canonical: indexDescriptor.kind)

        let scanner = GraphPropertyScanner(
            indexSubspace: indexSubspace,
            strategy: metadata.strategy,
            storedFieldNames: indexDescriptor.storedFieldNames
        )

        print("✓ GraphPropertyScanner storedFieldNames: \(indexDescriptor.storedFieldNames)")

        var edgeCount = 0
        var propertiesFound = false

        try await database.withTransaction { transaction in
            let stream = scanner.scanEdges(
                from: .identifier(alice),
                edge: "knows",
                to: nil,
                scope: .all,
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
#endif
