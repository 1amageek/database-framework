#if FOUNDATION_DB
/// SPARQLDebugTests.swift
/// Debug test to trace SPARQL property binding

import Testing
import Foundation
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import DatabaseKitFoundation
import DatabaseEngine
import StorageKit
import FDBStorage
import TestSupport
@testable import GraphIndex

@Persistable
private struct DebugEdge {
    #Directory<DebugEdge>("test", "debug_edge")
    var id: String = UUID().uuidString
    var from: String = ""
    var target: String = ""
    var label: String = ""
    var score: Int64 = 0

    #Index(
        .graph(
            name: "debug_graph",
            definition: .property(
                source: \DebugEdge.from, label: .field(\DebugEdge.label),
                target: \DebugEdge.target,
                graph: nil, strategy: .tripleStore), includedFields: [\DebugEdge.score]))
}

@Persistable
private struct DebugRDFStatement {
    #Directory<DebugRDFStatement>("test", "debug_rdf")
    #Index(
        .graph(
            name: "debug_rdf",
            definition: .rdf(
                subject: \DebugRDFStatement.subject, predicate: \DebugRDFStatement.predicate,
                object: \DebugRDFStatement.object,
        graph: \DebugRDFStatement.graph),
            includedFields: [\DebugRDFStatement.score]))

    var id: String = UUID().uuidString
    var subject: RDFTerm = .iri(.xsdString)
    var predicate: RDFTerm = .iri(.xsdString)
    var object: RDFTerm = .iri(.xsdString)
    var graph: RDFTerm? = nil
    var score: Int64 = 0
}

@Suite("SPARQL Debug Test", .serialized, .foundationDBScenario, .heartbeat)
struct SPARQLDebugTests {

    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    @Test("Debug: Check storedFieldNames propagation")
    func testStoredFieldNamesPropagation() async throws {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [try DebugRDFStatement.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )

        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(DebugEdge.self), try DatabaseFrameworkRuntime.entity(DebugRDFStatement.self),
                ]
            ),
            security: .testingDisabled
        )
        try await container.resetTestBaseData()

        // Set index to readable
        let subspace = try await container.testBaseDirectory(for: DebugRDFStatement.self)
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
        let context = container.testBaseContext()
        let alice = "https://example.com/person/alice-debug"
        let bob = "https://example.com/person/bob-debug"
        let knows = "https://example.com/vocabulary/knows"

        var statement = DebugRDFStatement()
        statement.subject = try .iri(validating: alice)
        statement.predicate = try .iri(validating: knows)
        statement.object = try .iri(validating: bob)
        statement.score = 100
        try context.insert(statement)
        try await context.save()

        // Check index descriptor
        guard let indexDescriptor =
            try DebugRDFStatement.indexDescriptors.first
        else {
            Issue.record("No index descriptor found")
            return
        }

        print("✓ Index descriptor includedFieldNames: \(indexDescriptor.includedFieldNames)")
        #expect(!indexDescriptor.includedFieldNames.isEmpty)
        #expect(indexDescriptor.includedFieldNames.contains("score"))

        // Execute simple pattern (no filter)
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .value(
                    .rdfTerm(try .iri(validating: alice))
                ),
                predicate: .value(
                    .rdfTerm(try .iri(validating: knows))
                ),
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

        #expect(
            binding["?target"]
                == .rdfTerm(try .iri(validating: bob))
        )
        #expect(binding["?score"] == .int64(100))
    }

    @Test("Debug: Direct GraphPropertyScanner test")
    func testDirectGraphPropertyScanner() async throws {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [try DebugEdge.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )

        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(DebugEdge.self), try DatabaseFrameworkRuntime.entity(DebugRDFStatement.self),
                ]
            ),
            security: .testingDisabled
        )
        try await container.resetTestBaseData()

        // Set index to readable
        let subspace = try await container.testBaseDirectory(for: DebugEdge.self)
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
        let context = container.testBaseContext()
        let alice = "alice-direct"
        let bob = "bob-direct"

        let edge = DebugEdge(from: alice, target: bob, label: "knows", score: 200)
        try context.insert(edge)
        try await context.save()

        // Get index descriptor
        guard let indexDescriptor = try DebugEdge.indexDescriptors.first else {
            Issue.record("No index descriptor found")
            return
        }

        // Direct GraphPropertyScanner test
        // Index entries are scoped by the complete declaration fingerprint.
        let typeSubspace = try await container.testBaseDirectory(for: DebugEdge.self)
        let indexSubspace = try IndexLifecycleStore(
            container: container,
            subspace: typeSubspace
        ).indexSubspace(for: indexName)

        guard
            case .graph(
                .property(_, _, _, _, let declaredStrategy), _
            ) = indexDescriptor.declaration.definition
        else {
            Issue.record("Expected a property graph index")
            return
        }

        let scanner = GraphPropertyScanner(
            indexSubspace: indexSubspace,
            strategy: declaredStrategy.storageStrategy,
            includedFieldNames: indexDescriptor.includedFieldNames
        )

        print(
            "✓ GraphPropertyScanner includedFieldNames: \(indexDescriptor.includedFieldNames)"
        )

        let (edgeCount, propertiesFound) = try await database.withTransaction {
            transaction in
            var edgeCount = 0
            var propertiesFound = false
            let scan = scanner.scanEdges(
                from: .identifier(alice),
                edge: "knows",
                to: nil,
                graphTarget: .all,
                propertyFilters: nil,
                transaction: transaction
            )

            var cursor = scan.makeCursor()
            while let scannedEdge = try await cursor.next() {
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
            return (edgeCount, propertiesFound)
        }

        print("✓ Total edges scanned: \(edgeCount)")
        #expect(edgeCount == 1)
        #expect(propertiesFound)
    }
}
#endif
