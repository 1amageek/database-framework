// SCCFinderTests.swift
// GraphIndexTests - Tests for Strongly Connected Components (SCC) Algorithm

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Model

@Persistable
struct EdgeForSCC {
    #Directory<EdgeForSCC>("test", "scc")
    var id: String = UUID().uuidString
    var from: String = ""
    var relationship: String = ""
    var to: String = ""

    #Index(GraphIndexKind<EdgeForSCC>(
        from: \.from,
        edge: \.relationship,
        to: \.to,
        strategy: .tripleStore
    ))
}

// MARK: - Test Suite

@Suite("SCC Finder Tests", .serialized)
struct SCCFinderTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Helpers

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> FDBContainer {
        let database = try FDBClient.openDatabase()
        let schema = Schema([EdgeForSCC.self], version: Schema.Version(1, 0, 0))
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    private func setIndexStatesToReadable(container: FDBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: EdgeForSCC.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in EdgeForSCC.indexDescriptors {
            // Use retry loop to handle concurrent state transitions from parallel tests
            let maxAttempts = 3
            for attempt in 1...maxAttempts {
                let currentState = try await indexStateManager.state(of: descriptor.name)

                switch currentState {
                case .disabled:
                    do {
                        try await indexStateManager.enable(descriptor.name)
                        try await indexStateManager.makeReadable(descriptor.name)
                        break  // Success
                    } catch let error as IndexStateError {
                        // Another test may have enabled it concurrently
                        if case .invalidTransition = error, attempt < maxAttempts {
                            continue  // Retry
                        }
                        throw error
                    }
                case .writeOnly:
                    do {
                        try await indexStateManager.makeReadable(descriptor.name)
                        break  // Success
                    } catch let error as IndexStateError {
                        if case .invalidTransition = error, attempt < maxAttempts {
                            continue
                        }
                        throw error
                    }
                case .readable:
                    break  // Already readable, success
                }
            }
        }
    }

    private func insertEdges(_ edges: [EdgeForSCC], context: FDBContext) async throws {
        for edge in edges {
            context.insert(edge)
        }
        try await context.save()
    }

    private func makeEdge(from: String, relationship: String, to: String) -> EdgeForSCC {
        var edge = EdgeForSCC()
        edge.from = from
        edge.relationship = relationship
        edge.to = to
        return edge
    }

    // MARK: - Basic SCC Tests

    @Test("Simple DAG - no SCCs")
    func testSimpleDAG() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let d = uniqueID("D")
        let predicate = uniqueID("edge")

        // Linear graph: A -> B -> C -> D
        let edges = [
            makeEdge(from: a, relationship: predicate, to: b),
            makeEdge(from: b, relationship: predicate, to: c),
            makeEdge(from: c, relationship: predicate, to: d),
        ]

        try await insertEdges(edges, context: context)

        let sccFinder = try await context.sccFinder(for: EdgeForSCC.self)
        let result = try await sccFinder.findSCCs(edgeLabel: predicate)

        // DAG should have no multi-node SCCs (all components are singletons)
        #expect(result.isDAG == true)
        #expect(result.componentCount == 4)
        #expect(result.largestComponentSize == 1)
        #expect(result.isComplete == true)
    }

    @Test("Single SCC - simple cycle")
    func testSingleSCCSimpleCycle() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let predicate = uniqueID("edge")

        // Cycle: A -> B -> C -> A
        let edges = [
            makeEdge(from: a, relationship: predicate, to: b),
            makeEdge(from: b, relationship: predicate, to: c),
            makeEdge(from: c, relationship: predicate, to: a),
        ]

        try await insertEdges(edges, context: context)

        let sccFinder = try await context.sccFinder(for: EdgeForSCC.self)
        let result = try await sccFinder.findSCCs(edgeLabel: predicate)

        // Single SCC with 3 nodes
        #expect(result.isDAG == false)
        #expect(result.componentCount == 1)
        #expect(result.largestComponentSize == 3)

        // All nodes should be in the same component
        let componentA = result.nodeToComponent[a]
        let componentB = result.nodeToComponent[b]
        let componentC = result.nodeToComponent[c]
        #expect(componentA == componentB)
        #expect(componentB == componentC)
    }

    @Test("Multiple SCCs")
    func testMultipleSCCs() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let d = uniqueID("D")
        let e = uniqueID("E")
        let f = uniqueID("F")
        let predicate = uniqueID("edge")

        // Two SCCs:
        // SCC1: A -> B -> C -> A
        // SCC2: D -> E -> F -> D
        // Connection: C -> D (inter-SCC edge)
        let edges = [
            // SCC1
            makeEdge(from: a, relationship: predicate, to: b),
            makeEdge(from: b, relationship: predicate, to: c),
            makeEdge(from: c, relationship: predicate, to: a),
            // SCC2
            makeEdge(from: d, relationship: predicate, to: e),
            makeEdge(from: e, relationship: predicate, to: f),
            makeEdge(from: f, relationship: predicate, to: d),
            // Inter-SCC edge
            makeEdge(from: c, relationship: predicate, to: d),
        ]

        try await insertEdges(edges, context: context)

        let sccFinder = try await context.sccFinder(for: EdgeForSCC.self)
        let result = try await sccFinder.findSCCs(edgeLabel: predicate)

        #expect(result.isDAG == false)
        #expect(result.componentCount == 2)
        #expect(result.largestComponentSize == 3)

        // Check that A, B, C are in one component
        let componentA = result.nodeToComponent[a]!
        let componentB = result.nodeToComponent[b]!
        let componentC = result.nodeToComponent[c]!
        #expect(componentA == componentB)
        #expect(componentB == componentC)

        // Check that D, E, F are in another component
        let componentD = result.nodeToComponent[d]!
        let componentE = result.nodeToComponent[e]!
        let componentF = result.nodeToComponent[f]!
        #expect(componentD == componentE)
        #expect(componentE == componentF)

        // SCCs should be different
        #expect(componentA != componentD)
    }

    // MARK: - isStronglyConnected Tests

    @Test("Strongly connected check - same SCC")
    func testStronglyConnectedSameSCC() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let predicate = uniqueID("edge")

        // A <-> B (bidirectional = strongly connected)
        let edges = [
            makeEdge(from: a, relationship: predicate, to: b),
            makeEdge(from: b, relationship: predicate, to: a),
        ]

        try await insertEdges(edges, context: context)

        let sccFinder = try await context.sccFinder(for: EdgeForSCC.self)

        let connected = try await sccFinder.isStronglyConnected(from: a, to: b, edgeLabel: predicate)
        #expect(connected == true)
    }

    @Test("Strongly connected check - different SCCs")
    func testStronglyConnectedDifferentSCCs() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let predicate = uniqueID("edge")

        // A -> B (one-way only = not strongly connected)
        let edges = [
            makeEdge(from: a, relationship: predicate, to: b),
        ]

        try await insertEdges(edges, context: context)

        let sccFinder = try await context.sccFinder(for: EdgeForSCC.self)

        let connected = try await sccFinder.isStronglyConnected(from: a, to: b, edgeLabel: predicate)
        #expect(connected == false)
    }

    // MARK: - Condensation Graph Tests

    @Test("Condensation graph")
    func testCondensationGraph() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let d = uniqueID("D")
        let predicate = uniqueID("edge")

        // SCC1: A -> B -> A (cycle)
        // SCC2: C -> D -> C (cycle)
        // Inter-SCC: B -> C
        let edges = [
            makeEdge(from: a, relationship: predicate, to: b),
            makeEdge(from: b, relationship: predicate, to: a),
            makeEdge(from: c, relationship: predicate, to: d),
            makeEdge(from: d, relationship: predicate, to: c),
            makeEdge(from: b, relationship: predicate, to: c),
        ]

        try await insertEdges(edges, context: context)

        let sccFinder = try await context.sccFinder(for: EdgeForSCC.self)
        let condensation = try await sccFinder.condensationGraph(edgeLabel: predicate)

        // Should have 2 components
        #expect(condensation.componentSizes.count == 2)

        // Each component has 2 nodes
        #expect(condensation.componentSizes.allSatisfy { $0 == 2 })

        // Should have 1 edge between components
        #expect(condensation.edgeCount == 1)
    }

    // MARK: - Edge Cases

    @Test("Single node graph")
    func testSingleNodeGraph() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let predicate = uniqueID("edge")

        // Self-loop
        let edges = [
            makeEdge(from: a, relationship: predicate, to: a),
        ]

        try await insertEdges(edges, context: context)

        let sccFinder = try await context.sccFinder(for: EdgeForSCC.self)
        let result = try await sccFinder.findSCCs(edgeLabel: predicate)

        // Single node with self-loop is a single SCC
        #expect(result.componentCount == 1)
        #expect(result.largestComponentSize == 1)
    }

    @Test("Complex graph with multiple SCC sizes")
    func testComplexGraphMultipleSCCSizes() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        // Create unique IDs
        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let d = uniqueID("D")
        let e = uniqueID("E")
        let predicate = uniqueID("edge")

        // SCC1: A -> B -> C -> A (size 3)
        // SCC2: D (singleton)
        // SCC3: E (singleton)
        // Edges: C -> D, D -> E
        let edges = [
            makeEdge(from: a, relationship: predicate, to: b),
            makeEdge(from: b, relationship: predicate, to: c),
            makeEdge(from: c, relationship: predicate, to: a),
            makeEdge(from: c, relationship: predicate, to: d),
            makeEdge(from: d, relationship: predicate, to: e),
        ]

        try await insertEdges(edges, context: context)

        let sccFinder = try await context.sccFinder(for: EdgeForSCC.self)
        let result = try await sccFinder.findSCCs(edgeLabel: predicate)

        // Should have 3 SCCs
        #expect(result.componentCount == 3)
        #expect(result.largestComponentSize == 3)

        // The graph is NOT a DAG because of the cycle in SCC1
        #expect(result.isDAG == false)
    }

    @Test("Edge label filtering")
    func testEdgeLabelFiltering() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let follows = uniqueID("follows")
        let blocks = uniqueID("blocks")

        // With "follows": A <-> B (cycle)
        // With "blocks": B -> C (no cycle)
        let edges = [
            makeEdge(from: a, relationship: follows, to: b),
            makeEdge(from: b, relationship: follows, to: a),
            makeEdge(from: b, relationship: blocks, to: c),
        ]

        try await insertEdges(edges, context: context)

        let sccFinder = try await context.sccFinder(for: EdgeForSCC.self)

        // Test with "follows" label only
        let followsResult = try await sccFinder.findSCCs(edgeLabel: follows)
        #expect(followsResult.componentCount == 1)
        #expect(followsResult.largestComponentSize == 2)
        #expect(followsResult.isDAG == false)

        // Test with "blocks" label only
        let blocksResult = try await sccFinder.findSCCs(edgeLabel: blocks)
        #expect(blocksResult.componentCount == 2)
        #expect(blocksResult.largestComponentSize == 1)
        #expect(blocksResult.isDAG == true)
    }
}

// MARK: - GraphEdgeScanner Batch Method Tests

@Suite("GraphEdgeScanner Batch Tests", .serialized)
struct GraphEdgeScannerBatchTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> FDBContainer {
        let database = try FDBClient.openDatabase()
        let schema = Schema([EdgeForSCC.self], version: Schema.Version(1, 0, 0))
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    private func setIndexStatesToReadable(container: FDBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: EdgeForSCC.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in EdgeForSCC.indexDescriptors {
            // Use retry loop to handle concurrent state transitions from parallel tests
            let maxAttempts = 3
            for attempt in 1...maxAttempts {
                let currentState = try await indexStateManager.state(of: descriptor.name)

                switch currentState {
                case .disabled:
                    do {
                        try await indexStateManager.enable(descriptor.name)
                        try await indexStateManager.makeReadable(descriptor.name)
                        break  // Success
                    } catch let error as IndexStateError {
                        // Another test may have enabled it concurrently
                        if case .invalidTransition = error, attempt < maxAttempts {
                            continue  // Retry
                        }
                        throw error
                    }
                case .writeOnly:
                    do {
                        try await indexStateManager.makeReadable(descriptor.name)
                        break  // Success
                    } catch let error as IndexStateError {
                        if case .invalidTransition = error, attempt < maxAttempts {
                            continue
                        }
                        throw error
                    }
                case .readable:
                    break  // Already readable, success
                }
            }
        }
    }

    private func insertEdges(_ edges: [EdgeForSCC], context: FDBContext) async throws {
        for edge in edges {
            context.insert(edge)
        }
        try await context.save()
    }

    private func makeEdge(from: String, relationship: String, to: String) -> EdgeForSCC {
        var edge = EdgeForSCC()
        edge.from = from
        edge.relationship = relationship
        edge.to = to
        return edge
    }

    @Test("batchScanAllOutgoing groups edges by source")
    func testBatchScanAllOutgoingGrouping() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let d = uniqueID("D")
        let predicate = uniqueID("edge")

        // A -> B, A -> C, B -> D
        let edges = [
            makeEdge(from: a, relationship: predicate, to: b),
            makeEdge(from: a, relationship: predicate, to: c),
            makeEdge(from: b, relationship: predicate, to: d),
        ]

        try await insertEdges(edges, context: context)

        // Get the graph scanner
        guard let descriptor = EdgeForSCC.indexDescriptors.first(where: {
            $0.kindIdentifier == GraphIndexKind<EdgeForSCC>.identifier
        }),
        let kind = descriptor.kind as? GraphIndexKind<EdgeForSCC> else {
            throw SCCError.graphIndexNotFound
        }

        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: EdgeForSCC.self)
        let graphSubspace = typeSubspace.subspace(descriptor.name)
        let scanner = GraphEdgeScanner(indexSubspace: graphSubspace, strategy: kind.strategy)

        let database = try FDBClient.openDatabase()
        let grouped = try await database.withTransaction(configuration: .default) { transaction in
            try await scanner.batchScanAllOutgoing(
                from: [a, b, c],
                edgeLabel: predicate,
                transaction: transaction
            )
        }

        // Check grouping
        #expect(grouped[a]?.count == 2)  // A has 2 outgoing
        #expect(grouped[b]?.count == 1)  // B has 1 outgoing
        #expect(grouped[c]?.count == 0)  // C has 0 outgoing (but key exists)

        // Check targets
        let aTargets = Set(grouped[a]?.map { $0.target } ?? [])
        #expect(aTargets.contains(b))
        #expect(aTargets.contains(c))

        let bTargets = Set(grouped[b]?.map { $0.target } ?? [])
        #expect(bTargets.contains(d))
    }

    @Test("batchScanAllIncoming groups edges by target")
    func testBatchScanAllIncomingGrouping() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let d = uniqueID("D")
        let predicate = uniqueID("edge")

        // A -> C, B -> C, C -> D
        let edges = [
            makeEdge(from: a, relationship: predicate, to: c),
            makeEdge(from: b, relationship: predicate, to: c),
            makeEdge(from: c, relationship: predicate, to: d),
        ]

        try await insertEdges(edges, context: context)

        // Get the graph scanner
        guard let descriptor = EdgeForSCC.indexDescriptors.first(where: {
            $0.kindIdentifier == GraphIndexKind<EdgeForSCC>.identifier
        }),
        let kind = descriptor.kind as? GraphIndexKind<EdgeForSCC> else {
            throw SCCError.graphIndexNotFound
        }

        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: EdgeForSCC.self)
        let graphSubspace = typeSubspace.subspace(descriptor.name)
        let scanner = GraphEdgeScanner(indexSubspace: graphSubspace, strategy: kind.strategy)

        let database = try FDBClient.openDatabase()
        let grouped = try await database.withTransaction(configuration: .default) { transaction in
            try await scanner.batchScanAllIncoming(
                to: [a, c, d],
                edgeLabel: predicate,
                transaction: transaction
            )
        }

        // Check grouping
        #expect(grouped[a]?.count == 0)  // A has 0 incoming (but key exists)
        #expect(grouped[c]?.count == 2)  // C has 2 incoming
        #expect(grouped[d]?.count == 1)  // D has 1 incoming

        // Check sources
        let cSources = Set(grouped[c]?.map { $0.source } ?? [])
        #expect(cSources.contains(a))
        #expect(cSources.contains(b))

        let dSources = Set(grouped[d]?.map { $0.source } ?? [])
        #expect(dSources.contains(c))
    }

    @Test("batchScanAllOutgoing returns empty dict for empty sources")
    func testBatchScanAllOutgoingEmpty() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let predicate = uniqueID("edge")

        // Insert some edges
        let edges = [
            makeEdge(from: "X", relationship: predicate, to: "Y"),
        ]
        try await insertEdges(edges, context: context)

        guard let descriptor = EdgeForSCC.indexDescriptors.first(where: {
            $0.kindIdentifier == GraphIndexKind<EdgeForSCC>.identifier
        }),
        let kind = descriptor.kind as? GraphIndexKind<EdgeForSCC> else {
            throw SCCError.graphIndexNotFound
        }

        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: EdgeForSCC.self)
        let graphSubspace = typeSubspace.subspace(descriptor.name)
        let scanner = GraphEdgeScanner(indexSubspace: graphSubspace, strategy: kind.strategy)

        let database = try FDBClient.openDatabase()
        let grouped = try await database.withTransaction(configuration: .default) { transaction in
            try await scanner.batchScanAllOutgoing(
                from: [],  // Empty sources
                edgeLabel: predicate,
                transaction: transaction
            )
        }

        #expect(grouped.isEmpty)
    }

    @Test("batchScanAllIncoming returns empty dict for empty targets")
    func testBatchScanAllIncomingEmpty() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let predicate = uniqueID("edge")

        // Insert some edges
        let edges = [
            makeEdge(from: "X", relationship: predicate, to: "Y"),
        ]
        try await insertEdges(edges, context: context)

        guard let descriptor = EdgeForSCC.indexDescriptors.first(where: {
            $0.kindIdentifier == GraphIndexKind<EdgeForSCC>.identifier
        }),
        let kind = descriptor.kind as? GraphIndexKind<EdgeForSCC> else {
            throw SCCError.graphIndexNotFound
        }

        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: EdgeForSCC.self)
        let graphSubspace = typeSubspace.subspace(descriptor.name)
        let scanner = GraphEdgeScanner(indexSubspace: graphSubspace, strategy: kind.strategy)

        let database = try FDBClient.openDatabase()
        let grouped = try await database.withTransaction(configuration: .default) { transaction in
            try await scanner.batchScanAllIncoming(
                to: [],  // Empty targets
                edgeLabel: predicate,
                transaction: transaction
            )
        }

        #expect(grouped.isEmpty)
    }

    @Test("batchScanAllOutgoing with wildcard edge label")
    func testBatchScanAllOutgoingWildcard() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let follows = uniqueID("follows")
        let blocks = uniqueID("blocks")

        // A -> B (follows), A -> C (blocks)
        let edges = [
            makeEdge(from: a, relationship: follows, to: b),
            makeEdge(from: a, relationship: blocks, to: c),
        ]
        try await insertEdges(edges, context: context)

        guard let descriptor = EdgeForSCC.indexDescriptors.first(where: {
            $0.kindIdentifier == GraphIndexKind<EdgeForSCC>.identifier
        }),
        let kind = descriptor.kind as? GraphIndexKind<EdgeForSCC> else {
            throw SCCError.graphIndexNotFound
        }

        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: EdgeForSCC.self)
        let graphSubspace = typeSubspace.subspace(descriptor.name)
        let scanner = GraphEdgeScanner(indexSubspace: graphSubspace, strategy: kind.strategy)

        let database = try FDBClient.openDatabase()
        let grouped = try await database.withTransaction(configuration: .default) { transaction in
            try await scanner.batchScanAllOutgoing(
                from: [a],
                edgeLabel: nil,  // Wildcard - match all labels
                transaction: transaction
            )
        }

        // A should have edges with both labels
        #expect(grouped[a]?.count == 2)
        let targets = Set(grouped[a]?.map { $0.target } ?? [])
        #expect(targets.contains(b))
        #expect(targets.contains(c))
    }

    @Test("batchScanAllOutgoing with single node batch")
    func testBatchScanAllOutgoingSingleNode() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let predicate = uniqueID("edge")

        let edges = [
            makeEdge(from: a, relationship: predicate, to: b),
            makeEdge(from: a, relationship: predicate, to: c),
        ]
        try await insertEdges(edges, context: context)

        guard let descriptor = EdgeForSCC.indexDescriptors.first(where: {
            $0.kindIdentifier == GraphIndexKind<EdgeForSCC>.identifier
        }),
        let kind = descriptor.kind as? GraphIndexKind<EdgeForSCC> else {
            throw SCCError.graphIndexNotFound
        }

        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: EdgeForSCC.self)
        let graphSubspace = typeSubspace.subspace(descriptor.name)
        let scanner = GraphEdgeScanner(indexSubspace: graphSubspace, strategy: kind.strategy)

        let database = try FDBClient.openDatabase()
        let grouped = try await database.withTransaction(configuration: .default) { transaction in
            try await scanner.batchScanAllOutgoing(
                from: [a],  // Single node
                edgeLabel: predicate,
                transaction: transaction
            )
        }

        #expect(grouped.count == 1)
        #expect(grouped[a]?.count == 2)
    }

    @Test("batchScanAllOutgoing includes nodes with no edges")
    func testBatchScanAllOutgoingNoEdges() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")  // Will have no outgoing edges
        let predicate = uniqueID("edge")

        // Only A -> B, no edges from C
        let edges = [
            makeEdge(from: a, relationship: predicate, to: b),
        ]
        try await insertEdges(edges, context: context)

        guard let descriptor = EdgeForSCC.indexDescriptors.first(where: {
            $0.kindIdentifier == GraphIndexKind<EdgeForSCC>.identifier
        }),
        let kind = descriptor.kind as? GraphIndexKind<EdgeForSCC> else {
            throw SCCError.graphIndexNotFound
        }

        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: EdgeForSCC.self)
        let graphSubspace = typeSubspace.subspace(descriptor.name)
        let scanner = GraphEdgeScanner(indexSubspace: graphSubspace, strategy: kind.strategy)

        let database = try FDBClient.openDatabase()
        let grouped = try await database.withTransaction(configuration: .default) { transaction in
            try await scanner.batchScanAllOutgoing(
                from: [a, c],  // Include node with no edges
                edgeLabel: predicate,
                transaction: transaction
            )
        }

        // Both nodes should be in result
        #expect(grouped.count == 2)
        #expect(grouped[a]?.count == 1)
        #expect(grouped[c]?.count == 0)  // Empty but present
        #expect(grouped[c] != nil)
    }
}
