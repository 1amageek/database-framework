import DatabaseKit
import DatabaseTypes
import Foundation
import StorageKit
import TestSupport
import Testing

@testable import DatabaseEngine
@testable import GraphIndex

@Persistable
private struct NamedGraphStoreQuad {
    var id: String = UUID().uuidString
    var subject: String
    var predicate: String
    var object: String
    var graph: String?
}

@Suite("NamedGraphStore Strategy Tests")
struct NamedGraphStoreStrategyTests {
    private func makeMaintainer(graphField: String? = "graph") throws -> (
        maintainer: GraphIndexMaintainer<NamedGraphStoreQuad>,
        indexSubspace: Subspace
    ) {
        let indexName = "NamedGraphStoreTestQuad_graph"
        let indexSubspace = Subspace(prefix: Tuple("test", "namedGraphStore").pack())
            .subspace("I")
            .subspace(indexName)
        let definition = propertyGraphIndexDefinition(
            source: NamedGraphStoreQuad.fields.subject.identity,
            label: NamedGraphStoreQuad.fields.predicate.identity,
            target: NamedGraphStoreQuad.fields.object.identity,
            namespace: graphField == nil
                ? nil
                : NamedGraphStoreQuad.fields.graph.identity,
            strategy: .namedGraphStore
        )
        let index = try ResolvedIndex(
            for: NamedGraphStoreQuad.self,
            name: indexName,
            definition: .graph(definition, includedFields: []),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "subject"),
                FieldKeyExpression(fieldName: "predicate"),
                FieldKeyExpression(fieldName: "object"),
            ]),
            itemTypes: Set(["NamedGraphStoreQuad"])
        )
        let maintainer = try GraphIndexMaintainer<NamedGraphStoreQuad>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            definition: definition
        )
        return (maintainer, indexSubspace)
    }

    private func unpack(_ key: ByteString, from subspace: Subspace) throws -> [any TupleElement] {
        let tuple = try subspace.unpack(key)
        return try Tuple.unpack(from: tuple.pack())
    }

    @Test("namedGraphStore generates GSPO, GPOS, and GOSP graph-first keys")
    func namedGraphStoreGeneratesGraphFirstKeys() async throws {
        let setup = try makeMaintainer()
        let quad = NamedGraphStoreQuad(
            id: "q1",
            subject: "Alice",
            predicate: "knows",
            object: "Bob",
            graph: "doc:invoice"
        )

        let keys = try await setup.maintainer.computeIndexKeys(for: quad, id: Tuple("q1"))
        #expect(keys.count == 3)

        let gspo = try unpack(keys[0], from: setup.indexSubspace.subspace(Int64(8)))
        #expect(gspo.count == 4)
        #expect(gspo[0] as? String == "doc:invoice")
        #expect(gspo[1] as? String == "Alice")
        #expect(gspo[2] as? String == "knows")
        #expect(gspo[3] as? String == "Bob")

        let gpos = try unpack(keys[1], from: setup.indexSubspace.subspace(Int64(9)))
        #expect(gpos.count == 4)
        #expect(gpos[0] as? String == "doc:invoice")
        #expect(gpos[1] as? String == "knows")
        #expect(gpos[2] as? String == "Bob")
        #expect(gpos[3] as? String == "Alice")

        let gosp = try unpack(keys[2], from: setup.indexSubspace.subspace(Int64(10)))
        #expect(gosp.count == 4)
        #expect(gosp[0] as? String == "doc:invoice")
        #expect(gosp[1] as? String == "Bob")
        #expect(gosp[2] as? String == "Alice")
        #expect(gosp[3] as? String == "knows")
    }

    @Test("namedGraphStore indexes nil graph as default graph sentinel")
    func namedGraphStoreIndexesNilGraphAsDefaultGraphSentinel() async throws {
        let setup = try makeMaintainer()
        let quad = NamedGraphStoreQuad(
            id: "q1",
            subject: "Alice",
            predicate: "knows",
            object: "Bob",
            graph: nil
        )

        let keys = try await setup.maintainer.computeIndexKeys(for: quad, id: Tuple("q1"))
        #expect(keys.count == 3)

        let gspo = try unpack(keys[0], from: setup.indexSubspace.subspace(Int64(8)))
        #expect(gspo[0] as? ByteString == ByteString())
        #expect(gspo[1] as? String == "Alice")
        #expect(gspo[2] as? String == "knows")
        #expect(gspo[3] as? String == "Bob")
    }

    @Test("namedGraphStore metadata exposes three graph-first indexes")
    func namedGraphStoreMetadataExposesThreeGraphFirstIndexes() throws {
        #expect(GraphIndexStrategy.namedGraphStore.indexCount == 3)
        #expect(GraphIndexOrdering.gspo.elementOrder == [0, 1, 2])
        #expect(GraphIndexOrdering.gpos.elementOrder == [1, 2, 0])
        #expect(GraphIndexOrdering.gosp.elementOrder == [2, 0, 1])
        #expect(GraphIndexOrdering.gspo.isGraphFirst)
        #expect(GraphIndexOrdering.gpos.isGraphFirst)
        #expect(GraphIndexOrdering.gosp.isGraphFirst)

        #expect(
            PropertyGraphIndexStrategy.namedGraphStore.storageStrategy
                == .namedGraphStore
        )
    }

    @Test("edge scanner applies named and default graph targets")
    func edgeScannerAppliesGraphTarget() async throws {
        let setup = try makeMaintainer()
        let database = InMemoryEngine()
        let named = NamedGraphStoreQuad(
            id: "named",
            subject: "Alice",
            predicate: "knows",
            object: "Bob",
            graph: "social"
        )
        let defaultGraph = NamedGraphStoreQuad(
            id: "default",
            subject: "Alice",
            predicate: "knows",
            object: "Carol",
            graph: nil
        )
        try await StorageTransactionExecutor(engine: database).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            for key in try await setup.maintainer.computeIndexKeys(
                for: named,
                id: Tuple(named.id)
            ) {
                try transaction.setValue([], for: key)
            }
            for key in try await setup.maintainer.computeIndexKeys(
                for: defaultGraph,
                id: Tuple(defaultGraph.id)
            ) {
                try transaction.setValue([], for: key)
            }
        }

        let namedEdges = try await scan(
            graphTarget: .named("social"),
            setup: setup,
            database: database
        )
        let defaultEdges = try await scan(
            graphTarget: .defaultGraph,
            setup: setup,
            database: database
        )
        let allEdges = try await scan(
            graphTarget: .all,
            setup: setup,
            database: database
        )

        #expect(namedEdges == [
            EdgeInfo(
                source: "Alice",
                target: "Bob",
                edgeLabel: "knows",
                graph: "social"
            )
        ])
        #expect(defaultEdges == [
            EdgeInfo(
                source: "Alice",
                target: "Carol",
                edgeLabel: "knows"
            )
        ])
        #expect(Set(allEdges.map(\.target)) == ["Bob", "Carol"])
    }

    private func scan(
        graphTarget: GraphScanTarget,
        setup: (
            maintainer: GraphIndexMaintainer<NamedGraphStoreQuad>,
            indexSubspace: Subspace
        ),
        database: InMemoryEngine
    ) async throws -> [EdgeInfo] {
        let scanner = GraphEdgeScanner(
            indexSubspace: setup.indexSubspace,
            strategy: .namedGraphStore,
            graphTarget: graphTarget
        )
        return try await StorageTransactionExecutor(
            engine: database
        ).withTransaction(
            configuration: .readOnly,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            var edges: [EdgeInfo] = []
            var cursor = scanner.scanAllEdges(
                edgeLabel: nil,
                transaction: transaction
            ).makeCursor()
            while let edge = try await cursor.next() {
                edges.append(edge)
            }
            return edges
        }
    }
}
