import Foundation
import Testing
import Core
import Graph
import StorageKit
@testable import DatabaseEngine
@testable import GraphIndex

private struct NamedGraphStoreTestQuad: Persistable {
    typealias ID = String

    var id: String
    var subject: String
    var predicate: String
    var object: String
    var graph: String?

    static var persistableType: String { "NamedGraphStoreTestQuad" }
    static var allFields: [String] { ["id", "subject", "predicate", "object", "graph"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "subject": return subject
        case "predicate": return predicate
        case "object": return object
        case "graph": return graph
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<NamedGraphStoreTestQuad, Value>) -> String {
        switch keyPath {
        case \NamedGraphStoreTestQuad.id: return "id"
        case \NamedGraphStoreTestQuad.subject: return "subject"
        case \NamedGraphStoreTestQuad.predicate: return "predicate"
        case \NamedGraphStoreTestQuad.object: return "object"
        case \NamedGraphStoreTestQuad.graph: return "graph"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<NamedGraphStoreTestQuad>) -> String {
        switch keyPath {
        case \NamedGraphStoreTestQuad.id: return "id"
        case \NamedGraphStoreTestQuad.subject: return "subject"
        case \NamedGraphStoreTestQuad.predicate: return "predicate"
        case \NamedGraphStoreTestQuad.object: return "object"
        case \NamedGraphStoreTestQuad.graph: return "graph"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        guard let keyPath = keyPath as? PartialKeyPath<NamedGraphStoreTestQuad> else {
            return "\(keyPath)"
        }
        return fieldName(for: keyPath)
    }
}

@Suite("NamedGraphStore Strategy Tests")
struct NamedGraphStoreStrategyTests {
    private func makeMaintainer(graphField: String? = "graph") -> (
        maintainer: GraphIndexMaintainer<NamedGraphStoreTestQuad>,
        indexSubspace: Subspace
    ) {
        let indexName = "NamedGraphStoreTestQuad_graph"
        let indexSubspace = Subspace(prefix: Tuple("test", "namedGraphStore").pack())
            .subspace("I")
            .subspace(indexName)
        let index = Index(
            name: indexName,
            kind: GraphIndexKind<NamedGraphStoreTestQuad>(
                fromField: "subject",
                edgeField: "predicate",
                toField: "object",
                graphField: graphField,
                strategy: .namedGraphStore
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "subject"),
                FieldKeyExpression(fieldName: "predicate"),
                FieldKeyExpression(fieldName: "object"),
            ]),
            subspaceKey: indexName,
            itemTypes: Set(["NamedGraphStoreTestQuad"])
        )
        let maintainer = GraphIndexMaintainer<NamedGraphStoreTestQuad>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            fromField: "subject",
            edgeField: "predicate",
            toField: "object",
            graphField: graphField,
            strategy: .namedGraphStore
        )
        return (maintainer, indexSubspace)
    }

    private func unpack(_ key: Bytes, from subspace: Subspace) throws -> [any TupleElement] {
        let tuple = try subspace.unpack(key)
        return try Tuple.unpack(from: tuple.pack())
    }

    @Test("namedGraphStore generates GSPO, GPOS, and GOSP graph-first keys")
    func namedGraphStoreGeneratesGraphFirstKeys() async throws {
        let setup = makeMaintainer()
        let quad = NamedGraphStoreTestQuad(
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
        let setup = makeMaintainer()
        let quad = NamedGraphStoreTestQuad(
            id: "q1",
            subject: "Alice",
            predicate: "knows",
            object: "Bob",
            graph: nil
        )

        let keys = try await setup.maintainer.computeIndexKeys(for: quad, id: Tuple("q1"))
        #expect(keys.count == 3)

        let gspo = try unpack(keys[0], from: setup.indexSubspace.subspace(Int64(8)))
        #expect(gspo[0] as? String == "")
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

        let data = try JSONEncoder().encode(GraphIndexStrategy.namedGraphStore)
        let decoded = try JSONDecoder().decode(GraphIndexStrategy.self, from: data)
        #expect(decoded == .namedGraphStore)
    }
}
