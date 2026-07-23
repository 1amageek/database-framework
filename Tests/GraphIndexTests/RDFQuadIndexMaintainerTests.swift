import Core
import DatabaseValue
import Graph
import StorageKit
import Testing
@testable import DatabaseEngine
@testable import GraphIndex

private struct RDFQuadIndexRecord: Persistable {
    typealias ID = String

    var id: String
    var subject: DatabaseRDFTerm
    var predicate: DatabaseRDFTerm
    var object: DatabaseRDFTerm
    var graph: DatabaseRDFTerm?

    static var persistableType: String { "RDFQuadIndexRecord" }
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

    static func fieldName<Value>(for keyPath: KeyPath<Self, Value>) -> String {
        fieldName(for: keyPath as AnyKeyPath)
    }

    static func fieldName(for keyPath: PartialKeyPath<Self>) -> String {
        fieldName(for: keyPath as AnyKeyPath)
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        switch keyPath {
        case \Self.id: return "id"
        case \Self.subject: return "subject"
        case \Self.predicate: return "predicate"
        case \Self.object: return "object"
        case \Self.graph: return "graph"
        default: return String(describing: keyPath)
        }
    }
}

@Suite("RDF quad index maintainer")
struct RDFQuadIndexMaintainerTests {
    @Test("Every ordering stores one canonical graph discriminator")
    func everyOrderingStoresCanonicalGraphDiscriminator() async throws {
        let setup = makeMaintainer()
        let graph = DatabaseRDFTerm.iri("https://calendar.example/graph/active")
        let record = RDFQuadIndexRecord(
            id: "statement-1",
            subject: .iri("https://calendar.example/event/1"),
            predicate: .iri("https://calendar.example/ontology/title"),
            object: .literal(
                DatabaseRDFLiteral(
                    lexicalForm: "Festival",
                    datatype: .xsdString
                )
            ),
            graph: graph
        )

        let keys = try await setup.maintainer.computeIndexKeys(
            for: record,
            id: Tuple([record.id])
        )
        #expect(keys.count == 6)

        let expectedGraph = Bytes(
            retaining: try DatabaseRDFTermCodec.encode(graph)
        )
        for (offset, subspaceKey) in [2, 3, 4, 8, 9, 10].enumerated() {
            let tuple = try setup.base.subspace(Int64(subspaceKey)).unpack(keys[offset])
            #expect(tuple.count == 4)
            let graphPosition = subspaceKey < 8 ? 3 : 0
            #expect(tuple[graphPosition] as? Bytes == expectedGraph)
        }
    }

    @Test("Default graph uses a reserved discriminator in all six orderings")
    func defaultGraphUsesReservedDiscriminator() async throws {
        let setup = makeMaintainer()
        let record = RDFQuadIndexRecord(
            id: "statement-2",
            subject: .iri("https://calendar.example/event/2"),
            predicate: .iri("https://calendar.example/ontology/title"),
            object: .literal(
                DatabaseRDFLiteral(
                    lexicalForm: "Parade",
                    datatype: .xsdString
                )
            ),
            graph: nil
        )

        let keys = try await setup.maintainer.computeIndexKeys(
            for: record,
            id: Tuple([record.id])
        )
        for (offset, subspaceKey) in [2, 3, 4, 8, 9, 10].enumerated() {
            let tuple = try setup.base.subspace(Int64(subspaceKey)).unpack(keys[offset])
            let graphPosition = subspaceKey < 8 ? 3 : 0
            #expect(
                (tuple[graphPosition] as? Bytes)
                    == Bytes(
                        retaining: RDFQuadIndexPhysicalLayout
                            .defaultGraphDiscriminator
                    )
            )
        }
    }

    private func makeMaintainer() -> (
        maintainer: RDFQuadIndexMaintainer<RDFQuadIndexRecord>,
        base: Subspace
    ) {
        let kind = RDFQuadIndexKind<RDFQuadIndexRecord>(
            subject: \.subject,
            predicate: \.predicate,
            object: \.object,
            graph: \.graph
        )
        let index = Index(
            name: "rdf_quad_test",
            kind: kind,
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "subject"),
                FieldKeyExpression(fieldName: "predicate"),
                FieldKeyExpression(fieldName: "object"),
                FieldKeyExpression(fieldName: "graph"),
            ]),
            subspaceKey: "rdf_quad_test",
            itemTypes: Set([RDFQuadIndexRecord.persistableType])
        )
        let base = Subspace(prefix: Tuple("rdf-quad-test").pack())
        return (
            RDFQuadIndexMaintainer(
                index: index,
                subspace: base,
                idExpression: FieldKeyExpression(fieldName: "id"),
                subjectField: "subject",
                predicateField: "predicate",
                objectField: "object",
                graphField: "graph"
            ),
            base
        )
    }
}
