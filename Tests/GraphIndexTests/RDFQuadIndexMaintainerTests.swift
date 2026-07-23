import Core
import DatabaseValue
import Graph
import StorageKit
import Testing
@testable import DatabaseEngine
@testable import GraphIndex

@Persistable
private struct RDFQuadIndexRecord {
    var id: String = ""
    var subject: DatabaseRDFTerm
    var predicate: DatabaseRDFTerm
    var object: DatabaseRDFTerm
    var graph: DatabaseRDFTerm?

    init(
        id: String,
        subject: DatabaseRDFTerm,
        predicate: DatabaseRDFTerm,
        object: DatabaseRDFTerm,
        graph: DatabaseRDFTerm?
    ) {
        self.id = id
        self.subject = subject
        self.predicate = predicate
        self.object = object
        self.graph = graph
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
