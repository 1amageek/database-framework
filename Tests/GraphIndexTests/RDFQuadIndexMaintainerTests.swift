import DatabaseKit
import DatabaseTypes
import StorageKit
import Testing
@testable import DatabaseEngine
@testable import GraphIndex

@Persistable
private struct RDFQuadIndexEntity {
    var id: String = ""
    var subject: RDFTerm
    var predicate: RDFTerm
    var object: RDFTerm
    var graph: RDFTerm?

    #Index(
        .rdfDataset,
        from: \RDFQuadIndexEntity.subject,
        edge: \RDFQuadIndexEntity.predicate,
        to: \RDFQuadIndexEntity.object,
        graph: \RDFQuadIndexEntity.graph,
        name: "rdf_quad_test"
    )

}

@Suite("RDF quad index maintainer")
struct RDFQuadIndexMaintainerTests {
    @Test("Every ordering stores one canonical graph discriminator")
    func everyOrderingStoresCanonicalGraphDiscriminator() async throws {
        let setup = try makeMaintainer()
        let graph = try RDFTerm.iri(
            validating:
                "https://calendar.example/graph/active"
        )
        let entity = RDFQuadIndexEntity(
            id: "statement-1",
            subject: try .iri(
                validating:
                    "https://calendar.example/event/1"
            ),
            predicate: try .iri(
                validating:
                    "https://calendar.example/ontology/title"
            ),
            object: .literal(
                RDFLiteral(
                    lexicalForm: "Festival",
                    datatype: .xsdString
                )
            ),
            graph: graph
        )

        let keys = try await setup.maintainer.computeIndexKeys(
            for: entity,
            id: Tuple([entity.id])
        )
        #expect(keys.count == 6)

        let expectedGraph = try RDFTermStorageFormat.encode(graph)
        for (offset, subspaceKey) in [2, 3, 4, 8, 9, 10].enumerated() {
            let tuple = try setup.base.subspace(Int64(subspaceKey)).unpack(keys[offset])
            #expect(tuple.count == 4)
            let graphPosition = subspaceKey < 8 ? 3 : 0
            #expect(tuple[graphPosition] as? ByteString == expectedGraph)
        }
    }

    @Test("Default graph uses a reserved discriminator in all six orderings")
    func defaultGraphUsesReservedDiscriminator() async throws {
        let setup = try makeMaintainer()
        let entity = RDFQuadIndexEntity(
            id: "statement-2",
            subject: try .iri(
                validating:
                    "https://calendar.example/event/2"
            ),
            predicate: try .iri(
                validating:
                    "https://calendar.example/ontology/title"
            ),
            object: .literal(
                RDFLiteral(
                    lexicalForm: "Parade",
                    datatype: .xsdString
                )
            ),
            graph: nil
        )

        let keys = try await setup.maintainer.computeIndexKeys(
            for: entity,
            id: Tuple([entity.id])
        )
        for (offset, subspaceKey) in [2, 3, 4, 8, 9, 10].enumerated() {
            let tuple = try setup.base.subspace(Int64(subspaceKey)).unpack(keys[offset])
            let graphPosition = subspaceKey < 8 ? 3 : 0
            #expect(
                (tuple[graphPosition] as? ByteString)
                    == RDFQuadIndexPhysicalLayout.defaultGraphDiscriminator
            )
        }
    }

    private func makeMaintainer() throws -> (
        maintainer: RDFQuadIndexMaintainer<RDFQuadIndexEntity>,
        base: Subspace
    ) {
        guard let descriptor =
            try RDFQuadIndexEntity.indexDescriptors.first
        else {
            throw RDFQuadIndexMaintainerTestError.missingDescriptor
        }
        let index = Index(
            name: "rdf_quad_test",
            kind: descriptor.kind,
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "subject"),
                FieldKeyExpression(fieldName: "predicate"),
                FieldKeyExpression(fieldName: "object"),
                FieldKeyExpression(fieldName: "graph"),
            ]),
            subspaceKey: "rdf_quad_test",
            itemTypes: Set([RDFQuadIndexEntity.persistableType])
        )
        let base = Subspace(prefix: Tuple("rdf-quad-test").pack())
        return (
            try RDFQuadIndexMaintainer(
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

private enum RDFQuadIndexMaintainerTestError: Error {
    case missingDescriptor
}
