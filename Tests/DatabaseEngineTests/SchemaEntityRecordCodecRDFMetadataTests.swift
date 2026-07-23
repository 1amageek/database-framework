import Core
import DatabaseValue
import DatabaseWire
import Testing
@testable import DatabaseEngine

@Suite("Schema entity canonical RDF metadata")
struct SchemaEntityRecordCodecRDFMetadataTests {
    @Test("RDF metadata round-trips through the binary schema catalog")
    func rdfMetadataRoundTrips() throws {
        let graph = DatabaseRDFTerm.blankNode("calendar")
        let entity = makeEntity(graph: graph)

        let encoded = try SchemaEntityRecordCodec.encode(entity)
        let decoded = try SchemaEntityRecordCodec.decode(encoded)

        #expect(decoded == entity)
        #expect(decoded.indexes[0].kind.metadata["graph"] == .rdfTerm(graph))
    }

    @Test("invalid RDF metadata cannot enter the schema catalog")
    func invalidRDFMetadataFailsEncoding() {
        let entity = makeEntity(graph: .iri("relative"))

        #expect(
            throws: DatabaseWireError.invalidCanonicalRDFTerm(
                .invalidIRI(.missingScheme)
            )
        ) {
            _ = try SchemaEntityRecordCodec.encode(entity)
        }
    }

    private func makeEntity(graph: DatabaseRDFTerm) -> Schema.Entity {
        Schema.Entity(
            name: "CalendarEvent",
            fields: [
                FieldSchema(
                    name: "id",
                    fieldNumber: 1,
                    type: .string
                ),
            ],
            directoryComponents: [.staticPath("calendar")],
            indexes: [
                IndexDescriptorMetadata(
                    name: "calendar_graph",
                    kind: IndexKindMetadata(
                        identifier: "owl_class_rdf",
                        subspaceStructure: .hierarchical,
                        fieldNames: [],
                        metadata: [
                            "individualIRIBase": .string("urn:calendar:event:"),
                            "graph": .rdfTerm(graph),
                        ]
                    ),
                    commonMetadata: [:]
                ),
            ]
        )
    }
}
