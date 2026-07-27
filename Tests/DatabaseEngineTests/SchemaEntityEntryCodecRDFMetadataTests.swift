import DatabaseKit
import DatabaseTypes
import StorageKit
import Testing
@testable import DatabaseEngine

@Suite("Schema entity canonical RDF metadata")
struct SchemaEntityEntryCodecRDFMetadataTests {
    @Test("RDF metadata round-trips through the binary schema catalog")
    func rdfMetadataRoundTrips() throws {
        let graph = try RDFTerm.blankNode(identifier: "calendar")
        let entity = try makeEntity(graph: graph)

        let encoded = try SchemaEntityEntryCodec.encode(entity)
        let decoded = try SchemaEntityEntryCodec.decode(encoded)

        #expect(decoded == entity)
        #expect(decoded.indexes[0].kind.metadata["graph"] == .rdfTerm(graph))
    }

    @Test("Invalid persisted RDF metadata is rejected during decoding")
    func invalidRDFMetadataFailsDecoding() throws {
        let graph = try RDFTerm.iri(validating: "urn:valid")
        let persisted = try SchemaEntityEntryCodec.encode(
            try makeEntity(graph: graph)
        )
        let validBytes = Array("urn:valid".utf8)
        let invalidBytes = Array("relative!".utf8)
        let start = try #require(
            firstOffset(of: validBytes, in: persisted)
        )
        let encoded = ByteString.copying(count: persisted.count) { destination in
            persisted.withUnsafeBytes { source in
                destination.copyMemory(from: source)
            }
            invalidBytes.withUnsafeBytes { replacement in
                for offset in replacement.indices {
                    destination[start + offset] = replacement[offset]
                }
            }
        }

        #expect(
            throws: StorageFrameError.invalidRDFTerm(
                .invalidIRI(.missingScheme)
            )
        ) {
            _ = try SchemaEntityEntryCodec.decode(encoded)
        }
    }

    private func makeEntity(graph: RDFTerm) throws -> Schema.Entity {
        try Schema.Entity(
            name: "CalendarEvent",
            identifierType: .string,
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
                    entityName: "CalendarEvent",
                    name: "calendar_graph",
                    kind: IndexKindMetadata(
                        identifier: "owl_class_rdf",
                        subspaceStructure: .hierarchical,
                        fields: [],
                        metadata: [
                            "individualIRIBase": .string("urn:calendar:event:"),
                            "graph": .rdfTerm(graph),
                        ]
                    )
                ),
            ]
        )
    }

    private func firstOffset(
        of signature: [UInt8],
        in bytes: ByteString
    ) -> Int? {
        guard !signature.isEmpty, signature.count <= bytes.count else {
            return nil
        }
        let finalStart = bytes.count - signature.count
        for start in 0...finalStart {
            var matches = true
            for offset in signature.indices
            where bytes[bytes.startIndex + start + offset] != signature[offset] {
                matches = false
                break
            }
            if matches {
                return start
            }
        }
        return nil
    }
}
