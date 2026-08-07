#if FOUNDATION_DB
import DatabaseKit
import DatabaseTypes
import Testing
@testable import GraphIndex

@Suite("RDF dataset index metadata")
struct RDFDatasetIndexMetadataTests {
    @Test("RDF quad metadata preserves an entity graph field")
    func rdfQuadEntityGraphField() throws {
        let descriptor = makeDescriptor(
            identifier: "rdf_quad",
            fieldNames: ["subject", "predicate", "object", "graph"],
            metadata: [:]
        )

        let selection = try #require(
            try RDFDatasetIndexSelection(descriptor: descriptor)
        )

        #expect(selection.metadata.subjectFieldName == "subject")
        #expect(selection.metadata.predicateFieldName == "predicate")
        #expect(selection.metadata.objectFieldName == "object")
        #expect(selection.metadata.graphMapping == .entityField("graph"))
        #expect(try selection.metadata.graphMapping.sourceCoverage == .dataset)
    }

    @Test("RDF quad metadata without a graph field selects the default graph")
    func rdfQuadDefaultGraph() throws {
        let descriptor = makeDescriptor(
            identifier: "rdf_quad",
            fieldNames: ["subject", "predicate", "object"],
            metadata: [:]
        )

        let selection = try #require(
            try RDFDatasetIndexSelection(descriptor: descriptor)
        )

        #expect(selection.metadata.graphMapping == .defaultGraph)
        #expect(try selection.metadata.graphMapping.sourceCoverage == .defaultGraph)
    }

    @Test("OWL metadata preserves a fixed named graph")
    func owlFixedNamedGraph() throws {
        let graph = try RDFTerm.iri(
            validating:
                "https://example.invalid/graph/calendar"
        )
        let descriptor = makeDescriptor(
            identifier: "owl_class_rdf",
            fieldNames: [],
            metadata: [
                "individualIRIBase": .string("https://example.invalid/entity/"),
                "graph": .rdfTerm(graph),
            ]
        )

        let selection = try #require(
            try RDFDatasetIndexSelection(descriptor: descriptor)
        )

        #expect(selection.metadata.graphMapping == .fixed(graph))
        #expect(
            try selection.metadata.graphMapping.sourceCoverage
                == .namedGraph(RDFGraphName(graph))
        )
    }

    @Test("Malformed canonical metadata fails instead of dropping the source")
    func malformedCanonicalMetadataFails() {
        let descriptor = makeDescriptor(
            identifier: "rdf_quad",
            fieldNames: ["subject", "predicate"],
            metadata: [:]
        )

        #expect(
            throws: IndexKindMetadataError.invalidFieldCount(
                identifier: "rdf_quad",
                expected: "3...4",
                actual: 2
            )
        ) {
            _ = try RDFDatasetIndexSelection(descriptor: descriptor)
        }
    }

    @Test("Non-RDF indexes do not participate in the logical dataset")
    func unrelatedIndexIsIgnored() throws {
        let descriptor = makeDescriptor(
            identifier: "scalar",
            fieldNames: ["timestamp"],
            metadata: [:]
        )

        #expect(try RDFDatasetIndexSelection(descriptor: descriptor) == nil)
    }

    private func makeDescriptor(
        identifier: String,
        fieldNames: [String],
        metadata: [String: FieldValue]
    ) -> IndexDescriptorMetadata {
        IndexDescriptorMetadata(
            entityName: "CalendarEntry",
            name: "Calendar_\(identifier)",
            kind: IndexKindMetadata(
                identifier: identifier,
                subspaceStructure: .hierarchical,
                fields: fieldNames.enumerated().map { offset, name in
                    IndexFieldMetadata(
                        identity: FieldIdentity(
                            name: name,
                            number: offset
                        )
                    )
                },
                metadata: metadata
            )
        )
    }
}
#endif
