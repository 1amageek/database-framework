#if !os(WASI)
import DatabaseKit
import DatabaseTypes
import Testing
@_spi(DatabaseExecution) @testable import GraphIndex

@Suite("RDF dataset index selection")
struct RDFDatasetIndexMetadataTests {
    private let fields = [
        FieldSchema(name: "subject", fieldNumber: 1, type: .rdfTerm),
        FieldSchema(name: "predicate", fieldNumber: 2, type: .rdfTerm),
        FieldSchema(name: "object", fieldNumber: 3, type: .rdfTerm),
        FieldSchema(
            name: "graph",
            fieldNumber: 4,
            type: .rdfTerm,
            isOptional: true
        ),
        FieldSchema(name: "timestamp", fieldNumber: 5, type: .timestamp),
    ]

    @Test("RDF declaration preserves an entity graph field")
    func rdfEntityGraphField() throws {
        let descriptor = try rdfDescriptor(graph: graphField)
        let selection = try #require(
            try RDFDatasetIndexSelection(descriptor: descriptor)
        )

        #expect(selection.indexType == .graph(.rdf))

        #expect(selection.metadata.subjectFieldName == "subject")
        #expect(selection.metadata.predicateFieldName == "predicate")
        #expect(selection.metadata.objectFieldName == "object")
        #expect(selection.metadata.graphMapping == .entityField("graph"))
        #expect(try selection.metadata.graphMapping.sourceCoverage == .dataset)
    }

    @Test("RDF declaration without a graph field selects the default graph")
    func rdfDefaultGraph() throws {
        let selection = try #require(
            try RDFDatasetIndexSelection(
                descriptor: rdfDescriptor(graph: nil)
            )
        )

        #expect(selection.metadata.graphMapping == .defaultGraph)
        #expect(try selection.metadata.graphMapping.sourceCoverage == .defaultGraph)
    }

    @Test("Ontology projection preserves a fixed named graph")
    func ontologyFixedNamedGraph() throws {
        let graph = try RDFGraphName(
            iri: "https://example.invalid/graph/calendar"
        )
        let descriptor = try IndexDescriptor(
            entityName: "CalendarEntry",
            declaration: IndexDeclaration(
                name: "calendar_ontology",
                definition: .graph(
                    .ontologyProjection(
                        individualIRIBase: "https://example.invalid/entity/",
                        graph: graph
                    ),
                    includedFields: []
                )
            ),
            fieldSchemas: fields
        )
        let selection = try #require(
            try RDFDatasetIndexSelection(descriptor: descriptor)
        )

        #expect(selection.metadata.graphMapping == .fixed(graph.term))
        #expect(
            try selection.metadata.graphMapping.sourceCoverage
                == .namedGraph(graph)
        )
    }

    @Test("Non-RDF indexes do not participate in the logical dataset")
    func unrelatedIndexIsIgnored() throws {
        let descriptor = try IndexDescriptor(
            entityName: "CalendarEntry",
            declaration: .ordered(
                name: "calendar_timestamp",
                keys: [.ascending(timestampField)]
            ),
            fieldSchemas: fields
        )

        #expect(try RDFDatasetIndexSelection(descriptor: descriptor) == nil)
    }

    private var subjectField: FieldIdentity {
        FieldIdentity(name: "subject", number: 1)
    }

    private var predicateField: FieldIdentity {
        FieldIdentity(name: "predicate", number: 2)
    }

    private var objectField: FieldIdentity {
        FieldIdentity(name: "object", number: 3)
    }

    private var graphField: FieldIdentity {
        FieldIdentity(name: "graph", number: 4)
    }

    private var timestampField: FieldIdentity {
        FieldIdentity(name: "timestamp", number: 5)
    }

    private func rdfDescriptor(
        graph: FieldIdentity?
    ) throws -> IndexDescriptor {
        try IndexDescriptor(
            entityName: "CalendarEntry",
            declaration: .graph(
                name: "calendar_rdf",
                definition: .rdf(
                    subject: subjectField,
                    predicate: predicateField,
                    object: objectField,
                    graph: graph
                )
            ),
            fieldSchemas: fields
        )
    }
}
#endif
