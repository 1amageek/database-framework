import Core
import DatabaseValue
import Graph

package struct RDFDatasetIndexMetadata: Sendable {
    package let subjectFieldName: String
    package let predicateFieldName: String
    package let objectFieldName: String
    package let graphScope: RDFDatasetGraphScope

    package var strategy: GraphIndexStrategy { .quadStore }
}

package struct RDFDatasetIndexSelection: Sendable {
    package let indexName: String
    package let metadata: RDFDatasetIndexMetadata
    package let storedFieldNames: [String]

    package init?(descriptor: IndexDescriptor) throws {
        guard let metadata = try Self.decode(
            kind: descriptor.kind,
            indexName: descriptor.name
        ) else {
            return nil
        }
        self.indexName = descriptor.name
        self.metadata = metadata
        self.storedFieldNames = descriptor.storedFieldNames
    }

    package init?(descriptor: IndexDescriptorMetadata) throws {
        guard let metadata = try Self.decode(
            kind: descriptor.kind,
            indexName: descriptor.name
        ) else {
            return nil
        }
        self.init(
            indexName: descriptor.name,
            metadata: metadata,
            storedFieldNames: descriptor.storedFieldNames
        )
    }

    private init(
        indexName: String,
        metadata: RDFDatasetIndexMetadata,
        storedFieldNames: [String]
    ) {
        self.indexName = indexName
        self.metadata = metadata
        self.storedFieldNames = storedFieldNames
    }

    private static func decode(
        kind: IndexKindMetadata,
        indexName: String
    ) throws -> RDFDatasetIndexMetadata? {
        switch kind.identifier {
        case "rdf_quad":
            try kind.validateIdentity(
                identifier: "rdf_quad",
                subspaceStructure: .hierarchical
            )
            try kind.validateMetadataKeys()
            try kind.validateFieldCount(minimum: 3, maximum: 4)
            return RDFDatasetIndexMetadata(
                subjectFieldName: kind.fieldNames[0],
                predicateFieldName: kind.fieldNames[1],
                objectFieldName: kind.fieldNames[2],
                graphScope: kind.fieldNames.count == 4
                    ? .entityField(kind.fieldNames[3])
                    : .defaultGraph
            )

        case "owl_class_rdf":
            try kind.validateIdentity(
                identifier: "owl_class_rdf",
                subspaceStructure: .hierarchical
            )
            try kind.validateMetadataKeys(
                required: ["individualIRIBase"],
                optional: ["graph"]
            )
            try kind.validateFieldCount(0)
            _ = try kind.requireString("individualIRIBase")

            let graphScope: RDFDatasetGraphScope
            if kind.metadata["graph"] != nil {
                let graph = try kind.requireRDFTerm("graph")
                do {
                    try DatabaseRDFTermCodec.validate(
                        graph,
                        role: .graphName
                    )
                    graphScope = .fixed(graph)
                } catch let error {
                    throw RDFDatasetIndexMetadataError.invalidFixedGraph(
                        indexName: indexName,
                        reason: error
                    )
                }
            } else {
                graphScope = .defaultGraph
            }
            return RDFDatasetIndexMetadata(
                subjectFieldName: "subject",
                predicateFieldName: "predicate",
                objectFieldName: "object",
                graphScope: graphScope
            )

        default:
            return nil
        }
    }
}
