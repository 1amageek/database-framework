import DatabaseKit
import DatabaseTypes

@_spi(DatabaseExecution)
public struct RDFDatasetIndexMetadata: Sendable {
    public let subjectFieldName: String
    public let predicateFieldName: String
    public let objectFieldName: String
    public let graphMapping: RDFDatasetGraphMapping

    public var strategy: GraphIndexStrategy { .quadStore }
}

@_spi(DatabaseExecution)
public struct RDFDatasetIndexSelection: Sendable {
    public let indexName: String
    public let kindIdentifier: String
    public let metadata: RDFDatasetIndexMetadata
    public let storedFieldNames: [String]

    public init?(descriptor: IndexDescriptor) throws {
        guard let metadata = try Self.decode(
            kind: descriptor.kind,
            indexName: descriptor.name
        ) else {
            return nil
        }
        self.indexName = descriptor.name
        self.kindIdentifier = descriptor.kind.identifier
        self.metadata = metadata
        self.storedFieldNames = descriptor.storedFieldNames
    }

    public init?(descriptor: IndexDescriptorMetadata) throws {
        guard let metadata = try Self.decode(
            kind: descriptor.kind,
            indexName: descriptor.name
        ) else {
            return nil
        }
        self.init(
            indexName: descriptor.name,
            kindIdentifier: descriptor.kind.identifier,
            metadata: metadata,
            storedFieldNames: descriptor.storedFieldNames
        )
    }

    private init(
        indexName: String,
        kindIdentifier: String,
        metadata: RDFDatasetIndexMetadata,
        storedFieldNames: [String]
    ) {
        self.indexName = indexName
        self.kindIdentifier = kindIdentifier
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
                graphMapping: kind.fieldNames.count == 4
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

            let graphMapping: RDFDatasetGraphMapping
            if kind.metadata["graph"] != nil {
                let graph = try kind.requireRDFTerm("graph")
                do {
                    try RDFTermValidation.validate(
                        graph,
                        role: .graphName
                    )
                    graphMapping = .fixed(graph)
                } catch let error {
                    throw RDFDatasetIndexMetadataError.invalidFixedGraph(
                        indexName: indexName,
                        reason: error
                    )
                }
            } else {
                graphMapping = .defaultGraph
            }
            return RDFDatasetIndexMetadata(
                subjectFieldName: "subject",
                predicateFieldName: "predicate",
                objectFieldName: "object",
                graphMapping: graphMapping
            )

        default:
            return nil
        }
    }
}
