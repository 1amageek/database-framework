import DatabaseKit

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
    public let indexType: IndexType
    public let metadata: RDFDatasetIndexMetadata
    public let includedFieldNames: [String]

    public init?(descriptor: IndexDescriptor) throws {
        guard let metadata = try Self.decode(
                definition: descriptor.declaration.definition,
                indexName: descriptor.name
        ) else {
            return nil
        }
        self.indexName = descriptor.name
        self.indexType = descriptor.type
        self.metadata = metadata
        self.includedFieldNames = descriptor.includedFieldNames
    }

    private static func decode(
        definition: IndexDefinition<FieldIdentity>,
        indexName: String
    ) throws -> RDFDatasetIndexMetadata? {
        guard case .graph(let graphDefinition, _) = definition else {
            return nil
        }
        switch graphDefinition {
        case .rdf(let subject, let predicate, let object, let graph):
            return RDFDatasetIndexMetadata(
                subjectFieldName: subject.name,
                predicateFieldName: predicate.name,
                objectFieldName: object.name,
                graphMapping: graph.map {
                    .entityField($0.name)
                } ?? .defaultGraph
            )
        case .ontologyProjection(_, let graph):
            return RDFDatasetIndexMetadata(
                subjectFieldName: "subject",
                predicateFieldName: "predicate",
                objectFieldName: "object",
                graphMapping: graph.map {
                    .fixed($0.term)
                } ?? .defaultGraph
            )
        case .property:
            return nil
        }
    }
}
