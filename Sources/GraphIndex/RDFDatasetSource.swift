import Graph
import StorageKit

/// One physical RDF quad index participating in a logical RDF dataset.
public struct RDFDatasetSource: Sendable {
    public let entityName: String
    public let indexName: String
    public let indexSubspace: Subspace
    public let coverage: RDFDatasetSourceCoverage
    package let physicalCodec: RDFQuadIndexPhysicalCodec

    public init(
        entityName: String,
        indexName: String,
        indexSubspace: Subspace,
        coverage: RDFDatasetSourceCoverage
    ) {
        self.entityName = entityName
        self.indexName = indexName
        self.indexSubspace = indexSubspace
        self.coverage = coverage
        self.physicalCodec = RDFQuadIndexPhysicalCodec(
            baseSubspace: indexSubspace
        )
    }

    package init(
        entityName: String,
        selection: RDFDatasetIndexSelection,
        indexSubspace: Subspace
    ) throws {
        self.init(
            entityName: entityName,
            indexName: selection.indexName,
            indexSubspace: indexSubspace,
            coverage: try selection.metadata.graphScope.sourceCoverage
        )
    }
}
