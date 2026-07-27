import DatabaseKit

/// Describes which RDF graphs can be produced by one physical dataset index.
public enum RDFDatasetSourceCoverage: Sendable, Hashable {
    case defaultGraph
    case namedGraph(RDFGraphName)
    case dataset
}
