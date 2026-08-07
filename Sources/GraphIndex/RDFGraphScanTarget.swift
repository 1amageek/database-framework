import DatabaseKit

/// The graph set visible to one atomic RDF quad scan.
public enum RDFGraphScanTarget: Sendable, Hashable {
    case empty
    case defaultGraph
    case named(RDFGraphName)
    /// The RDF merge of the selected named graphs, exposed as a default graph.
    case namedGraphUnion(RDFNamedGraphSet)
    case allNamedGraphs
}
