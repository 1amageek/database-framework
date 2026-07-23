import Graph

/// The graph set visible to one atomic RDF quad scan.
public enum RDFGraphScanScope: Sendable, Hashable {
    case empty
    case defaultGraph
    case named(RDFGraphName)
    /// The RDF merge of the selected named graphs, exposed as a default graph.
    case namedGraphUnion([RDFGraphName])
    case allNamedGraphs
}
