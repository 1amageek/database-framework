import Graph

/// Graphs affected by one atomic RDF graph-store mutation.
public enum RDFGraphMutationScope: Sendable, Hashable {
    case defaultGraph
    case named(RDFGraphName)
    case allNamedGraphs
    case allGraphs
}
