import DatabaseKit

/// Graphs affected by one atomic RDF graph-store mutation.
public enum RDFGraphMutationTarget: Sendable, Hashable {
    case defaultGraph
    case named(RDFGraphName)
    case allNamedGraphs
    case allGraphs
}
