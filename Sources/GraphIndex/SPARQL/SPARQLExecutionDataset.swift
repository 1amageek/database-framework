import DatabaseKit

/// The executable RDF dataset selected by SPARQL dataset clauses.
public struct SPARQLExecutionDataset: Sendable, Hashable {
    private enum NamedGraphSelection: Sendable, Hashable {
        case all
        case selected(RDFNamedGraphSet)
    }

    package let defaultGraphTarget: RDFGraphScanTarget
    private let namedGraphSelection: NamedGraphSelection

    public static let implicit = SPARQLExecutionDataset(
        defaultGraphTarget: .defaultGraph,
        namedGraphSelection: .all
    )

    public init(_ dataset: SPARQLDataset) throws {
        switch dataset {
        case .implicit:
            self = .implicit

        case .explicit(let defaultGraphIRIs, let namedGraphIRIs):
            let defaultGraphs = try Self.graphNames(from: defaultGraphIRIs)
            let namedGraphs = try Self.graphNames(from: namedGraphIRIs)
            self.defaultGraphTarget = defaultGraphs.isEmpty
                ? .empty
                : .namedGraphUnion(defaultGraphs)
            self.namedGraphSelection = .selected(namedGraphs)
        }
    }

    private init(
        defaultGraphTarget: RDFGraphScanTarget,
        namedGraphSelection: NamedGraphSelection
    ) {
        self.defaultGraphTarget = defaultGraphTarget
        self.namedGraphSelection = namedGraphSelection
    }

    package func contains(namedGraph: RDFGraphName) -> Bool {
        switch namedGraphSelection {
        case .all:
            return true
        case .selected(let graphs):
            return graphs.contains(namedGraph)
        }
    }

    /// `nil` means every named graph in the physical dataset is visible.
    package var selectedNamedGraphs: RDFNamedGraphSet? {
        switch namedGraphSelection {
        case .all:
            return nil
        case .selected(let graphs):
            return graphs
        }
    }

    private static func graphNames(
        from iris: [String]
    ) throws -> RDFNamedGraphSet {
        var graphs: [RDFGraphName] = []
        graphs.reserveCapacity(iris.count)
        for iri in iris {
            graphs.append(try RDFGraphName(iri: iri))
        }
        return RDFNamedGraphSet(graphs)
    }
}
