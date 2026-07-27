import DatabaseKit
import DatabaseKit

/// The executable RDF dataset selected by SPARQL dataset clauses.
public struct SPARQLDatasetExecutionScope: Sendable, Hashable {
    private enum NamedGraphSelection: Sendable, Hashable {
        case all
        case selected([RDFGraphName])
    }

    package let defaultGraphScope: RDFGraphScanScope
    private let namedGraphSelection: NamedGraphSelection

    public static let implicit = SPARQLDatasetExecutionScope(
        defaultGraphScope: .defaultGraph,
        namedGraphSelection: .all
    )

    public init(_ dataset: SPARQLDataset) throws {
        switch dataset {
        case .implicit:
            self = .implicit

        case .explicit(let defaultGraphIRIs, let namedGraphIRIs):
            let defaultGraphs = try Self.graphNames(from: defaultGraphIRIs)
            let namedGraphs = try Self.graphNames(from: namedGraphIRIs)
            self.defaultGraphScope = defaultGraphs.isEmpty
                ? .empty
                : .namedGraphUnion(defaultGraphs)
            self.namedGraphSelection = .selected(namedGraphs)
        }
    }

    private init(
        defaultGraphScope: RDFGraphScanScope,
        namedGraphSelection: NamedGraphSelection
    ) {
        self.defaultGraphScope = defaultGraphScope
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
    package var selectedNamedGraphs: [RDFGraphName]? {
        switch namedGraphSelection {
        case .all:
            return nil
        case .selected(let graphs):
            return graphs
        }
    }

    private static func graphNames(from iris: [String]) throws -> [RDFGraphName] {
        var seen = Set<RDFGraphName>()
        var graphs: [RDFGraphName] = []
        graphs.reserveCapacity(iris.count)
        for iri in iris {
            let graph = try RDFGraphName(iri: iri)
            if seen.insert(graph).inserted {
                graphs.append(graph)
            }
        }
        return graphs.sorted()
    }
}
