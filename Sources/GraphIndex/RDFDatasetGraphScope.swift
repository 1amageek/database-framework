import DatabaseTypes
import DatabaseKit

package enum RDFDatasetGraphScope: Sendable, Equatable {
    case defaultGraph
    case entityField(String)
    case fixed(RDFTerm)

    package var sourceCoverage: RDFDatasetSourceCoverage {
        get throws {
            switch self {
            case .defaultGraph:
                return .defaultGraph
            case .entityField:
                return .dataset
            case .fixed(let graph):
                return .namedGraph(try RDFGraphName(graph))
            }
        }
    }

    package var entityGraphFieldName: String? {
        switch self {
        case .defaultGraph:
            return nil
        case .entityField(let fieldName):
            return fieldName
        case .fixed:
            return "graph"
        }
    }
}
