import DatabaseValue
import Graph

package enum RDFDatasetGraphScope: Sendable, Equatable {
    case defaultGraph
    case recordField(String)
    case fixed(DatabaseRDFTerm)

    package var sourceCoverage: RDFDatasetSourceCoverage {
        get throws {
            switch self {
            case .defaultGraph:
                return .defaultGraph
            case .recordField:
                return .dataset
            case .fixed(let graph):
                return .namedGraph(try RDFGraphName(graph))
            }
        }
    }

    package var recordGraphFieldName: String? {
        switch self {
        case .defaultGraph:
            return nil
        case .recordField(let fieldName):
            return fieldName
        case .fixed:
            return "graph"
        }
    }
}
