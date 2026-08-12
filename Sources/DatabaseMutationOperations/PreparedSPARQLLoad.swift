import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_MUTATION_OPERATIONS_GRAPH_INDEXES
import DatabaseKit

package struct PreparedSPARQLLoad: Sendable {
    package let destination: String?
    package let triples: [RDFTriple]

    package init(destination: String?, triples: consuming [RDFTriple]) {
        self.destination = destination
        self.triples = consume triples
    }
}

#endif
