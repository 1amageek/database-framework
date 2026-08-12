import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_GRAPH_OPERATIONS_ENABLED
@_spi(DatabaseOperations) import DatabaseWire
import DatabaseKit

extension RDFDataset {
    init(databaseQuads: [RDFQuad]) {
        self.init(
            quads: databaseQuads.map { quad in
                RDFQuad(
                    subject: quad.subject,
                    predicate: quad.predicate,
                    object: quad.object,
                    graph: quad.graph
                )
            }
        )
    }
}

#endif
