#if DATABASE_SERVER_GRAPH_INDEXES
@_spi(DatabaseServer) import DatabaseWire
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
