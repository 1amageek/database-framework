import DatabaseWire
import Graph

extension RDFDataset {
    init(databaseQuads: [DatabaseRDFQuad]) {
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
