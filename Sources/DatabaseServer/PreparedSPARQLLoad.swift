import DatabaseKit

struct PreparedSPARQLLoad: Sendable {
    let destination: String?
    let triples: [RDFTriple]

    init(destination: String?, triples: consuming [RDFTriple]) {
        self.destination = destination
        self.triples = consume triples
    }
}
