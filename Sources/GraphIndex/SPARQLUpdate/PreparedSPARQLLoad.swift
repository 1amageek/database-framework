import DatabaseKit

@_spi(DatabaseExecution)
public struct PreparedSPARQLLoad: Sendable {
    public let destination: String?
    public let triples: [RDFTriple]

    public init(destination: String?, triples: consuming [RDFTriple]) {
        self.destination = destination
        self.triples = consume triples
    }
}
