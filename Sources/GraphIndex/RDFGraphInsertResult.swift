/// Observable effects of one authoritative RDF quad insertion.
public struct RDFGraphInsertResult: Sendable, Equatable {
    public let quadInserted: Bool
    public let graphCreated: Bool

    public init(quadInserted: Bool, graphCreated: Bool) {
        self.quadInserted = quadInserted
        self.graphCreated = graphCreated
    }
}
