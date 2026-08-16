import DatabaseKit

public struct DatabaseRDFStoredDocumentPage: Sendable, Hashable {
    public let identifier: String
    public let revision: UInt64
    public let auxiliaryIdentifiers: [String]
    public let quads: [RDFQuad]
    public let totalQuadCount: UInt64
    public let nextOffset: UInt64?

    public init(
        identifier: String,
        revision: UInt64,
        auxiliaryIdentifiers: [String],
        quads: [RDFQuad],
        totalQuadCount: UInt64,
        nextOffset: UInt64?
    ) {
        self.identifier = identifier
        self.revision = revision
        self.auxiliaryIdentifiers = auxiliaryIdentifiers
        self.quads = quads
        self.totalQuadCount = totalQuadCount
        self.nextOffset = nextOffset
    }
}
