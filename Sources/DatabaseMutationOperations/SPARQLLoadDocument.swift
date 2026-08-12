import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_MUTATION_OPERATIONS_GRAPH_INDEXES
import DatabaseKit

public struct SPARQLLoadDocument: Sendable {
    public let byteCount: UInt64
    private var storage: [RDFTriple]

    public init(byteCount: UInt64, triples: consuming [RDFTriple]) {
        self.byteCount = byteCount
        self.storage = consume triples
    }

    public var tripleCount: Int {
        storage.count
    }

    package consuming func takeTriples() -> [RDFTriple] {
        storage
    }
}

#endif
