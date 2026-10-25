import DatabaseKit

/// One retained positional row per advance, on one admitted read operation.
protocol SQLRelationProducer: Sendable {
    var schema: CanonicalRelationSchema { get }
    func next() async throws -> SQLRelationRow?
    func close() async throws
}
