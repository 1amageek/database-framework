import DatabaseKit
import DatabaseTypes

/// Typed results produced by one canonical Fusion execution.
public struct FusionResponse<Item: Persistable>: Sendable {
    public let results: [ScoredResult<Item>]
    public let continuation: QueryContinuation?
    public let metadata: [String: FieldValue]

    public init(
        results: [ScoredResult<Item>],
        continuation: QueryContinuation?,
        metadata: [String: FieldValue]
    ) {
        self.results = results
        self.continuation = continuation
        self.metadata = metadata
    }
}
