// Bounded result page returned by a typed database scan.
import DatabaseKit

public struct DatabaseScanPage<Model: Persistable>: Sendable {
    public let items: [Model]
    public let continuation: DatabaseScanContinuation?

    public init(
        items: [Model],
        continuation: DatabaseScanContinuation?
    ) {
        self.items = items
        self.continuation = continuation
    }
}
