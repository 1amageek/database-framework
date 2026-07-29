import DatabaseKit

/// A single-owner bounded result page returned by a typed database scan.
///
/// The page crosses an asynchronous transaction boundary exactly once. Its
/// models retain their ordinary value semantics after the receiving caller
/// consumes the page.
public struct DatabaseScanPage<Model: Persistable>: ~Copyable, Sendable {
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
