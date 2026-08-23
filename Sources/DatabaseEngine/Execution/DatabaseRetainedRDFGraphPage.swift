import DatabaseKit

/// One continuation page whose copied quads remain request-accounted until
/// they cross the final wire or caller output boundary.
@_spi(DatabaseExecution)
public struct DatabaseRetainedRDFGraphPage: Sendable {
    private let storage: DatabaseSharedRetainedArray<RDFQuad>

    package init(storage: DatabaseSharedRetainedArray<RDFQuad>) {
        self.storage = storage
    }

    public var count: Int { storage.count }
    public var isEmpty: Bool { storage.isEmpty }

    public consuming func promoteToOutput() -> [RDFQuad] {
        storage.promoteToOutput()
    }
}
