/// Copyable admitted ownership for a sequence of retained query responses.
@_spi(DatabaseExecution)
public struct DatabaseRetainedQueryResponses: Sendable {
    private let storage: DatabaseSharedRetainedArray<
        DatabaseRetainedQueryResponse
    >

    package init(
        storage: DatabaseSharedRetainedArray<DatabaseRetainedQueryResponse>
    ) {
        self.storage = storage
    }

    public var count: Int { storage.count }
    public var isEmpty: Bool { storage.isEmpty }

    public func response(at index: Int) -> DatabaseRetainedQueryResponse {
        precondition(index >= 0 && index < storage.count)
        return storage[index]
    }
}
