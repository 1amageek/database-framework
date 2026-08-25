/// Linear ownership for request-accounted logical-source rows.
///
/// A logical source keeps its row storage admitted until the canonical
/// dispatcher either consumes each row or promotes the final public output.
/// This prevents extension-point implementations from returning an
/// unaccounted Array between source execution and relational materialization.
public struct DatabaseRetainedQueryRows: ~Copyable, Sendable {
    private var storage: DatabaseRetainedBuffer<QueryRow>

    package init(
        storage: consuming DatabaseRetainedBuffer<QueryRow>
    ) {
        self.storage = consume storage
    }

    public var count: Int { storage.count }
    public var isEmpty: Bool { storage.isEmpty }

    package borrowing func withElement<Result, Failure: Error>(
        at index: Int,
        _ body: (borrowing QueryRow) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        try storage.withElement(at: index, body)
    }

    package borrowing func withElement<Result, Failure: Error>(
        at index: Int,
        _ body: (borrowing QueryRow) async throws(Failure) -> Result
    ) async throws(Failure) -> Result {
        try await storage.withElement(at: index, body)
    }

    public consuming func promoteToOutput() -> [QueryRow] {
        storage.promoteToOutput()
    }
}
