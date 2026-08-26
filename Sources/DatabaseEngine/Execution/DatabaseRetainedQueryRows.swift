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
    package var workMeter: DatabaseWorkMeter { storage.workMeter }

    package borrowing func withElement<Failure: Error>(
        at index: Int,
        _ body: (borrowing QueryRow) throws(Failure) -> Void
    ) throws(Failure) {
        try storage.withElement(at: index, body)
    }

    package borrowing func withElement<Failure: Error>(
        at index: Int,
        _ body: (borrowing QueryRow) async throws(Failure) -> Void
    ) async throws(Failure) {
        try await storage.withElement(at: index, body)
    }

    public consuming func promoteToOutput() -> [QueryRow] {
        storage.promoteToOutput()
    }

    /// Moves the unique row storage into immutable shared ownership without
    /// releasing its originating request reservation.
    package consuming func moveToSharedOwnership(
        at stage: DatabaseWorkStage
    ) throws -> DatabaseSharedRetainedArray<QueryRow> {
        try storage.moveToSharedOwnership(at: stage)
    }
}
