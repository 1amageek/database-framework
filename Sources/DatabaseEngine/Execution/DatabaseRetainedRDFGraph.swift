import DatabaseKit

/// Linear ownership for a request-accounted RDF graph result.
///
/// The full result remains admitted through canonical ordering, duplicate
/// removal, fingerprinting, and page selection. Only the final visible page
/// crosses into an ordinary Array.
public struct DatabaseRetainedRDFGraph: ~Copyable, Sendable {
    private var storage: DatabaseRetainedBuffer<RDFQuad>

    package init(
        storage: consuming DatabaseRetainedBuffer<RDFQuad>
    ) {
        self.storage = consume storage
    }

    public var count: Int { storage.count }
    public var isEmpty: Bool { storage.isEmpty }
    package var workMeter: DatabaseWorkMeter { storage.workMeter }

    package borrowing func withElement<Failure: Error>(
        at index: Int,
        _ body: (borrowing RDFQuad) throws(Failure) -> Void
    ) throws(Failure) {
        try storage.withElement(at: index, body)
    }

    package borrowing func withElement<Failure: Error>(
        at index: Int,
        _ body: (borrowing RDFQuad) async throws(Failure) -> Void
    ) async throws(Failure) {
        try await storage.withElement(at: index, body)
    }

    /// Moves retained graph storage into immutable shared ownership without
    /// releasing its originating request reservation.
    package consuming func moveToSharedOwnership(
        at stage: DatabaseWorkStage
    ) throws -> DatabaseSharedRetainedArray<RDFQuad> {
        try storage.moveToSharedOwnership(at: stage)
    }

    @_spi(DatabaseExecution)
    public consuming func sorting<Failure: Error>(
        by areInIncreasingOrder: (
            borrowing RDFQuad,
            borrowing RDFQuad
        ) throws(Failure) -> Bool
    ) throws(Failure) -> DatabaseRetainedRDFGraph {
        DatabaseRetainedRDFGraph(
            storage: try storage.sortingElements(
                by: areInIncreasingOrder
            )
        )
    }

    @_spi(DatabaseExecution)
    public consuming func removingAdjacentDuplicates<Failure: Error>(
        by areEquivalent: (
            borrowing RDFQuad,
            borrowing RDFQuad
        ) throws(Failure) -> Bool
    ) throws(Failure) -> DatabaseRetainedRDFGraph {
        DatabaseRetainedRDFGraph(
            storage: try storage.removingAdjacentDuplicates(
                by: areEquivalent
            )
        )
    }

    @_spi(DatabaseExecution)
    public consuming func promotePage(
        _ range: Range<Int>
    ) -> [RDFQuad] {
        precondition(
            range.lowerBound >= 0 && range.upperBound <= storage.count
        )
        guard range.count != storage.count else {
            return storage.promoteToOutput()
        }
        var output: [RDFQuad] = []
        output.reserveCapacity(range.count)
        for index in range {
            storage.withElement(at: index) { quad in
                output.append(copy quad)
            }
        }
        return output
    }

    public consuming func promoteToOutput() -> [RDFQuad] {
        storage.promoteToOutput()
    }
}
