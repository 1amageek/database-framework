import DatabaseWire

/// Linear ownership for a request-accounted RDF graph result.
///
/// The full result remains admitted through canonical ordering, duplicate
/// removal, fingerprinting, and page selection. Only the final visible page
/// crosses into an ordinary Array.
public struct DatabaseRetainedRDFGraph: ~Copyable, Sendable {
    private var storage: DatabaseRetainedBuffer<DatabaseRDFQuad>

    package init(
        storage: consuming DatabaseRetainedBuffer<DatabaseRDFQuad>
    ) {
        self.storage = consume storage
    }

    public var count: Int { storage.count }
    public var isEmpty: Bool { storage.isEmpty }

    package borrowing func withElement<Result, Failure: Error>(
        at index: Int,
        _ body: (borrowing DatabaseRDFQuad) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        try storage.withElement(at: index, body)
    }

    package consuming func sorting<Failure: Error>(
        by areInIncreasingOrder: (
            borrowing DatabaseRDFQuad,
            borrowing DatabaseRDFQuad
        ) throws(Failure) -> Bool
    ) throws(Failure) -> DatabaseRetainedRDFGraph {
        DatabaseRetainedRDFGraph(
            storage: try storage.sortingElements(
                by: areInIncreasingOrder
            )
        )
    }

    package consuming func removingAdjacentDuplicates<Failure: Error>(
        by areEquivalent: (
            borrowing DatabaseRDFQuad,
            borrowing DatabaseRDFQuad
        ) throws(Failure) -> Bool
    ) throws(Failure) -> DatabaseRetainedRDFGraph {
        DatabaseRetainedRDFGraph(
            storage: try storage.removingAdjacentDuplicates(
                by: areEquivalent
            )
        )
    }

    package consuming func promotePage(
        _ range: Range<Int>
    ) -> [DatabaseRDFQuad] {
        precondition(
            range.lowerBound >= 0 && range.upperBound <= storage.count
        )
        guard range.count != storage.count else {
            return storage.promoteToOutput()
        }
        var output: [DatabaseRDFQuad] = []
        output.reserveCapacity(range.count)
        for index in range {
            storage.withElement(at: index) { quad in
                output.append(copy quad)
            }
        }
        return output
    }

    public consuming func promoteToOutput() -> [DatabaseRDFQuad] {
        storage.promoteToOutput()
    }
}
