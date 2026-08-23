import DatabaseEngine

/// Copyable immutable handle for an explicitly shared SPARQL relation.
///
/// This type is used only by fan-out sidecars such as scan and SubSelect
/// caches. `SPARQLRetainedBindings.sharingForFanOut(at:)` creates the snapshot
/// and the current evaluation's retained view in one consuming transition.
enum SPARQLSharedBindingSnapshot: Sendable {
    case empty
    case shared(
        DatabaseSharedRetainedArray<VariableBinding>,
        visibleRange: Range<Int>
    )

    var count: Int {
        switch self {
        case .empty:
            return 0
        case .shared(_, let visibleRange):
            return visibleRange.count
        }
    }

    func withElement<Result, Failure: Error>(
        at index: Int,
        _ body: (borrowing VariableBinding) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        precondition(index >= 0 && index < count)
        switch self {
        case .empty:
            preconditionFailure("Cannot borrow an element from an empty relation")
        case .shared(let storage, let visibleRange):
            return try storage.withElement(
                at: visibleRange.lowerBound + index,
                body
            )
        }
    }

    func withElement<Result, Failure: Error>(
        at index: Int,
        _ body: (borrowing VariableBinding) async throws(Failure) -> Result
    ) async throws(Failure) -> Result {
        precondition(index >= 0 && index < count)
        switch self {
        case .empty:
            preconditionFailure("Cannot borrow an element from an empty relation")
        case .shared(let storage, let visibleRange):
            return try await storage.withElement(
                at: visibleRange.lowerBound + index,
                body
            )
        }
    }

    func retainedBindings() -> SPARQLRetainedBindings {
        switch self {
        case .empty:
            return .empty
        case .shared(let storage, let visibleRange):
            if visibleRange == 0..<storage.count {
                return .shared(storage)
            }
            return .sharedSlice(storage, visibleRange)
        }
    }
}
