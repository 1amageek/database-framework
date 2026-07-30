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
