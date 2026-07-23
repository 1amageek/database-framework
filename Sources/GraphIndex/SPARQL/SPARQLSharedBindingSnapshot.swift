import DatabaseEngine

/// Copyable immutable handle for an explicitly shared SPARQL relation.
///
/// This type is used only by fan-out sidecars such as scan and SubSelect
/// caches. Unique relations must be converted with `sharing(at:)` first.
enum SPARQLSharedBindingSnapshot: Sendable {
    case empty
    case shared(
        DatabaseSharedRetainedArray<VariableBinding>,
        visibleRange: Range<Int>
    )

    init(
        shared bindings: borrowing SPARQLRetainedBindings
    ) throws {
        if bindings.isEmpty {
            self = .empty
            return
        }
        guard let sharedStorage = bindings.sharedStorage() else {
            throw SPARQLQueryError.executionFailed(
                "a cache attempted to snapshot unique binding storage"
            )
        }
        self = .shared(
            sharedStorage.storage,
            visibleRange: sharedStorage.visibleRange
        )
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
