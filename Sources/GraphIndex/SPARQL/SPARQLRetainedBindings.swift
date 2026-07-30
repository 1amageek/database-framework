import DatabaseEngine

/// Linear ownership for request-scoped SPARQL solution storage.
///
/// Unique storage can move its Array buffer into the final response without a
/// copy. Shared storage is immutable and a sliced shared relation therefore
/// carries visible bounds instead of materializing a second Array.
enum SPARQLRetainedBindings: ~Copyable, Sendable {
    case empty
    case unique(DatabaseRetainedBuffer<VariableBinding>)
    case shared(DatabaseSharedRetainedArray<VariableBinding>)
    case sharedSlice(
        DatabaseSharedRetainedArray<VariableBinding>,
        Range<Int>
    )

    var count: Int {
        switch self {
        case .empty:
            return 0
        case .unique(let storage):
            return storage.count
        case .shared(let storage):
            return storage.count
        case .sharedSlice(_, let range):
            return range.count
        }
    }

    var isEmpty: Bool { count == 0 }

    var isUnique: Bool {
        borrowing get {
            switch self {
            case .unique:
                return true
            case .empty:
                return false
            case .shared:
                return false
            case .sharedSlice:
                return false
            }
        }
    }

    borrowing func withElement<Result, Failure: Error>(
        at index: Int,
        _ body: (borrowing VariableBinding) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        precondition(index >= 0 && index < count)
        switch self {
        case .empty:
            preconditionFailure("Cannot borrow an element from an empty relation")
        case .unique(let storage):
            return try storage.withElement(at: index, body)
        case .shared(let storage):
            return try storage.withElement(at: index, body)
        case .sharedSlice(let storage, let range):
            return try storage.withElement(
                at: range.lowerBound + index,
                body
            )
        }
    }

    borrowing func withElement<Result, Failure: Error>(
        at index: Int,
        _ body: (borrowing VariableBinding) async throws(Failure) -> Result
    ) async throws(Failure) -> Result {
        precondition(index >= 0 && index < count)
        switch self {
        case .empty:
            preconditionFailure("Cannot borrow an element from an empty relation")
        case .unique(let storage):
            return try await storage.withElement(at: index, body)
        case .shared(let storage):
            return try await storage.withElement(at: index, body)
        case .sharedSlice(let storage, let range):
            return try await storage.withElement(
                at: range.lowerBound + index,
                body
            )
        }
    }

    consuming func reorderingUniqueElements(
        destinationOriginalIndex: (Int) -> Int
    ) -> SPARQLRetainedBindings {
        switch consume self {
        case .empty:
            return .empty
        case .unique(let storage):
            return .unique(
                storage.reorderingElements(
                    destinationOriginalIndex: destinationOriginalIndex
                )
            )
        case .shared:
            preconditionFailure(
                "Shared SPARQL bindings must be normalized before mutation"
            )
        case .sharedSlice:
            preconditionFailure(
                "Shared SPARQL bindings must be normalized before mutation"
            )
        }
    }

    consuming func stableCompactingUnique<Failure: Error>(
        _ isIncluded: (borrowing VariableBinding) throws(Failure) -> Bool
    ) throws(Failure) -> SPARQLRetainedBindings {
        switch consume self {
        case .empty:
            return .empty
        case .unique(let storage):
            return .unique(try storage.stableCompacting(isIncluded))
        case .shared:
            preconditionFailure(
                "Shared SPARQL bindings must be normalized before compaction"
            )
        case .sharedSlice:
            preconditionFailure(
                "Shared SPARQL bindings must be normalized before compaction"
            )
        }
    }

    consuming func releasingRetainedFootprint(
        _ footprint: DatabaseIntermediateFootprint
    ) throws -> SPARQLRetainedBindings {
        switch consume self {
        case .empty:
            precondition(
                footprint.rows == 0 && footprint.bytes == 0,
                "An empty relation cannot release retained row payload"
            )
            return .empty
        case .unique(let storage):
            return .unique(
                try storage.releasingRetainedFootprint(footprint)
            )
        case .shared:
            preconditionFailure(
                "Shared SPARQL bindings own an immutable aggregate reservation"
            )
        case .sharedSlice:
            preconditionFailure(
                "Shared SPARQL bindings own an immutable aggregate reservation"
            )
        }
    }

    /// Creates the two ownership views required by a fan-out path without
    /// copying the element buffer. The retained relation continues through the
    /// current evaluation while the snapshot can be copied into a cache.
    consuming func sharingForFanOut(
        at stage: DatabaseWorkStage
    ) throws -> SPARQLSharedBindingOwnership {
        switch consume self {
        case .empty:
            return SPARQLSharedBindingOwnership(
                retained: .empty,
                snapshot: .empty
            )
        case .unique(let storage):
            let admission = try storage.prepareToShare(at: stage)
            let sharedStorage = storage.share(using: admission)
            return SPARQLSharedBindingOwnership(
                retained: .shared(sharedStorage),
                snapshot: .shared(
                    sharedStorage,
                    visibleRange: 0..<sharedStorage.count
                )
            )
        case .shared(let storage):
            return SPARQLSharedBindingOwnership(
                retained: .shared(storage),
                snapshot: .shared(
                    storage,
                    visibleRange: 0..<storage.count
                )
            )
        case .sharedSlice(let storage, let range):
            return SPARQLSharedBindingOwnership(
                retained: .sharedSlice(storage, range),
                snapshot: .shared(storage, visibleRange: range)
            )
        }
    }

    /// Applies OFFSET/LIMIT without allocating a second relation buffer.
    /// Unique input is compacted in place. Immutable shared input becomes a
    /// bounded owner view whose lifetime cannot outlast the shared storage.
    consuming func applyingSlice(
        offset: Int,
        limit: Int?
    ) -> SPARQLRetainedBindings {
        precondition(offset >= 0)
        precondition(limit.map { $0 >= 0 } ?? true)

        switch consume self {
        case .empty:
            return .empty
        case .unique(let storage):
            let range = Self.visibleRange(
                count: storage.count,
                offset: offset,
                limit: limit
            )
            guard !range.isEmpty else { return .empty }
            guard range.count != storage.count else {
                return .unique(storage)
            }
            let compacted = storage.retainingSubrange(range)
            return .unique(compacted)
        case .shared(let storage):
            let range = Self.visibleRange(
                count: storage.count,
                offset: offset,
                limit: limit
            )
            guard !range.isEmpty else { return .empty }
            guard range.count != storage.count else {
                return .shared(storage)
            }
            return .sharedSlice(storage, range)
        case .sharedSlice(let storage, let sourceRange):
            let relativeRange = Self.visibleRange(
                count: sourceRange.count,
                offset: offset,
                limit: limit
            )
            guard !relativeRange.isEmpty else { return .empty }
            guard relativeRange.count != sourceRange.count else {
                return .sharedSlice(storage, sourceRange)
            }
            let lowerBound = sourceRange.lowerBound
                + relativeRange.lowerBound
            let upperBound = sourceRange.lowerBound
                + relativeRange.upperBound
            return .sharedSlice(storage, lowerBound..<upperBound)
        }
    }

    /// Produces the public Array result at the top-level execution boundary.
    /// A unique relation transfers its buffer. Shared storage copies only row
    /// headers because other readers may retain the immutable owner.
    consuming func promoteToOutput() -> [VariableBinding] {
        switch consume self {
        case .empty:
            return []
        case .unique(let storage):
            return storage.promoteToOutput()
        case .shared(let storage):
            return storage.withSpan { span in
                Self.materialize(span, range: span.indices)
            }
        case .sharedSlice(let storage, let range):
            return storage.withSpan { span in
                Self.materialize(span, range: range)
            }
        }
    }

    /// Applies the public pagination boundary and transfers a unique buffer
    /// after in-place compaction whenever no shared cache owner exists.
    consuming func promoteToOutput(
        offset: Int,
        limit: Int?
    ) -> [VariableBinding] {
        (consume self)
            .applyingSlice(offset: offset, limit: limit)
            .promoteToOutput()
    }

    private static func visibleRange(
        count: Int,
        offset: Int,
        limit: Int?
    ) -> Range<Int> {
        guard offset < count else { return 0..<0 }
        let availableCount = count - offset
        let visibleCount = min(limit ?? availableCount, availableCount)
        return offset..<(offset + visibleCount)
    }

    private static func materialize(
        _ span: Span<VariableBinding>,
        range: Range<Int>
    ) -> [VariableBinding] {
        var output: [VariableBinding] = []
        output.reserveCapacity(range.count)
        for index in range {
            output.append(span[index])
        }
        return output
    }
}

struct SPARQLSharedBindingOwnership: ~Copyable, Sendable {
    let retained: SPARQLRetainedBindings
    let snapshot: SPARQLSharedBindingSnapshot

    /// Transfers the current evaluation's retained view while releasing the
    /// companion snapshot handle owned by this fan-out transition.
    consuming func takeRetainedBindings() -> SPARQLRetainedBindings {
        consume retained
    }
}
