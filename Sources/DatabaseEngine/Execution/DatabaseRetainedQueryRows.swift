/// Linear ownership for request-accounted logical-source rows.
///
/// A logical source keeps its row storage admitted until the canonical
/// dispatcher either consumes each row or promotes the final public output.
/// This prevents extension-point implementations from returning an
/// unaccounted Array between source execution and relational materialization.
public struct DatabaseRetainedQueryRows: ~Copyable, Sendable {
    private enum Storage: ~Copyable, Sendable {
        case unique(DatabaseRetainedBuffer<QueryRow>)
        case shared(DatabaseSharedRetainedArrayView<QueryRow>)
    }

    private var storage: Storage

    package init(
        storage: consuming DatabaseRetainedBuffer<QueryRow>
    ) {
        self.storage = .unique(consume storage)
    }

    init(sharedStorage: DatabaseSharedRetainedArrayView<QueryRow>) {
        self.storage = .shared(sharedStorage)
    }

    public var count: Int {
        switch storage {
        case .unique(let storage):
            return storage.count
        case .shared(let storage):
            return storage.count
        }
    }
    public var isEmpty: Bool { count == 0 }
    package var workMeter: DatabaseWorkMeter {
        switch storage {
        case .unique(let storage):
            return storage.workMeter
        case .shared(let storage):
            return storage.workMeter
        }
    }

    package borrowing func withElement<Failure: Error>(
        at index: Int,
        _ body: (borrowing QueryRow) throws(Failure) -> Void
    ) throws(Failure) {
        switch storage {
        case .unique(let storage):
            try storage.withElement(at: index, body)
        case .shared(let storage):
            try storage.withElement(at: index, body)
        }
    }

    package borrowing func withElement<Failure: Error>(
        at index: Int,
        _ body: (borrowing QueryRow) async throws(Failure) -> Void
    ) async throws(Failure) {
        switch storage {
        case .unique(let storage):
            try await storage.withElement(at: index, body)
        case .shared(let storage):
            try await storage.withElement(at: index, body)
        }
    }

    package borrowing func withSpan<Result, Failure: Error>(
        _ body: (Span<QueryRow>) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        switch storage {
        case .unique(let storage):
            return try storage.withSpan(body)
        case .shared(let storage):
            return try storage.withSpan(body)
        }
    }

    public consuming func promoteToOutput() -> [QueryRow] {
        switch consume storage {
        case .unique(let storage):
            return storage.promoteToOutput()
        case .shared(let storage):
            return Array(storage)
        }
    }

    /// Moves the complete logical row sequence into canonical shared
    /// ownership. The normal unique producer path transfers its COW Array
    /// buffer without materializing a second relation. A bounded shared view
    /// is rebuilt only because it cannot expose rows outside its visible
    /// range through the returned full-range owner.
    package consuming func moveToSharedOwnership(
        at stage: DatabaseWorkStage
    ) throws -> DatabaseSharedRetainedArray<QueryRow> {
        switch consume storage {
        case .unique(let storage):
            return try storage.moveToSharedOwnership(at: stage)
        case .shared(let storage):
            var rebuilt = try DatabaseRetainedQueryRowsBuilder(
                workMeter: storage.workMeter,
                stage: stage,
                expectedCount: storage.count
            )
            for index in 0..<storage.count {
                try storage.withElement(at: index) { row in
                    try rebuilt.append(row)
                }
            }
            return try rebuilt.finish().moveToSharedOwnership(at: stage)
        }
    }

    package consuming func moveToSharedView(
        at stage: DatabaseWorkStage
    ) throws -> DatabaseSharedRetainedArrayView<QueryRow> {
        switch consume storage {
        case .unique(let storage):
            let shared = try storage.moveToSharedOwnership(at: stage)
            return shared.boundedView(shared.startIndex..<shared.endIndex)
        case .shared(let storage):
            return storage
        }
    }
}
