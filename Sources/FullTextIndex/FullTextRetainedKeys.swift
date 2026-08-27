import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

/// A full-text match identifier owner that remains coupled to the request
/// meter while it crosses the feature-to-engine fetch boundary.
///
/// The raw posting-list collections are consumed before this value is
/// returned. Consumers can inspect one identifier only through the retained
/// primary-key capability; no unmetered `[Tuple]` bridge is exposed.
struct FullTextRetainedKeys: DatabaseRetainedPrimaryKeyCollection, Sendable {
    private let storage: DatabaseSharedRetainedArray<Tuple>

    private init(storage: consuming DatabaseSharedRetainedArray<Tuple>) {
        self.storage = storage
    }

    /// Admits identifiers directly into retained storage. The builder is the
    /// only output boundary for posting-list candidates; callers do not need
    /// to create an intermediate `[Tuple]` collection.
    struct Builder: ~Copyable {
        private var storage: DatabaseRetainedArrayBuilder<Tuple>
        private let stage: DatabaseWorkStage

        init(
            workMeter: DatabaseWorkMeter,
            expectedCount: Int = 0,
            stage: DatabaseWorkStage = .indexScan
        ) throws {
            self.storage = try DatabaseRetainedArrayBuilder(
                workMeter: workMeter,
                stage: stage,
                layout: try DatabaseRetainedArrayLayout.forElement(Tuple.self),
                expectedCount: expectedCount
            )
            self.stage = stage
        }

        /// The tuple's element Array is already owned by the candidate batch.
        /// Tuple construction therefore retains that storage without making a
        /// second identifier collection. The retained output admission is
        /// performed before the tuple enters the destination buffer.
        mutating func append(
            elements: [any TupleElement]
        ) throws {
            try append(Tuple(elements))
        }

        mutating func append(
            _ identifier: consuming Tuple
        ) throws {
            let admission = try storage.prepareAppend(
                footprint: DatabaseIntermediateFootprint(
                    rows: 1,
                    bytes: UInt64(identifier.packedByteCount)
                ),
                at: stage
            )
            storage.append(
                consume identifier,
                using: consume admission
            )
        }

        consuming func finish() throws -> FullTextRetainedKeys {
            try FullTextRetainedKeys(
                storage: storage.finish().moveToSharedOwnership(at: stage)
            )
        }
    }

    package var count: Int { storage.count }

    package var workMeter: DatabaseWorkMeter { storage.workMeter }

    package func withRetainedPrimaryKey<Failure: Error>(
        at position: Int,
        _ body: (borrowing Tuple) throws(Failure) -> Void
    ) throws(Failure) {
        try storage.withElement(at: position, body)
    }

    package func withRetainedPrimaryKey<Failure: Error>(
        at position: Int,
        _ body: (borrowing Tuple) async throws(Failure) -> Void
    ) async throws(Failure) {
        try await storage.withElement(at: position, body)
    }

    func packedIdentifier(at position: Int) -> ByteString {
        storage[position].pack()
    }

    func prefix(limit: Int?) -> some DatabaseRetainedPrimaryKeyCollection {
        let count = min(max(limit ?? count, 0), count)
        return FullTextRetainedKeySlice(
            storage: storage.boundedView(0..<count)
        )
    }
}

/// A retained full-text match owner that keeps the score next to the key
/// until the destination rows have been admitted.
struct FullTextRetainedScoredKeys: DatabaseRetainedPrimaryKeyCollection,
    Sendable {
    struct Match: Sendable {
        let identifier: Tuple
        let score: Double
    }

    private let storage: DatabaseSharedRetainedArray<Match>

    init(
        matches: consuming [(id: Tuple, score: Double)],
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .indexScan
    ) throws {
        var builder = try DatabaseRetainedArrayBuilder<Match>(
            workMeter: workMeter,
            stage: stage,
            layout: try DatabaseRetainedArrayLayout.forElement(Match.self),
            expectedCount: matches.count
        )
        for match in matches {
            try builder.append(
                footprint: DatabaseIntermediateFootprint(
                    rows: 1,
                    bytes: UInt64(match.id.packedByteCount)
                        + UInt64(MemoryLayout<Double>.stride)
                ),
                at: stage
            ) {
                Match(identifier: match.id, score: match.score)
            }
        }
        self.storage = try builder.finish().moveToSharedOwnership(at: stage)
    }

    package var count: Int { storage.count }

    package var workMeter: DatabaseWorkMeter { storage.workMeter }

    package func withRetainedPrimaryKey<Failure: Error>(
        at position: Int,
        _ body: (borrowing Tuple) throws(Failure) -> Void
    ) throws(Failure) {
        func apply(
            _ match: borrowing FullTextRetainedScoredKeys.Match
        ) throws(Failure) {
            try body(match.identifier)
        }
        try storage.withElement(at: position, apply)
    }

    package func withRetainedPrimaryKey<Failure: Error>(
        at position: Int,
        _ body: (borrowing Tuple) async throws(Failure) -> Void
    ) async throws(Failure) {
        func apply(
            _ match: borrowing FullTextRetainedScoredKeys.Match
        ) async throws(Failure) {
            try await body(match.identifier)
        }
        try await storage.withElement(at: position, apply)
    }

    func score(at position: Int) -> Double {
        storage[position].score
    }

    func packedIdentifier(at position: Int) -> ByteString {
        storage[position].identifier.pack()
    }

    func prefix(limit: Int?) -> some DatabaseRetainedPrimaryKeyCollection {
        let count = min(max(limit ?? count, 0), count)
        return FullTextRetainedScoredKeySlice(
            storage: storage.boundedView(0..<count)
        )
    }
}

private struct FullTextRetainedKeySlice:
    DatabaseRetainedPrimaryKeyCollection,
    Sendable {
    private let storage: DatabaseSharedRetainedArrayView<Tuple>

    init(storage: DatabaseSharedRetainedArrayView<Tuple>) {
        self.storage = storage
    }

    package var count: Int { storage.count }
    package var workMeter: DatabaseWorkMeter { storage.workMeter }

    package func withRetainedPrimaryKey<Failure: Error>(
        at position: Int,
        _ body: (borrowing Tuple) throws(Failure) -> Void
    ) throws(Failure) {
        try storage.withElement(at: position, body)
    }

    package func withRetainedPrimaryKey<Failure: Error>(
        at position: Int,
        _ body: (borrowing Tuple) async throws(Failure) -> Void
    ) async throws(Failure) {
        try await storage.withElement(at: position, body)
    }
}

private struct FullTextRetainedScoredKeySlice:
    DatabaseRetainedPrimaryKeyCollection,
    Sendable {
    private let storage: DatabaseSharedRetainedArrayView<FullTextRetainedScoredKeys.Match>

    init(storage: DatabaseSharedRetainedArrayView<FullTextRetainedScoredKeys.Match>) {
        self.storage = storage
    }

    package var count: Int { storage.count }
    package var workMeter: DatabaseWorkMeter { storage.workMeter }

    package func withRetainedPrimaryKey<Failure: Error>(
        at position: Int,
        _ body: (borrowing Tuple) throws(Failure) -> Void
    ) throws(Failure) {
        func apply(
            _ match: borrowing FullTextRetainedScoredKeys.Match
        ) throws(Failure) {
            try body(match.identifier)
        }
        try storage.withElement(at: position, apply)
    }

    package func withRetainedPrimaryKey<Failure: Error>(
        at position: Int,
        _ body: (borrowing Tuple) async throws(Failure) -> Void
    ) async throws(Failure) {
        func apply(
            _ match: borrowing FullTextRetainedScoredKeys.Match
        ) async throws(Failure) {
            try await body(match.identifier)
        }
        try await storage.withElement(at: position, apply)
    }
}
