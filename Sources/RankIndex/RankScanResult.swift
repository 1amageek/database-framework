import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Rank metadata for one retained ordered entry.
///
/// The primary key is deliberately absent. Consumers that need the key must
/// use `RankScanResult.withRetainedPrimaryKey`, which keeps the key borrow
/// scoped to the operation that consumes it.
struct RankScanAnnotation: Sendable {
    let scoreElement: any TupleElement
    let rank: Int
}

/// One retained ordered rank entry.
///
/// The primary-key tuple is retained in this storage-only representation and
/// is never exposed through the annotation API.
struct RankScanEntry: Sendable {
    let scoreElement: any TupleElement
    let primaryKey: DatabaseRetainedPrimaryKey
    let rank: Int

    /// Keeps the detached key owner alive for byte-backed tuple elements and
    /// for the reservation that accounts for decoding the complete key.
    /// Consumers cannot access this storage except through the scoped primary
    /// key borrow below.
    private let retainedSourceKey: ByteString

    init(
        scoreElement: any TupleElement,
        primaryKey: DatabaseRetainedPrimaryKey,
        rank: Int,
        retainedSourceKey: ByteString
    ) {
        self.scoreElement = scoreElement
        self.primaryKey = primaryKey
        self.rank = rank
        self.retainedSourceKey = retainedSourceKey
    }
}

/// Request-owned ordered rank entries and their primary-key view.
///
/// The shared backing array keeps the decoded primary keys, score elements,
/// ranks, and their reservation alive together while DatabaseEngine consumes
/// the primary-key view. The owner is released after destination materialization
/// or when the operation fails.
final class RankScanResult:
    DatabaseRetainedPrimaryKeyCollection,
    Sendable
{
    private let entries: DatabaseSharedRetainedArray<RankScanEntry>

    init(buffer: consuming DatabaseRetainedBuffer<RankScanEntry>) throws {
        self.entries = try buffer.moveToSharedOwnership(at: .indexScan)
    }

    package var count: Int { entries.count }
    package var workMeter: DatabaseWorkMeter { entries.workMeter }

    func withAnnotation<Failure: Error>(
        at position: Int,
        _ body: (borrowing RankScanAnnotation) throws(Failure) -> Void
    ) throws(Failure) {
        func apply(
            _ entry: borrowing RankScanEntry
        ) throws(Failure) {
            let annotation = RankScanAnnotation(
                scoreElement: entry.scoreElement,
                rank: entry.rank
            )
            try body(annotation)
        }
        try entries.withElement(at: position, apply)
    }

    package func withRetainedPrimaryKey<Failure: Error>(
        at position: Int,
        _ body: (borrowing Tuple) throws(Failure) -> Void
    ) throws(Failure) {
        func apply(
            _ entry: borrowing RankScanEntry
        ) throws(Failure) {
            try entry.primaryKey.withValue(body)
        }
        try entries.withElement(at: position, apply)
    }

    package func withRetainedPrimaryKey<Failure: Error>(
        at position: Int,
        _ body: (borrowing Tuple) async throws(Failure) -> Void
    ) async throws(Failure) {
        func apply(
            _ entry: borrowing RankScanEntry
        ) async throws(Failure) {
            try await entry.primaryKey.withValue(body)
        }
        try await entries.withElement(at: position, apply)
    }
}
