import DatabaseEngine
import DatabaseTypes

/// Immutable version entries whose physical payload remains charged to the
/// request until every entry and its byte owners are released.
///
/// The owner exposes entries only through scoped borrows. It never exposes the
/// mutable producer array or a copyable history element, so the payload cannot
/// cross this boundary without an explicit destination operation.
final class VersionRetainedHistory: Sendable {

    /// One versionstamp and its persisted snapshot payload.
    struct Entry: Sendable {
        private let version: Version
        private let data: ByteString

        init(version: Version, data: ByteString) {
            self.version = version
            self.data = data
        }

        func withValues<Failure: Error>(
            _ body: (
                borrowing Version,
                borrowing ByteString
            ) throws(Failure) -> Void
        ) throws(Failure) {
            try body(version, data)
            withExtendedLifetime(version) {
                withExtendedLifetime(data) {}
            }
        }
    }

    private let storage: DatabaseSharedRetainedArray<Entry>

    init(storage: DatabaseSharedRetainedArray<Entry>) {
        self.storage = storage
    }

    static func empty(
        workMeter: DatabaseWorkMeter
    ) throws -> VersionRetainedHistory {
        let builder = try DatabaseRetainedArrayBuilder<Entry>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try DatabaseRetainedArrayLayout.forElement(Entry.self)
        )
        return VersionRetainedHistory(
            storage: try builder.finish().moveToSharedOwnership(
                at: .indexScan
            )
        )
    }

    var count: Int { storage.count }

    func withEntry<Failure: Error>(
        at index: Int,
        _ body: (borrowing Entry) throws(Failure) -> Void
    ) throws(Failure) {
        try storage.withElement(at: index, body)
    }
}
