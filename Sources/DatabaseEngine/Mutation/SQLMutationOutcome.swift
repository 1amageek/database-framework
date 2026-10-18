import DatabaseKit

/// Collecting statement output; SQLBlock alone owns public publication.
struct SQLMutationOutcome: Sendable {
    let effects: DatabaseSharedRetainedArray<DatabaseSQLMutationOutcome>
    let affectedRows: UInt64
    let returning: SQLBlockCollectedRows?
}
