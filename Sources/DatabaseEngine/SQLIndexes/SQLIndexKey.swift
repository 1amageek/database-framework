import DatabaseKit

struct SQLIndexKey: Sendable {
    let values: DatabaseSharedRetainedArray<CanonicalSQLValueIdentity>
    let hasNull: Bool
    let leases: DatabaseSharedRetainedArray<DatabaseQueryPayloadLease>

    func equal(to other: SQLIndexKey, workMeter: DatabaseWorkMeter) throws -> Bool {
        try SQLConstraintRows.equal(values, other.values, workMeter: workMeter)
    }
}
