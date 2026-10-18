import DatabaseKit

/// Immutable target/source capture retained until the complete SQL statement ends.
struct SQLMutationPreparation: Sendable {
    let executor: DatabaseEntityStatementMutationExecutor
    let input: QueryStatement
    let scopeIdentity: ObjectIdentifier
    let transactionIdentity: ObjectIdentifier
    let mutationOwner: UInt64
    let target: TableRef
    let relation: SQLRelation
    let models: DatabaseRetainedPersistedModels
    let targetLayout: CanonicalSourceLayout
    let source: DataSource?
    let sourceReadQuery: SelectQuery?
    let sourceLayout: CanonicalSourceLayout?
    let returningPlan: CanonicalBoundSelect?
    let returningReadFields: (current: Set<String>, old: Set<String>, new: Set<String>)
    var returningColumns: [QueryColumn]? { returningPlan?.columns }
    let readClaims: DatabaseSharedRetainedArray<DatabaseQueryPayloadLease>
}
