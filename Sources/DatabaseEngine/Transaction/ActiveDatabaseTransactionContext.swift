import DatabaseKit
import StorageKit

package struct DatabaseTransactionExecutionBinding: Sendable {
    package let transaction: any TransactionAccess
    package let baseID: Base.ID
    package let authorization: AuthorizationContext
    package let grantedAccess: Security.Access
    package let databaseTransaction: DatabaseTransaction?
}

package enum ActiveDatabaseTransactionContext {
    @TaskLocal package static var binding: DatabaseTransactionExecutionBinding?
}
