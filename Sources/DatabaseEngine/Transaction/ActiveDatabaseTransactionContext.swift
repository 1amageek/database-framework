import DatabaseKit
import StorageKit

package struct DatabaseTransactionExecutionBinding: Sendable {
    package let transaction: any TransactionAccess
    #if DATABASE_MULTIPLE_BASES
    package let resource: Security.Resource
    package let authorization: AuthorizationContext
    package let grantedAccess: Security.Access
    #endif
    package let databaseTransaction: DatabaseTransaction?
}

package enum ActiveDatabaseTransactionContext {
    @TaskLocal package static var binding: DatabaseTransactionExecutionBinding?
}
