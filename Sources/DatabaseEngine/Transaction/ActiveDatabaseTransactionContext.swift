import DatabaseKit
import StorageKit

package struct DatabaseTransactionExecutionBinding: Sendable {
    package let identity: DatabaseTransactionExecutionIdentity
    package let transaction: any TransactionAccess
    package let grantedAccess: Security.Access
    package let accessMode: DatabaseTransactionAccessMode
    package let operationScope: DatabaseReadScopeGate
    #if DATABASE_MULTI_BASE
    package let resource: Security.Resource
    package let authorization: AuthorizationContext
    #endif
    package let databaseTransaction: DatabaseTransaction?
}

package enum ActiveDatabaseTransactionContext {
    @TaskLocal package static var binding: DatabaseTransactionExecutionBinding?
}
