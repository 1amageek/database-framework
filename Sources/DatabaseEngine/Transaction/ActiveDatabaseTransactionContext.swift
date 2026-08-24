import DatabaseKit
import StorageKit

package struct DatabaseTransactionExecutionBinding: Sendable {
    package let transaction: any TransactionAccess
    package let container: DBContainer
    package let schemaLease: DatabaseSchemaLease
    package let storageEngineIdentity: ObjectIdentifier
    package let dataRoot: Subspace
    package let dataRootGeneration: UInt64
    #if DATABASE_MULTI_BASE
    package let resource: Security.Resource
    package let authorization: AuthorizationContext
    package let grantedAccess: Security.Access
    #endif
    package let databaseTransaction: DatabaseTransaction?

    #if DATABASE_MULTI_BASE
    package init(
        context: DatabaseContext,
        transaction: any TransactionAccess,
        grantedAccess: Security.Access,
        databaseTransaction: DatabaseTransaction?
    ) throws {
        self.transaction = transaction
        self.container = context.container
        self.schemaLease = context.container.acquireActiveSchemaLease()
        let dataRoot = try context.requireOperationDataRoot()
        self.storageEngineIdentity = ObjectIdentifier(dataRoot.domain.engine)
        self.dataRoot = dataRoot.root
        self.dataRootGeneration = dataRoot.generation
        self.resource = context.resource
        self.authorization = context.authorization
        self.grantedAccess = grantedAccess
        self.databaseTransaction = databaseTransaction
    }
    #else
    package init(
        context: DatabaseContext,
        transaction: any TransactionAccess,
        databaseTransaction: DatabaseTransaction?
    ) throws {
        self.transaction = transaction
        self.container = context.container
        self.schemaLease = context.container.acquireActiveSchemaLease()
        self.storageEngineIdentity = ObjectIdentifier(context.container.engine)
        self.dataRoot = context.container.databaseRoot
        self.dataRootGeneration = 0
        self.databaseTransaction = databaseTransaction
    }
    #endif

    package func validate(for context: DatabaseContext) throws {
        guard container === context.container,
              schemaLease === context.container.acquireActiveSchemaLease()
        else {
            throw DatabaseTransactionError.invalidOperationContext
        }
        #if DATABASE_MULTI_BASE
        guard resource == context.resource else {
            throw DatabaseGrantAuthorizationError.resourceMismatch(
                expected: resource,
                actual: context.resource
            )
        }
        let currentRoot = try context.requireOperationDataRoot()
        guard storageEngineIdentity == ObjectIdentifier(currentRoot.domain.engine),
              dataRoot == currentRoot.root,
              dataRootGeneration == currentRoot.generation
        else {
            throw DatabaseTransactionError.invalidOperationContext
        }
        #else
        guard storageEngineIdentity == ObjectIdentifier(context.container.engine),
              dataRoot == context.container.databaseRoot,
              dataRootGeneration == 0 else {
            throw DatabaseTransactionError.invalidOperationContext
        }
        #endif
    }
}

package enum ActiveDatabaseTransactionContext {
    @TaskLocal package static var binding: DatabaseTransactionExecutionBinding?
}
