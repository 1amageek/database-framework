import DatabaseKit
import StorageKit

package struct DatabaseTransactionExecutionBinding: Sendable {
    package let transaction: any TransactionAccess
    package let container: DBContainer
    package let schemaLease: DatabaseSchemaLease
    package let storageEngineIdentity: ObjectIdentifier
    package let dataRoot: Subspace
    package let dataRootGeneration: UInt64
    package let authorization: AuthorizationContext
    #if DATABASE_MULTI_BASE
    package let resource: Security.Resource
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
        self.authorization = context.authorization
        self.databaseTransaction = databaseTransaction
    }
    #endif

    /// Narrows the ambient transaction to read authority while preserving the
    /// exact schema generation, data root, and authorization admission that
    /// were already validated by the owning execution scope.
    func admittingRead(
        _ transaction: ReadAuthorizedTransactionAccess
    ) -> DatabaseTransactionExecutionBinding {
        DatabaseTransactionExecutionBinding(
            reading: self,
            transaction: transaction
        )
    }

    private init(
        reading binding: DatabaseTransactionExecutionBinding,
        transaction: ReadAuthorizedTransactionAccess
    ) {
        self.transaction = transaction
        self.container = binding.container
        self.schemaLease = binding.schemaLease
        self.storageEngineIdentity = binding.storageEngineIdentity
        self.dataRoot = binding.dataRoot
        self.dataRootGeneration = binding.dataRootGeneration
        self.authorization = binding.authorization
        #if DATABASE_MULTI_BASE
        self.resource = binding.resource
        self.grantedAccess = .read
        #endif
        self.databaseTransaction = nil
    }

    package func validate(for context: DatabaseContext) throws {
        guard container === context.container,
              authorization == context.authorization
        else {
            throw DatabaseTransactionError.invalidOperationContext
        }
        // A read-authorized binding owns the schema generation admitted at
        // session creation. Requiring it to equal the container's later active
        // generation would revoke an operation that was already admitted.
        if !(transaction is ReadAuthorizedTransactionAccess) {
            guard schemaLease === context.container.acquireActiveSchemaLease()
            else {
                throw DatabaseTransactionError.invalidOperationContext
            }
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
