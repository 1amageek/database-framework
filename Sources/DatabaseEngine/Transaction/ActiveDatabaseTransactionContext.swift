import DatabaseKit
import StorageKit

package struct DatabaseTransactionExecutionBinding: Sendable {
    package let transaction: any TransactionAccess
    package let container: DBContainer
    package let schemaLease: DatabaseSchemaLease
    package let storageDomainIdentity: ObjectIdentifier
    /// Root Subspace of the bound Tenant Partition. This value identifies the
    /// Partition an admitted operation was bound to; it is never used to derive
    /// stored content.
    package let partitionRoot: Subspace
    package let partitionGeneration: UInt64
    /// `system/database-framework` of the bound Tenant Partition.
    package let systemRoot: Subspace
    /// `data` of the bound Tenant Partition.
    package let dataRoot: Subspace
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
        let lease = try context.requireOperationDataRoot()
        self.storageDomainIdentity = ObjectIdentifier(
            lease.domain.engine.transactionDomain
        )
        self.partitionRoot = lease.partitionRoot
        self.partitionGeneration = lease.generation
        self.systemRoot = lease.systemRoot
        self.dataRoot = lease.dataDirectory.root
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
        self.storageDomainIdentity = ObjectIdentifier(
            context.container.engine.transactionDomain
        )
        self.partitionRoot = context.container.defaultTenant.partitionRoot
        self.partitionGeneration = 0
        self.systemRoot = context.container.defaultTenant.systemRoot
        self.dataRoot = context.container.defaultTenant.data.root
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
        self.storageDomainIdentity = binding.storageDomainIdentity
        self.partitionRoot = binding.partitionRoot
        self.partitionGeneration = binding.partitionGeneration
        self.systemRoot = binding.systemRoot
        self.dataRoot = binding.dataRoot
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
        guard storageDomainIdentity == ObjectIdentifier(
                  currentRoot.domain.engine.transactionDomain
              ),
              partitionRoot == currentRoot.partitionRoot,
              partitionGeneration == currentRoot.generation
        else {
            throw DatabaseTransactionError.invalidOperationContext
        }
        #else
        guard storageDomainIdentity == ObjectIdentifier(
                  context.container.engine.transactionDomain
              ),
              partitionRoot == context.container.defaultTenant.partitionRoot,
              partitionGeneration == 0 else {
            throw DatabaseTransactionError.invalidOperationContext
        }
        #endif
    }
}

package enum ActiveDatabaseTransactionContext {
    @TaskLocal package static var binding: DatabaseTransactionExecutionBinding?
}
