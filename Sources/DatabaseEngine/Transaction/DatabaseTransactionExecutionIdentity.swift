import StorageKit

/// Immutable database ownership retained by one storage transaction attempt.
///
/// Storage transaction reuse is valid only while every database execution
/// coordinate still identifies the operation that admitted the transaction.
package struct DatabaseTransactionExecutionIdentity: Sendable {
    package let container: DBContainer
    package let schemaGeneration: UInt64
    package let dataRoot: Subspace
    package let dataRootGeneration: UInt64
    package let storageDomainIdentifier: String

    package init(context: DatabaseContext) throws {
        let storage = try context.executionStorage()
        self.container = context.container
        self.schemaGeneration = context.container.schemaGeneration
        self.dataRoot = storage.root
        self.dataRootGeneration = storage.generation
        self.storageDomainIdentifier = storage.domainIdentifier
    }

    package func validateReuse(by context: DatabaseContext) throws {
        guard container === context.container else {
            throw DatabaseTransactionExecutionScopeError.containerMismatch
        }
        guard schemaGeneration == context.container.schemaGeneration else {
            throw DatabaseTransactionExecutionScopeError.schemaGenerationMismatch
        }
        let storage = try context.executionStorage()
        guard dataRoot == storage.root,
              dataRootGeneration == storage.generation else {
            throw DatabaseTransactionExecutionScopeError.dataRootMismatch
        }
        guard storageDomainIdentifier == storage.domainIdentifier else {
            throw DatabaseTransactionExecutionScopeError.storageDomainMismatch
        }
    }
}
