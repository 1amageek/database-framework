import DatabaseKit
import StorageKit

extension SQLReadResolution {
    /// The storage snapshot must still represent the leased physical row contract.
    func validatePhysicalSource(container: DBContainer,
        transaction: any TransactionAccess) async throws {
        #if DATABASE_MULTI_BASE
        guard transaction.transactionDomain === container.storageTopology.controlDomain.engine.transactionDomain else { return }
        #endif
        try workMeter.consume(at: .validation)
        let actual = try await DBContainer.loadActiveSchemaFingerprint(
            metadataSubspace: container.metadataSubspace, transaction: transaction)
        guard actual == snapshot.physicalSchemaFingerprint else {
            let generation = try await DBContainer.loadPersistedSchemaGeneration(
                metadataSubspace: container.metadataSubspace, transaction: transaction) ?? 0
            throw DatabaseSchemaPublicationError.generationConflict(
                expected: policy.schemaGeneration, actual: generation)
        }
    }
}
