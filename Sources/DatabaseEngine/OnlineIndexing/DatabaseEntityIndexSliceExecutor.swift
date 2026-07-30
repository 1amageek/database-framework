import DatabaseTypes
import DatabaseKit
import StorageKit

enum DatabaseEntityIndexSliceExecutor {
    static func run(
        for runtime: EntityRuntimeRegistration,
        container: DBContainer,
        storeSubspace: Subspace,
        index: Index,
        lastProcessedKey: ByteString?,
        maximumWorkUnits: Int,
        transaction: any TransactionAccess
    ) async throws -> EntityIndexSliceResult {
        try await runtime.runIndexSlice(
            container: container,
            storeSubspace: storeSubspace,
            index: index,
            lastProcessedKey: lastProcessedKey,
            maximumWorkUnits: maximumWorkUnits,
            transaction: transaction
        )
    }
}
