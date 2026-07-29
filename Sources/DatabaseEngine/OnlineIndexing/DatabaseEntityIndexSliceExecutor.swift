import DatabaseTypes
import DatabaseKit
import StorageKit

enum DatabaseEntityIndexSliceExecutor {
    struct Result: Sendable {
        let processed: UInt64
        let lastProcessedKey: ByteString?
        let hasMore: Bool
    }

    static func run(
        for persistableType: any Persistable.Type,
        container: DBContainer,
        storeSubspace: Subspace,
        index: Index,
        lastProcessedKey: ByteString?,
        maximumWorkUnits: Int,
        transaction: any TransactionAccess
    ) async throws -> Result {
        func execute<T: Persistable>(_: T.Type) async throws -> Result {
            let maintainer: any IndexMaintainer<T> = try container.runtimeConfiguration
                .indexMaintainerProviders.makeIndexMaintainer(
                    index: index,
                    subspace: storeSubspace
                        .subspace(SubspaceKey.indexes)
                        .subspace(index.subspaceKey),
                    idExpression: FieldKeyExpression(fieldName: "id"),
                    configurations: container.indexConfigurations[index.name] ?? []
                )
            let uniquenessMaintainer: (any IndexUniquenessMaintainer<T>)?
            if index.isUnique {
                uniquenessMaintainer = try container.runtimeConfiguration
                    .indexMaintainerProviders.makeIndexUniquenessMaintainer(
                        index: index,
                        subspace: storeSubspace
                            .subspace(SubspaceKey.indexes)
                            .subspace(index.subspaceKey),
                        idExpression: FieldKeyExpression(fieldName: "id"),
                        configurations: container.indexConfigurations[index.name] ?? []
                    )
            } else {
                uniquenessMaintainer = nil
            }
            let itemTypeSubspace = storeSubspace
                .subspace(SubspaceKey.items)
                .subspace(T.persistableType)
            let range = itemTypeSubspace.range()
            let begin = lastProcessedKey.map { $0.appending(0) } ?? range.begin
            let storage = container.itemStorageFactory.make(
                transaction: transaction,
                blobsSubspace: storeSubspace.subspace(SubspaceKey.blobs)
            )
            let sequence = storage.scan(
                begin: begin,
                end: range.end,
                snapshot: false,
                limit: maximumWorkUnits + 1
            )
            var batch: [(item: T, id: Tuple)] = []
            batch.reserveCapacity(maximumWorkUnits)
            var lastKey: ByteString?
            var hasMore = false

            for try await (key, data) in sequence {
                if batch.count == maximumWorkUnits {
                    hasMore = true
                    break
                }
                let item: T = try DataAccess.deserialize(data)
                batch.append((item: item, id: try itemTypeSubspace.unpack(key)))
                lastKey = key
            }
            try await OnlineIndexBatchWriter.write(
                batch,
                index: index,
                maintainer: maintainer,
                uniquenessMaintainer: uniquenessMaintainer,
                violationTracker: UniquenessViolationTracker(
                    container: container,
                    metadataSubspace: storeSubspace.subspace(SubspaceKey.metadata)
                ),
                transaction: transaction
            )
            return Result(
                processed: UInt64(batch.count),
                lastProcessedKey: lastKey,
                hasMore: hasMore
            )
        }

        return try await _openExistential(persistableType, do: execute)
    }
}
