import Core
import StorageKit

/// Typed record access bound to the transaction supplied by an operation handler.
public struct DatabaseTransactionRecords: Sendable {
    private let container: DBContainer
    private let transaction: any Transaction
    private let persistence: any ModelPersistenceHandler

    public init(
        container: DBContainer,
        transaction: any Transaction
    ) {
        self.container = container
        self.transaction = transaction
        self.persistence = container.newContext().makePersistenceHandler()
    }

    public func get<Record: Persistable>(
        _ type: Record.Type,
        id: Record.ID
    ) async throws -> Record? {
        try await get(
            type,
            id: id,
            directoryPath: DirectoryPath<Record>()
        )
    }

    public func get<Record: Persistable>(
        _ type: Record.Type,
        id: Record.ID,
        directoryPath: DirectoryPath<Record>
    ) async throws -> Record? {
        let subspaces = try await resolveSubspaces(
            for: type,
            directoryPath: directoryPath
        )
        let typeSubspace = subspaces.items.subspace(Record.persistableType)
        let identifier = try RecordIdentifierKeyCodec.tuple(for: id)
        let storage = self.container.itemStorageFactory.make(
            transaction: transaction,
            blobsSubspace: subspaces.blobs
        )
        guard let bytes = try await storage.read(
            for: typeSubspace.pack(identifier),
            snapshot: false
        ) else {
            return nil
        }
        return try DataAccess.deserialize(bytes)
    }

    public func scan<Record: Persistable>(
        _ type: Record.Type,
        directoryPath: DirectoryPath<Record> = DirectoryPath<Record>(),
        after continuation: Bytes? = nil,
        limit: Int
    ) async throws -> DatabaseRecordPage<Record> {
        guard limit > 0, limit < Int.max else {
            throw DatabaseTransactionRecordsError.invalidLimit(limit)
        }

        let subspaces = try await resolveSubspaces(
            for: type,
            directoryPath: directoryPath
        )
        let typeSubspace = subspaces.items.subspace(Record.persistableType)
        let (begin, end) = typeSubspace.range()
        let beginSelector: KeySelector
        if let continuation {
            let precedesBegin = continuation.lexicographicallyPrecedes(begin)
            let precedesEnd = continuation.lexicographicallyPrecedes(end)
            guard !precedesBegin, precedesEnd else {
                throw DatabaseTransactionRecordsError.continuationOutsideRecordRange
            }
            beginSelector = .firstGreaterThan(continuation)
        } else {
            beginSelector = .firstGreaterOrEqual(begin)
        }

        var entries = try await transaction.collectRange(
            from: beginSelector,
            to: .firstGreaterOrEqual(end),
            limit: limit + 1,
            reverse: false,
            snapshot: false,
            streamingMode: .iterator
        )
        let hasMore = entries.count > limit
        if hasMore {
            entries.removeLast(entries.count - limit)
        }

        let storage = self.container.itemStorageFactory.make(
            transaction: transaction,
            blobsSubspace: subspaces.blobs
        )
        var records: [Record] = []
        records.reserveCapacity(entries.count)
        for entry in entries {
            guard let bytes = try await storage.read(
                for: entry.0,
                snapshot: false
            ) else {
                throw DatabaseTransactionRecordsError.recordDisappeared(entry.0)
            }
            let record: Record = try DataAccess.deserialize(bytes)
            records.append(record)
        }

        return DatabaseRecordPage(
            records: records,
            continuation: hasMore ? entries.last?.0 : nil
        )
    }

    public func save<Record: Persistable>(
        _ record: Record,
        precondition: WritePrecondition = .none
    ) async throws {
        try await persistence.save(
            record,
            precondition: precondition,
            transaction: transaction
        )
    }

    public func delete<Record: Persistable>(
        _ record: Record,
        precondition: WritePrecondition = .exists
    ) async throws {
        try await persistence.delete(
            record,
            precondition: precondition,
            transaction: transaction
        )
    }

    private func resolveSubspaces<Record: Persistable>(
        for type: Record.Type,
        directoryPath: DirectoryPath<Record>
    ) async throws -> (items: Subspace, blobs: Subspace) {
        let root = try await container.resolveDirectory(
            for: type,
            path: try AnyDirectoryPath(directoryPath),
            transaction: transaction
        )
        return (
            items: root.subspace(SubspaceKey.items),
            blobs: root.subspace(SubspaceKey.blobs)
        )
    }
}
