import Core
import StorageKit

/// Typed record reader that does not expose save or delete capabilities.
public struct DatabaseReadTransactionRecords: Sendable {
    private let records: DatabaseTransactionRecords

    package init(
        container: DBContainer,
        transaction: any Transaction
    ) {
        self.records = DatabaseTransactionRecords(
            container: container,
            transaction: transaction
        )
    }

    public func get<Record: Persistable>(
        _ type: Record.Type,
        id: Record.ID
    ) async throws -> Record? {
        try await records.get(type, id: id)
    }

    public func get<Record: Persistable>(
        _ type: Record.Type,
        id: Record.ID,
        directoryPath: DirectoryPath<Record>
    ) async throws -> Record? {
        try await records.get(
            type,
            id: id,
            directoryPath: directoryPath
        )
    }

    public func scan<Record: Persistable>(
        _ type: Record.Type,
        directoryPath: DirectoryPath<Record> = DirectoryPath<Record>(),
        after continuation: Bytes? = nil,
        limit: Int
    ) async throws -> DatabaseRecordPage<Record> {
        try await records.scan(
            type,
            directoryPath: directoryPath,
            after: continuation,
            limit: limit
        )
    }
}
