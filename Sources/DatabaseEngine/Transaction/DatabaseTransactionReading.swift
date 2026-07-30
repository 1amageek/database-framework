import DatabaseKit

/// Typed persistence reads scoped to one database transaction.
public protocol DatabaseTransactionReading: Sendable {
    func fetch<Model: Persistable>(
        _ type: Model.Type,
        identifiedBy id: Model.ID,
        consistency: DatabaseReadConsistency
    ) async throws -> Model?

    func fetch<Model: Persistable>(
        _ type: Model.Type,
        identifiedBy id: Model.ID,
        in partition: DirectoryPath<Model>,
        consistency: DatabaseReadConsistency
    ) async throws -> Model?

    func scan<Model: Persistable>(
        _ type: Model.Type,
        in partition: DirectoryPath<Model>,
        after continuation: DatabaseScanContinuation?,
        limit: Int,
        consistency: DatabaseReadConsistency
    ) async throws -> sending DatabaseScanPage<Model>
}

public extension DatabaseTransactionReading {
    func fetch<Model: Persistable>(
        _ type: Model.Type,
        identifiedBy id: Model.ID
    ) async throws -> Model? {
        try await fetch(
            type,
            identifiedBy: id,
            consistency: .serializable
        )
    }

    func fetch<Model: Persistable>(
        _ type: Model.Type,
        identifiedBy id: Model.ID,
        in partition: DirectoryPath<Model>
    ) async throws -> Model? {
        try await fetch(
            type,
            identifiedBy: id,
            in: partition,
            consistency: .serializable
        )
    }

    func scan<Model: Persistable>(
        _ type: Model.Type,
        after continuation: DatabaseScanContinuation? = nil,
        limit: Int
    ) async throws -> sending DatabaseScanPage<Model> {
        try await scan(
            type,
            in: DirectoryPath<Model>(),
            after: continuation,
            limit: limit,
            consistency: .serializable
        )
    }

    func scan<Model: Persistable>(
        _ type: Model.Type,
        in partition: DirectoryPath<Model>,
        after continuation: DatabaseScanContinuation? = nil,
        limit: Int
    ) async throws -> sending DatabaseScanPage<Model> {
        try await scan(
            type,
            in: partition,
            after: continuation,
            limit: limit,
            consistency: .serializable
        )
    }
}
