import DatabaseKit

/// Typed persistence mutations scoped to one database transaction.
public protocol DatabaseTransactionWriting: DatabaseTransactionReading {
    func save<Model: Persistable>(
        _ model: Model,
        precondition: WritePrecondition
    ) async throws

    func delete<Model: Persistable>(
        _ model: Model,
        precondition: WritePrecondition
    ) async throws
}

public extension DatabaseTransactionWriting {
    func save<Model: Persistable>(_ model: Model) async throws {
        try await save(model, precondition: .none)
    }

    func delete<Model: Persistable>(_ model: Model) async throws {
        try await delete(model, precondition: .exists)
    }
}
