import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire
import StorageKit

/// Serializes accepted schema transitions in the control transaction domain.
package struct DatabaseSchemaApplicationStore: Sendable {
    private let applications: Subspace
    private let activeKey: ByteString

    package init(metadataSubspace: Subspace) {
        let root = metadataSubspace
            .subspace("schema")
            .subspace("applications")
        self.applications = root.subspace("records")
        self.activeKey = root.pack(Tuple("active"))
    }

    package func load(
        idempotencyKey: String,
        transaction: any TransactionAccess
    ) async throws -> DatabaseSchemaApplicationRecord? {
        try await load(
            key: applications.pack(Tuple(idempotencyKey)),
            transaction: transaction
        )
    }

    package func loadActive(
        transaction: any TransactionAccess
    ) async throws -> DatabaseSchemaApplicationRecord? {
        try await load(key: activeKey, transaction: transaction)
    }

    package func requireNoActiveTransition(
        transaction: any TransactionAccess
    ) async throws {
        if let active = try await loadActive(transaction: transaction) {
            throw DatabaseSchemaPublicationError.transitionInProgress(
                active.job
            )
        }
    }

    package func insert(
        _ record: DatabaseSchemaApplicationRecord,
        transaction: any TransactionAccess
    ) async throws {
        guard !record.idempotencyKey.isEmpty else {
            throw DatabaseSchemaPublicationError.invalidIdempotencyKey
        }
        if let existing = try await load(
            idempotencyKey: record.idempotencyKey,
            transaction: transaction
        ) {
            guard existing.expectedFingerprint == record.expectedFingerprint,
                  existing.targetFingerprint == record.targetFingerprint else {
                throw DatabaseSchemaPublicationError.idempotencyKeyReused(
                    record.idempotencyKey
                )
            }
            guard existing.job == record.job else {
                throw DatabaseSchemaPublicationError.corruptedState(
                    "schema application identity changed"
                )
            }
            return
        }
        if let active = try await loadActive(transaction: transaction) {
            throw DatabaseSchemaPublicationError.transitionInProgress(active.job)
        }
        let encoded = try StorageFrameCodec.encode(record)
        try transaction.setValue(
            encoded,
            for: applications.pack(Tuple(record.idempotencyKey))
        )
        try transaction.setValue(encoded, for: activeKey)
    }

    package func finish(
        job: JobIdentity,
        transaction: any TransactionAccess
    ) async throws {
        guard let active = try await loadActive(transaction: transaction) else {
            return
        }
        guard active.job == job else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "active schema transition is owned by another job"
            )
        }
        try transaction.clear(key: activeKey)
    }

    private func load(
        key: ByteString,
        transaction: any TransactionAccess
    ) async throws -> DatabaseSchemaApplicationRecord? {
        guard let bytes = try await transaction.getValue(
            for: key,
            snapshot: false
        ) else {
            return nil
        }
        do {
            return try StorageFrameCodec.decode(
                DatabaseSchemaApplicationRecord.self,
                from: bytes
            )
        } catch {
            throw DatabaseSchemaPublicationError.corruptedState(
                "schema application record cannot be decoded"
            )
        }
    }
}
