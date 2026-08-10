import DatabaseTypes
import StorageKit

/// Owns the v2 Base-layout marker in the control domain and probes the
/// pre-Base database-wide format without mutating legacy data.
package struct DatabaseLayoutCatalog: Sendable {
    private static let formatVersion: UInt8 = 2

    private let transactionExecutor: StorageTransactionExecutor
    private let clock: any StorageMonotonicClock
    private let markerKey: ByteString
    private let legacyFormatKey: ByteString

    package init(
        engine: any StorageEngine,
        controlRoot: Subspace,
        clock: any StorageMonotonicClock
    ) {
        self.transactionExecutor = StorageTransactionExecutor(engine: engine)
        self.clock = clock
        self.markerKey = controlRoot
            .subspace("_metadata")
            .pack(Tuple("base-layout"))
        self.legacyFormatKey = Subspace()
            .subspace("_database-framework")
            .pack(Tuple("format"))
    }

    package func load() async throws -> DatabaseLayoutStatus? {
        try await transactionExecutor.withTransaction(
            configuration: .readOnly,
            clock: clock
        ) { transaction in
            try await load(transaction: transaction)
        }
    }

    package func load(
        transaction: any TransactionAccess
    ) async throws -> DatabaseLayoutStatus? {
        guard let value = try await transaction.getValue(
            for: markerKey,
            snapshot: false
        ) else {
            return nil
        }
        guard value.count == 2,
              value[0] == Self.formatVersion,
              let status = DatabaseLayoutStatus(rawValue: value[1]) else {
            throw DatabaseRuntimeError.internalError(
                "Invalid Base layout marker"
            )
        }
        return status
    }

    package func legacyFormat() async throws -> DatabaseFormatDescriptor? {
        try await transactionExecutor.withTransaction(
            configuration: .readOnly,
            clock: clock
        ) { transaction in
            guard let value = try await transaction.getValue(
                for: legacyFormatKey,
                snapshot: true
            ) else {
                return nil
            }
            return try DatabaseFormatDescriptor.deserialize(value)
        }
    }

    package func storeInitial(
        _ status: DatabaseLayoutStatus,
        transaction: any TransactionAccess
    ) async throws {
        if let existing = try await load(transaction: transaction) {
            guard existing == status else {
                throw DatabaseRuntimeError.internalError(
                    "Base layout state changed during initialization"
                )
            }
            return
        }
        try transaction.setValue(
            ByteString([Self.formatVersion, status.rawValue]),
            for: markerKey
        )
    }

    package func markCurrent(
        transaction: any TransactionAccess
    ) async throws {
        guard try await load(transaction: transaction) == .migrationRequired else {
            throw DatabaseRuntimeError.internalError(
                "Legacy layout cutover requires migrationRequired state"
            )
        }
        try transaction.setValue(
            ByteString([Self.formatVersion, DatabaseLayoutStatus.current.rawValue]),
            for: markerKey
        )
    }
}
