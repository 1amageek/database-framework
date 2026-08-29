import DatabaseTypes
import StorageKit

/// Database-wide source of truth for the immutable physical storage format.
public struct DatabaseFormatCatalog: Sendable {
    private let transactionExecutor: StorageTransactionExecutor
    private let clock: any StorageMonotonicClock
    private let root: Subspace
    private let descriptorKey: ByteString

    /// Opens the format catalog for one ordinary database.
    public init(
        database: any StorageEngine,
        clock: any StorageMonotonicClock
    ) {
        self.init(database: database, root: Subspace(), clock: clock)
    }

    /// Opens a format catalog below an explicitly selected database root.
    public init(
        database: any StorageEngine,
        root: Subspace,
        clock: any StorageMonotonicClock
    ) {
        self.transactionExecutor = StorageTransactionExecutor(engine: database)
        self.clock = clock
        self.root = root
        self.descriptorKey = root.pack(Tuple("format"))
    }

    /// Installs a descriptor only in an empty database, or validates it exactly.
    package func installIfEmptyOrValidate(
        _ expected: DatabaseFormatDescriptor
    ) async throws -> DatabaseFormatDescriptor {
        try await installIfEmptyOrValidate(expected) { _ in }
    }

    /// Installs database-wide metadata in the same transaction as the initial
    /// physical format. This is the only safe bootstrap point: a crash cannot
    /// leave a formatted database without its mandatory security root.
    package func installIfEmptyOrValidate(
        _ expected: DatabaseFormatDescriptor,
        initializeEmptyDatabase: @Sendable @escaping (
            any TransactionAccess
        ) async throws -> Void
    ) async throws -> DatabaseFormatDescriptor {
        try await transactionExecutor.withTransaction(
            configuration: .default,
            clock: clock
        ) {
            transaction in
            if let bytes = try await transaction.getValue(
                for: descriptorKey,
                snapshot: false
            ) {
                let stored = try DatabaseFormatDescriptor.deserialize(bytes)
                guard stored == expected else {
                    throw DatabaseFormatCatalogError.descriptorMismatch(
                        stored: stored,
                        expected: expected
                    )
                }
                return stored
            }

            let range = root.range()
            let existing = try await TransactionRangeCollection.collect(using: transaction,
                from: .firstGreaterOrEqual(range.begin),
                to: .firstGreaterOrEqual(range.end),
                limit: 1,
                reverse: false,
                snapshot: false,
                streamingMode: .small
            )
            guard existing.isEmpty else {
                throw DatabaseFormatCatalogError
                    .descriptorMissingInNonEmptyDatabase
            }

            try await initializeEmptyDatabase(transaction)
            try transaction.setValue(
                expected.serialize(),
                for: descriptorKey
            )
            return expected
        }
    }

    /// Loads the persisted descriptor without assuming or installing a default.
    public func loadRequired() async throws -> DatabaseFormatDescriptor {
        try await transactionExecutor.withTransaction(
            configuration: .default,
            clock: clock
        ) {
            transaction in
            guard let bytes = try await transaction.getValue(
                for: descriptorKey,
                snapshot: true
            ) else {
                throw DatabaseFormatCatalogError.missingDescriptor
            }
            return try DatabaseFormatDescriptor.deserialize(bytes)
        }
    }
}
