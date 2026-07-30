import DatabaseTypes
import StorageKit

/// Database-wide source of truth for the immutable physical storage format.
public struct DatabaseFormatCatalog: Sendable {
    private static let descriptorKey = Tuple([
        "_database-framework",
        "format"
    ]).pack()

    private let transactionExecutor: StorageTransactionExecutor
    private let clock: any StorageMonotonicClock

    public init(
        database: any StorageEngine,
        clock: any StorageMonotonicClock
    ) {
        self.transactionExecutor = StorageTransactionExecutor(engine: database)
        self.clock = clock
    }

    /// Installs a descriptor only in an empty database, or validates it exactly.
    package func installIfEmptyOrValidate(
        _ expected: DatabaseFormatDescriptor
    ) async throws -> DatabaseFormatDescriptor {
        try await transactionExecutor.withTransaction(
            configuration: .default,
            clock: clock
        ) {
            transaction in
            if let bytes = try await transaction.getValue(
                for: Self.descriptorKey,
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

            let existing = try await TransactionRangeCollection.collect(using: transaction,
                from: .firstGreaterOrEqual(ByteString()),
                to: .firstGreaterOrEqual([0xFF]),
                limit: 1,
                reverse: false,
                snapshot: false,
                streamingMode: .small
            )
            guard existing.isEmpty else {
                throw DatabaseFormatCatalogError
                    .descriptorMissingInNonEmptyDatabase
            }

            try transaction.setValue(
                expected.serialize(),
                for: Self.descriptorKey
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
                for: Self.descriptorKey,
                snapshot: true
            ) else {
                throw DatabaseFormatCatalogError.missingDescriptor
            }
            return try DatabaseFormatDescriptor.deserialize(bytes)
        }
    }
}
