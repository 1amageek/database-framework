import StorageKit

/// Database-wide source of truth for the immutable physical storage format.
public struct DatabaseFormatCatalog: Sendable {
    private static let descriptorKey = Tuple([
        "_database-framework",
        "format"
    ]).pack()

    private let database: any StorageEngine

    public init(database: any StorageEngine) {
        self.database = database
    }

    /// Installs a descriptor only in an empty database, or validates it exactly.
    package func installIfEmptyOrValidate(
        _ expected: DatabaseFormatDescriptor
    ) async throws -> DatabaseFormatDescriptor {
        try await database.withTransaction(configuration: .default) {
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

            let existing = try await transaction.collectRange(
                from: .firstGreaterOrEqual(Bytes()),
                to: .firstGreaterOrEqual([0xFF]),
                limit: 1,
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
        try await database.withTransaction(configuration: .default) {
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
