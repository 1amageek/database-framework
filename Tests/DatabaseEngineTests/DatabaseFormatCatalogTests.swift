import StorageKit
import StorageKitSystemClock
import Testing
@testable import DatabaseEngine

@Suite("Database Format Catalog Tests")
struct DatabaseFormatCatalogTests {
    @Test("Empty database installs v1 and exact reopen succeeds")
    func installsAndReopens() async throws {
        let engine = InMemoryEngine()
        let catalog = DatabaseFormatCatalog(
            database: engine,
            clock: SystemStorageClock()
        )
        let expected = DatabaseFormatDescriptor.v1(itemStorage: .v1)

        let installed = try await catalog.installIfEmptyOrValidate(expected)
        let reopened = try await catalog.installIfEmptyOrValidate(expected)
        let loaded = try await catalog.loadRequired()

        #expect(installed == expected)
        #expect(reopened == expected)
        #expect(loaded == expected)
    }

    @Test("Missing descriptor in a nonempty database fails without writing")
    func rejectsNonemptyDatabaseWithoutDescriptor() async throws {
        let engine = InMemoryEngine()
        try await engine.withTransaction { transaction in
            try transaction.setValue([0x01], for: [0x10])
        }
        let catalog = DatabaseFormatCatalog(
            database: engine,
            clock: SystemStorageClock()
        )

        await #expect(
            throws: DatabaseFormatCatalogError
                .descriptorMissingInNonEmptyDatabase
        ) {
            _ = try await catalog.installIfEmptyOrValidate(
                .v1(itemStorage: .v1)
            )
        }
        await #expect(throws: DatabaseFormatCatalogError.missingDescriptor) {
            _ = try await catalog.loadRequired()
        }
    }

    @Test("A different physical configuration is rejected")
    func rejectsConfigurationMismatch() async throws {
        let engine = InMemoryEngine()
        let catalog = DatabaseFormatCatalog(
            database: engine,
            clock: SystemStorageClock()
        )
        let stored = DatabaseFormatDescriptor.v1(itemStorage: .v1)
        _ = try await catalog.installIfEmptyOrValidate(stored)
        let differentConfiguration = try ItemStorageConfiguration(
            encoding: .identity,
            maximumPlainByteCount: 64 * 1_024 * 1_024,
            maximumStoredByteCount: 64 * 1_024 * 1_024,
            maximumInlineByteCount: 80_000,
            chunkByteCount: 80_000
        )
        let expected = DatabaseFormatDescriptor.v1(
            itemStorage: differentConfiguration
        )

        await #expect(
            throws: DatabaseFormatCatalogError.descriptorMismatch(
                stored: stored,
                expected: expected
            )
        ) {
            _ = try await catalog.installIfEmptyOrValidate(expected)
        }
    }

    @Test("Corrupted persisted descriptor is never replaced")
    func rejectsCorruptedDescriptor() async throws {
        let engine = InMemoryEngine()
        let descriptorKey = Tuple([
            "_database-framework",
            "format"
        ]).pack()
        try await engine.withTransaction { transaction in
            try transaction.setValue([0x00], for: descriptorKey)
        }
        let catalog = DatabaseFormatCatalog(
            database: engine,
            clock: SystemStorageClock()
        )

        await #expect(throws: DatabaseFormatDescriptorError.self) {
            _ = try await catalog.installIfEmptyOrValidate(
                .v1(itemStorage: .v1)
            )
        }
        let persisted = try await engine.withTransaction { transaction in
            try await transaction.getValue(for: descriptorKey, snapshot: true)
        }
        #expect(persisted == [0x00])
    }
}
