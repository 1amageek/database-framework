#if SQLITE
import Foundation
import Testing
import Database
import TestHeartbeat

@Persistable
private struct DatabaseFrameworkE2EAccount {
    #Directory<DatabaseFrameworkE2EAccount>("database-framework-e2e", "accounts")
    #Index(
        ScalarIndexKind<DatabaseFrameworkE2EAccount>(fields: [\.email]),
        name: "database_framework_e2e_account_email"
    )

    var id: String = UUID().uuidString
    var email: String = ""
    var age: Int = 0
}

@Persistable
private struct DatabaseFrameworkE2EOrder {
    #Directory<DatabaseFrameworkE2EOrder>("database-framework-e2e", "orders")

    var id: String = UUID().uuidString
    var tenantID: String = ""
    var status: String = ""
    var total: Double = 0
}

@Persistable(type: "DatabaseFrameworkE2EMigratedAccount")
private struct DatabaseFrameworkE2EMigratedAccountV1 {
    #Directory<DatabaseFrameworkE2EMigratedAccountV1>("database-framework-e2e", "migrated-accounts")

    var id: String = UUID().uuidString
    var name: String
    var email: String
}

@Persistable(type: "DatabaseFrameworkE2EMigratedAccount")
private struct DatabaseFrameworkE2EMigratedAccountV2 {
    #Directory<DatabaseFrameworkE2EMigratedAccountV2>("database-framework-e2e", "migrated-accounts")
    #Index(
        ScalarIndexKind<DatabaseFrameworkE2EMigratedAccountV2>(fields: [\.fullName]),
        name: "database_framework_e2e_migrated_account_full_name"
    )

    var id: String = UUID().uuidString
    var fullName: String
    var email: String
    var age: Int = 0
}

private enum DatabaseFrameworkE2EMigrationSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [DatabaseFrameworkE2EMigratedAccountV1.self]
}

private enum DatabaseFrameworkE2EMigrationSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [DatabaseFrameworkE2EMigratedAccountV2.self]
}

private enum DatabaseFrameworkE2EMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [DatabaseFrameworkE2EMigrationSchemaV1.self, DatabaseFrameworkE2EMigrationSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .custom(
                fromVersion: DatabaseFrameworkE2EMigrationSchemaV1.self,
                toVersion: DatabaseFrameworkE2EMigrationSchemaV2.self,
                willMigrate: migrateAccounts,
                didMigrate: nil
            )
        ]
    }

    static func migrateAccounts(context: MigrationContext) async throws {
        var migrated: [DatabaseFrameworkE2EMigratedAccountV2] = []

        for try await legacyAccount in context.enumerate(DatabaseFrameworkE2EMigratedAccountV1.self) {
            var account = DatabaseFrameworkE2EMigratedAccountV2(
                fullName: legacyAccount.name,
                email: legacyAccount.email
            )
            account.id = legacyAccount.id
            migrated.append(account)
        }

        guard !migrated.isEmpty else {
            return
        }

        try await context.batchUpdate(migrated, batchSize: 100)
    }
}

private enum DatabaseFrameworkE2ETransactionError: Error {
    case expectedRollback
}

@Suite("DatabaseFramework E2E Tests", .serialized, .heartbeat)
struct DatabaseFrameworkE2ETests {
    private func order(id: String, tenantID: String, status: String, total: Double) -> DatabaseFrameworkE2EOrder {
        var order = DatabaseFrameworkE2EOrder(tenantID: tenantID, status: status, total: total)
        order.id = id
        return order
    }

    @Test("file-backed SQLite container persists indexed records across container reopen")
    func fileBackedSQLiteContainerPersistsIndexedRecordsAcrossContainerReopen() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-framework-e2e-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer {
            do {
                try FileManager.default.removeItem(at: directory)
            } catch {
                Issue.record("Failed to clean up temporary E2E directory: \(error)")
            }
        }

        let databasePath = directory.appendingPathComponent("database-framework.sqlite").path
        let schema = Schema([DatabaseFrameworkE2EAccount.self], version: .init(1, 0, 0))

        let writer = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let writeContext = writer.newContext()

        var alice = DatabaseFrameworkE2EAccount(email: "alice@example.com", age: 31)
        alice.id = "database-framework-e2e-alice"
        writeContext.insert(alice)

        var bob = DatabaseFrameworkE2EAccount(email: "bob@example.com", age: 17)
        bob.id = "database-framework-e2e-bob"
        writeContext.insert(bob)

        try await writeContext.save()

        let reader = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let readContext = reader.newContext()
        let adults = try await readContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.age >= 18)
            .orderBy(\.email)
            .execute()

        #expect(adults.map(\.id) == ["database-framework-e2e-alice"])
        #expect(adults.first?.email == "alice@example.com")

        readContext.delete(alice)
        try await readContext.save()

        let verifier = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let remaining = try await verifier.newContext()
            .fetch(DatabaseFrameworkE2EAccount.self)
            .orderBy(\.email)
            .execute()

        #expect(remaining.map(\.id) == ["database-framework-e2e-bob"])
    }

    @Test("SQLite scalar index removes old entries when indexed field is replaced")
    func sqliteScalarIndexRemovesOldEntriesWhenIndexedFieldIsReplaced() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-framework-index-update-e2e-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer {
            do {
                try FileManager.default.removeItem(at: directory)
            } catch {
                Issue.record("Failed to clean up temporary E2E directory: \(error)")
            }
        }

        let databasePath = directory.appendingPathComponent("database-framework.sqlite").path
        let schema = Schema([DatabaseFrameworkE2EAccount.self], version: .init(1, 0, 0))
        let container = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let context = container.newContext()

        var original = DatabaseFrameworkE2EAccount(email: "old@example.com", age: 31)
        original.id = "database-framework-indexed-account"
        context.insert(original)
        try await context.save()

        let beforeReplace = try await context.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "old@example.com")
            .execute()
        #expect(beforeReplace.map(\.id) == ["database-framework-indexed-account"])

        var updated = original
        updated.email = "new@example.com"
        updated.age = 32
        context.replace(old: original, with: updated)
        try await context.save()

        let oldEmailResults = try await context.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "old@example.com")
            .execute()
        let newEmailResults = try await context.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "new@example.com")
            .execute()

        #expect(oldEmailResults.isEmpty)
        #expect(newEmailResults.map(\.id) == ["database-framework-indexed-account"])
        #expect(newEmailResults.first?.age == 32)

        context.delete(updated)
        try await context.save()

        let afterDelete = try await context.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "new@example.com")
            .execute()
        #expect(afterDelete.isEmpty)
    }

    @Test("SQLite failed replace leaves no partial writes and same-context retry succeeds")
    func sqliteFailedReplaceLeavesNoPartialWritesAndSameContextRetrySucceeds() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-framework-replace-retry-e2e-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer {
            do {
                try FileManager.default.removeItem(at: directory)
            } catch {
                Issue.record("Failed to clean up temporary E2E directory: \(error)")
            }
        }

        let databasePath = directory.appendingPathComponent("database-framework.sqlite").path
        let schema = Schema([DatabaseFrameworkE2EAccount.self], version: .init(1, 0, 0))
        let container = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let retryingContext = container.newContext()

        var original = DatabaseFrameworkE2EAccount(email: "before@example.com", age: 29)
        original.id = "database-framework-retry-account"
        var updated = original
        updated.email = "after@example.com"
        updated.age = 30

        retryingContext.replace(old: original, with: updated)

        do {
            try await retryingContext.save()
            Issue.record("Expected replace on missing row to fail")
        } catch let error as FDBContextError {
            if case .preconditionFailed(let typeName, let idDescription, let precondition, _) = error {
                #expect(typeName == DatabaseFrameworkE2EAccount.persistableType)
                #expect(idDescription == "database-framework-retry-account")
                #expect(precondition == .exists)
            } else {
                Issue.record("Unexpected context error: \(error)")
            }
        }

        let afterFailureContainer = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let afterFailureContext = afterFailureContainer.newContext()

        let pendingView = try await retryingContext.model(
            for: "database-framework-retry-account",
            as: DatabaseFrameworkE2EAccount.self
        )
        let persistedBeforeSeed = try await afterFailureContext.model(
            for: "database-framework-retry-account",
            as: DatabaseFrameworkE2EAccount.self
        )
        let leakedNewEmail = try await afterFailureContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "after@example.com")
            .execute()

        #expect(pendingView?.email == "after@example.com")
        #expect(persistedBeforeSeed == nil)
        #expect(leakedNewEmail.isEmpty)

        let seedingContext = container.newContext()
        seedingContext.insert(original)
        try await seedingContext.save()

        try await retryingContext.save()

        let verificationContainer = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let verificationContext = verificationContainer.newContext()
        let stored = try await verificationContext.model(
            for: "database-framework-retry-account",
            as: DatabaseFrameworkE2EAccount.self
        )
        let oldEmailResults = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "before@example.com")
            .execute()
        let newEmailResults = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "after@example.com")
            .execute()

        #expect(stored?.email == "after@example.com")
        #expect(stored?.age == 30)
        #expect(oldEmailResults.isEmpty)
        #expect(newEmailResults.map(\.id) == ["database-framework-retry-account"])
    }

    @Test("SQLite failed multi-change save restores all pending mutations for retry")
    func sqliteFailedMultiChangeSaveRestoresAllPendingMutationsForRetry() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-framework-multi-retry-e2e-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer {
            do {
                try FileManager.default.removeItem(at: directory)
            } catch {
                Issue.record("Failed to clean up temporary E2E directory: \(error)")
            }
        }

        let databasePath = directory.appendingPathComponent("database-framework.sqlite").path
        let schema = Schema([DatabaseFrameworkE2EAccount.self], version: .init(1, 0, 0))
        let container = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let retryingContext = container.newContext()

        var created = DatabaseFrameworkE2EAccount(email: "created@example.com", age: 21)
        created.id = "database-framework-created-account"
        var missingOriginal = DatabaseFrameworkE2EAccount(email: "legacy-before@example.com", age: 44)
        missingOriginal.id = "database-framework-missing-account"
        var missingUpdated = missingOriginal
        missingUpdated.email = "legacy-after@example.com"
        missingUpdated.age = 45

        retryingContext.create(created)
        retryingContext.replace(old: missingOriginal, with: missingUpdated)

        do {
            try await retryingContext.save()
            Issue.record("Expected multi-change save to fail")
        } catch let error as FDBContextError {
            if case .preconditionFailed(let typeName, let idDescription, let precondition, _) = error {
                #expect(typeName == DatabaseFrameworkE2EAccount.persistableType)
                #expect(idDescription == "database-framework-missing-account")
                #expect(precondition == .exists)
            } else {
                Issue.record("Unexpected context error: \(error)")
            }
        }

        let afterFailureContainer = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let afterFailureContext = afterFailureContainer.newContext()

        let pendingCreated = try await retryingContext.model(
            for: created.id,
            as: DatabaseFrameworkE2EAccount.self
        )
        let pendingUpdated = try await retryingContext.model(
            for: missingUpdated.id,
            as: DatabaseFrameworkE2EAccount.self
        )
        let persistedCreated = try await afterFailureContext.model(
            for: created.id,
            as: DatabaseFrameworkE2EAccount.self
        )
        let persistedUpdated = try await afterFailureContext.model(
            for: missingUpdated.id,
            as: DatabaseFrameworkE2EAccount.self
        )

        #expect(pendingCreated?.email == "created@example.com")
        #expect(pendingUpdated?.email == "legacy-after@example.com")
        #expect(persistedCreated == nil)
        #expect(persistedUpdated == nil)

        let seedingContext = container.newContext()
        seedingContext.insert(missingOriginal)
        try await seedingContext.save()

        try await retryingContext.save()

        let verificationContainer = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let verificationContext = verificationContainer.newContext()
        let storedCreated = try await verificationContext.model(
            for: created.id,
            as: DatabaseFrameworkE2EAccount.self
        )
        let storedUpdated = try await verificationContext.model(
            for: missingUpdated.id,
            as: DatabaseFrameworkE2EAccount.self
        )
        let oldUpdatedEmailResults = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "legacy-before@example.com")
            .execute()
        let newUpdatedEmailResults = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "legacy-after@example.com")
            .execute()

        #expect(storedCreated?.email == "created@example.com")
        #expect(storedUpdated?.email == "legacy-after@example.com")
        #expect(storedUpdated?.age == 45)
        #expect(oldUpdatedEmailResults.isEmpty)
        #expect(newUpdatedEmailResults.map(\.id) == ["database-framework-missing-account"])
    }

    @Test("SQLite transaction rollback discards staged writes and index entries")
    func sqliteTransactionRollbackDiscardsStagedWritesAndIndexEntries() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-framework-transaction-rollback-e2e-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer {
            do {
                try FileManager.default.removeItem(at: directory)
            } catch {
                Issue.record("Failed to clean up temporary E2E directory: \(error)")
            }
        }

        let databasePath = directory.appendingPathComponent("database-framework.sqlite").path
        let schema = Schema([DatabaseFrameworkE2EAccount.self], version: .init(1, 0, 0))
        let container = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let context = container.newContext()

        do {
            try await context.withTransaction { tx in
                var alice = DatabaseFrameworkE2EAccount(email: "rollback-alice@example.com", age: 31)
                alice.id = "database-framework-rollback-alice"
                var bob = DatabaseFrameworkE2EAccount(email: "rollback-bob@example.com", age: 18)
                bob.id = "database-framework-rollback-bob"

                try await tx.set(alice)
                try await tx.set(bob)

                throw DatabaseFrameworkE2ETransactionError.expectedRollback
            }
            Issue.record("Expected transaction to throw")
        } catch let error as DatabaseFrameworkE2ETransactionError {
            #expect(error == .expectedRollback)
        }

        let storedAccounts = try await context.fetch(DatabaseFrameworkE2EAccount.self)
            .orderBy(\.email)
            .execute()
        let aliceIndexHits = try await context.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "rollback-alice@example.com")
            .execute()
        let bobIndexHits = try await context.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "rollback-bob@example.com")
            .execute()

        #expect(storedAccounts.isEmpty)
        #expect(aliceIndexHits.isEmpty)
        #expect(bobIndexHits.isEmpty)
    }

    @Test("SQLite container filters, sorts, pages, updates, and deletes within tenant scoped queries")
    func sqliteContainerHandlesComplexTenantScopedWorkflow() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-framework-partition-e2e-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer {
            do {
                try FileManager.default.removeItem(at: directory)
            } catch {
                Issue.record("Failed to clean up temporary E2E directory: \(error)")
            }
        }

        let databasePath = directory.appendingPathComponent("database-framework.sqlite").path
        let schema = Schema([DatabaseFrameworkE2EOrder.self], version: .init(1, 0, 0))
        let container = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let context = container.newContext()

        let tenantA = "tenant-a"
        let tenantB = "tenant-b"
        let orders = [
            order(id: "order-a-1", tenantID: tenantA, status: "open", total: 40),
            order(id: "order-a-2", tenantID: tenantA, status: "open", total: 95),
            order(id: "order-a-3", tenantID: tenantA, status: "closed", total: 125),
            order(id: "order-a-4", tenantID: tenantA, status: "open", total: 70),
            order(id: "order-b-1", tenantID: tenantB, status: "open", total: 250),
        ]
        for order in orders {
            context.insert(order)
        }
        try await context.save()

        let topOpenOrders = try await context.fetch(DatabaseFrameworkE2EOrder.self)
            .partition(\.tenantID, equals: tenantA)
            .where(\.status == "open")
            .orderBy(\.total, .descending)
            .limit(2)
            .execute()

        #expect(topOpenOrders.map(\.id) == ["order-a-2", "order-a-4"])
        #expect(topOpenOrders.allSatisfy { $0.tenantID == tenantA && $0.status == "open" })

        let skippedOrder = try await context.fetch(DatabaseFrameworkE2EOrder.self)
            .partition(\.tenantID, equals: tenantA)
            .where(\.status == "open")
            .orderBy(\.total, .descending)
            .offset(2)
            .limit(1)
            .execute()

        #expect(skippedOrder.map(\.id) == ["order-a-1"])

        let updated = order(id: "order-a-1", tenantID: tenantA, status: "closed", total: 150)
        context.insert(updated)
        try await context.save()

        let openCountAfterUpdate = try await context.fetch(DatabaseFrameworkE2EOrder.self)
            .partition(\.tenantID, equals: tenantA)
            .where(\.status == "open")
            .count()
        #expect(openCountAfterUpdate == 2)

        let tenantBOpen = try await context.fetch(DatabaseFrameworkE2EOrder.self)
            .partition(\.tenantID, equals: tenantB)
            .where(\.status == "open")
            .execute()
        #expect(tenantBOpen.map(\.id) == ["order-b-1"])

        context.delete(updated)
        try await context.save()

        let tenantAAfterDelete = try await context.fetch(DatabaseFrameworkE2EOrder.self)
            .partition(\.tenantID, equals: tenantA)
            .orderBy(\.id)
            .execute()
        #expect(tenantAAfterDelete.map(\.id) == ["order-a-2", "order-a-3", "order-a-4"])
    }

    @Test("SQLite migration rewrites legacy records and serves them through the new indexed schema")
    func sqliteMigrationRewritesLegacyRecordsAndServesNewIndexedSchema() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        let initialContainer = try await DBContainer(
            for: DatabaseFrameworkE2EMigrationSchemaV1.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let initialContext = initialContainer.newContext()

        var alice = DatabaseFrameworkE2EMigratedAccountV1(
            name: "Alice Jones",
            email: "alice@example.com"
        )
        alice.id = "database-framework-migrated-alice"
        var bob = DatabaseFrameworkE2EMigratedAccountV1(
            name: "Bob Stone",
            email: "bob@example.com"
        )
        bob.id = "database-framework-migrated-bob"

        initialContext.insert(alice)
        initialContext.insert(bob)
        try await initialContext.save()
        try await initialContainer.setCurrentSchemaVersion(Schema.Version(1, 0, 0))

        let migratedContainer = try await DBContainer(
            for: DatabaseFrameworkE2EMigrationSchemaV2.self,
            migrationPlan: DatabaseFrameworkE2EMigrationPlan.self,
            configuration: .init(backend: .custom(engine))
        )
        try await migratedContainer.migrateIfNeeded()
        let migratedVersion = try await migratedContainer.getCurrentSchemaVersion()

        let verificationContainer = try await DBContainer(
            for: DatabaseFrameworkE2EMigrationSchemaV2.makeSchema(),
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let verificationContext = verificationContainer.newContext()

        let migratedAlice = try await verificationContext.fetch(DatabaseFrameworkE2EMigratedAccountV2.self)
            .where(\.fullName == "Alice Jones")
            .execute()
        let allMigrated = try await verificationContext.fetch(DatabaseFrameworkE2EMigratedAccountV2.self)
            .orderBy(\.fullName)
            .execute()

        #expect(migratedVersion == Schema.Version(2, 0, 0))
        #expect(migratedAlice.map(\.id) == ["database-framework-migrated-alice"])
        #expect(migratedAlice.first?.email == "alice@example.com")
        #expect(migratedAlice.first?.age == 0)
        #expect(allMigrated.map(\.fullName) == ["Alice Jones", "Bob Stone"])
    }
}
#endif
