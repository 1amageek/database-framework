#if SQLITE
import Foundation
import Testing
import Database
import StorageKit
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

@Persistable
private struct DatabaseFrameworkE2ETenantAccount {
    #Directory<DatabaseFrameworkE2ETenantAccount>(
        "database-framework-e2e",
        Field<DatabaseFrameworkE2ETenantAccount>(\.tenantID),
        "tenant-accounts",
        layer: .partition
    )
    #Index(
        ScalarIndexKind<DatabaseFrameworkE2ETenantAccount>(fields: [\.email]),
        name: "database_framework_e2e_tenant_account_email"
    )

    var id: String = UUID().uuidString
    var tenantID: String = ""
    var email: String = ""
    var status: String = ""
}

@Persistable
private struct DatabaseFrameworkE2ELargeDocument {
    #Directory<DatabaseFrameworkE2ELargeDocument>("database-framework-e2e", "large-documents")
    #Index(
        ScalarIndexKind<DatabaseFrameworkE2ELargeDocument>(fields: [\.title]),
        name: "database_framework_e2e_large_document_title"
    )

    var id: String = UUID().uuidString
    var title: String = ""
    var body: String = ""
}

@Persistable
private struct DatabaseFrameworkE2ESecuredDocument: SecurityPolicy {
    #Directory<DatabaseFrameworkE2ESecuredDocument>("database-framework-e2e", "secured-documents")

    var id: String = UUID().uuidString
    var ownerID: String = ""
    var title: String = ""

    static func allowGet(
        resource: DatabaseFrameworkE2ESecuredDocument,
        auth: (any AuthContext)?
    ) -> Bool {
        resource.ownerID == auth?.userID
    }

    static func allowList(
        query: SecurityQuery<DatabaseFrameworkE2ESecuredDocument>,
        auth: (any AuthContext)?
    ) -> Bool {
        auth != nil
    }

    static func allowCreate(
        newResource: DatabaseFrameworkE2ESecuredDocument,
        auth: (any AuthContext)?
    ) -> Bool {
        newResource.ownerID == auth?.userID
    }

    static func allowUpdate(
        resource: DatabaseFrameworkE2ESecuredDocument,
        newResource: DatabaseFrameworkE2ESecuredDocument,
        auth: (any AuthContext)?
    ) -> Bool {
        resource.ownerID == auth?.userID
    }

    static func allowDelete(
        resource: DatabaseFrameworkE2ESecuredDocument,
        auth: (any AuthContext)?
    ) -> Bool {
        resource.ownerID == auth?.userID
    }
}

private struct DatabaseFrameworkE2EAuth: AuthContext {
    let userID: String
    var roles: Set<String> = []
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

private func databaseFrameworkE2ECountKeys(
    in subspace: Subspace,
    engine: any StorageEngine
) async throws -> Int {
    let range = subspace.range()
    return try await engine.withTransaction { transaction in
        var count = 0
        for _ in try await transaction.collectRange(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            snapshot: true
        ) {
            count += 1
        }
        return count
    }
}

private func databaseFrameworkE2ELargeText(repetitions: Int = 8_000) -> String {
    (0..<repetitions)
        .map { "segment-\($0)-\(UUID().uuidString)" }
        .joined(separator: "|")
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

    @Test("SQLite stale replace keeps scalar index consistent with stored row")
    func sqliteStaleReplaceKeepsScalarIndexConsistentWithStoredRow() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-framework-stale-replace-e2e-\(UUID().uuidString)", isDirectory: true)
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

        var original = DatabaseFrameworkE2EAccount(email: "stale-original@example.com", age: 31)
        original.id = "database-framework-stale-account"
        let seedContext = container.newContext()
        seedContext.insert(original)
        try await seedContext.save()

        let firstContext = container.newContext()
        var firstUpdate = original
        firstUpdate.email = "stale-first@example.com"
        firstUpdate.age = 32
        firstContext.replace(old: original, with: firstUpdate)
        try await firstContext.save()

        let secondContext = container.newContext()
        var secondUpdate = original
        secondUpdate.email = "stale-second@example.com"
        secondUpdate.age = 33
        secondContext.replace(old: original, with: secondUpdate)
        try await secondContext.save()

        let verificationContainer = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let verificationContext = verificationContainer.newContext()

        let stored = try await verificationContext.model(
            for: original.id,
            as: DatabaseFrameworkE2EAccount.self
        )
        let originalEmailHits = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "stale-original@example.com")
            .execute()
        let firstEmailHits = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "stale-first@example.com")
            .execute()
        let secondEmailHits = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "stale-second@example.com")
            .execute()

        #expect(stored?.email == "stale-second@example.com")
        #expect(stored?.age == 33)
        #expect(originalEmailHits.isEmpty)
        #expect(firstEmailHits.isEmpty)
        #expect(secondEmailHits.map(\.id) == [original.id])
    }

    @Test("SQLite stale delete clears the current scalar index entry")
    func sqliteStaleDeleteClearsCurrentScalarIndexEntry() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-framework-stale-delete-e2e-\(UUID().uuidString)", isDirectory: true)
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

        var original = DatabaseFrameworkE2EAccount(email: "delete-original@example.com", age: 31)
        original.id = "database-framework-stale-delete-account"
        let seedContext = container.newContext()
        seedContext.insert(original)
        try await seedContext.save()

        var updated = original
        updated.email = "delete-current@example.com"
        updated.age = 32
        let updateContext = container.newContext()
        updateContext.replace(old: original, with: updated)
        try await updateContext.save()

        let deleteContext = container.newContext()
        deleteContext.delete(original, precondition: .exists)
        try await deleteContext.save()

        let verificationContainer = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let verificationContext = verificationContainer.newContext()

        let stored = try await verificationContext.model(
            for: original.id,
            as: DatabaseFrameworkE2EAccount.self
        )
        let originalEmailHits = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "delete-original@example.com")
            .execute()
        let currentEmailHits = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "delete-current@example.com")
            .execute()

        #expect(stored == nil)
        #expect(originalEmailHits.isEmpty)
        #expect(currentEmailHits.isEmpty)
    }

    @Test("SQLite default stale delete clears the current scalar index entry")
    func sqliteDefaultStaleDeleteClearsCurrentScalarIndexEntry() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-framework-default-stale-delete-e2e-\(UUID().uuidString)", isDirectory: true)
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

        var original = DatabaseFrameworkE2EAccount(email: "default-delete-original@example.com", age: 31)
        original.id = "database-framework-default-stale-delete-account"
        let seedContext = container.newContext()
        seedContext.insert(original)
        try await seedContext.save()

        var updated = original
        updated.email = "default-delete-current@example.com"
        updated.age = 32
        let updateContext = container.newContext()
        updateContext.replace(old: original, with: updated)
        try await updateContext.save()

        let deleteContext = container.newContext()
        deleteContext.delete(original)
        try await deleteContext.save()

        let verificationContainer = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let verificationContext = verificationContainer.newContext()

        let stored = try await verificationContext.model(
            for: original.id,
            as: DatabaseFrameworkE2EAccount.self
        )
        let originalEmailHits = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "default-delete-original@example.com")
            .execute()
        let currentEmailHits = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "default-delete-current@example.com")
            .execute()

        #expect(stored == nil)
        #expect(originalEmailHits.isEmpty)
        #expect(currentEmailHits.isEmpty)
    }

    @Test("SQLite stale delete evaluates security against the stored row")
    func sqliteStaleDeleteEvaluatesSecurityAgainstStoredRow() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-framework-secure-stale-delete-e2e-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer {
            do {
                try FileManager.default.removeItem(at: directory)
            } catch {
                Issue.record("Failed to clean up temporary E2E directory: \(error)")
            }
        }

        let databasePath = directory.appendingPathComponent("database-framework.sqlite").path
        let schema = Schema([DatabaseFrameworkE2ESecuredDocument.self], version: .init(1, 0, 0))
        let container = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .enabled()
        )

        var original = DatabaseFrameworkE2ESecuredDocument(ownerID: "alice", title: "Original")
        original.id = "database-framework-secure-stale-delete-document"
        try await AuthContextKey.$current.withValue(DatabaseFrameworkE2EAuth(userID: "alice")) {
            let createContext = container.newContext()
            createContext.insert(original)
            try await createContext.save()
        }

        var transferred = original
        transferred.ownerID = "bob"
        transferred.title = "Transferred"
        try await AuthContextKey.$current.withValue(DatabaseFrameworkE2EAuth(userID: "alice")) {
            let updateContext = container.newContext()
            updateContext.replace(old: original, with: transferred)
            try await updateContext.save()
        }

        do {
            try await AuthContextKey.$current.withValue(DatabaseFrameworkE2EAuth(userID: "alice")) {
                let deleteContext = container.newContext()
                deleteContext.delete(original)
                try await deleteContext.save()
            }
            Issue.record("Expected stale delete to be denied by current stored owner")
        } catch let error as SecurityError {
            #expect(error.operation == .delete)
            #expect(error.resourceID == original.id)
            #expect(error.userID == "alice")
        }

        let verificationContainer = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let stored = try await verificationContainer.newContext().model(
            for: original.id,
            as: DatabaseFrameworkE2ESecuredDocument.self
        )

        #expect(stored?.ownerID == "bob")
        #expect(stored?.title == "Transferred")
    }

    @Test("SQLite duplicate create preserves stored row and scalar index")
    func sqliteDuplicateCreatePreservesStoredRowAndScalarIndex() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-framework-duplicate-create-e2e-\(UUID().uuidString)", isDirectory: true)
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

        var original = DatabaseFrameworkE2EAccount(email: "create-original@example.com", age: 31)
        original.id = "database-framework-duplicate-create-account"
        let seedContext = container.newContext()
        seedContext.insert(original)
        try await seedContext.save()

        var duplicate = original
        duplicate.email = "create-duplicate@example.com"
        duplicate.age = 32
        let duplicateContext = container.newContext()
        duplicateContext.create(duplicate)

        do {
            try await duplicateContext.save()
            Issue.record("Expected duplicate create to fail")
        } catch let error as FDBContextError {
            if case .preconditionFailed(let typeName, let idDescription, let precondition, _) = error {
                #expect(typeName == DatabaseFrameworkE2EAccount.persistableType)
                #expect(idDescription == original.id)
                #expect(precondition == .notExists)
            } else {
                Issue.record("Unexpected context error: \(error)")
            }
        }

        let verificationContainer = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let verificationContext = verificationContainer.newContext()

        let stored = try await verificationContext.model(
            for: original.id,
            as: DatabaseFrameworkE2EAccount.self
        )
        let originalEmailHits = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "create-original@example.com")
            .execute()
        let duplicateEmailHits = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "create-duplicate@example.com")
            .execute()

        #expect(stored?.email == "create-original@example.com")
        #expect(stored?.age == 31)
        #expect(originalEmailHits.map(\.id) == [original.id])
        #expect(duplicateEmailHits.isEmpty)
    }

    @Test("SQLite blind upsert clears the current scalar index entry")
    func sqliteBlindUpsertClearsCurrentScalarIndexEntry() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-framework-upsert-index-e2e-\(UUID().uuidString)", isDirectory: true)
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

        var original = DatabaseFrameworkE2EAccount(email: "upsert-original@example.com", age: 31)
        original.id = "database-framework-upsert-account"
        let seedContext = container.newContext()
        seedContext.insert(original)
        try await seedContext.save()

        var current = original
        current.email = "upsert-current@example.com"
        current.age = 32
        let updateContext = container.newContext()
        updateContext.replace(old: original, with: current)
        try await updateContext.save()

        var upserted = original
        upserted.email = "upsert-final@example.com"
        upserted.age = 33
        let upsertContext = container.newContext()
        upsertContext.upsert(upserted)
        try await upsertContext.save()

        let verificationContainer = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let verificationContext = verificationContainer.newContext()

        let stored = try await verificationContext.model(
            for: original.id,
            as: DatabaseFrameworkE2EAccount.self
        )
        let originalEmailHits = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "upsert-original@example.com")
            .execute()
        let currentEmailHits = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "upsert-current@example.com")
            .execute()
        let finalEmailHits = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "upsert-final@example.com")
            .execute()

        #expect(stored?.email == "upsert-final@example.com")
        #expect(stored?.age == 33)
        #expect(originalEmailHits.isEmpty)
        #expect(currentEmailHits.isEmpty)
        #expect(finalEmailHits.map(\.id) == [original.id])
    }

    @Test("SQLite dynamic directory keeps tenant indexes isolated across update and delete")
    func sqliteDynamicDirectoryKeepsTenantIndexesIsolatedAcrossUpdateAndDelete() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-framework-dynamic-index-e2e-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer {
            do {
                try FileManager.default.removeItem(at: directory)
            } catch {
                Issue.record("Failed to clean up temporary E2E directory: \(error)")
            }
        }

        let databasePath = directory.appendingPathComponent("database-framework.sqlite").path
        let schema = Schema([DatabaseFrameworkE2ETenantAccount.self], version: .init(1, 0, 0))
        let container = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let context = container.newContext()

        var tenantA = DatabaseFrameworkE2ETenantAccount(
            tenantID: "tenant-a",
            email: "shared@example.com",
            status: "active"
        )
        tenantA.id = "database-framework-tenant-a-account"
        var tenantB = DatabaseFrameworkE2ETenantAccount(
            tenantID: "tenant-b",
            email: "shared@example.com",
            status: "active"
        )
        tenantB.id = "database-framework-tenant-b-account"

        context.insert(tenantA)
        context.insert(tenantB)
        try await context.save()

        var updatedTenantA = tenantA
        updatedTenantA.email = "tenant-a-updated@example.com"
        let updateContext = container.newContext()
        updateContext.replace(old: tenantA, with: updatedTenantA)
        try await updateContext.save()

        let tenantAAfterUpdate = try await container.newContext()
            .fetch(DatabaseFrameworkE2ETenantAccount.self)
            .partition(\.tenantID, equals: "tenant-a")
            .where(\.email == "tenant-a-updated@example.com")
            .execute()
        let tenantAOldEmail = try await container.newContext()
            .fetch(DatabaseFrameworkE2ETenantAccount.self)
            .partition(\.tenantID, equals: "tenant-a")
            .where(\.email == "shared@example.com")
            .execute()
        let tenantBSharedEmail = try await container.newContext()
            .fetch(DatabaseFrameworkE2ETenantAccount.self)
            .partition(\.tenantID, equals: "tenant-b")
            .where(\.email == "shared@example.com")
            .execute()

        #expect(tenantAAfterUpdate.map(\.id) == ["database-framework-tenant-a-account"])
        #expect(tenantAOldEmail.isEmpty)
        #expect(tenantBSharedEmail.map(\.id) == ["database-framework-tenant-b-account"])

        let deleteContext = container.newContext()
        deleteContext.delete(updatedTenantA)
        try await deleteContext.save()

        let tenantAAfterDelete = try await container.newContext()
            .fetch(DatabaseFrameworkE2ETenantAccount.self)
            .partition(\.tenantID, equals: "tenant-a")
            .where(\.email == "tenant-a-updated@example.com")
            .execute()
        let tenantBAfterDelete = try await container.newContext()
            .fetch(DatabaseFrameworkE2ETenantAccount.self)
            .partition(\.tenantID, equals: "tenant-b")
            .where(\.email == "shared@example.com")
            .execute()

        #expect(tenantAAfterDelete.isEmpty)
        #expect(tenantBAfterDelete.map(\.id) == ["database-framework-tenant-b-account"])
    }

    @Test("SQLite dynamic directory replace moves rows and indexes across partitions")
    func sqliteDynamicDirectoryReplaceMovesRowsAndIndexesAcrossPartitions() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-framework-dynamic-move-e2e-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer {
            do {
                try FileManager.default.removeItem(at: directory)
            } catch {
                Issue.record("Failed to clean up temporary E2E directory: \(error)")
            }
        }

        let databasePath = directory.appendingPathComponent("database-framework.sqlite").path
        let schema = Schema([DatabaseFrameworkE2ETenantAccount.self], version: .init(1, 0, 0))
        let container = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )

        var original = DatabaseFrameworkE2ETenantAccount(
            tenantID: "tenant-move-a",
            email: "move-original@example.com",
            status: "active"
        )
        original.id = "database-framework-tenant-move-account"
        let seedContext = container.newContext()
        seedContext.insert(original)
        try await seedContext.save()

        var moved = original
        moved.tenantID = "tenant-move-b"
        moved.email = "move-current@example.com"
        moved.status = "moved"
        let moveContext = container.newContext()
        moveContext.replace(old: original, with: moved)
        try await moveContext.save()

        let verificationContainer = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let oldPartitionAll = try await verificationContainer.newContext()
            .fetch(DatabaseFrameworkE2ETenantAccount.self)
            .partition(\.tenantID, equals: "tenant-move-a")
            .execute()
        let oldPartitionOldEmail = try await verificationContainer.newContext()
            .fetch(DatabaseFrameworkE2ETenantAccount.self)
            .partition(\.tenantID, equals: "tenant-move-a")
            .where(\.email == "move-original@example.com")
            .execute()
        let oldPartitionNewEmail = try await verificationContainer.newContext()
            .fetch(DatabaseFrameworkE2ETenantAccount.self)
            .partition(\.tenantID, equals: "tenant-move-a")
            .where(\.email == "move-current@example.com")
            .execute()
        let newPartitionNewEmail = try await verificationContainer.newContext()
            .fetch(DatabaseFrameworkE2ETenantAccount.self)
            .partition(\.tenantID, equals: "tenant-move-b")
            .where(\.email == "move-current@example.com")
            .execute()

        #expect(oldPartitionAll.isEmpty)
        #expect(oldPartitionOldEmail.isEmpty)
        #expect(oldPartitionNewEmail.isEmpty)
        #expect(newPartitionNewEmail.map(\.id) == [original.id])
        #expect(newPartitionNewEmail.first?.tenantID == "tenant-move-b")
        #expect(newPartitionNewEmail.first?.status == "moved")
    }

    @Test("SQLite large blob indexed update delete and rollback keep blobs and indexes consistent")
    func sqliteLargeBlobIndexedUpdateDeleteAndRollbackKeepBlobsAndIndexesConsistent() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let schema = Schema([DatabaseFrameworkE2ELargeDocument.self], version: .init(1, 0, 0))
        let container = try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        let subspace = try await container.resolveDirectory(for: DatabaseFrameworkE2ELargeDocument.self)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)

        var original = DatabaseFrameworkE2ELargeDocument(
            title: "large-original",
            body: databaseFrameworkE2ELargeText()
        )
        original.id = "database-framework-large-document"
        let seedContext = container.newContext()
        seedContext.insert(original)
        try await seedContext.save()

        let blobCountAfterInsert = try await databaseFrameworkE2ECountKeys(
            in: blobsSubspace,
            engine: engine
        )
        let originalTitleHits = try await container.newContext()
            .fetch(DatabaseFrameworkE2ELargeDocument.self)
            .where(\.title == "large-original")
            .execute()

        #expect(blobCountAfterInsert > 0)
        #expect(originalTitleHits.map(\.id) == [original.id])

        var compact = original
        compact.title = "large-compact"
        compact.body = "small body"
        let updateContext = container.newContext()
        updateContext.replace(old: original, with: compact)
        try await updateContext.save()

        let blobCountAfterCompact = try await databaseFrameworkE2ECountKeys(
            in: blobsSubspace,
            engine: engine
        )
        let oldTitleHits = try await container.newContext()
            .fetch(DatabaseFrameworkE2ELargeDocument.self)
            .where(\.title == "large-original")
            .execute()
        let compactTitleHits = try await container.newContext()
            .fetch(DatabaseFrameworkE2ELargeDocument.self)
            .where(\.title == "large-compact")
            .execute()

        #expect(blobCountAfterCompact == 0)
        #expect(oldTitleHits.isEmpty)
        #expect(compactTitleHits.map(\.id) == [original.id])

        let compactForRollback = compact
        do {
            try await container.newContext().withTransaction { transaction in
                var rolledBack = compactForRollback
                rolledBack.title = "large-rolled-back"
                rolledBack.body = databaseFrameworkE2ELargeText()
                try await transaction.set(rolledBack)
                throw DatabaseFrameworkE2ETransactionError.expectedRollback
            }
            Issue.record("Expected transaction rollback")
        } catch let error as DatabaseFrameworkE2ETransactionError {
            #expect(error == .expectedRollback)
        }

        let blobCountAfterRollback = try await databaseFrameworkE2ECountKeys(
            in: blobsSubspace,
            engine: engine
        )
        let rolledBackTitleHits = try await container.newContext()
            .fetch(DatabaseFrameworkE2ELargeDocument.self)
            .where(\.title == "large-rolled-back")
            .execute()
        let compactAfterRollbackHits = try await container.newContext()
            .fetch(DatabaseFrameworkE2ELargeDocument.self)
            .where(\.title == "large-compact")
            .execute()

        #expect(blobCountAfterRollback == 0)
        #expect(rolledBackTitleHits.isEmpty)
        #expect(compactAfterRollbackHits.map(\.id) == [original.id])

        let deleteContext = container.newContext()
        deleteContext.delete(compact)
        try await deleteContext.save()

        let blobCountAfterDelete = try await databaseFrameworkE2ECountKeys(
            in: blobsSubspace,
            engine: engine
        )
        let compactAfterDeleteHits = try await container.newContext()
            .fetch(DatabaseFrameworkE2ELargeDocument.self)
            .where(\.title == "large-compact")
            .execute()

        #expect(blobCountAfterDelete == 0)
        #expect(compactAfterDeleteHits.isEmpty)
    }

    @Test("SQLite transaction stale delete clears the current scalar index entry")
    func sqliteTransactionStaleDeleteClearsCurrentScalarIndexEntry() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-framework-transaction-stale-delete-e2e-\(UUID().uuidString)", isDirectory: true)
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

        var original = DatabaseFrameworkE2EAccount(
            email: "transaction-delete-original@example.com",
            age: 31
        )
        original.id = "database-framework-transaction-stale-delete-account"
        let seedContext = container.newContext()
        seedContext.insert(original)
        try await seedContext.save()

        var current = original
        current.email = "transaction-delete-current@example.com"
        current.age = 32
        let updateContext = container.newContext()
        updateContext.replace(old: original, with: current)
        try await updateContext.save()

        let originalForTransactionDelete = original
        try await container.newContext().withTransaction { transaction in
            try await transaction.delete(originalForTransactionDelete)
        }

        let verificationContainer = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let stored = try await verificationContainer.newContext().model(
            for: original.id,
            as: DatabaseFrameworkE2EAccount.self
        )
        let originalEmailHits = try await verificationContainer.newContext()
            .fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "transaction-delete-original@example.com")
            .execute()
        let currentEmailHits = try await verificationContainer.newContext()
            .fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "transaction-delete-current@example.com")
            .execute()

        #expect(stored == nil)
        #expect(originalEmailHits.isEmpty)
        #expect(currentEmailHits.isEmpty)
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

    @Test("SQLite failed cross-store save rolls back static and dynamic directory writes")
    func sqliteFailedCrossStoreSaveRollsBackStaticAndDynamicDirectoryWrites() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("database-framework-cross-store-retry-e2e-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer {
            do {
                try FileManager.default.removeItem(at: directory)
            } catch {
                Issue.record("Failed to clean up temporary E2E directory: \(error)")
            }
        }

        let databasePath = directory.appendingPathComponent("database-framework.sqlite").path
        let schema = Schema(
            [
                DatabaseFrameworkE2EAccount.self,
                DatabaseFrameworkE2ETenantAccount.self,
            ],
            version: .init(1, 0, 0)
        )
        let container = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let retryingContext = container.newContext()

        var staticCreated = DatabaseFrameworkE2EAccount(email: "cross-store-created@example.com", age: 20)
        staticCreated.id = "database-framework-cross-store-created"
        var tenantCreated = DatabaseFrameworkE2ETenantAccount(
            tenantID: "tenant-cross",
            email: "cross-store-tenant-created@example.com",
            status: "new"
        )
        tenantCreated.id = "database-framework-cross-store-tenant-created"
        var missingOriginal = DatabaseFrameworkE2EAccount(email: "cross-store-missing-before@example.com", age: 40)
        missingOriginal.id = "database-framework-cross-store-missing"
        var missingUpdated = missingOriginal
        missingUpdated.email = "cross-store-missing-after@example.com"
        missingUpdated.age = 41

        retryingContext.create(staticCreated)
        retryingContext.create(tenantCreated)
        retryingContext.replace(old: missingOriginal, with: missingUpdated)

        do {
            try await retryingContext.save()
            Issue.record("Expected cross-store save to fail")
        } catch let error as FDBContextError {
            if case .preconditionFailed(let typeName, let idDescription, let precondition, _) = error {
                #expect(typeName == DatabaseFrameworkE2EAccount.persistableType)
                #expect(idDescription == missingOriginal.id)
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
        let leakedStatic = try await afterFailureContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "cross-store-created@example.com")
            .execute()
        let leakedTenant = try await afterFailureContainer.newContext()
            .fetch(DatabaseFrameworkE2ETenantAccount.self)
            .partition(\.tenantID, equals: "tenant-cross")
            .where(\.email == "cross-store-tenant-created@example.com")
            .execute()
        let leakedReplacement = try await afterFailureContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "cross-store-missing-after@example.com")
            .execute()

        #expect(leakedStatic.isEmpty)
        #expect(leakedTenant.isEmpty)
        #expect(leakedReplacement.isEmpty)

        let seedContext = container.newContext()
        seedContext.insert(missingOriginal)
        try await seedContext.save()

        try await retryingContext.save()

        let verificationContainer = try await DBContainer.sqlite(
            for: schema,
            path: databasePath,
            security: .disabled
        )
        let verificationContext = verificationContainer.newContext()
        let storedStatic = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "cross-store-created@example.com")
            .execute()
        let storedTenant = try await verificationContainer.newContext()
            .fetch(DatabaseFrameworkE2ETenantAccount.self)
            .partition(\.tenantID, equals: "tenant-cross")
            .where(\.email == "cross-store-tenant-created@example.com")
            .execute()
        let oldReplacementHits = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "cross-store-missing-before@example.com")
            .execute()
        let newReplacementHits = try await verificationContext.fetch(DatabaseFrameworkE2EAccount.self)
            .where(\.email == "cross-store-missing-after@example.com")
            .execute()

        #expect(storedStatic.map(\.id) == [staticCreated.id])
        #expect(storedTenant.map(\.id) == [tenantCreated.id])
        #expect(oldReplacementHits.isEmpty)
        #expect(newReplacementHits.map(\.id) == [missingUpdated.id])
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
