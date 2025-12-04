import Testing
import Foundation
import FoundationDB
@testable import DatabaseEngine
@testable import Core

/// Tests for Uniqueness Enforcement
///
/// **Coverage**:
/// - UniquenessViolation struct
/// - UniquenessViolationError
/// - UniquenessCheckMode
/// - UniquenessViolationTracker operations
/// - FDBContext violation API
@Suite("Uniqueness Enforcement Tests", .serialized)
struct UniquenessEnforcementTests {

    // MARK: - Helper Types

    /// Test model with unique index
    @Persistable
    struct UniqueTestUser {
        #Directory<UniqueTestUser>("test", "uniqueness", "users")
        #Index<UniqueTestUser>(ScalarIndexKind<UniqueTestUser>(fields: [\.email]), unique: true, name: "UniqueTestUser_email")

        var id: String = ULID().ulidString
        var email: String
        var name: String
    }

    /// Test model without unique constraint
    @Persistable
    struct NonUniqueTestProduct {
        #Directory<NonUniqueTestProduct>("test", "uniqueness", "products")
        #Index<NonUniqueTestProduct>(ScalarIndexKind<NonUniqueTestProduct>(fields: [\.category]), name: "NonUniqueTestProduct_category")

        var id: String = ULID().ulidString
        var category: String
        var name: String
    }

    // MARK: - Helper Methods

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let schema = Schema(
            [UniqueTestUser.self, NonUniqueTestProduct.self],
            version: Schema.Version(1, 0, 0)
        )

        return FDBContainer(
            database: database,
            schema: schema
        )
    }

    private func cleanup(container: FDBContainer) async throws {
        let context = container.newContext()
        try await context.deleteAll(UniqueTestUser.self)
        try await context.deleteAll(NonUniqueTestProduct.self)
        try await context.save()
    }

    // MARK: - UniquenessViolation Tests

    @Test("UniquenessViolation creation and properties")
    func violationCreation() {
        let valueKey: [UInt8] = Tuple("test@example.com").pack()
        let pk1: [UInt8] = Tuple("user1").pack()
        let pk2: [UInt8] = Tuple("user2").pack()

        let violation = UniquenessViolation(
            indexName: "UniqueTestUser_email",
            persistableType: "UniqueTestUser",
            valueKey: valueKey,
            primaryKeys: [pk1, pk2],
            detectedAt: Date()
        )

        #expect(violation.indexName == "UniqueTestUser_email")
        #expect(violation.persistableType == "UniqueTestUser")
        #expect(violation.valueKey == valueKey)
        #expect(violation.primaryKeys.count == 2)
    }

    @Test("UniquenessViolation unpacking")
    func violationUnpacking() {
        let valueKey: [UInt8] = Tuple("test@example.com").pack()
        let pk1: [UInt8] = Tuple("user1").pack()
        let pk2: [UInt8] = Tuple("user2").pack()

        let violation = UniquenessViolation(
            indexName: "test_idx",
            persistableType: "TestType",
            valueKey: valueKey,
            primaryKeys: [pk1, pk2]
        )

        let unpackedValue = violation.unpackedValue()
        #expect(unpackedValue.count == 1)
        #expect(unpackedValue[0] == "test@example.com")

        let unpackedPKs = violation.unpackedPrimaryKeys()
        #expect(unpackedPKs.count == 2)
    }

    @Test("UniquenessViolation valueDescription")
    func violationValueDescription() {
        let valueKey: [UInt8] = Tuple("hello", 123).pack()

        let violation = UniquenessViolation(
            indexName: "test_idx",
            persistableType: "TestType",
            valueKey: valueKey,
            primaryKeys: []
        )

        let description = violation.valueDescription
        #expect(description.contains("hello"))
        #expect(description.contains("123"))
    }

    @Test("UniquenessViolation Codable")
    func violationCodable() throws {
        let valueKey: [UInt8] = Tuple("test").pack()
        let pk: [UInt8] = Tuple("id1").pack()

        let violation = UniquenessViolation(
            indexName: "idx",
            persistableType: "Type",
            valueKey: valueKey,
            primaryKeys: [pk],
            detectedAt: Date()
        )

        let encoder = JSONEncoder()
        let data = try encoder.encode(violation)

        let decoder = JSONDecoder()
        let decoded = try decoder.decode(UniquenessViolation.self, from: data)

        #expect(decoded.indexName == violation.indexName)
        #expect(decoded.persistableType == violation.persistableType)
        #expect(decoded.valueKey == violation.valueKey)
        #expect(decoded.primaryKeys == violation.primaryKeys)
    }

    @Test("UniquenessViolation CustomStringConvertible")
    func violationDescription() {
        let valueKey: [UInt8] = Tuple("email@test.com").pack()
        let pk: [UInt8] = Tuple("user1").pack()

        let violation = UniquenessViolation(
            indexName: "email_idx",
            persistableType: "User",
            valueKey: valueKey,
            primaryKeys: [pk]
        )

        let description = violation.description
        #expect(description.contains("email_idx"))
        #expect(description.contains("User"))
        #expect(description.contains("email@test.com"))
    }

    // MARK: - UniquenessViolationError Tests

    @Test("UniquenessViolationError properties")
    func violationErrorProperties() {
        let error = UniquenessViolationError(
            indexName: "email_idx",
            persistableType: "User",
            conflictingValues: ["test@example.com"],
            existingPrimaryKey: Tuple("user1"),
            newPrimaryKey: Tuple("user2")
        )

        #expect(error.indexName == "email_idx")
        #expect(error.persistableType == "User")
        #expect(error.conflictingValues == ["test@example.com"])
        #expect(error.valueDescription == "test@example.com")
    }

    @Test("UniquenessViolationError description")
    func violationErrorDescription() {
        let error = UniquenessViolationError(
            indexName: "email_idx",
            persistableType: "User",
            conflictingValues: ["test@example.com"],
            existingPrimaryKey: Tuple("user1"),
            newPrimaryKey: Tuple("user2")
        )

        let description = error.description
        #expect(description.contains("email_idx"))
        #expect(description.contains("User"))
        #expect(description.contains("test@example.com"))
        #expect(description.contains("already exists"))
    }

    // MARK: - UniquenessCheckMode Tests

    @Test("UniquenessCheckMode values")
    func checkModeValues() {
        let immediate = UniquenessCheckMode.immediate
        let track = UniquenessCheckMode.track
        let skip = UniquenessCheckMode.skip

        #expect(immediate == .immediate)
        #expect(track == .track)
        #expect(skip == .skip)
    }

    @Test("UniquenessCheckMode Hashable")
    func checkModeHashable() {
        var set: Set<UniquenessCheckMode> = []
        set.insert(.immediate)
        set.insert(.track)
        set.insert(.skip)

        #expect(set.count == 3)
        #expect(set.contains(.immediate))
        #expect(set.contains(.track))
        #expect(set.contains(.skip))
    }

    // MARK: - ViolationResolution Tests

    @Test("ViolationResolution cases")
    func violationResolutionCases() {
        let resolved = ViolationResolution.resolved
        let notFound = ViolationResolution.notFound

        if case .resolved = resolved {
            // OK
        } else {
            Issue.record("Expected .resolved")
        }

        if case .notFound = notFound {
            // OK
        } else {
            Issue.record("Expected .notFound")
        }
    }

    // MARK: - ViolationSummary Tests

    @Test("ViolationSummary properties")
    func violationSummaryProperties() {
        let summary = ViolationSummary(
            indexName: "email_idx",
            violationCount: 5,
            totalConflictingRecords: 12
        )

        #expect(summary.indexName == "email_idx")
        #expect(summary.violationCount == 5)
        #expect(summary.totalConflictingRecords == 12)
        #expect(summary.hasViolations == true)
    }

    @Test("ViolationSummary hasViolations false when no violations")
    func violationSummaryNoViolations() {
        let summary = ViolationSummary(
            indexName: "email_idx",
            violationCount: 0,
            totalConflictingRecords: 0
        )

        #expect(summary.hasViolations == false)
    }

    // MARK: - UniquenessViolationTracker Tests

    @Test("UniquenessViolationTracker record and scan violations")
    func trackerRecordAndScan() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let store = try await container.store(for: UniqueTestUser.self)
        guard let fdbStore = store as? FDBDataStore else {
            Issue.record("Store is not FDBDataStore")
            return
        }

        let tracker = fdbStore.violationTracker
        let indexName = "test_violation_idx"

        // Record a violation
        try await container.database.withTransaction { transaction in
            try await tracker.recordViolation(
                indexName: indexName,
                persistableType: "TestType",
                valueKey: Tuple("duplicate@email.com").pack(),
                existingPrimaryKey: Tuple("pk1"),
                newPrimaryKey: Tuple("pk2"),
                transaction: transaction
            )
        }

        // Scan violations
        let violations = try await tracker.scanViolations(indexName: indexName)
        #expect(violations.count == 1)
        #expect(violations[0].primaryKeys.count == 2)

        // Cleanup
        try await tracker.clearAllViolations(indexName: indexName)
    }

    @Test("UniquenessViolationTracker hasViolations")
    func trackerHasViolations() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let store = try await container.store(for: UniqueTestUser.self)
        guard let fdbStore = store as? FDBDataStore else {
            Issue.record("Store is not FDBDataStore")
            return
        }

        let tracker = fdbStore.violationTracker
        let indexName = "test_has_violations_idx"

        // Initially no violations
        let hasBefore = try await tracker.hasViolations(indexName: indexName)
        #expect(hasBefore == false)

        // Add a violation
        try await container.database.withTransaction { transaction in
            try await tracker.recordViolation(
                indexName: indexName,
                persistableType: "TestType",
                valueKey: Tuple("value").pack(),
                existingPrimaryKey: Tuple("pk1"),
                newPrimaryKey: Tuple("pk2"),
                transaction: transaction
            )
        }

        // Now has violations
        let hasAfter = try await tracker.hasViolations(indexName: indexName)
        #expect(hasAfter == true)

        // Cleanup
        try await tracker.clearAllViolations(indexName: indexName)
    }

    @Test("UniquenessViolationTracker countViolations")
    func trackerCountViolations() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let store = try await container.store(for: UniqueTestUser.self)
        guard let fdbStore = store as? FDBDataStore else {
            Issue.record("Store is not FDBDataStore")
            return
        }

        let tracker = fdbStore.violationTracker
        let indexName = "test_count_idx"

        // Add multiple violations
        try await container.database.withTransaction { transaction in
            for i in 0..<5 {
                try await tracker.recordViolation(
                    indexName: indexName,
                    persistableType: "TestType",
                    valueKey: Tuple("value\(i)").pack(),
                    existingPrimaryKey: Tuple("pk\(i)a"),
                    newPrimaryKey: Tuple("pk\(i)b"),
                    transaction: transaction
                )
            }
        }

        let count = try await tracker.countViolations(indexName: indexName)
        #expect(count == 5)

        // Cleanup
        try await tracker.clearAllViolations(indexName: indexName)
    }

    @Test("UniquenessViolationTracker clearViolation")
    func trackerClearViolation() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let store = try await container.store(for: UniqueTestUser.self)
        guard let fdbStore = store as? FDBDataStore else {
            Issue.record("Store is not FDBDataStore")
            return
        }

        let tracker = fdbStore.violationTracker
        let indexName = "test_clear_idx"
        let valueKey = Tuple("clearme").pack()

        // Add violation
        try await container.database.withTransaction { transaction in
            try await tracker.recordViolation(
                indexName: indexName,
                persistableType: "TestType",
                valueKey: valueKey,
                existingPrimaryKey: Tuple("pk1"),
                newPrimaryKey: Tuple("pk2"),
                transaction: transaction
            )
        }

        // Verify it exists
        let countBefore = try await tracker.countViolations(indexName: indexName)
        #expect(countBefore == 1)

        // Clear it
        try await tracker.clearViolation(indexName: indexName, valueKey: valueKey)

        // Verify it's gone
        let countAfter = try await tracker.countViolations(indexName: indexName)
        #expect(countAfter == 0)
    }

    @Test("UniquenessViolationTracker violationSummary")
    func trackerViolationSummary() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let store = try await container.store(for: UniqueTestUser.self)
        guard let fdbStore = store as? FDBDataStore else {
            Issue.record("Store is not FDBDataStore")
            return
        }

        let tracker = fdbStore.violationTracker
        let indexName = "test_summary_idx"

        // Add violations with different conflict counts
        try await container.database.withTransaction { transaction in
            // Violation 1: 2 conflicts
            try await tracker.recordViolation(
                indexName: indexName,
                persistableType: "TestType",
                valueKey: Tuple("val1").pack(),
                existingPrimaryKey: Tuple("pk1a"),
                newPrimaryKey: Tuple("pk1b"),
                transaction: transaction
            )

            // Violation 2: 2 conflicts
            try await tracker.recordViolation(
                indexName: indexName,
                persistableType: "TestType",
                valueKey: Tuple("val2").pack(),
                existingPrimaryKey: Tuple("pk2a"),
                newPrimaryKey: Tuple("pk2b"),
                transaction: transaction
            )
        }

        let summary = try await tracker.violationSummary(indexName: indexName)
        #expect(summary.indexName == indexName)
        #expect(summary.violationCount == 2)
        #expect(summary.totalConflictingRecords == 4)
        #expect(summary.hasViolations == true)

        // Cleanup
        try await tracker.clearAllViolations(indexName: indexName)
    }

    // MARK: - FDBContext Violation API Tests

    @Test("FDBContext scanUniquenessViolations")
    func contextScanViolations() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.newContext()
        let indexName = "test_context_scan_idx"

        // Add a violation directly to tracker
        let store = try await container.store(for: UniqueTestUser.self)
        guard let fdbStore = store as? FDBDataStore else {
            Issue.record("Store is not FDBDataStore")
            return
        }

        try await container.database.withTransaction { transaction in
            try await fdbStore.violationTracker.recordViolation(
                indexName: indexName,
                persistableType: "UniqueTestUser",
                valueKey: Tuple("context@test.com").pack(),
                existingPrimaryKey: Tuple("pk1"),
                newPrimaryKey: Tuple("pk2"),
                transaction: transaction
            )
        }

        // Use context API to scan
        let violations = try await context.scanUniquenessViolations(
            for: UniqueTestUser.self,
            indexName: indexName
        )
        #expect(violations.count == 1)

        // Cleanup
        try await context.clearAllUniquenessViolations(
            for: UniqueTestUser.self,
            indexName: indexName
        )
    }

    @Test("FDBContext hasUniquenessViolations")
    func contextHasViolations() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.newContext()
        let indexName = "test_context_has_idx"

        // Check no violations initially
        let hasBefore = try await context.hasUniquenessViolations(
            for: UniqueTestUser.self,
            indexName: indexName
        )
        #expect(hasBefore == false)

        // Add a violation
        let store = try await container.store(for: UniqueTestUser.self)
        guard let fdbStore = store as? FDBDataStore else {
            Issue.record("Store is not FDBDataStore")
            return
        }

        try await container.database.withTransaction { transaction in
            try await fdbStore.violationTracker.recordViolation(
                indexName: indexName,
                persistableType: "UniqueTestUser",
                valueKey: Tuple("test").pack(),
                existingPrimaryKey: Tuple("pk1"),
                newPrimaryKey: Tuple("pk2"),
                transaction: transaction
            )
        }

        // Check violations exist
        let hasAfter = try await context.hasUniquenessViolations(
            for: UniqueTestUser.self,
            indexName: indexName
        )
        #expect(hasAfter == true)

        // Cleanup
        try await context.clearAllUniquenessViolations(
            for: UniqueTestUser.self,
            indexName: indexName
        )
    }

    @Test("FDBContext uniquenessViolationSummary")
    func contextViolationSummary() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.newContext()
        let indexName = "test_context_summary_idx"

        // Add violations
        let store = try await container.store(for: UniqueTestUser.self)
        guard let fdbStore = store as? FDBDataStore else {
            Issue.record("Store is not FDBDataStore")
            return
        }

        try await container.database.withTransaction { transaction in
            try await fdbStore.violationTracker.recordViolation(
                indexName: indexName,
                persistableType: "UniqueTestUser",
                valueKey: Tuple("val1").pack(),
                existingPrimaryKey: Tuple("pk1"),
                newPrimaryKey: Tuple("pk2"),
                transaction: transaction
            )
        }

        // Get summary via context API
        let summary = try await context.uniquenessViolationSummary(
            for: UniqueTestUser.self,
            indexName: indexName
        )
        #expect(summary.violationCount == 1)
        #expect(summary.totalConflictingRecords == 2)

        // Cleanup
        try await context.clearAllUniquenessViolations(
            for: UniqueTestUser.self,
            indexName: indexName
        )
    }

    // MARK: - OnlineIndexerError Tests

    @Test("OnlineIndexerError uniquenessViolationsDetected")
    func indexerErrorViolations() {
        let error = OnlineIndexerError.uniquenessViolationsDetected(
            indexName: "email_idx",
            violationCount: 3,
            totalConflictingRecords: 7
        )

        let description = error.description
        #expect(description.contains("email_idx"))
        #expect(description.contains("3"))
        #expect(description.contains("7"))
        #expect(description.contains("write-only state"))
    }

    // MARK: - Index isUnique Property Tests

    @Test("Index isUnique defaults to false")
    func indexIsUniqueDefault() {
        let index = Index(
            name: "test_idx",
            kind: ScalarIndexKind<UniqueTestUser>(fields: [\.email]),
            rootExpression: FieldKeyExpression(fieldName: "email")
        )

        #expect(index.isUnique == false)
    }

    @Test("Index isUnique can be set to true")
    func indexIsUniqueTrue() {
        let index = Index(
            name: "unique_idx",
            kind: ScalarIndexKind<UniqueTestUser>(fields: [\.email]),
            rootExpression: FieldKeyExpression(fieldName: "email"),
            isUnique: true
        )

        #expect(index.isUnique == true)
    }
}
