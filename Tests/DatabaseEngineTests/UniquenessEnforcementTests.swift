#if !os(WASI)
#if FOUNDATION_DB
import DatabaseTypes
import Testing
import Foundation
import StorageKit
import FDBStorage
import TestSupport
@testable import DatabaseEngine
import DatabaseRuntime
@testable import DatabaseKit

/// Tests for Uniqueness Enforcement
///
/// **Coverage**:
/// - UniquenessViolation struct
/// - UniquenessViolationError
/// - UniquenessCheckMode
/// - UniquenessViolationTracker operations
/// - DatabaseContext violation API
@Suite("Uniqueness Enforcement Tests", .foundationDBScenario, .serialized, .heartbeat)
struct UniquenessEnforcementTests {

    // MARK: - Helper Types

    /// Test model with unique index
    @Persistable
    struct UniquenessConstrainedUser {
        #Directory<UniquenessConstrainedUser>("test", "uniqueness", "users")
        #Index(
            .ordered(
                name: "UniqueTestUser_email",
                keys: [.ascending(\UniquenessConstrainedUser.email)], unique: true))

        var id: String = UUID().uuidString
        var email: String
        var name: String
    }

    /// Test model without unique constraint
    @Persistable
    struct UnconstrainedProduct {
        #Directory<UnconstrainedProduct>("test", "uniqueness", "products")
        #Index(
            .ordered(
                name: "NonUniqueTestProduct_category",
                keys: [.ascending(\UnconstrainedProduct.category)], unique: false))

        var id: String = UUID().uuidString
        var category: String
        var name: String
    }

    // MARK: - Helper Methods

    private func setupContainer() async throws -> DBContainer {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()

        let schema = try Schema(
            entities: [
                try UniquenessConstrainedUser.schemaEntity,
                try UnconstrainedProduct.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(UniquenessConstrainedUser.self), try DatabaseFrameworkRuntime.entity(UnconstrainedProduct.self),
                ]),
            security: .testingDisabled
            )
    }

    private func cleanup(container: DBContainer) async throws {
        let context = container.testBaseContext()
        try await context.deleteAll(UniquenessConstrainedUser.self)
        try await context.deleteAll(UnconstrainedProduct.self)
        try await context.save()
    }

    // MARK: - UniquenessViolation Tests

    @Test("UniquenessViolation creation and properties")
    func violationCreation() {
        let valueKey: ByteString = Tuple("test@example.com").pack()
        let pk1: ByteString = Tuple("user1").pack()
        let pk2: ByteString = Tuple("user2").pack()

        let violation = UniquenessViolation(
            indexName: "UniqueTestUser_email",
            persistableType: "UniquenessConstrainedUser",
            valueKey: valueKey,
            conflictingValues: [.string("test@example.com")],
            primaryKeys: [pk1, pk2],
            detectedAt: Timestamp(secondsSinceUnixEpoch: 1_000)
        )

        #expect(violation.indexName == "UniqueTestUser_email")
        #expect(violation.persistableType == "UniquenessConstrainedUser")
        #expect(violation.valueKey == valueKey)
        #expect(violation.primaryKeys.count == 2)
    }

    @Test("UniquenessViolation exposes semantic conflicting values")
    func violationConflictingValues() throws {
        let valueKey: ByteString = Tuple("test@example.com").pack()
        let pk1: ByteString = Tuple("user1").pack()
        let pk2: ByteString = Tuple("user2").pack()

        let violation = UniquenessViolation(
            indexName: "test_idx",
            persistableType: "TestType",
            valueKey: valueKey,
            conflictingValues: [.string("test@example.com")],
            primaryKeys: [pk1, pk2],
            detectedAt: Timestamp(secondsSinceUnixEpoch: 1_000)
        )

        #expect(violation.conflictingValues == [.string("test@example.com")])

        let unpackedPKs = try violation.unpackedPrimaryKeys()
        #expect(unpackedPKs.count == 2)
    }

    @Test("UniquenessViolation valueDescription")
    func violationValueDescription() {
        let valueKey: ByteString = Tuple("hello", 123).pack()

        let violation = UniquenessViolation(
            indexName: "test_idx",
            persistableType: "TestType",
            valueKey: valueKey,
            conflictingValues: [.string("hello"), .int64(123)],
            primaryKeys: [],
            detectedAt: Timestamp(secondsSinceUnixEpoch: 1_000)
        )

        let description = violation.valueDescription
        #expect(description.contains("hello"))
        #expect(description.contains("123"))
    }

    @Test("UniquenessViolation storage encoding round-trips")
    func uniquenessViolationStorageEncodingRoundTrips() throws {
        let valueKey: ByteString = Tuple("test").pack()
        let pk: ByteString = Tuple("id1").pack()

        let violation = UniquenessViolation(
            indexName: "idx",
            persistableType: "Type",
            valueKey: valueKey,
            conflictingValues: [.string("test")],
            primaryKeys: [pk],
            detectedAt: Timestamp(secondsSinceUnixEpoch: 1_000)
        )

        let data = try UniquenessViolationCodec.encode(violation)
        let decoded = try UniquenessViolationCodec.decode(data)

        #expect(decoded.indexName == violation.indexName)
        #expect(decoded.persistableType == violation.persistableType)
        #expect(decoded.valueKey == violation.valueKey)
        #expect(decoded.conflictingValues == violation.conflictingValues)
        #expect(decoded.primaryKeys == violation.primaryKeys)
    }

    @Test("UniquenessViolation CustomStringConvertible")
    func violationDescription() {
        let valueKey: ByteString = Tuple("email@test.com").pack()
        let pk: ByteString = Tuple("user1").pack()

        let violation = UniquenessViolation(
            indexName: "email_idx",
            persistableType: "User",
            valueKey: valueKey,
            conflictingValues: [.string("email@test.com")],
            primaryKeys: [pk],
            detectedAt: Timestamp(secondsSinceUnixEpoch: 1_000)
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
            conflictingValues: [.string("test@example.com")],
            existingPrimaryKey: Tuple("user1"),
            newPrimaryKey: Tuple("user2")
        )

        #expect(error.indexName == "email_idx")
        #expect(error.persistableType == "User")
        #expect(error.conflictingValues == [.string("test@example.com")])
        #expect(error.valueDescription == "test@example.com")
    }

    @Test("UniquenessViolationError description")
    func violationErrorDescription() {
        let error = UniquenessViolationError(
            indexName: "email_idx",
            persistableType: "User",
            conflictingValues: [.string("test@example.com")],
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
            totalConflictingEntities: 12
        )

        #expect(summary.indexName == "email_idx")
        #expect(summary.violationCount == 5)
        #expect(summary.totalConflictingEntities == 12)
        #expect(summary.hasViolations == true)
    }

    @Test("ViolationSummary hasViolations false when no violations")
    func violationSummaryNoViolations() {
        let summary = ViolationSummary(
            indexName: "email_idx",
            violationCount: 0,
            totalConflictingEntities: 0
        )

        #expect(summary.hasViolations == false)
    }

    // MARK: - UniquenessViolationTracker Tests

    @Test("UniquenessViolationTracker entity and scan violations")
    func trackerEntityAndScan() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let databaseStore = try await container.testBaseStore(for: UniquenessConstrainedUser.self)

            let tracker = databaseStore.violationTracker
            let indexName = "UniqueTestUser_email"
            try await tracker.clearAllViolations(indexName: indexName)

            // Entity a violation
            try await container.engine.withTransaction { transaction in
                try await tracker.recordViolation(
                    indexName: indexName,
                    persistableType: "TestType",
                    valueKey: Tuple("duplicate@email.com").pack(),
                    conflictingValues: [.string("duplicate@email.com")],
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
    }

    @Test("UniquenessViolationTracker hasViolations")
    func trackerHasViolations() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let databaseStore = try await container.testBaseStore(for: UniquenessConstrainedUser.self)

            let tracker = databaseStore.violationTracker
            let indexName = "UniqueTestUser_email"
            try await tracker.clearAllViolations(indexName: indexName)

            // Initially no violations
            let hasBefore = try await tracker.hasViolations(indexName: indexName)
            #expect(hasBefore == false)

            // Add a violation
            try await container.engine.withTransaction { transaction in
                try await tracker.recordViolation(
                    indexName: indexName,
                    persistableType: "TestType",
                    valueKey: Tuple("value").pack(),
                    conflictingValues: [.string("value")],
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
    }

    @Test("UniquenessViolationTracker countViolations")
    func trackerCountViolations() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let databaseStore = try await container.testBaseStore(for: UniquenessConstrainedUser.self)

            let tracker = databaseStore.violationTracker
            let indexName = "UniqueTestUser_email"
            try await tracker.clearAllViolations(indexName: indexName)

            // Add multiple violations
            try await container.engine.withTransaction { transaction in
                for i in 0..<5 {
                    try await tracker.recordViolation(
                        indexName: indexName,
                        persistableType: "TestType",
                        valueKey: Tuple("value\(i)").pack(),
                        conflictingValues: [.string("value\(i)")],
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
    }

    @Test("UniquenessViolationTracker clearViolation")
    func trackerClearViolation() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let databaseStore = try await container.testBaseStore(for: UniquenessConstrainedUser.self)

            let tracker = databaseStore.violationTracker
            let indexName = "UniqueTestUser_email"
            try await tracker.clearAllViolations(indexName: indexName)
            let valueKey = Tuple("clearme").pack()

            // Add violation
            try await container.engine.withTransaction { transaction in
                try await tracker.recordViolation(
                    indexName: indexName,
                    persistableType: "TestType",
                    valueKey: valueKey,
                    conflictingValues: [.string("clearme")],
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
    }

    @Test("UniquenessViolationTracker composes a relative value key exactly once")
    func trackerResolutionUsesRelativeValueKey() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let databaseStore = try await container.testBaseStore(
                for: UniquenessConstrainedUser.self
            )
            let tracker = databaseStore.violationTracker
            let indexName = "UniqueTestUser_email"
            try await tracker.clearAllViolations(indexName: indexName)
            let duplicateValue = "duplicate@example.com"
            let valueKey = Tuple(duplicateValue).pack()
            let indexSubspace = Subspace(
                Tuple("test", "uniqueness", indexName).pack()
            )
            let firstIndexKey = indexSubspace.pack(
                Tuple(duplicateValue, "pk1")
            )
            let secondIndexKey = indexSubspace.pack(
                Tuple(duplicateValue, "pk2")
            )

            try await container.engine.withTransaction { transaction in
                try transaction.setValue(ByteString(), for: firstIndexKey)
                try transaction.setValue(ByteString(), for: secondIndexKey)
                try await tracker.recordViolation(
                    indexName: indexName,
                    persistableType: "TestType",
                    valueKey: valueKey,
                    conflictingValues: [.string(duplicateValue)],
                    existingPrimaryKey: Tuple("pk1"),
                    newPrimaryKey: Tuple("pk2"),
                    transaction: transaction
                )
            }

            let unresolved = try await tracker.verifyResolution(
                indexName: indexName,
                valueKey: valueKey,
                indexSubspace: indexSubspace
            )
            switch unresolved {
            case .unresolved(let violation):
                #expect(violation.primaryKeys.count == 2)
                #expect(violation.conflictingValues == [.string(duplicateValue)])
            case .resolved, .notFound:
                Issue.record("Expected the duplicate index entries to remain unresolved")
            }

            try await container.engine.withTransaction { transaction in
                try transaction.clear(key: secondIndexKey)
            }

            let resolved = try await tracker.verifyResolution(
                indexName: indexName,
                valueKey: valueKey,
                indexSubspace: indexSubspace
            )
            switch resolved {
            case .resolved:
                break
            case .unresolved, .notFound:
                Issue.record("Expected one remaining index entry to resolve the violation")
            }

            try await container.engine.withTransaction { transaction in
                try transaction.clear(key: firstIndexKey)
            }
            try await tracker.clearAllViolations(indexName: indexName)
        }
    }

    @Test("UniquenessViolationTracker violationSummary")
    func trackerViolationSummary() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let databaseStore = try await container.testBaseStore(for: UniquenessConstrainedUser.self)

            let tracker = databaseStore.violationTracker
            let indexName = "UniqueTestUser_email"
            try await tracker.clearAllViolations(indexName: indexName)

            // Add violations with different conflict counts
            try await container.engine.withTransaction { transaction in
                // Violation 1: 2 conflicts
                try await tracker.recordViolation(
                    indexName: indexName,
                    persistableType: "TestType",
                    valueKey: Tuple("val1").pack(),
                    conflictingValues: [.string("val1")],
                    existingPrimaryKey: Tuple("pk1a"),
                    newPrimaryKey: Tuple("pk1b"),
                    transaction: transaction
                )

                // Violation 2: 2 conflicts
                try await tracker.recordViolation(
                    indexName: indexName,
                    persistableType: "TestType",
                    valueKey: Tuple("val2").pack(),
                    conflictingValues: [.string("val2")],
                    existingPrimaryKey: Tuple("pk2a"),
                    newPrimaryKey: Tuple("pk2b"),
                    transaction: transaction
                )
            }

            let summary = try await tracker.violationSummary(indexName: indexName)
            #expect(summary.indexName == indexName)
            #expect(summary.violationCount == 2)
            #expect(summary.totalConflictingEntities == 4)
            #expect(summary.hasViolations == true)

            // Cleanup
            try await tracker.clearAllViolations(indexName: indexName)
        }
    }

    // MARK: - DatabaseContext Violation API Tests

    @Test("DatabaseContext scanUniquenessViolations")
    func contextScanViolations() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.testBaseContext()
            let indexName = "UniqueTestUser_email"

            // Add a violation directly to tracker
            let databaseStore = try await container.testBaseStore(for: UniquenessConstrainedUser.self)
            try await databaseStore.violationTracker.clearAllViolations(
                indexName: indexName
            )

            try await container.engine.withTransaction { transaction in
                try await databaseStore.violationTracker.recordViolation(
                    indexName: indexName,
                    persistableType: "UniquenessConstrainedUser",
                    valueKey: Tuple("context@test.com").pack(),
                    conflictingValues: [.string("context@test.com")],
                    existingPrimaryKey: Tuple("pk1"),
                    newPrimaryKey: Tuple("pk2"),
                    transaction: transaction
                )
            }

            // Use context API to scan
            let violations = try await context.scanUniquenessViolations(
                for: UniquenessConstrainedUser.self,
                indexName: indexName
            )
            #expect(violations.count == 1)

            // Cleanup
            try await context.clearAllUniquenessViolations(
                for: UniquenessConstrainedUser.self,
                indexName: indexName
            )
        }
    }

    @Test("DatabaseContext hasUniquenessViolations")
    func contextHasViolations() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.testBaseContext()
            let indexName = "UniqueTestUser_email"

            let databaseStore = try await container.testBaseStore(
                for: UniquenessConstrainedUser.self
            )
            try await databaseStore.violationTracker.clearAllViolations(
                indexName: indexName
            )

            // Check no violations initially
            let hasBefore = try await context.hasUniquenessViolations(
                for: UniquenessConstrainedUser.self,
                indexName: indexName
            )
            #expect(hasBefore == false)

            // Add a violation
            try await container.engine.withTransaction { transaction in
                try await databaseStore.violationTracker.recordViolation(
                    indexName: indexName,
                    persistableType: "UniquenessConstrainedUser",
                    valueKey: Tuple("test").pack(),
                    conflictingValues: [.string("test")],
                    existingPrimaryKey: Tuple("pk1"),
                    newPrimaryKey: Tuple("pk2"),
                    transaction: transaction
                )
            }

            // Check violations exist
            let hasAfter = try await context.hasUniquenessViolations(
                for: UniquenessConstrainedUser.self,
                indexName: indexName
            )
            #expect(hasAfter == true)

            // Cleanup
            try await context.clearAllUniquenessViolations(
                for: UniquenessConstrainedUser.self,
                indexName: indexName
            )
        }
    }

    @Test("DatabaseContext uniquenessViolationSummary")
    func contextViolationSummary() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.testBaseContext()
            let indexName = "UniqueTestUser_email"

            // Add violations
            let databaseStore = try await container.testBaseStore(for: UniquenessConstrainedUser.self)
            try await databaseStore.violationTracker.clearAllViolations(
                indexName: indexName
            )

            try await container.engine.withTransaction { transaction in
                try await databaseStore.violationTracker.recordViolation(
                    indexName: indexName,
                    persistableType: "UniquenessConstrainedUser",
                    valueKey: Tuple("val1").pack(),
                    conflictingValues: [.string("val1")],
                    existingPrimaryKey: Tuple("pk1"),
                    newPrimaryKey: Tuple("pk2"),
                    transaction: transaction
                )
            }

            // Get summary via context API
            let summary = try await context.uniquenessViolationSummary(
                for: UniquenessConstrainedUser.self,
                indexName: indexName
            )
            #expect(summary.violationCount == 1)
            #expect(summary.totalConflictingEntities == 2)

            // Cleanup
            try await context.clearAllUniquenessViolations(
                for: UniquenessConstrainedUser.self,
                indexName: indexName
            )
        }
    }

    #if MultiBase
    @Test("Uniqueness violation inspection requires Base administration")
    func contextViolationInspectionRequiresAdministration() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let readerID = "uniqueness-reader"
            try await container.grantTestBaseAccess(
                to: .principal(readerID),
                access: .read
            )
            let authorization = AuthorizationContext.authenticated(
                Principal(identifier: readerID)
            )
            let baseID = try TestBaseEnvironment.id()
            let expectedResource = Security.Resource.base(baseID)
            let context = container.testBaseContext(
                authorization: authorization
            )

            do {
                _ = try await context.scanUniquenessViolations(
                    for: UniquenessConstrainedUser.self,
                    indexName: "UniqueTestUser_email"
                )
                Issue.record("Expected Base administration authorization")
            } catch let error as DatabaseGrantAuthorizationError {
                #expect(
                    error == .denied(
                        resource: expectedResource,
                        required: .administer
                    )
                )
            }
        }
    }
    #endif

    // MARK: - OnlineIndexBuildError Tests

    @Test("OnlineIndexBuildError uniquenessViolationsDetected")
    func indexerErrorViolations() {
        let error = OnlineIndexBuildError.uniquenessViolationsDetected(
            indexName: "email_idx",
            violationCount: 3,
            totalConflictingEntities: 7
        )

        let description = error.description
        #expect(description.contains("email_idx"))
        #expect(description.contains("3"))
        #expect(description.contains("7"))
        #expect(description.contains("write-only state"))
    }

    // MARK: - Index isUnique Property Tests

    @Test("Index isUnique defaults to false")
    func indexIsUniqueDefault() throws {
        let descriptor = try #require(
            UnconstrainedProduct.indexDescriptors.first
        )
        let index = ResolvedIndex(
            descriptor: descriptor,
            rootExpression: FieldKeyExpression(fieldName: "category")
        )

        #expect(index.isUnique == false)
    }

    @Test("Index isUnique can be set to true")
    func indexIsUniqueTrue() throws {
        let descriptor = try #require(
            UniquenessConstrainedUser.indexDescriptors.first
        )
        let index = ResolvedIndex(
            descriptor: descriptor,
            rootExpression: FieldKeyExpression(fieldName: "email")
        )

        #expect(index.isUnique == true)
    }
}
#endif

#endif
