#if !os(WASI)
#if FOUNDATION_DB
import Testing
import TestHeartbeat
import Foundation
import StorageKit
import StorageKitSystemClock
import FDBStorage
import TestSupport
@testable import DatabaseEngine
import DatabaseRuntime
@testable import DatabaseKit

/// Tests for CachePolicy
///
/// **Coverage**:
/// - CachePolicy enum values
/// - ReadVersionCache integration with CachePolicy
/// - Query.cachePolicy fluent API
/// - QueryExecutor.cachePolicy fluent API
/// - DatabaseContext.fetch() cache integration
@Suite("CachePolicy Tests", .foundationDBScenario, .serialized, .heartbeat)
struct CachePolicyTests {

    // MARK: - CachePolicy + ReadVersionCache Tests

    @Test("CachePolicy.server returns nil from cache")
    func serverPolicyReturnsNilFromCache() {
        let cache = ReadVersionCache(monotonicClock: SystemStorageClock())
        cache.updateFromCommit(version: 12345)

        // .server should not use cache
        let result = cache.getCachedVersion(policy: .server)
        #expect(result == nil)
    }

    @Test("CachePolicy.cached returns cached version (no time limit)")
    func cachedPolicyReturnsCachedVersion() {
        let cache = ReadVersionCache(monotonicClock: SystemStorageClock())
        cache.updateFromCommit(version: 12345)

        // .cached should return cached version regardless of age
        let result = cache.getCachedVersion(policy: .cached)
        #expect(result == 12345)
    }

    @Test("CachePolicy.stale(N) returns version if fresh enough")
    func stalePolicyReturnsVersionIfFresh() {
        let cache = ReadVersionCache(monotonicClock: SystemStorageClock())
        cache.updateFromCommit(version: 12345)

        // .stale(.seconds(30)) should return cached version (just created, age < 30s)
        let result = cache.getCachedVersion(policy: .stale(.seconds(30)))
        #expect(result == 12345)
    }

    @Test("CachePolicy.stale(.zero) returns nil immediately")
    func staleZeroPolicyReturnsNil() {
        let cache = ReadVersionCache(monotonicClock: SystemStorageClock())
        cache.updateFromCommit(version: 12345)

        // .stale(.zero) should return nil (cache is already older than 0 seconds)
        let result = cache.getCachedVersion(policy: .stale(.zero))
        #expect(result == nil)
    }

    // MARK: - CachePolicy Description Tests

    @Test("CachePolicy descriptions are correct")
    func policyDescriptions() {
        #expect(CachePolicy.server.description == "CachePolicy.server")
        #expect(CachePolicy.cached.description == "CachePolicy.cached")
        #expect(CachePolicy.stale(.seconds(30)).description == "CachePolicy.stale")
        #expect(CachePolicy.stale(.seconds(60)).description == "CachePolicy.stale")
    }

    // MARK: - CachePolicy Equatable Tests

    @Test("CachePolicy is Equatable")
    func policyEquatable() {
        #expect(CachePolicy.server == CachePolicy.server)
        #expect(CachePolicy.cached == CachePolicy.cached)
        #expect(CachePolicy.stale(.seconds(30)) == CachePolicy.stale(.seconds(30)))
        #expect(CachePolicy.server != CachePolicy.cached)
        #expect(CachePolicy.stale(.seconds(30)) != CachePolicy.stale(.seconds(60)))
    }

    // MARK: - CachePolicy Hashable Tests

    @Test("CachePolicy is Hashable")
    func policyHashable() {
        var set = Set<CachePolicy>()
        set.insert(.server)
        set.insert(.cached)
        set.insert(.stale(.seconds(30)))
        set.insert(.stale(.seconds(30)))  // Duplicate
        #expect(set.count == 3)
    }

    // MARK: - Query.cachePolicy Tests

    @Test("Query.cachePolicy defaults to .server")
    func queryDefaultCachePolicy() {
        let query = Query<CachePolicyEntity>()
        #expect(query.cachePolicy == .server)
    }

    @Test("Query.cachePolicy() fluent method sets policy")
    func queryCachePolicyFluent() {
        let query = Query<CachePolicyEntity>()
            .cachePolicy(.server)
        #expect(query.cachePolicy == .server)

        let query2 = Query<CachePolicyEntity>()
            .cachePolicy(.stale(.seconds(60)))
        #expect(query2.cachePolicy == .stale(.seconds(60)))
    }

    @Test("Query.cachePolicy() can be chained with other methods")
    func queryCachePolicyChaining() {
        let query = Query<CachePolicyEntity>()
            .cachePolicy(.server)
            .limit(10)
            .offset(5)
        #expect(query.cachePolicy == .server)
        #expect(query.fetchLimit == 10)
        #expect(query.fetchOffset == 5)
    }

    // MARK: - QueryExecutor.cachePolicy Tests

    @Test("QueryExecutor.cachePolicy() propagates to query")
    func executorCachePolicyPropagates() async throws {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()

        let schema = try Schema(
            entities: [try CachePolicyEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: database), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(CachePolicyEntity.self)]), security: .testingDisabled)
        let context = container.testBaseContext()

        // Create executor with cache policy
        let executor = context.fetch(CachePolicyEntity.self)
            .cachePolicy(.server)

        // Verify the underlying query has the cache policy set
        #expect(executor.query.cachePolicy == .server)
    }

    @Test("QueryExecutor.cachePolicy() can be chained with filters")
    func executorCachePolicyChainingWithFilters() async throws {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()

        let schema = try Schema(
            entities: [try CachePolicyEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: database), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(CachePolicyEntity.self)]), security: .testingDisabled)
        let context = container.testBaseContext()

        // Chain cache policy with other query methods
        let executor = context.fetch(CachePolicyEntity.self)
            .where(CachePolicyEntity.fields.value > Int64(10))
            .cachePolicy(.stale(.seconds(30)))
            .orderBy(CachePolicyEntity.fields.value)
            .limit(5)

        #expect(executor.query.cachePolicy == .stale(.seconds(30)))
        #expect(executor.query.fetchLimit == 5)
    }

    // MARK: - DatabaseContext Integration Tests

    @Test("fetch() with .cached uses ReadVersionCache")
    func fetchWithCachedUsesCache() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()

            let schema = try Schema(
                entities: [try CachePolicyEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            )
            let container = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: database), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(CachePolicyEntity.self)]), security: .testingDisabled)
            let context = container.testBaseContext()

            // Insert test data
            let testId = "cache-test-\(UUID().uuidString.prefix(8))"
            let model = CachePolicyEntity(id: testId, value: 42)
            try context.insert(model)
            try await context.save()

            // First fetch with .cached - should populate cache
            _ = try await context.fetch(CachePolicyEntity.self)
                .cachePolicy(.cached)
                .execute()

            // Cache should now have a version
            let cacheInfo1 = context.readVersionCacheInfo()
            #expect(cacheInfo1 != nil)

            // Second fetch with .cached - should use cached version
            _ = try await context.fetch(CachePolicyEntity.self)
                .cachePolicy(.cached)
                .execute()

            // Cache should still be populated
            let cacheInfo2 = context.readVersionCacheInfo()
            #expect(cacheInfo2 != nil)
        }
    }

    @Test("fetch() with .server bypasses cache")
    func fetchWithServerBypassesCache() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()

            let schema = try Schema(
                entities: [try CachePolicyEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            )
            let container = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: database), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(CachePolicyEntity.self)]), security: .testingDisabled)
            let context = container.testBaseContext()

            // Insert test data
            let testId = "server-test-\(UUID().uuidString.prefix(8))"
            let model = CachePolicyEntity(id: testId, value: 100)
            try context.insert(model)
            try await context.save()

            // First fetch with .server
            let results = try await context.fetch(CachePolicyEntity.self)
                .cachePolicy(.server)
                .where(CachePolicyEntity.fields.id == testId)
                .execute()

            #expect(results.count == 1)
            #expect(results.first?.value == 100)

            // Cache should still be updated after the transaction completes
            // (TransactionRunner updates cache after successful commit)
            // But the .server policy means we didn't USE the cache for the read
        }
    }

    @Test("count() respects cachePolicy")
    func countRespectsCache() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()

            let schema = try Schema(
                entities: [try CachePolicyEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            )
            let container = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: database), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(CachePolicyEntity.self)]), security: .testingDisabled)
            let context = container.testBaseContext()

            // Insert test data
            let testId = "count-test-\(UUID().uuidString.prefix(8))"
            let model = CachePolicyEntity(id: testId, value: 200)
            try context.insert(model)
            try await context.save()

            // Count with .cached policy
            let count = try await context.fetch(CachePolicyEntity.self)
                .cachePolicy(.cached)
                .where(CachePolicyEntity.fields.id == testId)
                .count()

            #expect(count == 1)
        }
    }

    @Test("default cachePolicy is .server for new queries")
    func defaultCachePolicyIsServer() {
        // Query default
        let query = Query<CachePolicyEntity>()
        #expect(query.cachePolicy == .server)
    }

    @Test("fetch() executes correctly with all CachePolicy values")
    func fetchWithAllPolicyValues() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()

            let schema = try Schema(
                entities: [try CachePolicyEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            )
            let container = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: database), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(CachePolicyEntity.self)]), security: .testingDisabled)
            let context = container.testBaseContext()

            // Insert test data
            let testId = "all-policies-\(UUID().uuidString.prefix(8))"
            let model = CachePolicyEntity(id: testId, value: 300)
            try context.insert(model)
            try await context.save()

            // Test .server
            let serverResults = try await context.fetch(CachePolicyEntity.self)
                .cachePolicy(.server)
                .where(CachePolicyEntity.fields.id == testId)
                .execute()
            #expect(serverResults.count == 1)

            // Test .cached
            let cachedResults = try await context.fetch(CachePolicyEntity.self)
                .cachePolicy(.cached)
                .where(CachePolicyEntity.fields.id == testId)
                .execute()
            #expect(cachedResults.count == 1)

            // Test .stale(.seconds(60))
            let staleResults = try await context.fetch(CachePolicyEntity.self)
                .cachePolicy(.stale(.seconds(60)))
                .where(CachePolicyEntity.fields.id == testId)
                .execute()
            #expect(staleResults.count == 1)
        }
    }

    // MARK: - model(for:as:) CachePolicy Tests

    @Test("model(for:as:) with default cachePolicy uses .server")
    func modelDefaultCachePolicy() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()

            let schema = try Schema(
                entities: [try CachePolicyEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            )
            let container = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: database), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(CachePolicyEntity.self)]), security: .testingDisabled)
            let context = container.testBaseContext()

            // Insert test data
            let testId = "model-default-\(UUID().uuidString.prefix(8))"
            let model = CachePolicyEntity(id: testId, value: 500)
            try context.insert(model)
            try await context.save()

            // model(for:as:) with default should work
            let result = try await context.model(for: testId, as: CachePolicyEntity.self)
            #expect(result != nil)
            #expect(result?.value == 500)
        }
    }

    @Test("model(for:as:) with .cached uses ReadVersionCache")
    func modelWithCachedPolicy() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()

            let schema = try Schema(
                entities: [try CachePolicyEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            )
            let container = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: database), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(CachePolicyEntity.self)]), security: .testingDisabled)
            let context = container.testBaseContext()

            // Insert test data
            let testId = "model-cached-\(UUID().uuidString.prefix(8))"
            let model = CachePolicyEntity(id: testId, value: 600)
            try context.insert(model)
            try await context.save()

            // First fetch with .cached
            let result1 = try await context.model(
                for: testId,
                as: CachePolicyEntity.self,
                cachePolicy: .cached
            )
            #expect(result1 != nil)

            // Cache should be populated after transaction
            let cacheInfo = context.readVersionCacheInfo()
            #expect(cacheInfo != nil)

            // Second fetch with .cached should use cached version
            let result2 = try await context.model(
                for: testId,
                as: CachePolicyEntity.self,
                cachePolicy: .cached
            )
            #expect(result2 != nil)
            #expect(result2?.value == 600)
        }
    }

    @Test("model(for:as:) with .stale uses cache within window")
    func modelWithStalePolicy() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()

            let schema = try Schema(
                entities: [try CachePolicyEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            )
            let container = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: database), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(CachePolicyEntity.self)]), security: .testingDisabled)
            let context = container.testBaseContext()

            // Insert test data
            let testId = "model-stale-\(UUID().uuidString.prefix(8))"
            let model = CachePolicyEntity(id: testId, value: 700)
            try context.insert(model)
            try await context.save()

            // Fetch with .stale(.seconds(60)) - should work within 60 second window
            let result = try await context.model(
                for: testId,
                as: CachePolicyEntity.self,
                cachePolicy: .stale(.seconds(60))
            )
            #expect(result != nil)
            #expect(result?.value == 700)
        }
    }

    @Test("model(for:as:) returns nil for non-existent ID")
    func modelReturnsNilForNonExistent() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()

            let schema = try Schema(
                entities: [try CachePolicyEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            )
            let container = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: database), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(CachePolicyEntity.self)]), security: .testingDisabled)
            let context = container.testBaseContext()

            // Try to fetch non-existent ID with various cache policies
            let result1 = try await context.model(for: "non-existent-id", as: CachePolicyEntity.self)
            #expect(result1 == nil)

            let result2 = try await context.model(
                for: "non-existent-id",
                as: CachePolicyEntity.self,
                cachePolicy: .cached
            )
            #expect(result2 == nil)
        }
    }

    // MARK: - Test Model

    @Persistable
    struct CachePolicyEntity {
        #Directory<CachePolicyEntity>("test", "cachepolicy")

        var id: String = UUID().uuidString
        var value: Int64 = 0

        #Index(.scalar, fields: [\CachePolicyEntity.value])
    }
}
#endif

#endif
