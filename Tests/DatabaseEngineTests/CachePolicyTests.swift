import Testing
import Foundation
import FoundationDB
@testable import DatabaseEngine
@testable import Core

/// Tests for CachePolicy
///
/// **Coverage**:
/// - CachePolicy enum values
/// - ReadVersionCache integration with CachePolicy
/// - Query.cachePolicy fluent API
/// - QueryExecutor.cachePolicy fluent API
/// - FDBContext.fetch() cache integration
@Suite("CachePolicy Tests", .serialized)
struct CachePolicyTests {

    // MARK: - CachePolicy + ReadVersionCache Tests

    @Test("CachePolicy.server returns nil from cache")
    func serverPolicyReturnsNilFromCache() {
        let cache = ReadVersionCache()
        cache.updateFromCommit(version: 12345)

        // .server should not use cache
        let result = cache.getCachedVersion(policy: .server)
        #expect(result == nil)
    }

    @Test("CachePolicy.cached returns cached version (no time limit)")
    func cachedPolicyReturnsCachedVersion() {
        let cache = ReadVersionCache()
        cache.updateFromCommit(version: 12345)

        // .cached should return cached version regardless of age
        let result = cache.getCachedVersion(policy: .cached)
        #expect(result == 12345)
    }

    @Test("CachePolicy.stale(N) returns version if fresh enough")
    func stalePolicyReturnsVersionIfFresh() {
        let cache = ReadVersionCache()
        cache.updateFromCommit(version: 12345)

        // .stale(30) should return cached version (just created, age < 30s)
        let result = cache.getCachedVersion(policy: .stale(30))
        #expect(result == 12345)
    }

    @Test("CachePolicy.stale(0) returns nil immediately")
    func staleZeroPolicyReturnsNil() {
        let cache = ReadVersionCache()
        cache.updateFromCommit(version: 12345)

        // .stale(0) should return nil (cache is already older than 0 seconds)
        let result = cache.getCachedVersion(policy: .stale(0))
        #expect(result == nil)
    }

    // MARK: - CachePolicy Description Tests

    @Test("CachePolicy descriptions are correct")
    func policyDescriptions() {
        #expect(CachePolicy.server.description == "CachePolicy.server")
        #expect(CachePolicy.cached.description == "CachePolicy.cached")
        #expect(CachePolicy.stale(30).description == "CachePolicy.stale(30s)")
        #expect(CachePolicy.stale(60).description == "CachePolicy.stale(60s)")
    }

    // MARK: - CachePolicy Equatable Tests

    @Test("CachePolicy is Equatable")
    func policyEquatable() {
        #expect(CachePolicy.server == CachePolicy.server)
        #expect(CachePolicy.cached == CachePolicy.cached)
        #expect(CachePolicy.stale(30) == CachePolicy.stale(30))
        #expect(CachePolicy.server != CachePolicy.cached)
        #expect(CachePolicy.stale(30) != CachePolicy.stale(60))
    }

    // MARK: - CachePolicy Hashable Tests

    @Test("CachePolicy is Hashable")
    func policyHashable() {
        var set = Set<CachePolicy>()
        set.insert(.server)
        set.insert(.cached)
        set.insert(.stale(30))
        set.insert(.stale(30))  // Duplicate
        #expect(set.count == 3)
    }

    // MARK: - Query.cachePolicy Tests

    @Test("Query.cachePolicy defaults to .server")
    func queryDefaultCachePolicy() {
        let query = Query<CachePolicyTestModel>()
        #expect(query.cachePolicy == .server)
    }

    @Test("Query.cachePolicy() fluent method sets policy")
    func queryCachePolicyFluent() {
        let query = Query<CachePolicyTestModel>()
            .cachePolicy(.server)
        #expect(query.cachePolicy == .server)

        let query2 = Query<CachePolicyTestModel>()
            .cachePolicy(.stale(60))
        #expect(query2.cachePolicy == .stale(60))
    }

    @Test("Query.cachePolicy() can be chained with other methods")
    func queryCachePolicyChaining() {
        let query = Query<CachePolicyTestModel>()
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
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let schema = Schema(
            [CachePolicyTestModel.self],
            version: Schema.Version(1, 0, 0)
        )
        let container = FDBContainer(database: database, schema: schema, security: .disabled)
        let context = container.newContext()

        // Create executor with cache policy
        let executor = context.fetch(CachePolicyTestModel.self)
            .cachePolicy(.server)

        // Verify the underlying query has the cache policy set
        #expect(executor.query.cachePolicy == .server)
    }

    @Test("QueryExecutor.cachePolicy() can be chained with filters")
    func executorCachePolicyChainingWithFilters() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let schema = Schema(
            [CachePolicyTestModel.self],
            version: Schema.Version(1, 0, 0)
        )
        let container = FDBContainer(database: database, schema: schema, security: .disabled)
        let context = container.newContext()

        // Chain cache policy with other query methods
        let executor = context.fetch(CachePolicyTestModel.self)
            .where(\.value > 10)
            .cachePolicy(.stale(30))
            .orderBy(\.value)
            .limit(5)

        #expect(executor.query.cachePolicy == .stale(30))
        #expect(executor.query.fetchLimit == 5)
    }

    // MARK: - FDBContext Integration Tests

    @Test("fetch() with .cached uses ReadVersionCache")
    func fetchWithCachedUsesCache() async throws {
        try await FDBTestEnvironment.shared.withSerializedAccess {
            let database = try FDBClient.openDatabase()

            let schema = Schema(
                [CachePolicyTestModel.self],
                version: Schema.Version(1, 0, 0)
            )
            let container = FDBContainer(database: database, schema: schema, security: .disabled)
            let context = container.newContext()

            // Insert test data
            let testId = "cache-test-\(UUID().uuidString.prefix(8))"
            let model = CachePolicyTestModel(id: testId, value: 42)
            context.insert(model)
            try await context.save()

            // First fetch with .cached - should populate cache
            _ = try await context.fetch(CachePolicyTestModel.self)
                .cachePolicy(.cached)
                .execute()

            // Cache should now have a version
            let cacheInfo1 = context.readVersionCacheInfo()
            #expect(cacheInfo1 != nil)

            // Second fetch with .cached - should use cached version
            _ = try await context.fetch(CachePolicyTestModel.self)
                .cachePolicy(.cached)
                .execute()

            // Cache should still be populated
            let cacheInfo2 = context.readVersionCacheInfo()
            #expect(cacheInfo2 != nil)
        }
    }

    @Test("fetch() with .server bypasses cache")
    func fetchWithServerBypassesCache() async throws {
        try await FDBTestEnvironment.shared.withSerializedAccess {
            let database = try FDBClient.openDatabase()

            let schema = Schema(
                [CachePolicyTestModel.self],
                version: Schema.Version(1, 0, 0)
            )
            let container = FDBContainer(database: database, schema: schema, security: .disabled)
            let context = container.newContext()

            // Insert test data
            let testId = "server-test-\(UUID().uuidString.prefix(8))"
            let model = CachePolicyTestModel(id: testId, value: 100)
            context.insert(model)
            try await context.save()

            // First fetch with .server
            let results = try await context.fetch(CachePolicyTestModel.self)
                .cachePolicy(.server)
                .where(\.id == testId)
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
        try await FDBTestEnvironment.shared.withSerializedAccess {
            let database = try FDBClient.openDatabase()

            let schema = Schema(
                [CachePolicyTestModel.self],
                version: Schema.Version(1, 0, 0)
            )
            let container = FDBContainer(database: database, schema: schema, security: .disabled)
            let context = container.newContext()

            // Insert test data
            let testId = "count-test-\(UUID().uuidString.prefix(8))"
            let model = CachePolicyTestModel(id: testId, value: 200)
            context.insert(model)
            try await context.save()

            // Count with .cached policy
            let count = try await context.fetch(CachePolicyTestModel.self)
                .cachePolicy(.cached)
                .where(\.id == testId)
                .count()

            #expect(count == 1)
        }
    }

    @Test("default cachePolicy is .server for new queries")
    func defaultCachePolicyIsServer() {
        // Query default
        let query = Query<CachePolicyTestModel>()
        #expect(query.cachePolicy == .server)
    }

    @Test("fetch() executes correctly with all CachePolicy values")
    func fetchWithAllPolicyValues() async throws {
        try await FDBTestEnvironment.shared.withSerializedAccess {
            let database = try FDBClient.openDatabase()

            let schema = Schema(
                [CachePolicyTestModel.self],
                version: Schema.Version(1, 0, 0)
            )
            let container = FDBContainer(database: database, schema: schema, security: .disabled)
            let context = container.newContext()

            // Insert test data
            let testId = "all-policies-\(UUID().uuidString.prefix(8))"
            let model = CachePolicyTestModel(id: testId, value: 300)
            context.insert(model)
            try await context.save()

            // Test .server
            let serverResults = try await context.fetch(CachePolicyTestModel.self)
                .cachePolicy(.server)
                .where(\.id == testId)
                .execute()
            #expect(serverResults.count == 1)

            // Test .cached
            let cachedResults = try await context.fetch(CachePolicyTestModel.self)
                .cachePolicy(.cached)
                .where(\.id == testId)
                .execute()
            #expect(cachedResults.count == 1)

            // Test .stale(60)
            let staleResults = try await context.fetch(CachePolicyTestModel.self)
                .cachePolicy(.stale(60))
                .where(\.id == testId)
                .execute()
            #expect(staleResults.count == 1)
        }
    }

    // MARK: - model(for:as:) CachePolicy Tests

    @Test("model(for:as:) with default cachePolicy uses .server")
    func modelDefaultCachePolicy() async throws {
        try await FDBTestEnvironment.shared.withSerializedAccess {
            let database = try FDBClient.openDatabase()

            let schema = Schema(
                [CachePolicyTestModel.self],
                version: Schema.Version(1, 0, 0)
            )
            let container = FDBContainer(database: database, schema: schema, security: .disabled)
            let context = container.newContext()

            // Insert test data
            let testId = "model-default-\(UUID().uuidString.prefix(8))"
            let model = CachePolicyTestModel(id: testId, value: 500)
            context.insert(model)
            try await context.save()

            // model(for:as:) with default should work
            let result = try await context.model(for: testId, as: CachePolicyTestModel.self)
            #expect(result != nil)
            #expect(result?.value == 500)
        }
    }

    @Test("model(for:as:) with .cached uses ReadVersionCache")
    func modelWithCachedPolicy() async throws {
        try await FDBTestEnvironment.shared.withSerializedAccess {
            let database = try FDBClient.openDatabase()

            let schema = Schema(
                [CachePolicyTestModel.self],
                version: Schema.Version(1, 0, 0)
            )
            let container = FDBContainer(database: database, schema: schema, security: .disabled)
            let context = container.newContext()

            // Insert test data
            let testId = "model-cached-\(UUID().uuidString.prefix(8))"
            let model = CachePolicyTestModel(id: testId, value: 600)
            context.insert(model)
            try await context.save()

            // First fetch with .cached
            let result1 = try await context.model(
                for: testId,
                as: CachePolicyTestModel.self,
                cachePolicy: .cached
            )
            #expect(result1 != nil)

            // Cache should be populated after transaction
            let cacheInfo = context.readVersionCacheInfo()
            #expect(cacheInfo != nil)

            // Second fetch with .cached should use cached version
            let result2 = try await context.model(
                for: testId,
                as: CachePolicyTestModel.self,
                cachePolicy: .cached
            )
            #expect(result2 != nil)
            #expect(result2?.value == 600)
        }
    }

    @Test("model(for:as:) with .stale uses cache within window")
    func modelWithStalePolicy() async throws {
        try await FDBTestEnvironment.shared.withSerializedAccess {
            let database = try FDBClient.openDatabase()

            let schema = Schema(
                [CachePolicyTestModel.self],
                version: Schema.Version(1, 0, 0)
            )
            let container = FDBContainer(database: database, schema: schema, security: .disabled)
            let context = container.newContext()

            // Insert test data
            let testId = "model-stale-\(UUID().uuidString.prefix(8))"
            let model = CachePolicyTestModel(id: testId, value: 700)
            context.insert(model)
            try await context.save()

            // Fetch with .stale(60) - should work within 60 second window
            let result = try await context.model(
                for: testId,
                as: CachePolicyTestModel.self,
                cachePolicy: .stale(60)
            )
            #expect(result != nil)
            #expect(result?.value == 700)
        }
    }

    @Test("model(for:as:) returns nil for non-existent ID")
    func modelReturnsNilForNonExistent() async throws {
        try await FDBTestEnvironment.shared.withSerializedAccess {
            let database = try FDBClient.openDatabase()

            let schema = Schema(
                [CachePolicyTestModel.self],
                version: Schema.Version(1, 0, 0)
            )
            let container = FDBContainer(database: database, schema: schema, security: .disabled)
            let context = container.newContext()

            // Try to fetch non-existent ID with various cache policies
            let result1 = try await context.model(for: "non-existent-id", as: CachePolicyTestModel.self)
            #expect(result1 == nil)

            let result2 = try await context.model(
                for: "non-existent-id",
                as: CachePolicyTestModel.self,
                cachePolicy: .cached
            )
            #expect(result2 == nil)
        }
    }

    // MARK: - Test Model

    @Persistable
    struct CachePolicyTestModel {
        #Directory<CachePolicyTestModel>("test", "cachepolicy")

        var id: String = ULID().ulidString
        var value: Int = 0

        init(id: String = ULID().ulidString, value: Int = 0) {
            self.id = id
            self.value = value
        }

        #Index(ScalarIndexKind<CachePolicyTestModel>(fields: [\.value]))
    }
}
