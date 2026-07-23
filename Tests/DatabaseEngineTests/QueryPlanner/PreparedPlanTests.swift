#if !os(WASI)
#if FOUNDATION_DB
// PreparedPlanTests.swift
// Tests for PreparedPlan, PlanCache, and QueryFingerprint

import Testing
import TestHeartbeat
import Foundation
@testable import DatabaseEngine
@testable import ScalarIndex
@testable import Core

// Re-use QueryPlannerUser from QueryPlannerTests.swift

@Suite("PreparedPlan Tests", .heartbeat)
struct PreparedPlanTests {

    // MARK: - QueryFingerprint Tests

    @Test("Different literal values produce different fingerprints")
    func testLiteralValuesProduceDifferentFingerprints() {
        let builder = QueryFingerprintBuilder<QueryPlannerUser>()

        // Two queries with same structure but different values
        var query1 = Query<QueryPlannerUser>()
        query1 = query1.where(\QueryPlannerUser.age == 25)
        query1 = query1.orderBy(\QueryPlannerUser.name)

        var query2 = Query<QueryPlannerUser>()
        query2 = query2.where(\QueryPlannerUser.age == 30)
        query2 = query2.orderBy(\QueryPlannerUser.name)

        let fingerprint1 = builder.build(from: query1)
        let fingerprint2 = builder.build(from: query2)

        #expect(fingerprint1 != fingerprint2)
    }

    @Test("Different query structure produces different fingerprint")
    func testDifferentStructureDifferentFingerprint() {
        let builder = QueryFingerprintBuilder<QueryPlannerUser>()

        var query1 = Query<QueryPlannerUser>()
        query1 = query1.where(\QueryPlannerUser.age == 25)

        var query2 = Query<QueryPlannerUser>()
        query2 = query2.where(\QueryPlannerUser.name == "Alice")

        let fingerprint1 = builder.build(from: query1)
        let fingerprint2 = builder.build(from: query2)

        #expect(fingerprint1 != fingerprint2)
    }

    @Test("Different operators produce different fingerprint")
    func testDifferentOperatorsDifferentFingerprint() {
        let builder = QueryFingerprintBuilder<QueryPlannerUser>()

        var query1 = Query<QueryPlannerUser>()
        query1 = query1.where(\QueryPlannerUser.age == 25)

        var query2 = Query<QueryPlannerUser>()
        query2 = query2.where(\QueryPlannerUser.age > 25)

        let fingerprint1 = builder.build(from: query1)
        let fingerprint2 = builder.build(from: query2)

        #expect(fingerprint1 != fingerprint2)
    }

    @Test("Fingerprint includes exact limit and offset")
    func testFingerprintIncludesExactLimitAndOffset() {
        let builder = QueryFingerprintBuilder<QueryPlannerUser>()

        var query1 = Query<QueryPlannerUser>()
        query1 = query1.where(\QueryPlannerUser.age > 18)

        var query2 = Query<QueryPlannerUser>()
        query2 = query2.where(\QueryPlannerUser.age > 18)
        query2 = query2.limit(10)

        let fingerprint1 = builder.build(from: query1)
        let fingerprint2 = builder.build(from: query2)

        #expect(fingerprint1.fetchLimit == nil)
        #expect(fingerprint2.fetchLimit == 10)
        #expect(fingerprint1 != fingerprint2)

        let query3 = query1.limit(20).offset(5)
        let fingerprint3 = builder.build(from: query3)
        #expect(fingerprint3.fetchLimit == 20)
        #expect(fingerprint3.fetchOffset == 5)
        #expect(fingerprint2 != fingerprint3)
    }

    @Test("Fingerprint includes sort structure")
    func testFingerprintIncludesSortStructure() {
        let builder = QueryFingerprintBuilder<QueryPlannerUser>()

        var query1 = Query<QueryPlannerUser>()
        query1 = query1.orderBy(\QueryPlannerUser.name)

        var query2 = Query<QueryPlannerUser>()
        query2 = query2.orderBy(\QueryPlannerUser.age)

        let fingerprint1 = builder.build(from: query1)
        let fingerprint2 = builder.build(from: query2)

        #expect(fingerprint1 != fingerprint2)
        #expect(fingerprint1.sorting.map(\.fieldName) == ["name"])
        #expect(fingerprint2.sorting.map(\.fieldName) == ["age"])
    }

    // MARK: - PlanCache Tests

    @Test("Cache stores and retrieves plans")
    func testCacheStoresAndRetrievesPlan() throws {
        let cache = PlanCache(maxSize: 100, ttl: nil)
        let planner = QueryPlanner<QueryPlannerUser>(indexes: QueryPlannerUser.indexDescriptors)

        var query = Query<QueryPlannerUser>()
        query = query.where(\QueryPlannerUser.age > 18)

        let prepared = try planner.prepare(query: query, cache: cache)

        let fingerprint = QueryFingerprintBuilder<QueryPlannerUser>().build(from: query)
        let retrieved: PreparedPlan<QueryPlannerUser>? = cache.get(fingerprint: fingerprint, type: QueryPlannerUser.self)

        #expect(retrieved != nil)
        #expect(retrieved?.id == prepared.id)
    }

    @Test("Cache hit increments hit count")
    func testCacheHitCount() throws {
        let cache = PlanCache(maxSize: 100, ttl: nil)
        let planner = QueryPlanner<QueryPlannerUser>(indexes: QueryPlannerUser.indexDescriptors)

        var query = Query<QueryPlannerUser>()
        query = query.where(\QueryPlannerUser.email == "test@example.com")

        let _ = try planner.prepare(query: query, cache: cache)
        let fingerprint = QueryFingerprintBuilder<QueryPlannerUser>().build(from: query)

        // Retrieve multiple times
        let _: PreparedPlan<QueryPlannerUser>? = cache.get(fingerprint: fingerprint, type: QueryPlannerUser.self)
        let _: PreparedPlan<QueryPlannerUser>? = cache.get(fingerprint: fingerprint, type: QueryPlannerUser.self)
        let _: PreparedPlan<QueryPlannerUser>? = cache.get(fingerprint: fingerprint, type: QueryPlannerUser.self)

        let stats = cache.statistics
        #expect(stats.hitCount == 3)
    }

    @Test("Cache miss increments miss count")
    func testCacheMissCount() {
        let cache = PlanCache(maxSize: 100, ttl: nil)

        let fingerprint = QueryFingerprint(
            typeName: "QueryPlannerUser",
            predicates: [.alwaysFalse],
            sorting: [],
            fetchLimit: nil,
            fetchOffset: nil
        )

        let _: PreparedPlan<QueryPlannerUser>? = cache.get(fingerprint: fingerprint, type: QueryPlannerUser.self)
        let _: PreparedPlan<QueryPlannerUser>? = cache.get(fingerprint: fingerprint, type: QueryPlannerUser.self)

        let stats = cache.statistics
        #expect(stats.missCount == 2)
        #expect(stats.hitCount == 0)
    }

    @Test("Cache evicts LRU entry when full")
    func testCacheEvictsLRU() throws {
        let cache = PlanCache(maxSize: 2, ttl: nil)
        let planner = QueryPlanner<QueryPlannerUser>(indexes: QueryPlannerUser.indexDescriptors)

        // Add 3 plans with DIFFERENT structures to a cache of size 2
        // (same field + same op = same fingerprint, so use different fields/ops)
        var query1 = Query<QueryPlannerUser>()
        query1 = query1.where(\QueryPlannerUser.age == 20)
        let prepared1 = try planner.prepare(query: query1, cache: cache)

        var query2 = Query<QueryPlannerUser>()
        query2 = query2.where(\QueryPlannerUser.email == "test@example.com")
        let _ = try planner.prepare(query: query2, cache: cache)

        var query3 = Query<QueryPlannerUser>()
        query3 = query3.where(\QueryPlannerUser.name == "Alice")
        let _ = try planner.prepare(query: query3, cache: cache)

        // First plan should be evicted (LRU)
        let retrieved: PreparedPlan<QueryPlannerUser>? = cache.get(fingerprint: prepared1.fingerprint, type: QueryPlannerUser.self)
        #expect(retrieved == nil)

        #expect(cache.statistics.size == 2)
    }

    @Test("Cache clear removes all entries")
    func testCacheClear() throws {
        let cache = PlanCache(maxSize: 100, ttl: nil)
        let planner = QueryPlanner<QueryPlannerUser>(indexes: QueryPlannerUser.indexDescriptors)

        var query = Query<QueryPlannerUser>()
        query = query.where(\QueryPlannerUser.age > 18)
        let prepared = try planner.prepare(query: query, cache: cache)

        #expect(cache.statistics.size == 1)

        cache.clear()

        #expect(cache.statistics.size == 0)
        let retrieved: PreparedPlan<QueryPlannerUser>? = cache.get(fingerprint: prepared.fingerprint, type: QueryPlannerUser.self)
        #expect(retrieved == nil)
    }

    @Test("Cache invalidate removes type-specific entries")
    func testCacheInvalidate() throws {
        let cache = PlanCache(maxSize: 100, ttl: nil)
        let planner = QueryPlanner<QueryPlannerUser>(indexes: QueryPlannerUser.indexDescriptors)

        var query = Query<QueryPlannerUser>()
        query = query.where(\QueryPlannerUser.age > 18)
        let _ = try planner.prepare(query: query, cache: cache)

        #expect(cache.statistics.size == 1)

        cache.invalidate(typeName: "QueryPlannerUser")

        #expect(cache.statistics.size == 0)
    }

    // MARK: - PreparedPlan Tests

    @Test("PreparedPlan has correct metadata")
    func testPreparedPlanMetadata() throws {
        let planner = QueryPlanner<QueryPlannerUser>(indexes: QueryPlannerUser.indexDescriptors)

        var query = Query<QueryPlannerUser>()
        query = query.where(\QueryPlannerUser.email == "test@example.com")
        query = query.orderBy(\QueryPlannerUser.name)
        query = query.limit(10)

        let prepared = try planner.prepare(query: query)

        #expect(prepared.fingerprint.typeName == "QueryPlannerUser")
        #expect(prepared.fingerprint.fetchLimit == 10)
        #expect(prepared.createdAt <= Date())
    }

    @Test("PreparedPlan never reuses a plan with different literals")
    func testPreparedPlanDoesNotReuseDifferentLiterals() throws {
        let cache = PlanCache(maxSize: 100, ttl: nil)
        let planner = QueryPlanner<QueryPlannerUser>(indexes: QueryPlannerUser.indexDescriptors)

        var query1 = Query<QueryPlannerUser>()
        query1 = query1.where(\QueryPlannerUser.age == 25)

        var query2 = Query<QueryPlannerUser>()
        query2 = query2.where(\QueryPlannerUser.age == 30)

        let prepared1 = try planner.prepare(query: query1, cache: cache)
        let prepared2 = try planner.prepare(query: query2, cache: cache)

        #expect(prepared1.id != prepared2.id)
        #expect(prepared1.fingerprint != prepared2.fingerprint)
        #expect(cache.statistics.hitCount == 0)
        #expect(cache.statistics.size == 2)
    }

    @Test("PreparedPlan reuses an exact query")
    func testPreparedPlanReusesExactQuery() throws {
        let cache = PlanCache(maxSize: 100, ttl: nil)
        let planner = QueryPlanner<QueryPlannerUser>(
            indexes: QueryPlannerUser.indexDescriptors
        )
        let query = Query<QueryPlannerUser>()
            .where(\QueryPlannerUser.age == 25)

        let first = try planner.prepare(query: query, cache: cache)
        let second = try planner.prepare(query: query, cache: cache)

        #expect(first.id == second.id)
        #expect(cache.statistics.hitCount == 1)
    }

    // MARK: - PlanValidator Tests

    @Test("Validator accepts valid plan")
    func testValidatorAcceptsValidPlan() throws {
        let planner = QueryPlanner<QueryPlannerUser>(indexes: QueryPlannerUser.indexDescriptors)

        var query = Query<QueryPlannerUser>()
        query = query.where(\QueryPlannerUser.age > 18)

        let prepared = try planner.prepare(query: query)

        let validator = PlanValidator<QueryPlannerUser>(availableIndexes: QueryPlannerUser.indexDescriptors)
        #expect(validator.isValid(prepared))
    }

    @Test("Validator rejects plan with missing index")
    func testValidatorRejectsMissingIndex() throws {
        let planner = QueryPlanner<QueryPlannerUser>(indexes: QueryPlannerUser.indexDescriptors)

        var query = Query<QueryPlannerUser>()
        query = query.where(\QueryPlannerUser.email == "test@example.com")

        let prepared = try planner.prepare(query: query)

        // Validate with empty indexes (simulating index drop)
        let validator = PlanValidator<QueryPlannerUser>(availableIndexes: [])

        // If the plan uses idx_email, it should be invalid
        if !prepared.planTemplate.usedIndexes.isEmpty {
            #expect(!validator.isValid(prepared))
        }
    }

}
#endif

#endif
