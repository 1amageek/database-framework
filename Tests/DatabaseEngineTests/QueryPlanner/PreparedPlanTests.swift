// PreparedPlanTests.swift
// Tests for PreparedPlan, PlanCache, and QueryFingerprint

import Testing
import Foundation
@testable import DatabaseEngine
@testable import ScalarIndex
@testable import Core

// Re-use QPTestUser from QueryPlannerTests.swift

@Suite("PreparedPlan Tests")
struct PreparedPlanTests {

    // MARK: - QueryFingerprint Tests

    @Test("Same query structure produces same fingerprint")
    func testSameStructureSameFingerprint() {
        let builder = QueryFingerprintBuilder<QPTestUser>()

        // Two queries with same structure but different values
        var query1 = Query<QPTestUser>()
        query1 = query1.where(\QPTestUser.age == 25)
        query1 = query1.orderBy(\QPTestUser.name)

        var query2 = Query<QPTestUser>()
        query2 = query2.where(\QPTestUser.age == 30)
        query2 = query2.orderBy(\QPTestUser.name)

        let fingerprint1 = builder.build(from: query1)
        let fingerprint2 = builder.build(from: query2)

        #expect(fingerprint1 == fingerprint2)
    }

    @Test("Different query structure produces different fingerprint")
    func testDifferentStructureDifferentFingerprint() {
        let builder = QueryFingerprintBuilder<QPTestUser>()

        var query1 = Query<QPTestUser>()
        query1 = query1.where(\QPTestUser.age == 25)

        var query2 = Query<QPTestUser>()
        query2 = query2.where(\QPTestUser.name == "Alice")

        let fingerprint1 = builder.build(from: query1)
        let fingerprint2 = builder.build(from: query2)

        #expect(fingerprint1 != fingerprint2)
    }

    @Test("Different operators produce different fingerprint")
    func testDifferentOperatorsDifferentFingerprint() {
        let builder = QueryFingerprintBuilder<QPTestUser>()

        var query1 = Query<QPTestUser>()
        query1 = query1.where(\QPTestUser.age == 25)

        var query2 = Query<QPTestUser>()
        query2 = query2.where(\QPTestUser.age > 25)

        let fingerprint1 = builder.build(from: query1)
        let fingerprint2 = builder.build(from: query2)

        #expect(fingerprint1 != fingerprint2)
    }

    @Test("Fingerprint includes limit/offset presence")
    func testFingerprintIncludesLimitOffset() {
        let builder = QueryFingerprintBuilder<QPTestUser>()

        var query1 = Query<QPTestUser>()
        query1 = query1.where(\QPTestUser.age > 18)

        var query2 = Query<QPTestUser>()
        query2 = query2.where(\QPTestUser.age > 18)
        query2 = query2.limit(10)

        let fingerprint1 = builder.build(from: query1)
        let fingerprint2 = builder.build(from: query2)

        #expect(fingerprint1.hasLimit == false)
        #expect(fingerprint2.hasLimit == true)
        #expect(fingerprint1 != fingerprint2)
    }

    @Test("Fingerprint includes sort structure")
    func testFingerprintIncludesSortStructure() {
        let builder = QueryFingerprintBuilder<QPTestUser>()

        var query1 = Query<QPTestUser>()
        query1 = query1.orderBy(\QPTestUser.name)

        var query2 = Query<QPTestUser>()
        query2 = query2.orderBy(\QPTestUser.age)

        let fingerprint1 = builder.build(from: query1)
        let fingerprint2 = builder.build(from: query2)

        #expect(fingerprint1 != fingerprint2)
        #expect(fingerprint1.sortStructure.contains("name"))
        #expect(fingerprint2.sortStructure.contains("age"))
    }

    // MARK: - PlanCache Tests

    @Test("Cache stores and retrieves plans")
    func testCacheStoresAndRetrievesPlan() throws {
        let cache = PlanCache(maxSize: 100, ttl: nil)
        let planner = QueryPlanner<QPTestUser>(indexes: QPTestUser.indexDescriptors)

        var query = Query<QPTestUser>()
        query = query.where(\QPTestUser.age > 18)

        let prepared = try planner.prepare(query: query, cache: cache)

        let fingerprint = QueryFingerprintBuilder<QPTestUser>().build(from: query)
        let retrieved: PreparedPlan<QPTestUser>? = cache.get(fingerprint: fingerprint, type: QPTestUser.self)

        #expect(retrieved != nil)
        #expect(retrieved?.id == prepared.id)
    }

    @Test("Cache hit increments hit count")
    func testCacheHitCount() throws {
        let cache = PlanCache(maxSize: 100, ttl: nil)
        let planner = QueryPlanner<QPTestUser>(indexes: QPTestUser.indexDescriptors)

        var query = Query<QPTestUser>()
        query = query.where(\QPTestUser.email == "test@example.com")

        let _ = try planner.prepare(query: query, cache: cache)
        let fingerprint = QueryFingerprintBuilder<QPTestUser>().build(from: query)

        // Retrieve multiple times
        let _: PreparedPlan<QPTestUser>? = cache.get(fingerprint: fingerprint, type: QPTestUser.self)
        let _: PreparedPlan<QPTestUser>? = cache.get(fingerprint: fingerprint, type: QPTestUser.self)
        let _: PreparedPlan<QPTestUser>? = cache.get(fingerprint: fingerprint, type: QPTestUser.self)

        let stats = cache.statistics
        #expect(stats.hitCount == 3)
    }

    @Test("Cache miss increments miss count")
    func testCacheMissCount() {
        let cache = PlanCache(maxSize: 100, ttl: nil)

        let fingerprint = QueryFingerprint(
            typeName: "QPTestUser",
            conditionStructure: "nonexistent",
            sortStructure: "",
            hasLimit: false,
            hasOffset: false
        )

        let _: PreparedPlan<QPTestUser>? = cache.get(fingerprint: fingerprint, type: QPTestUser.self)
        let _: PreparedPlan<QPTestUser>? = cache.get(fingerprint: fingerprint, type: QPTestUser.self)

        let stats = cache.statistics
        #expect(stats.missCount == 2)
        #expect(stats.hitCount == 0)
    }

    @Test("Cache evicts LRU entry when full")
    func testCacheEvictsLRU() throws {
        let cache = PlanCache(maxSize: 2, ttl: nil)
        let planner = QueryPlanner<QPTestUser>(indexes: QPTestUser.indexDescriptors)

        // Add 3 plans with DIFFERENT structures to a cache of size 2
        // (same field + same op = same fingerprint, so use different fields/ops)
        var query1 = Query<QPTestUser>()
        query1 = query1.where(\QPTestUser.age == 20)
        let prepared1 = try planner.prepare(query: query1, cache: cache)

        var query2 = Query<QPTestUser>()
        query2 = query2.where(\QPTestUser.email == "test@example.com")
        let _ = try planner.prepare(query: query2, cache: cache)

        var query3 = Query<QPTestUser>()
        query3 = query3.where(\QPTestUser.name == "Alice")
        let _ = try planner.prepare(query: query3, cache: cache)

        // First plan should be evicted (LRU)
        let retrieved: PreparedPlan<QPTestUser>? = cache.get(fingerprint: prepared1.fingerprint, type: QPTestUser.self)
        #expect(retrieved == nil)

        #expect(cache.statistics.size == 2)
    }

    @Test("Cache clear removes all entries")
    func testCacheClear() throws {
        let cache = PlanCache(maxSize: 100, ttl: nil)
        let planner = QueryPlanner<QPTestUser>(indexes: QPTestUser.indexDescriptors)

        var query = Query<QPTestUser>()
        query = query.where(\QPTestUser.age > 18)
        let prepared = try planner.prepare(query: query, cache: cache)

        #expect(cache.statistics.size == 1)

        cache.clear()

        #expect(cache.statistics.size == 0)
        let retrieved: PreparedPlan<QPTestUser>? = cache.get(fingerprint: prepared.fingerprint, type: QPTestUser.self)
        #expect(retrieved == nil)
    }

    @Test("Cache invalidate removes type-specific entries")
    func testCacheInvalidate() throws {
        let cache = PlanCache(maxSize: 100, ttl: nil)
        let planner = QueryPlanner<QPTestUser>(indexes: QPTestUser.indexDescriptors)

        var query = Query<QPTestUser>()
        query = query.where(\QPTestUser.age > 18)
        let _ = try planner.prepare(query: query, cache: cache)

        #expect(cache.statistics.size == 1)

        cache.invalidate(typeName: "QPTestUser")

        #expect(cache.statistics.size == 0)
    }

    // MARK: - PreparedPlan Tests

    @Test("PreparedPlan has correct metadata")
    func testPreparedPlanMetadata() throws {
        let planner = QueryPlanner<QPTestUser>(indexes: QPTestUser.indexDescriptors)

        var query = Query<QPTestUser>()
        query = query.where(\QPTestUser.email == "test@example.com")
        query = query.orderBy(\QPTestUser.name)
        query = query.limit(10)

        let prepared = try planner.prepare(query: query)

        #expect(prepared.fingerprint.typeName == "QPTestUser")
        #expect(prepared.fingerprint.hasLimit == true)
        #expect(prepared.createdAt <= Date())
    }

    @Test("PreparedPlan reuses cached plan")
    func testPreparedPlanReusesCachedPlan() throws {
        let cache = PlanCache(maxSize: 100, ttl: nil)
        let planner = QueryPlanner<QPTestUser>(indexes: QPTestUser.indexDescriptors)

        var query1 = Query<QPTestUser>()
        query1 = query1.where(\QPTestUser.age == 25)

        var query2 = Query<QPTestUser>()
        query2 = query2.where(\QPTestUser.age == 30)

        let prepared1 = try planner.prepare(query: query1, cache: cache)
        let prepared2 = try planner.prepare(query: query2, cache: cache)

        // Both should have the same plan ID (reused from cache)
        #expect(prepared1.id == prepared2.id)
        #expect(cache.statistics.hitCount == 1)
    }

    // MARK: - PlanValidator Tests

    @Test("Validator accepts valid plan")
    func testValidatorAcceptsValidPlan() throws {
        let planner = QueryPlanner<QPTestUser>(indexes: QPTestUser.indexDescriptors)

        var query = Query<QPTestUser>()
        query = query.where(\QPTestUser.age > 18)

        let prepared = try planner.prepare(query: query)

        let validator = PlanValidator<QPTestUser>(availableIndexes: QPTestUser.indexDescriptors)
        #expect(validator.isValid(prepared))
    }

    @Test("Validator rejects plan with missing index")
    func testValidatorRejectsMissingIndex() throws {
        let planner = QueryPlanner<QPTestUser>(indexes: QPTestUser.indexDescriptors)

        var query = Query<QPTestUser>()
        query = query.where(\QPTestUser.email == "test@example.com")

        let prepared = try planner.prepare(query: query)

        // Validate with empty indexes (simulating index drop)
        let validator = PlanValidator<QPTestUser>(availableIndexes: [])

        // If the plan uses idx_email, it should be invalid
        if !prepared.planTemplate.usedIndexes.isEmpty {
            #expect(!validator.isValid(prepared))
        }
    }

    // MARK: - ParameterType Tests

    @Test("ParameterType matches correct types")
    func testParameterTypeMatches() {
        #expect(ParameterType.string.matches("hello"))
        #expect(ParameterType.int.matches(42))
        #expect(ParameterType.int64.matches(Int64(42)))
        #expect(ParameterType.double.matches(3.14))
        #expect(ParameterType.bool.matches(true))
        #expect(ParameterType.date.matches(Date()))
        #expect(ParameterType.data.matches(Data()))
        #expect(ParameterType.any.matches("anything"))
        #expect(ParameterType.any.matches(123))
    }

    @Test("ParameterType rejects incorrect types")
    func testParameterTypeRejectsIncorrect() {
        #expect(!ParameterType.string.matches(42))
        #expect(!ParameterType.int.matches("hello"))
        #expect(!ParameterType.bool.matches("true"))
        #expect(!ParameterType.double.matches(42))
    }
}

// MARK: - ParameterBindingError Tests

@Suite("ParameterBindingError Tests")
struct ParameterBindingErrorTests {

    @Test("Error descriptions are informative")
    func testErrorDescriptions() {
        let missingError = ParameterBindingError.missingParameter(name: "userId")
        #expect(missingError.description.contains("userId"))
        #expect(missingError.description.contains("Missing"))

        let typeError = ParameterBindingError.typeMismatch(
            parameter: "age",
            expected: .int,
            actualType: "String"
        )
        #expect(typeError.description.contains("age"))
        #expect(typeError.description.contains("String"))

        let countError = ParameterBindingError.invalidParameterCount(expected: 2, actual: 1)
        #expect(countError.description.contains("2"))
        #expect(countError.description.contains("1"))
    }
}
