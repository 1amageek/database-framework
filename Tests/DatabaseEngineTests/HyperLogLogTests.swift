import DatabaseTypes
import Testing
@testable import DatabaseEngine

@Suite("HyperLogLog Tests")
struct HyperLogLogTests {

    // MARK: - Initialization

    @Test("Empty HyperLogLog has zero cardinality")
    func testEmptyCardinality() throws {
        let hll = HyperLogLog()
        #expect(try hll.cardinality() == 0)
        #expect(hll.isEmpty == true)
    }

    // MARK: - Basic Operations

    @Test("Single element cardinality")
    func testSingleElement() throws {
        var hll = HyperLogLog()
        try hll.add(.string("test"))

        let cardinality = try hll.cardinality()
        #expect(cardinality >= 1)
        #expect(hll.isEmpty == false)
    }

    @Test("Same element added multiple times")
    func testDuplicateElements() throws {
        var hll = HyperLogLog()

        for _ in 0..<100 {
            try hll.add(.string("same"))
        }

        let cardinality = try hll.cardinality()
        // Should be approximately 1, with some tolerance for HLL error
        #expect(cardinality >= 1)
        #expect(cardinality <= 3)  // Allow some error margin
    }

    @Test("Multiple distinct elements")
    func testDistinctElements() throws {
        var hll = HyperLogLog()
        let expectedCount = 10000

        // Use string values for better hash distribution
        for i in 0..<expectedCount {
            try hll.add(.string("user_\(i)_email@example.com"))
        }

        let cardinality = try hll.cardinality()

        // HyperLogLog has ~2% error rate, but allow 20% margin for hash quality variations
        let lowerBound = Int64(Double(expectedCount) * 0.80)
        let upperBound = Int64(Double(expectedCount) * 1.20)

        #expect(cardinality >= lowerBound)
        #expect(cardinality <= upperBound)
    }

    // MARK: - Different Field Types

    @Test("Add different field value types")
    func testDifferentTypes() throws {
        var hll = HyperLogLog()

        try hll.add(.int64(42))
        try hll.add(.float64(3.14))
        try hll.add(.string("hello"))
        try hll.add(.bool(true))
        try hll.add(.bytes([1, 2, 3]))
        try hll.add(.null)

        let cardinality = try hll.cardinality()
        #expect(cardinality >= 4)  // At least 4 distinct values
        #expect(cardinality <= 8)  // Allow some error margin
    }

    // MARK: - Merge Operations

    @Test("Merge two HyperLogLogs")
    func testMerge() throws {
        var hll1 = HyperLogLog()
        var hll2 = HyperLogLog()

        // Add 5000 unique to hll1
        for i in 0..<5000 {
            try hll1.add(.string("set1_user_\(i)"))
        }

        // Add 5000 unique to hll2 (different range)
        for i in 0..<5000 {
            try hll2.add(.string("set2_user_\(i)"))
        }

        // Merge
        try hll1.merge(hll2)

        let cardinality = try hll1.cardinality()

        // Should be approximately 10000, allow 25% margin
        let lowerBound: Int64 = 7500
        let upperBound: Int64 = 12500

        #expect(cardinality >= lowerBound)
        #expect(cardinality <= upperBound)
    }

    @Test("Merge with overlapping elements")
    func testMergeOverlapping() throws {
        var hll1 = HyperLogLog()
        var hll2 = HyperLogLog()

        // Add unique users to hll1 (shared prefix "common_")
        for i in 0..<5000 {
            try hll1.add(.string("common_user_\(i)"))
        }

        // Add some overlapping and some new users to hll2
        for i in 2500..<7500 {
            try hll2.add(.string("common_user_\(i)"))
        }

        // Merge
        try hll1.merge(hll2)

        let cardinality = try hll1.cardinality()

        // Union is 0-7499 = 7500 distinct elements, allow 25% margin
        let lowerBound: Int64 = 5625
        let upperBound: Int64 = 9375

        #expect(cardinality >= lowerBound)
        #expect(cardinality <= upperBound)
    }

    @Test("Static merged function")
    func testStaticMerged() throws {
        var hll1 = HyperLogLog()
        var hll2 = HyperLogLog()

        for i in 0..<100 {
            try hll1.add(.int64(Int64(i)))
        }

        for i in 100..<200 {
            try hll2.add(.int64(Int64(i)))
        }

        let merged = try HyperLogLog.merged(hll1, hll2)

        let cardinality = try merged.cardinality()
        #expect(cardinality >= 180)
        #expect(cardinality <= 220)
    }

    // MARK: - Reset

    @Test("Reset clears the estimator")
    func testReset() throws {
        var hll = HyperLogLog()

        for i in 0..<100 {
            try hll.add(.int64(Int64(i)))
        }

        #expect(hll.isEmpty == false)

        hll.reset()

        #expect(hll.isEmpty == true)
        #expect(try hll.cardinality() == 0)
    }

    // MARK: - Register State

    @Test("Restoring borrowed register state preserves the estimator")
    func restoringRegisterStatePreservesEstimator() throws {
        var original = HyperLogLog()

        for i in 0..<500 {
            try original.add(.int64(Int64(i)))
        }

        let originalCardinality = try original.cardinality()
        let registers = original.withUnsafeRegisters { buffer in
            Array(buffer)
        }
        let restored = try HyperLogLog(
            precision: original.precision,
            registers: registers
        )

        let restoredCardinality = try restored.cardinality()

        #expect(restored == original)
        #expect(originalCardinality == restoredCardinality)
    }

    // MARK: - Statistics

    @Test("Estimated relative error")
    func testEstimatedRelativeError() {
        let hll = HyperLogLog()

        // With 16384 registers, error should be approximately 1.04 / sqrt(16384) ≈ 0.0081
        let error = hll.estimatedRelativeError
        #expect(error > 0.007)
        #expect(error < 0.009)
    }

    @Test("Memory size")
    func testMemorySize() {
        let hll = HyperLogLog()

        // 16384 registers × 1 byte = 16384 bytes
        #expect(hll.memorySizeInBytes == 16384)
    }

    @Test("Impossible register state fails instead of trapping")
    func testCardinalityOutOfRange() throws {
        let precision = 4
        let maximumRegister = UInt8(65 - precision)
        let hll = try HyperLogLog(
            precision: precision,
            registers: [UInt8](
                repeating: maximumRegister,
                count: 1 << precision
            )
        )

        #expect(throws: HyperLogLogError.cardinalityOutOfRange) {
            try hll.cardinality()
        }
    }

    // MARK: - Description

    @Test("CustomStringConvertible")
    func testDescription() throws {
        var hll = HyperLogLog()

        for i in 0..<100 {
            try hll.add(.int64(Int64(i)))
        }

        let description = hll.description
        #expect(description.contains("HyperLogLog"))
        #expect(description.contains("cardinality"))
        #expect(description.contains("error"))
    }

    // MARK: - Hash Stability

    @Test("Same value produces same hash")
    func testHashStability() throws {
        let value1 = FieldValue.string("test")
        let value2 = FieldValue.string("test")

        let hash1 = value1.stableHash()
        let hash2 = value2.stableHash()

        #expect(hash1 == hash2)
    }

    @Test("Different values produce different hashes")
    func testHashDifference() throws {
        let value1 = FieldValue.string("test1")
        let value2 = FieldValue.string("test2")

        let hash1 = value1.stableHash()
        let hash2 = value2.stableHash()

        #expect(hash1 != hash2)
    }
}
