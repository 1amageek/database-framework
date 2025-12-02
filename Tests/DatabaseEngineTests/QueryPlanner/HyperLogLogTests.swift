// HyperLogLogTests.swift
// Tests for HyperLogLog++ cardinality estimation

import Testing
import Foundation
@testable import DatabaseEngine
import Core

// Use type alias to disambiguate from Core.HyperLogLog
typealias HyperLogLogPP = DatabaseEngine.HyperLogLog

@Suite("HyperLogLog++ Tests")
struct HyperLogLogTests {

    // MARK: - Basic Functionality

    @Test("Empty HyperLogLog should return zero cardinality")
    func testEmptyHLL() {
        let hll = HyperLogLogPP()

        #expect(hll.cardinality() == 0)
        #expect(hll.isEmpty)
    }

    @Test("HyperLogLog should estimate single value correctly")
    func testSingleValue() {
        var hll = HyperLogLogPP()
        hll.add("hello")

        #expect(hll.cardinality() == 1)
        #expect(!hll.isEmpty)
    }

    @Test("HyperLogLog should handle duplicate values")
    func testDuplicateValues() {
        var hll = HyperLogLogPP()

        // Add same value multiple times
        for _ in 0..<1000 {
            hll.add("same")
        }

        #expect(hll.cardinality() == 1)
    }

    @Test("HyperLogLog should estimate small cardinality accurately")
    func testSmallCardinality() {
        var hll = HyperLogLogPP()
        let targetCount = 100

        for i in 0..<targetCount {
            hll.add(i)
        }

        let estimated = hll.cardinality()
        let error = abs(Double(estimated) - Double(targetCount)) / Double(targetCount)

        // For p=14, standard error is ~0.81%, allow 5% for small samples
        #expect(error < 0.05, "Estimated \(estimated), expected ~\(targetCount)")
    }

    @Test("HyperLogLog should estimate medium cardinality with good accuracy")
    func testMediumCardinality() {
        var hll = HyperLogLogPP()
        let targetCount = 10_000

        for i in 0..<targetCount {
            hll.add(i)
        }

        let estimated = hll.cardinality()
        let error = abs(Double(estimated) - Double(targetCount)) / Double(targetCount)

        // For p=14, standard error is ~0.81%, allow 3%
        #expect(error < 0.03, "Estimated \(estimated), expected ~\(targetCount), error: \(error * 100)%")
    }

    @Test("HyperLogLog should estimate large cardinality accurately")
    func testLargeCardinality() {
        var hll = HyperLogLogPP()
        let targetCount = 100_000

        for i in 0..<targetCount {
            hll.add(i)
        }

        let estimated = hll.cardinality()
        let error = abs(Double(estimated) - Double(targetCount)) / Double(targetCount)

        // For p=14, standard error is ~0.81%, allow 2%
        #expect(error < 0.02, "Estimated \(estimated), expected ~\(targetCount), error: \(error * 100)%")
    }

    // MARK: - Different Value Types

    @Test("HyperLogLog should handle Int64 values")
    func testInt64Values() {
        var hll = HyperLogLogPP()

        for i in 0..<1000 {
            hll.add(Int64(i))
        }

        let estimated = hll.cardinality()
        let error = abs(Double(estimated) - 1000.0) / 1000.0
        #expect(error < 0.05)
    }

    @Test("HyperLogLog should handle string values")
    func testStringValues() {
        var hll = HyperLogLogPP()

        for i in 0..<1000 {
            hll.add("value_\(i)")
        }

        let estimated = hll.cardinality()
        let error = abs(Double(estimated) - 1000.0) / 1000.0
        #expect(error < 0.05)
    }

    @Test("HyperLogLog should handle FieldValue types")
    func testFieldValueTypes() {
        var hll = HyperLogLogPP()

        // Add different FieldValue types
        for i in 0..<500 {
            hll.add(FieldValue.int64(Int64(i)))
        }
        for i in 0..<500 {
            hll.add(FieldValue.string("str_\(i)"))
        }

        let estimated = hll.cardinality()
        let error = abs(Double(estimated) - 1000.0) / 1000.0
        #expect(error < 0.05)
    }

    // MARK: - Merging

    @Test("HyperLogLog merge should combine estimates correctly")
    func testMerge() {
        var hll1 = HyperLogLogPP()
        var hll2 = HyperLogLogPP()

        // Add disjoint sets
        for i in 0..<1000 {
            hll1.add(i)
        }
        for i in 1000..<2000 {
            hll2.add(i)
        }

        // Merge
        let merged = hll1.merged(with: hll2)
        let estimated = merged.cardinality()
        let error = abs(Double(estimated) - 2000.0) / 2000.0

        #expect(error < 0.05)
    }

    @Test("HyperLogLog merge with overlapping sets should not double count")
    func testMergeOverlapping() {
        var hll1 = HyperLogLogPP()
        var hll2 = HyperLogLogPP()

        // Add overlapping sets
        for i in 0..<1000 {
            hll1.add(i)
        }
        for i in 500..<1500 {
            hll2.add(i)
        }

        // Merge should give ~1500 (not 2000)
        let merged = hll1.merged(with: hll2)
        let estimated = merged.cardinality()
        let error = abs(Double(estimated) - 1500.0) / 1500.0

        #expect(error < 0.05)
    }

    @Test("HyperLogLog mutating merge should work correctly")
    func testMutatingMerge() {
        var hll1 = HyperLogLogPP()
        var hll2 = HyperLogLogPP()

        for i in 0..<500 {
            hll1.add(i)
            hll2.add(i + 500)
        }

        hll1.merge(with: hll2)
        let estimated = hll1.cardinality()
        let error = abs(Double(estimated) - 1000.0) / 1000.0

        #expect(error < 0.05)
    }

    // MARK: - Precision Tests

    @Test("HyperLogLog with different precision should have expected memory size")
    func testPrecisionMemorySize() {
        let hll12 = HyperLogLogPP(precision: 12)
        let hll14 = HyperLogLogPP(precision: 14)
        let hll16 = HyperLogLogPP(precision: 16)

        // p=12: 4096 bytes, p=14: 16384 bytes, p=16: 65536 bytes
        #expect(hll12.serializedSize == 4096)
        #expect(hll14.serializedSize == 16384)
        #expect(hll16.serializedSize == 65536)
    }

    @Test("Higher precision should give better accuracy")
    func testPrecisionAccuracy() {
        let targetCount = 50_000

        var hll12 = HyperLogLogPP(precision: 12)
        var hll16 = HyperLogLogPP(precision: 16)

        for i in 0..<targetCount {
            hll12.add(i)
            hll16.add(i)
        }

        let error12 = abs(Double(hll12.cardinality()) - Double(targetCount)) / Double(targetCount)
        let error16 = abs(Double(hll16.cardinality()) - Double(targetCount)) / Double(targetCount)

        // p=16 should generally have lower error than p=12
        // Standard error: p=12 ~1.63%, p=16 ~0.41%
        #expect(error12 < 0.05, "p=12 error: \(error12 * 100)%")
        #expect(error16 < 0.02, "p=16 error: \(error16 * 100)%")
    }

    // MARK: - Serialization

    @Test("HyperLogLog should be Codable")
    func testCodable() throws {
        var hll = HyperLogLogPP()

        for i in 0..<1000 {
            hll.add(i)
        }

        let originalCardinality = hll.cardinality()

        // Encode
        let encoder = JSONEncoder()
        let data = try encoder.encode(hll)

        // Decode
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(HyperLogLogPP.self, from: data)

        #expect(decoded.cardinality() == originalCardinality)
    }

    // MARK: - Clear

    @Test("HyperLogLog clear should reset to empty state")
    func testClear() {
        var hll = HyperLogLogPP()

        for i in 0..<1000 {
            hll.add(i)
        }

        #expect(hll.cardinality() > 0)

        hll.clear()

        #expect(hll.cardinality() == 0)
        #expect(hll.isEmpty)
    }

    // MARK: - Bias Correction (HyperLogLog++ specific)

    @Test("HyperLogLog++ should apply bias correction for small cardinalities")
    func testBiasCorrection() {
        // Test that bias correction works for small cardinalities
        // which is a key feature of HyperLogLog++
        var hll = HyperLogLogPP()

        // Add exactly 100 distinct values
        for i in 0..<100 {
            hll.add(i)
        }

        let estimated = hll.cardinality()

        // Without bias correction, raw estimate would be higher
        // With bias correction, should be closer to 100
        let error = abs(Double(estimated) - 100.0) / 100.0
        #expect(error < 0.10, "Estimated \(estimated), expected ~100")
    }

    @Test("HyperLogLog++ should use LinearCounting for very small cardinalities")
    func testLinearCounting() {
        var hll = HyperLogLogPP()

        // Add just 10 values - should use LinearCounting
        for i in 0..<10 {
            hll.add(i)
        }

        let estimated = hll.cardinality()

        // LinearCounting is used when many registers are zero
        #expect(estimated >= 8 && estimated <= 12, "Estimated \(estimated)")
    }

    // MARK: - Hash Distribution

    @Test("HyperLogLog should handle similar string values correctly")
    func testSimilarStrings() {
        var hll = HyperLogLogPP()

        // Add strings that are very similar
        for i in 0..<1000 {
            hll.add("user_\(String(format: "%04d", i))")
        }

        let estimated = hll.cardinality()
        let error = abs(Double(estimated) - 1000.0) / 1000.0
        #expect(error < 0.05)
    }

    @Test("HyperLogLog should handle sequential integers correctly")
    func testSequentialIntegers() {
        var hll = HyperLogLogPP()

        // Sequential integers might have poor hash distribution with weak hashes
        // MurmurHash64 should handle this well
        for i in 0..<10_000 {
            hll.add(i)
        }

        let estimated = hll.cardinality()
        let error = abs(Double(estimated) - 10000.0) / 10000.0
        #expect(error < 0.03)
    }
}
