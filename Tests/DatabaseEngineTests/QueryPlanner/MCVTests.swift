// MCVTests.swift
// Tests for Most Common Values (MCV) implementation

import Testing
import Foundation
@testable import DatabaseEngine
import Core

// Use type aliases to disambiguate from Core types
typealias CV = DatabaseEngine.ComparableValue

@Suite("MCV Tests")
struct MCVTests {

    // MARK: - MCVBuilder Tests

    @Test("MCVBuilder should track value frequencies")
    func testMCVBuilderFrequencies() {
        var builder = MCVBuilder(maxSize: 100, minFrequency: 0.01)

        // Add values with different frequencies
        for _ in 0..<50 { builder.add(CV.int64(1)) }  // 50%
        for _ in 0..<30 { builder.add(CV.int64(2)) }  // 30%
        for _ in 0..<15 { builder.add(CV.int64(3)) }  // 15%
        for _ in 0..<5  { builder.add(CV.int64(4)) }  // 5%

        let mcv = builder.build(totalCount: 100)

        #expect(mcv.entries.count == 4)
        #expect(mcv.entries[0].value == CV.int64(1))
        #expect(abs(mcv.entries[0].frequency - 0.50) < 0.01)
        #expect(mcv.entries[1].value == CV.int64(2))
        #expect(abs(mcv.entries[1].frequency - 0.30) < 0.01)
    }

    @Test("MCVBuilder should respect minimum frequency threshold")
    func testMCVBuilderMinFrequency() {
        var builder = MCVBuilder(maxSize: 100, minFrequency: 0.10)

        // Add values: only 1 and 2 should meet 10% threshold
        for _ in 0..<50 { builder.add(CV.int64(1)) }  // 50%
        for _ in 0..<30 { builder.add(CV.int64(2)) }  // 30%
        for _ in 0..<5  { builder.add(CV.int64(3)) }  // 5% - below threshold
        for _ in 0..<5  { builder.add(CV.int64(4)) }  // 5% - below threshold
        for _ in 0..<10 { builder.add(CV.int64(5)) }  // 10% - at threshold

        let mcv = builder.build(totalCount: 100)

        // Should include values with frequency >= 10%
        #expect(mcv.entries.count == 3)
        #expect(mcv.entries.contains { $0.value == CV.int64(1) })
        #expect(mcv.entries.contains { $0.value == CV.int64(2) })
        #expect(mcv.entries.contains { $0.value == CV.int64(5) })
    }

    @Test("MCVBuilder should respect maximum size")
    func testMCVBuilderMaxSize() {
        var builder = MCVBuilder(maxSize: 3, minFrequency: 0.01)

        // Add 5 different values with distinct frequencies
        for _ in 0..<30 { builder.add(CV.int64(1)) }  // 30%
        for _ in 0..<25 { builder.add(CV.int64(2)) }  // 25%
        for _ in 0..<20 { builder.add(CV.int64(3)) }  // 20%
        for _ in 0..<15 { builder.add(CV.int64(4)) }  // 15%
        for _ in 0..<10 { builder.add(CV.int64(5)) }  // 10%

        let mcv = builder.build(totalCount: 100)

        // Should keep only top 3 most frequent
        #expect(mcv.entries.count == 3)

        // Verify top 3 values are included (order by frequency)
        let topValues = Set(mcv.entries.map { $0.value })
        #expect(topValues.contains(CV.int64(1)))
        #expect(topValues.contains(CV.int64(2)))
        #expect(topValues.contains(CV.int64(3)))

        // Verify values 4 and 5 are NOT included
        #expect(!topValues.contains(CV.int64(4)))
        #expect(!topValues.contains(CV.int64(5)))
    }

    @Test("MCVBuilder should handle string values")
    func testMCVBuilderWithStrings() {
        var builder = MCVBuilder(maxSize: 100, minFrequency: 0.01)

        for _ in 0..<40 { builder.add(CV.string("apple")) }
        for _ in 0..<30 { builder.add(CV.string("banana")) }
        for _ in 0..<20 { builder.add(CV.string("cherry")) }
        for _ in 0..<10 { builder.add(CV.string("date")) }

        let mcv = builder.build(totalCount: 100)

        #expect(mcv.entries[0].value == CV.string("apple"))
        #expect(mcv.entries[1].value == CV.string("banana"))
    }

    // MARK: - MostCommonValues Tests

    @Test("MCV should return correct selectivity for known values")
    func testMCVSelectivity() {
        let entries = [
            MostCommonValues.Entry(value: CV.int64(1), frequency: 0.30, count: 3000),
            MostCommonValues.Entry(value: CV.int64(2), frequency: 0.20, count: 2000),
            MostCommonValues.Entry(value: CV.int64(3), frequency: 0.10, count: 1000)
        ]
        let mcv = MostCommonValues(entries: entries)

        #expect(mcv.selectivity(for: CV.int64(1)) == 0.30)
        #expect(mcv.selectivity(for: CV.int64(2)) == 0.20)
        #expect(mcv.selectivity(for: CV.int64(3)) == 0.10)
        #expect(mcv.selectivity(for: CV.int64(999)) == nil)
    }

    @Test("MCV should calculate correct histogram fraction")
    func testMCVHistogramFraction() {
        let entries = [
            MostCommonValues.Entry(value: CV.int64(1), frequency: 0.30, count: 3000),
            MostCommonValues.Entry(value: CV.int64(2), frequency: 0.20, count: 2000)
        ]
        let mcv = MostCommonValues(entries: entries)

        // Total frequency = 0.50, so histogram fraction = 0.50
        #expect(abs(mcv.histogramFraction - 0.50) < 0.001)
        #expect(abs(mcv.totalFrequency - 0.50) < 0.001)
    }

    @Test("MCV should calculate IN clause selectivity")
    func testMCVInSelectivity() {
        let entries = [
            MostCommonValues.Entry(value: CV.int64(1), frequency: 0.30, count: 3000),
            MostCommonValues.Entry(value: CV.int64(2), frequency: 0.20, count: 2000),
            MostCommonValues.Entry(value: CV.int64(3), frequency: 0.10, count: 1000)
        ]
        let mcv = MostCommonValues(entries: entries)

        // IN (1, 2) should return 0.30 + 0.20 = 0.50
        let sel = mcv.selectivity(forIn: [CV.int64(1), CV.int64(2)])
        #expect(abs(sel - 0.50) < 0.001)

        // IN (1, 999) should return 0.30 (999 not in MCV)
        let sel2 = mcv.selectivity(forIn: [CV.int64(1), CV.int64(999)])
        #expect(abs(sel2 - 0.30) < 0.001)
    }

    @Test("MCV should calculate range selectivity")
    func testMCVRangeSelectivity() {
        let entries = [
            MostCommonValues.Entry(value: CV.int64(10), frequency: 0.20, count: 2000),
            MostCommonValues.Entry(value: CV.int64(20), frequency: 0.15, count: 1500),
            MostCommonValues.Entry(value: CV.int64(30), frequency: 0.10, count: 1000),
            MostCommonValues.Entry(value: CV.int64(40), frequency: 0.05, count: 500)
        ]
        let mcv = MostCommonValues(entries: entries)

        // Range [15, 35] should include 20 and 30 = 0.15 + 0.10 = 0.25
        let sel = mcv.rangeSelectivity(
            min: CV.int64(15),
            max: CV.int64(35),
            minInclusive: true,
            maxInclusive: true
        )
        #expect(abs(sel - 0.25) < 0.001)
    }

    // MARK: - CombinedSelectivityEstimator Tests

    @Test("Combined estimator should use MCV for known values")
    func testCombinedEstimatorMCVValues() {
        // Create MCV with known values
        let mcvEntries = [
            MostCommonValues.Entry(value: CV.int64(100), frequency: 0.30, count: 3000),
            MostCommonValues.Entry(value: CV.int64(200), frequency: 0.20, count: 2000)
        ]
        let mcv = MostCommonValues(entries: mcvEntries)

        // Create histogram (excluding MCV values)
        let buckets = [
            Histogram.Bucket(lowerBound: CV.int64(1), upperBound: CV.int64(50), count: 2500, distinctCount: 50),
            Histogram.Bucket(lowerBound: CV.int64(51), upperBound: CV.int64(99), count: 2500, distinctCount: 49)
        ]
        let histogram = Histogram(buckets: buckets, totalCount: 5000, nullCount: 0, distinctCount: 99)

        let estimator = CombinedSelectivityEstimator(mcv: mcv, histogram: histogram)

        // For MCV value: should return MCV frequency directly
        let sel = estimator.equalitySelectivity(value: CV.int64(100))
        #expect(abs(sel - 0.30) < 0.001)
    }

    @Test("Combined estimator should use histogram for non-MCV values")
    func testCombinedEstimatorHistogramValues() {
        let mcvEntries = [
            MostCommonValues.Entry(value: CV.int64(100), frequency: 0.30, count: 3000)
        ]
        let mcv = MostCommonValues(entries: mcvEntries)

        let buckets = [
            Histogram.Bucket(lowerBound: CV.int64(1), upperBound: CV.int64(50), count: 3500, distinctCount: 50)
        ]
        let histogram = Histogram(buckets: buckets, totalCount: 7000, nullCount: 0, distinctCount: 50)

        let estimator = CombinedSelectivityEstimator(mcv: mcv, histogram: histogram)

        // For non-MCV value: should use histogram estimate * histogram fraction
        let sel = estimator.equalitySelectivity(value: CV.int64(25))
        // Histogram fraction = 1 - 0.30 = 0.70
        // Histogram selectivity ≈ 1/50 = 0.02
        // Combined ≈ 0.02 * 0.70 = 0.014
        #expect(sel > 0 && sel < 0.05)
    }

    @Test("Combined estimator should calculate range selectivity correctly")
    func testCombinedEstimatorRangeSelectivity() {
        // MCV: values 100 and 200 with total 40% frequency
        let mcvEntries = [
            MostCommonValues.Entry(value: CV.int64(100), frequency: 0.25, count: 2500),
            MostCommonValues.Entry(value: CV.int64(200), frequency: 0.15, count: 1500)
        ]
        let mcv = MostCommonValues(entries: mcvEntries)

        // Histogram: covers 1-300 excluding MCV values
        let buckets = [
            Histogram.Bucket(lowerBound: CV.int64(1), upperBound: CV.int64(100), count: 2000, distinctCount: 100),
            Histogram.Bucket(lowerBound: CV.int64(101), upperBound: CV.int64(200), count: 2000, distinctCount: 100),
            Histogram.Bucket(lowerBound: CV.int64(201), upperBound: CV.int64(300), count: 2000, distinctCount: 100)
        ]
        let histogram = Histogram(buckets: buckets, totalCount: 6000, nullCount: 0, distinctCount: 300)

        let estimator = CombinedSelectivityEstimator(mcv: mcv, histogram: histogram)

        // Range [50, 150]: includes MCV 100 (0.25) + histogram portion
        let sel = estimator.rangeSelectivity(
            min: CV.int64(50),
            max: CV.int64(150),
            minInclusive: true,
            maxInclusive: true
        )

        // MCV contribution: 0.25 (value 100 is in range)
        // Histogram contribution scaled by histogram fraction (0.60)
        #expect(sel > 0.25 && sel < 1.0)
    }

    // MARK: - Empty/Edge Cases

    @Test("Empty MCV should return nil selectivity")
    func testEmptyMCV() {
        let mcv = MostCommonValues.empty

        #expect(mcv.entries.isEmpty)
        #expect(mcv.selectivity(for: CV.int64(1)) == nil)
        #expect(mcv.histogramFraction == 1.0)
    }

    @Test("MCVBuilder with no samples should return empty MCV")
    func testMCVBuilderNoSamples() {
        let builder = MCVBuilder(maxSize: 100, minFrequency: 0.01)
        let mcv = builder.build(totalCount: 0)

        #expect(mcv.entries.isEmpty)
    }

    @Test("MCV should handle single value correctly")
    func testMCVSingleValue() {
        var builder = MCVBuilder(maxSize: 100, minFrequency: 0.01)

        for _ in 0..<100 { builder.add(CV.string("only")) }

        let mcv = builder.build(totalCount: 100)

        #expect(mcv.entries.count == 1)
        #expect(abs(mcv.entries[0].frequency - 1.0) < 0.001)
    }
}
