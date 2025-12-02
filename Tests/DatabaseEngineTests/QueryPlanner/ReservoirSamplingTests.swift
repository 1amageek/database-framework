// ReservoirSamplingTests.swift
// Tests for Reservoir Sampling with Algorithm L

import Testing
import Foundation
@testable import DatabaseEngine
@testable import Core

@Suite("Reservoir Sampling Tests")
struct ReservoirSamplingTests {

    // MARK: - Basic Functionality

    @Test("Empty reservoir should return empty sample")
    func testEmptySample() {
        let sampler = ReservoirSampling<Int>(reservoirSize: 100)

        #expect(sampler.sample.isEmpty)
        #expect(sampler.elementsSeen == 0)
        #expect(!sampler.isFull)
    }

    @Test("Reservoir should fill up completely for small streams")
    func testSmallStream() {
        var sampler = ReservoirSampling<Int>(reservoirSize: 100)

        // Add fewer elements than reservoir size
        for i in 0..<50 {
            sampler.add(i)
        }

        #expect(sampler.sample.count == 50)
        #expect(sampler.elementsSeen == 50)
        #expect(!sampler.isFull)

        // Verify all elements are present (no sampling yet)
        let sortedSample = sampler.sample.sorted()
        #expect(sortedSample == Array(0..<50))
    }

    @Test("Reservoir should be exactly full when stream size equals reservoir size")
    func testExactlyFull() {
        var sampler = ReservoirSampling<Int>(reservoirSize: 100)

        for i in 0..<100 {
            sampler.add(i)
        }

        #expect(sampler.sample.count == 100)
        #expect(sampler.isFull)
        #expect(sampler.elementsSeen == 100)
    }

    @Test("Reservoir should not exceed max size for large streams")
    func testLargeStream() {
        var sampler = ReservoirSampling<Int>(reservoirSize: 100)

        for i in 0..<10_000 {
            sampler.add(i)
        }

        #expect(sampler.sample.count == 100)
        #expect(sampler.elementsSeen == 10_000)
        #expect(sampler.isFull)
    }

    // MARK: - Sample Rate

    @Test("Sample rate should be accurate for large streams")
    func testSampleRate() {
        var sampler = ReservoirSampling<Int>(reservoirSize: 1000)

        for i in 0..<100_000 {
            sampler.add(i)
        }

        // Sample rate should be approximately 1000/100000 = 0.01
        let rate = sampler.sampleRate
        #expect(abs(rate - 0.01) < 0.001)
    }

    @Test("Sample rate should be 1.0 for small streams")
    func testSampleRateSmallStream() {
        var sampler = ReservoirSampling<Int>(reservoirSize: 1000)

        for i in 0..<500 {
            sampler.add(i)
        }

        // All elements are sampled
        #expect(sampler.sampleRate == 1.0)
    }

    // MARK: - Uniform Sampling (Statistical Test)

    @Test("Algorithm L should produce uniform random sample")
    func testUniformSampling() {
        // Run multiple trials to verify uniform distribution
        let reservoirSize = 100
        let streamSize = 10_000
        let trials = 20

        // Count how often each element appears across trials
        var counts = [Int: Int]()

        for _ in 0..<trials {
            var sampler = ReservoirSampling<Int>(reservoirSize: reservoirSize)
            for i in 0..<streamSize {
                sampler.add(i)
            }

            for element in sampler.sample {
                counts[element, default: 0] += 1
            }
        }

        // Expected count per element: (reservoirSize * trials) / streamSize = 0.2
        // With uniform sampling, all elements should have similar counts

        // Verify we have samples from throughout the stream
        let sampledElements = Set(counts.keys)
        let firstThird = sampledElements.filter { $0 < streamSize / 3 }.count
        let middleThird = sampledElements.filter { $0 >= streamSize / 3 && $0 < 2 * streamSize / 3 }.count
        let lastThird = sampledElements.filter { $0 >= 2 * streamSize / 3 }.count

        // Each third should have roughly similar representation
        let total = firstThird + middleThird + lastThird
        #expect(Double(firstThird) / Double(total) > 0.2, "First third underrepresented")
        #expect(Double(middleThird) / Double(total) > 0.2, "Middle third underrepresented")
        #expect(Double(lastThird) / Double(total) > 0.2, "Last third underrepresented")
    }

    // MARK: - addAll

    @Test("addAll should process all elements")
    func testAddAll() {
        var sampler = ReservoirSampling<Int>(reservoirSize: 100)

        let elements = Array(0..<1000)
        sampler.addAll(elements)

        #expect(sampler.elementsSeen == 1000)
        #expect(sampler.sample.count == 100)
    }

    // MARK: - Reset

    @Test("Reset should clear all state")
    func testReset() {
        var sampler = ReservoirSampling<Int>(reservoirSize: 100)

        for i in 0..<1000 {
            sampler.add(i)
        }

        #expect(!sampler.sample.isEmpty)
        #expect(sampler.elementsSeen > 0)

        sampler.reset()

        #expect(sampler.sample.isEmpty)
        #expect(sampler.elementsSeen == 0)
        #expect(!sampler.isFull)
    }

    // MARK: - Different Types

    @Test("Reservoir should work with string values")
    func testStringValues() {
        var sampler = ReservoirSampling<String>(reservoirSize: 50)

        for i in 0..<500 {
            sampler.add("item_\(i)")
        }

        #expect(sampler.sample.count == 50)
        #expect(sampler.sample.allSatisfy { $0.hasPrefix("item_") })
    }

    @Test("Reservoir should work with FieldValue types")
    func testFieldValueTypes() {
        var sampler = ReservoirSampling<FieldValue>(reservoirSize: 100)

        for i in 0..<1000 {
            if i % 2 == 0 {
                sampler.add(.int64(Int64(i)))
            } else {
                sampler.add(.string("value_\(i)"))
            }
        }

        #expect(sampler.sample.count == 100)
        #expect(sampler.elementsSeen == 1000)
    }

    // MARK: - Histogram Building

    @Test("Reservoir should build histogram from numeric samples")
    func testBuildHistogramNumeric() {
        var sampler = ReservoirSampling<Int>(reservoirSize: 1000)

        // Add uniformly distributed values
        for i in 0..<10_000 {
            sampler.add(i % 100)  // Values 0-99
        }

        let histogram = sampler.buildHistogram(bucketCount: 10)

        #expect(!histogram.isEmpty)
        #expect(histogram.count <= 10)

        // All buckets should have counts
        for bucket in histogram {
            #expect(bucket.count > 0)
        }
    }

    @Test("Reservoir should build histogram from FieldValue samples")
    func testBuildHistogramFieldValue() {
        var sampler = ReservoirSampling<FieldValue>(reservoirSize: 1000)

        for i in 0..<5000 {
            sampler.add(.int64(Int64(i % 50)))
        }

        let histogram = sampler.buildFieldValueHistogram(bucketCount: 10)

        #expect(!histogram.isEmpty)
    }

    // MARK: - Numeric Statistics

    @Test("Reservoir should compute statistics from numeric samples")
    func testComputeStatistics() {
        var sampler = ReservoirSampling<Double>(reservoirSize: 1000)

        // Add values with known mean (50) and range (0-100)
        for i in 0..<10_000 {
            sampler.add(Double(i % 101))
        }

        guard let stats = sampler.computeStatistics() else {
            Issue.record("Statistics should not be nil")
            return
        }

        // Mean should be close to 50
        #expect(abs(stats.mean - 50.0) < 5.0)

        // Min/Max should be close to 0 and 100
        #expect(stats.min >= 0 && stats.min <= 5)
        #expect(stats.max >= 95 && stats.max <= 100)

        // Standard deviation of uniform distribution on [0,100] ≈ 29.15
        #expect(stats.stdDev > 20 && stats.stdDev < 40)
    }

    // MARK: - Edge Cases

    @Test("Reservoir size of 1 should work correctly")
    func testReservoirSizeOne() {
        var sampler = ReservoirSampling<Int>(reservoirSize: 1)

        for i in 0..<1000 {
            sampler.add(i)
        }

        #expect(sampler.sample.count == 1)
        #expect(sampler.elementsSeen == 1000)
    }

    @Test("Very large reservoir should handle efficiently")
    func testLargeReservoir() {
        var sampler = ReservoirSampling<Int>(reservoirSize: 10_000)

        // Add elements - should fill reservoir then sample
        for i in 0..<50_000 {
            sampler.add(i)
        }

        #expect(sampler.sample.count == 10_000)
        #expect(sampler.elementsSeen == 50_000)
    }

    // MARK: - Algorithm L Efficiency

    @Test("Algorithm L should be efficient for large streams")
    func testAlgorithmLEfficiency() {
        // This test verifies that Algorithm L is used (constant time per skip)
        // by checking that a very large stream completes quickly
        var sampler = ReservoirSampling<Int>(reservoirSize: 100)

        // Time the sampling of a large stream
        let start = Date()

        for i in 0..<1_000_000 {
            sampler.add(i)
        }

        let duration = Date().timeIntervalSince(start)

        #expect(sampler.sample.count == 100)
        #expect(sampler.elementsSeen == 1_000_000)

        // Should complete in reasonable time (Algorithm L is O(k log(N/k)))
        // Algorithm R would be slower (O(N))
        #expect(duration < 5.0, "Sampling took \(duration)s - Algorithm L should be faster")
    }
}
