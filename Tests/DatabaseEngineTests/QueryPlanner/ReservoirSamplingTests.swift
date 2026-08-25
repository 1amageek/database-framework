#if !os(WASI)
// ReservoirSamplingTests.swift
// Tests for Reservoir Sampling with Algorithm L

import Testing
import TestHeartbeat
import Foundation
import DatabaseTypes
@testable import DatabaseEngine
@testable import DatabaseKit

private struct DeterministicReservoirSamplingRandomSource:
    ReservoirSamplingRandomSource
{
    var unitRandomValues: [Double]
    var replacementIndices: [Int]

    mutating func nextUnitInterval() -> Double {
        precondition(!unitRandomValues.isEmpty)
        return unitRandomValues.removeFirst()
    }

    mutating func replacementIndex(in range: Range<Int>) -> Int {
        precondition(!replacementIndices.isEmpty)
        let index = replacementIndices.removeFirst()
        precondition(range.contains(index))
        return index
    }
}

@Suite("Reservoir Sampling Tests", .heartbeat)
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

    @Test("Reservoir should not exceed max size after reaching capacity")
    func reservoirDoesNotExceedCapacity() {
        var sampler = ReservoirSampling<Int>(reservoirSize: 100)

        for i in 0..<200 {
            sampler.add(i)
        }

        #expect(sampler.sample.count == 100)
        #expect(sampler.elementsSeen == 200)
        #expect(sampler.isFull)
    }

    @Test("Algorithm L replaces an accepted value after reaching capacity")
    func algorithmLReplacesAcceptedValue() {
        var randomSource = DeterministicReservoirSamplingRandomSource(
            unitRandomValues: [0.25, 0.75, 0.25, 0.75, 0.25],
            replacementIndices: [0]
        )
        var sampler = ReservoirSampling<Int>(reservoirSize: 2)

        sampler.add(0, using: &randomSource)
        sampler.add(1, using: &randomSource)
        sampler.add(2, using: &randomSource)

        #expect(sampler.sample == [2, 1])
        #expect(sampler.elementsSeen == 3)
    }

    // MARK: - Sample Rate

    @Test("Sample rate reflects retained values over observed values")
    func testSampleRate() {
        var sampler = ReservoirSampling<Int>(reservoirSize: 10)

        for i in 0..<100 {
            sampler.add(i)
        }

        #expect(sampler.sampleRate == 0.1)
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

    // MARK: - addAll

    @Test("addAll should process all elements")
    func testAddAll() {
        var sampler = ReservoirSampling<Int>(reservoirSize: 100)

        let elements = Array(0..<200)
        sampler.addAll(elements)

        #expect(sampler.elementsSeen == 200)
        #expect(sampler.sample.count == 100)
    }

    // MARK: - Reset

    @Test("Reset should clear all state")
    func testReset() {
        var sampler = ReservoirSampling<Int>(reservoirSize: 100)

        for i in 0..<20 {
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

        for i in 0..<100 {
            sampler.add("item_\(i)")
        }

        #expect(sampler.sample.count == 50)
        #expect(sampler.sample.allSatisfy { $0.hasPrefix("item_") })
    }

    @Test("Reservoir should work with FieldValue types")
    func testFieldValueTypes() {
        var sampler = ReservoirSampling<FieldValue>(reservoirSize: 100)

        for i in 0..<200 {
            if i % 2 == 0 {
                sampler.add(.int64(Int64(i)))
            } else {
                sampler.add(.string("value_\(i)"))
            }
        }

        #expect(sampler.sample.count == 100)
        #expect(sampler.elementsSeen == 200)
    }

    // MARK: - Histogram Building

    @Test("Reservoir should build histogram from numeric samples")
    func testBuildHistogramNumeric() throws {
        var sampler = ReservoirSampling<Int>(reservoirSize: 1000)

        // Add uniformly distributed values
        for i in 0..<1_000 {
            sampler.add(i % 100)  // Values 0-99
        }

        let histogram = try sampler.buildHistogram(bucketCount: 10)

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

        for i in 0..<500 {
            sampler.add(.int64(Int64(i % 50)))
        }

        let histogram = sampler.buildFieldValueHistogram(bucketCount: 10)

        #expect(!histogram.isEmpty)
    }

    // MARK: - Numeric Statistics

    @Test("Reservoir should compute statistics from numeric samples")
    func testComputeStatistics() {
        var sampler = ReservoirSampling<Double>(reservoirSize: 101)

        // Add values with known mean (50) and range (0-100)
        for value in 0...100 {
            sampler.add(Double(value))
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

        for i in 0..<10 {
            sampler.add(i)
        }

        #expect(sampler.sample.count == 1)
        #expect(sampler.elementsSeen == 10)
    }

}
#endif
