#if !os(WASI)
#if FOUNDATION_DB
// TDigestTests.swift
// Tests for TDigest streaming quantile estimation

import DatabaseTypes
import Testing
import TestHeartbeat
import Foundation
import StorageKit
@testable import DatabaseEngine

@Suite("TDigest Tests", .heartbeat)
struct TDigestTests {

    // MARK: - Basic Operations

    @Test("Empty digest fails with a typed error")
    func testEmptyDigest() {
        var digest = TDigest()
        #expect(throws: TDigestError.emptyDigest) {
            try digest.quantile(0.5)
        }
        #expect(digest.isEmpty)
        #expect(digest.count == 0)
    }

    @Test("Single value digest")
    func testSingleValue() throws {
        var digest = TDigest()
        try digest.add(42.0)

        #expect(digest.count == 1)
        #expect(!digest.isEmpty)
        #expect(digest.min == 42.0)
        #expect(digest.max == 42.0)

        let median = try digest.quantile(0.5)
        #expect(median == 42.0)
    }

    @Test("Two values")
    func testTwoValues() throws {
        var digest = TDigest()
        try digest.add(10.0)
        try digest.add(20.0)

        #expect(digest.count == 2)
        #expect(digest.min == 10.0)
        #expect(digest.max == 20.0)

        // p0 should be min, p1 should be max
        #expect(try digest.quantile(0) == 10.0)
        #expect(try digest.quantile(1) == 20.0)
    }

    @Test("Multiple identical values")
    func testIdenticalValues() throws {
        var digest = TDigest()
        for _ in 0..<100 {
            try digest.add(50.0)
        }

        #expect(digest.count == 100)
        #expect(try digest.quantile(0.5) == 50.0)
        #expect(try digest.quantile(0.99) == 50.0)
    }

    // MARK: - Accuracy Tests

    @Test("Uniform distribution quantiles")
    func testUniformDistribution() async throws {
        var digest = try TDigest(compression: 100)

        // Add values 1 to 1000 (uniform distribution)
        for i in 1...1000 {
            try digest.add(Double(i))
        }

        #expect(digest.count == 1000)

        // Check known quantiles with tolerance
        // For uniform [1, 1000], p50 should be ~500
        let p50 = try digest.quantile(0.5)
        #expect(abs(p50 - 500) < 50, "p50=\(p50) should be near 500")

        // p90 should be ~900
        let p90 = try digest.quantile(0.9)
        #expect(abs(p90 - 900) < 50, "p90=\(p90) should be near 900")

        // p99 should be ~990
        let p99 = try digest.quantile(0.99)
        #expect(abs(p99 - 990) < 20, "p99=\(p99) should be near 990")
    }

    @Test("Normal distribution quantiles")
    func testNormalDistributionQuantiles() async throws {
        var digest = try TDigest(compression: 100)

        // Generate normal distribution samples using Box-Muller transform
        // Mean = 100, StdDev = 15
        let mean = 100.0
        let stddev = 15.0
        var rng = SeededRandomNumberGenerator(seed: 12345)

        for _ in 0..<10000 {
            let u1 = Double.random(in: 0.001..<1.0, using: &rng)
            let u2 = Double.random(in: 0.0..<1.0, using: &rng)
            let z = sqrt(-2.0 * log(u1)) * cos(2.0 * .pi * u2)
            let value = mean + z * stddev
            try digest.add(value)
        }

        // Normal distribution quantiles:
        // p50 ≈ mean = 100
        // p84 ≈ mean + 1*stddev = 115
        // p97.7 ≈ mean + 2*stddev = 130
        // p99.9 ≈ mean + 3.09*stddev ≈ 146

        let p50 = try digest.quantile(0.5)
        #expect(abs(p50 - 100) < 5, "p50=\(p50) should be near 100 (mean)")

        let p84 = try digest.quantile(0.84)
        #expect(abs(p84 - 115) < 10, "p84=\(p84) should be near 115 (mean+1σ)")

        // t-digest is especially accurate at extremes
        let p99 = try digest.quantile(0.99)
        // For normal dist, p99 ≈ mean + 2.33*stddev = 100 + 34.95 = 134.95
        #expect(abs(p99 - 135) < 15, "p99=\(p99) should be near 135")
    }

    @Test("Extreme quantile accuracy")
    func testExtremeQuantileAccuracy() async throws {
        var digest = try TDigest(compression: 200)  // Higher compression for better accuracy

        // Add 100,000 values: mostly small, few large outliers
        var rng = SeededRandomNumberGenerator(seed: 42)
        for _ in 0..<95000 {
            // 95% of values are between 0-100
            try digest.add(Double.random(in: 0..<100, using: &rng))
        }
        for _ in 0..<4990 {
            // 4.99% are between 100-1000
            try digest.add(Double.random(in: 100..<1000, using: &rng))
        }
        for _ in 0..<10 {
            // 0.01% are extreme outliers (10000+)
            try digest.add(Double.random(in: 10000..<20000, using: &rng))
        }

        // p50 should be in the 0-100 range
        let p50 = try digest.quantile(0.5)
        #expect(p50 >= 0 && p50 <= 100, "p50=\(p50) should be in [0,100]")

        // p99 should capture the transition
        let p99 = try digest.quantile(0.99)
        #expect(p99 > 100, "p99=\(p99) should be > 100")

        // p99.99 should be near the outliers
        let p9999 = try digest.quantile(0.9999)
        #expect(p9999 > 1000, "p99.99=\(p9999) should capture outliers")
    }

    // MARK: - Merge Tests

    @Test("Merge two digests")
    func testMergeTwoDigests() async throws {
        var digest1 = TDigest()
        var digest2 = TDigest()

        // Add 1-500 to first, 501-1000 to second
        for i in 1...500 {
            try digest1.add(Double(i))
        }
        for i in 501...1000 {
            try digest2.add(Double(i))
        }

        // Merge
        try digest1.merge(with: digest2)

        #expect(digest1.count == 1000)
        #expect(digest1.min == 1.0)
        #expect(digest1.max == 1000.0)

        // Should behave like uniform [1, 1000]
        let p50 = try digest1.quantile(0.5)
        #expect(abs(p50 - 500) < 50, "Merged p50=\(p50) should be near 500")
    }

    @Test("Merge multiple digests")
    func testMergeMultipleDigests() async throws {
        var digests: [TDigest] = []

        // Create 10 digests, each with 100 values
        for batch in 0..<10 {
            var d = TDigest()
            for i in 0..<100 {
                try d.add(Double(batch * 100 + i + 1))
            }
            digests.append(d)
        }

        let merged = try TDigest.merge(digests)

        #expect(merged.count == 1000)
        #expect(merged.min == 1.0)
        #expect(merged.max == 1000.0)
    }

    // MARK: - CDF Tests

    @Test("CDF for uniform distribution")
    func testCDF() async throws {
        var digest = TDigest()

        for i in 1...100 {
            try digest.add(Double(i))
        }

        // CDF(50) should be approximately 0.5 for uniform [1,100]
        let cdf50 = try digest.cdf(50.0)
        #expect(abs(cdf50 - 0.5) < 0.1, "CDF(50)=\(cdf50) should be near 0.5")

        // CDF(min) should be near 0
        let cdfMin = try digest.cdf(1.0)
        #expect(cdfMin < 0.1, "CDF(min)=\(cdfMin) should be near 0")

        // CDF(max) should be 1
        let cdfMax = try digest.cdf(100.0)
        #expect(cdfMax > 0.9, "CDF(max)=\(cdfMax) should be near 1")

        // CDF below min should be 0
        let cdfBelow = try digest.cdf(0.0)
        #expect(cdfBelow == 0)

        // CDF above max should be 1
        let cdfAbove = try digest.cdf(101.0)
        #expect(cdfAbove == 1)
    }

    // MARK: - Serialization Tests

    @Test("Encode and decode")
    func testSerialization() async throws {
        var original = try TDigest(compression: 100)

        for i in 1...1000 {
            try original.add(Double(i))
        }

        // Force compression before encoding
        _ = try original.quantile(0.5)

        // Encode
        let data = try original.encodeBytes()

        // Decode
        let decoded = try TDigest.decode(from: data)

        // Verify equality
        #expect(original == decoded)
        #expect(decoded.count == 1000)
        #expect(decoded.min == 1.0)
        #expect(decoded.max == 1000.0)

        // Verify quantiles match
        var decodedMut = decoded
        let decodedMedian = try decodedMut.quantile(0.5)
        let originalMedian = try original.quantile(0.5)
        #expect(abs(decodedMedian - originalMedian) < 0.001)
    }

    @Test("Decode accepts a retained non-zero-index slice")
    func decodeRetainedSlice() throws {
        var original = TDigest()
        try original.add(1)
        try original.add(2)
        try original.add(3)
        _ = try original.quantile(0.5)
        let encoded = try original.encodeBytes()
        let framed = ByteString([0xFF])
            .appending(contentsOf: encoded)
            .appending(0xEE)
        let slice = framed[1..<(1 + encoded.count)]

        #expect(slice.startIndex == 1)
        var decoded = try TDigest.decode(from: slice)
        _ = try decoded.quantile(0.5)
        #expect(decoded == original)
    }

    @Test("Decode invalid data throws a typed error")
    func testDecodeInvalidData() {
        let invalidData = ByteString([0, 1, 2, 3])
        #expect(
            throws: TDigestError.invalidByteCount(expected: 40, actual: 4)
        ) {
            try TDigest.decode(from: invalidData)
        }
    }

    @Test("Decode rejects trailing bytes")
    func testDecodeRejectsTrailingBytes() throws {
        var digest = TDigest()
        try digest.add(42)
        let encoded = try digest.encodeBytes()
        let bytes = encoded.appending(0)
        let expectedByteCount = encoded.count

        #expect(
            throws: TDigestError.invalidByteCount(
                expected: expectedByteCount,
                actual: expectedByteCount + 1
            )
        ) {
            try TDigest.decode(from: bytes)
        }
    }

    @Test("Quantile rejects out-of-range values instead of clamping")
    func testQuantileRejectsOutOfRangeValues() throws {
        var digest = TDigest()
        try digest.add(42)

        #expect(throws: TDigestError.invalidQuantile(-0.1)) {
            try digest.quantile(-0.1)
        }
        #expect(throws: TDigestError.invalidQuantile(1.1)) {
            try digest.quantile(1.1)
        }
        #expect(throws: TDigestError.self) {
            try digest.quantile(.nan)
        }
    }

    // MARK: - Edge Cases

    @Test("Rejects non-finite values without mutating")
    func testRejectsNonFinite() throws {
        var digest = TDigest()

        #expect(throws: TDigestError.self) {
            try digest.add(Double.nan)
        }
        #expect(throws: TDigestError.invalidValue(.infinity)) {
            try digest.add(Double.infinity)
        }
        #expect(throws: TDigestError.invalidValue(-.infinity)) {
            try digest.add(-Double.infinity)
        }
        try digest.add(10.0)
        try digest.add(20.0)

        #expect(digest.count == 2)  // Only 10 and 20 counted
        #expect(digest.min == 10.0)
        #expect(digest.max == 20.0)
    }

    @Test("Rejects unsupported compression at initialization")
    func testRejectsInvalidCompression() {
        #expect(throws: TDigestError.invalidCompression(0)) {
            try TDigest(compression: 0)
        }
        #expect(throws: TDigestError.invalidCompression(1_001)) {
            try TDigest(compression: 1_001)
        }
        #expect(throws: TDigestError.self) {
            try TDigest(compression: .nan)
        }
    }

    @Test("Rejects zero or negative weight without mutating")
    func testRejectsInvalidWeight() throws {
        var digest = TDigest()

        #expect(throws: TDigestError.invalidWeight(0)) {
            try digest.add(10.0, weight: 0)
        }
        #expect(throws: TDigestError.invalidWeight(-5)) {
            try digest.add(20.0, weight: -5)
        }
        try digest.add(30.0, weight: 1)

        #expect(digest.count == 1)  // Only 30 counted
    }

    @Test("Weighted values")
    func testWeightedValues() throws {
        var digest = TDigest()

        // Add 10 with weight 99, and 100 with weight 1
        try digest.add(10.0, weight: 99)
        try digest.add(100.0, weight: 1)

        #expect(digest.count == 100)

        // p50 should be near 10 (since 99% of weight is there)
        let p50 = try digest.quantile(0.5)
        #expect(p50 < 50, "p50=\(p50) should be near 10 (heavily weighted)")
    }

    @Test("Weighted tail quantiles remain monotonic and bounded")
    func testWeightedTailQuantilesAreMonotonicAndBounded() throws {
        var digest = try TDigest(compression: 100)
        try digest.add(-1_000, weight: 1)
        try digest.add(0, weight: 10_000)
        try digest.add(1, weight: 25)
        try digest.add(1_000_000, weight: 1)

        let probabilities = [
            0.0,
            0.000_001,
            0.001,
            0.5,
            0.99,
            0.999_999,
            1.0,
        ]
        var previous = -Double.infinity
        for probability in probabilities {
            let value = try digest.quantile(probability)
            #expect(value >= digest.min)
            #expect(value <= digest.max)
            #expect(value >= previous)
            previous = value
        }
    }

    @Test("Large dataset compression")
    func testLargeDatasetCompression() async throws {
        var digest = try TDigest(compression: 100)

        // Add 1 million values
        for i in 0..<1_000_000 {
            try digest.add(Double(i % 10000))
        }

        // Should have compressed significantly
        #expect(digest.centroidCount < 500, "Should compress to ~\(digest.centroidCount) centroids")

        // Memory should be reasonable
        #expect(digest.estimatedMemoryBytes < 20000, "Memory=\(digest.estimatedMemoryBytes) should be < 20KB")
    }

    // MARK: - Performance Characteristics

    @Test("Memory usage stays bounded")
    func retainedStateRemainsBounded() async throws {
        var digest = try TDigest(compression: 100)

        // Add values in batches, checking memory doesn't grow unbounded
        for batch in 0..<100 {
            for i in 0..<1000 {
                try digest.add(Double(batch * 1000 + i))
            }

            // Force compression
            _ = try digest.quantile(0.5)

            // Memory should stay bounded regardless of data size
            #expect(digest.estimatedMemoryBytes < 20000,
                   "Batch \(batch): memory=\(digest.estimatedMemoryBytes) should be bounded")
        }

        #expect(digest.count == 100_000)
    }

    @Test("Quantile queries multiple times")
    func testMultipleQuantileQueries() throws {
        var digest = TDigest()

        for i in 1...1000 {
            try digest.add(Double(i))
        }

        // Query multiple quantiles
        let quantiles = try digest.quantiles(
            [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
        )

        #expect(quantiles.count == 7)
        #expect(quantiles[0.5]! > 400 && quantiles[0.5]! < 600)
        #expect(quantiles[0.99]! > 950)
    }
}

// MARK: - Seeded Random Number Generator

/// Deterministic random number generator for reproducible tests
struct SeededRandomNumberGenerator: RandomNumberGenerator {
    private var state: UInt64

    init(seed: UInt64) {
        self.state = seed
    }

    mutating func next() -> UInt64 {
        // xorshift64
        state ^= state << 13
        state ^= state >> 7
        state ^= state << 17
        return state
    }
}
#endif

#endif
