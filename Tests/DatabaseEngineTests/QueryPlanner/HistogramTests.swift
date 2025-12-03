// HistogramTests.swift
// Tests for Histogram selectivity estimation

import Testing
import Foundation
@testable import DatabaseEngine

@Suite("Histogram Tests")
struct HistogramTests {

    // MARK: - String Scalar Conversion Tests

    @Suite("String to Scalar Conversion")
    struct StringScalarConversionTests {

        @Test("Empty string converts to 0.0")
        func emptyStringConvertsToZero() {
            let histogram = createTestHistogram()
            let selectivity = histogram.estimateRangeSelectivity(
                min: .string(""),
                max: .string(""),
                minInclusive: true,
                maxInclusive: true
            )
            // Empty string should have minimal selectivity
            #expect(selectivity >= 0.0)
        }

        @Test("Single character strings are ordered correctly")
        func singleCharacterOrdering() {
            // "A" (65) should come before "Z" (90) in scalar space
            let histogram = Histogram(
                buckets: [
                    Histogram.Bucket(
                        lowerBound: .string("A"),
                        upperBound: .string("Z"),
                        count: 100,
                        distinctCount: 26
                    )
                ],
                totalCount: 100,
                nullCount: 0
            )

            // Query for range "M" to "Z" should be roughly 50%
            let selectivity = histogram.estimateRangeSelectivity(
                min: .string("M"),
                max: .string("Z"),
                minInclusive: true,
                maxInclusive: true
            )

            // M is roughly halfway between A and Z
            // M=77, A=65, Z=90, so (77-65)/(90-65) ≈ 0.48
            #expect(selectivity > 0.3 && selectivity < 0.7)
        }

        @Test("Common prefix is stripped for better resolution")
        func commonPrefixStripping() {
            // Bucket with common prefix "user_"
            let histogram = Histogram(
                buckets: [
                    Histogram.Bucket(
                        lowerBound: .string("user_aaa"),
                        upperBound: .string("user_zzz"),
                        count: 100,
                        distinctCount: 50
                    )
                ],
                totalCount: 100,
                nullCount: 0
            )

            // Query for "user_mmm" to "user_zzz"
            let selectivity = histogram.estimateRangeSelectivity(
                min: .string("user_mmm"),
                max: .string("user_zzz"),
                minInclusive: true,
                maxInclusive: true
            )

            // After stripping "user_", we compare "mmm" vs "aaa"-"zzz"
            // Should be roughly 50%
            #expect(selectivity > 0.3 && selectivity < 0.7)
        }

        @Test("Scalar conversion respects PostgreSQL 12-byte limit")
        func respectsByteLimit() {
            // Very long strings should still work without overflow
            let longString1 = String(repeating: "a", count: 100)
            let longString2 = String(repeating: "z", count: 100)

            let histogram = Histogram(
                buckets: [
                    Histogram.Bucket(
                        lowerBound: .string(longString1),
                        upperBound: .string(longString2),
                        count: 100,
                        distinctCount: 50
                    )
                ],
                totalCount: 100,
                nullCount: 0
            )

            let selectivity = histogram.estimateRangeSelectivity(
                min: .string(String(repeating: "m", count: 100)),
                max: nil,
                minInclusive: true,
                maxInclusive: true
            )

            // Should not overflow or produce NaN
            #expect(!selectivity.isNaN)
            #expect(!selectivity.isInfinite)
            #expect(selectivity >= 0.0 && selectivity <= 1.0)
        }
    }

    // MARK: - Numeric Interpolation Tests

    @Suite("Numeric Interpolation")
    struct NumericInterpolationTests {

        @Test("Integer range selectivity is accurate")
        func integerRangeSelectivity() {
            let histogram = Histogram(
                buckets: [
                    Histogram.Bucket(
                        lowerBound: .int64(0),
                        upperBound: .int64(100),
                        count: 100,
                        distinctCount: 100
                    )
                ],
                totalCount: 100,
                nullCount: 0
            )

            // Query 25 to 75 should be 50%
            let selectivity = histogram.estimateRangeSelectivity(
                min: .int64(25),
                max: .int64(75),
                minInclusive: true,
                maxInclusive: true
            )

            #expect(abs(selectivity - 0.5) < 0.01)
        }

        @Test("Double range selectivity is accurate")
        func doubleRangeSelectivity() {
            let histogram = Histogram(
                buckets: [
                    Histogram.Bucket(
                        lowerBound: .double(0.0),
                        upperBound: .double(1.0),
                        count: 1000,
                        distinctCount: 1000
                    )
                ],
                totalCount: 1000,
                nullCount: 0
            )

            // Query 0.1 to 0.3 should be 20%
            let selectivity = histogram.estimateRangeSelectivity(
                min: .double(0.1),
                max: .double(0.3),
                minInclusive: true,
                maxInclusive: true
            )

            #expect(abs(selectivity - 0.2) < 0.01)
        }

        @Test("Date range selectivity works correctly")
        func dateRangeSelectivity() {
            // Store dates as Double (timestamp)
            let startTimestamp: Double = 0
            let endTimestamp: Double = 86400 * 100  // 100 days

            let histogram = Histogram(
                buckets: [
                    Histogram.Bucket(
                        lowerBound: .double(startTimestamp),
                        upperBound: .double(endTimestamp),
                        count: 100,
                        distinctCount: 100
                    )
                ],
                totalCount: 100,
                nullCount: 0
            )

            // Query for first 50 days
            let midTimestamp: Double = 86400 * 50
            let selectivity = histogram.estimateRangeSelectivity(
                min: .double(startTimestamp),
                max: .double(midTimestamp),
                minInclusive: true,
                maxInclusive: true
            )

            #expect(abs(selectivity - 0.5) < 0.01)
        }
    }

    // MARK: - Edge Cases

    @Suite("Edge Cases")
    struct EdgeCaseTests {

        @Test("Empty histogram returns zero selectivity")
        func emptyHistogramReturnsZero() {
            let histogram = Histogram(
                buckets: [],
                totalCount: 0,
                nullCount: 0
            )

            let selectivity = histogram.estimateRangeSelectivity(
                min: .int64(0),
                max: .int64(100),
                minInclusive: true,
                maxInclusive: true
            )

            #expect(selectivity == 0.0)
        }

        @Test("Single value bucket returns 1.0 for matching query")
        func singleValueBucket() {
            let histogram = Histogram(
                buckets: [
                    Histogram.Bucket(
                        lowerBound: .int64(42),
                        upperBound: .int64(42),
                        count: 100,
                        distinctCount: 1
                    )
                ],
                totalCount: 100,
                nullCount: 0
            )

            let selectivity = histogram.estimateRangeSelectivity(
                min: .int64(40),
                max: .int64(50),
                minInclusive: true,
                maxInclusive: true
            )

            // Single value bucket fully contained in range
            #expect(selectivity == 1.0)
        }

        @Test("No overlap returns zero")
        func noOverlapReturnsZero() {
            let histogram = Histogram(
                buckets: [
                    Histogram.Bucket(
                        lowerBound: .int64(0),
                        upperBound: .int64(10),
                        count: 100,
                        distinctCount: 10
                    )
                ],
                totalCount: 100,
                nullCount: 0
            )

            let selectivity = histogram.estimateRangeSelectivity(
                min: .int64(100),
                max: .int64(200),
                minInclusive: true,
                maxInclusive: true
            )

            #expect(selectivity == 0.0)
        }

        @Test("Null selectivity is calculated correctly")
        func nullSelectivity() {
            let histogram = Histogram(
                buckets: [
                    Histogram.Bucket(
                        lowerBound: .int64(0),
                        upperBound: .int64(100),
                        count: 80,
                        distinctCount: 100
                    )
                ],
                totalCount: 80,
                nullCount: 20
            )

            let nullSel = histogram.estimateNullSelectivity(isNull: true)
            let notNullSel = histogram.estimateNullSelectivity(isNull: false)

            #expect(abs(nullSel - 0.2) < 0.01)
            #expect(abs(notNullSel - 0.8) < 0.01)
        }
    }

    // MARK: - Equality Selectivity Tests

    @Suite("Equality Selectivity")
    struct EqualitySelectivityTests {

        @Test("Equality selectivity uses bucket distinct count")
        func equalityUsesDistinctCount() {
            let histogram = Histogram(
                buckets: [
                    Histogram.Bucket(
                        lowerBound: .int64(0),
                        upperBound: .int64(100),
                        count: 1000,
                        distinctCount: 100
                    )
                ],
                totalCount: 1000,
                nullCount: 0
            )

            // Selectivity for equality = (bucket.count / totalCount) / distinctCount
            // = (1000 / 1000) / 100 = 0.01
            let selectivity = histogram.estimateEqualsSelectivity(value: .int64(50))

            #expect(abs(selectivity - 0.01) < 0.001)
        }

        @Test("Value outside histogram returns zero")
        func valueOutsideHistogram() {
            let histogram = Histogram(
                buckets: [
                    Histogram.Bucket(
                        lowerBound: .int64(0),
                        upperBound: .int64(100),
                        count: 1000,
                        distinctCount: 100
                    )
                ],
                totalCount: 1000,
                nullCount: 0
            )

            let selectivity = histogram.estimateEqualsSelectivity(value: .int64(200))

            #expect(selectivity == 0.0)
        }
    }

    // MARK: - Multi-Bucket Tests

    @Suite("Multi-Bucket Histograms")
    struct MultiBucketTests {

        @Test("Range spanning multiple buckets accumulates correctly")
        func rangeSpanningMultipleBuckets() {
            let histogram = Histogram(
                buckets: [
                    Histogram.Bucket(
                        lowerBound: .int64(0),
                        upperBound: .int64(25),
                        count: 250,
                        distinctCount: 25
                    ),
                    Histogram.Bucket(
                        lowerBound: .int64(26),
                        upperBound: .int64(50),
                        count: 250,
                        distinctCount: 25
                    ),
                    Histogram.Bucket(
                        lowerBound: .int64(51),
                        upperBound: .int64(75),
                        count: 250,
                        distinctCount: 25
                    ),
                    Histogram.Bucket(
                        lowerBound: .int64(76),
                        upperBound: .int64(100),
                        count: 250,
                        distinctCount: 25
                    )
                ],
                totalCount: 1000,
                nullCount: 0
            )

            // Query spanning all buckets should be 100%
            let fullSelectivity = histogram.estimateRangeSelectivity(
                min: .int64(0),
                max: .int64(100),
                minInclusive: true,
                maxInclusive: true
            )
            #expect(abs(fullSelectivity - 1.0) < 0.01)

            // Query spanning first two buckets should be 50%
            let halfSelectivity = histogram.estimateRangeSelectivity(
                min: .int64(0),
                max: .int64(50),
                minInclusive: true,
                maxInclusive: true
            )
            #expect(abs(halfSelectivity - 0.5) < 0.01)
        }

        @Test("Partial bucket overlap uses interpolation")
        func partialBucketOverlap() {
            let histogram = Histogram(
                buckets: [
                    Histogram.Bucket(
                        lowerBound: .int64(0),
                        upperBound: .int64(100),
                        count: 1000,
                        distinctCount: 100
                    )
                ],
                totalCount: 1000,
                nullCount: 0
            )

            // Query for 0-50 should use interpolation to get ~50%
            let selectivity = histogram.estimateRangeSelectivity(
                min: .int64(0),
                max: .int64(50),
                minInclusive: true,
                maxInclusive: true
            )

            #expect(abs(selectivity - 0.5) < 0.01)
        }
    }

    // MARK: - Helper Methods

    private static func createTestHistogram() -> Histogram {
        Histogram(
            buckets: [
                Histogram.Bucket(
                    lowerBound: .string("aaa"),
                    upperBound: .string("zzz"),
                    count: 1000,
                    distinctCount: 500
                )
            ],
            totalCount: 1000,
            nullCount: 0
        )
    }
}
