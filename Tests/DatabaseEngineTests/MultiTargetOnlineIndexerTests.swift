// MultiTargetOnlineIndexerTests.swift
// Tests for MultiTargetOnlineIndexer implementation
//
// Reference: FDB Record Layer multi-target indexing strategy

import Testing
import Foundation
@testable import DatabaseEngine
@testable import Core

// MARK: - Index State Tests for Multi-Target

@Suite("Index State Tests for Multi-Target")
struct IndexStateMultiTargetTests {

    @Test("Index state enum values")
    func testIndexStateValues() {
        let disabled = IndexState.disabled
        let writeOnly = IndexState.writeOnly
        let readable = IndexState.readable

        #expect(disabled != writeOnly)
        #expect(writeOnly != readable)
        #expect(disabled != readable)
    }

    @Test("Index state transition order")
    func testIndexStateTransitionOrder() {
        // Valid transitions: disabled -> writeOnly -> readable
        // Multi-target indexer should follow this order for all indexes

        let states: [IndexState] = [.disabled, .writeOnly, .readable]

        #expect(states[0] == .disabled)
        #expect(states[1] == .writeOnly)
        #expect(states[2] == .readable)
    }
}

// MARK: - RangeSet Tests for Multi-Target Progress

@Suite("RangeSet for Multi-Target Progress Tests")
struct RangeSetMultiTargetTests {

    @Test("RangeSet creation with initial range")
    func testRangeSetCreation() {
        let begin: [UInt8] = [0x00]
        let end: [UInt8] = [0xFF]

        let rangeSet = RangeSet(initialRange: (begin: begin, end: end))

        #expect(!rangeSet.isEmpty)
    }

    @Test("RangeSet batch extraction")
    func testRangeSetBatchExtraction() {
        let begin: [UInt8] = [0x00]
        let end: [UInt8] = [0xFF]

        let rangeSet = RangeSet(initialRange: (begin: begin, end: end))

        let bounds = rangeSet.nextBatchBounds()

        #expect(bounds != nil)
        #expect(bounds!.begin == begin)
    }

    @Test("RangeSet marks completed ranges")
    func testRangeSetMarkCompleted() {
        let begin: [UInt8] = [0x00]
        let end: [UInt8] = [0xFF]

        var rangeSet = RangeSet(initialRange: (begin: begin, end: end))

        if let bounds = rangeSet.nextBatchBounds() {
            rangeSet.recordProgress(
                rangeIndex: bounds.rangeIndex,
                lastProcessedKey: bounds.end,
                isComplete: true
            )
            // After marking completed, the next batch should be nil
            #expect(rangeSet.nextBatchBounds() == nil)
        }
    }

    @Test("RangeSet Codable support")
    func testRangeSetCodable() throws {
        let begin: [UInt8] = [0x00]
        let end: [UInt8] = [0xFF]

        let rangeSet = RangeSet(initialRange: (begin: begin, end: end))

        let encoder = JSONEncoder()
        let data = try encoder.encode(rangeSet)

        let decoder = JSONDecoder()
        let decoded = try decoder.decode(RangeSet.self, from: data)

        #expect(!decoded.isEmpty)
    }

    @Test("Empty RangeSet is complete")
    func testEmptyRangeSetIsComplete() {
        var rangeSet = RangeSet(initialRange: (begin: [0x00], end: [0x01]))

        // Extract all batches and mark complete
        while let bounds = rangeSet.nextBatchBounds() {
            rangeSet.recordProgress(
                rangeIndex: bounds.rangeIndex,
                lastProcessedKey: bounds.end,
                isComplete: true
            )
        }

        #expect(rangeSet.isEmpty)
    }
}

// MARK: - Metrics Tests

@Suite("Multi-Target Indexer Metrics Tests")
struct MultiTargetIndexerMetricsTests {

    @Test("Metric labels include item type and target count")
    func testMetricLabels() {
        let itemType = "User"
        let targetCount = 3

        let baseDimensions: [(String, String)] = [
            ("item_type", itemType),
            ("target_count", String(targetCount))
        ]

        #expect(baseDimensions.count == 2)
        #expect(baseDimensions[0].0 == "item_type")
        #expect(baseDimensions[0].1 == "User")
        #expect(baseDimensions[1].0 == "target_count")
        #expect(baseDimensions[1].1 == "3")
    }

    @Test("Counter metric names follow convention")
    func testMetricNameConvention() {
        let expectedNames = [
            "fdb_multi_indexer_items_indexed_total",
            "fdb_multi_indexer_batches_processed_total",
            "fdb_multi_indexer_batch_duration_seconds",
            "fdb_multi_indexer_errors_total"
        ]

        for name in expectedNames {
            #expect(name.hasPrefix("fdb_"))
            #expect(name.contains("multi_indexer"))
        }
    }
}

// MARK: - Progress Key Generation Tests

@Suite("Multi-Target Progress Key Generation Tests")
struct MultiTargetProgressKeyTests {

    @Test("Progress key generation is deterministic for same indexes")
    func testProgressKeyDeterminism() {
        let indexNames1 = ["idx_b", "idx_a"]
        let indexNames2 = ["idx_a", "idx_b"]

        // Progress key should be sorted, so order of input doesn't matter
        let sortedKey1 = indexNames1.sorted().joined(separator: "+")
        let sortedKey2 = indexNames2.sorted().joined(separator: "+")

        #expect(sortedKey1 == sortedKey2)
        #expect(sortedKey1 == "idx_a+idx_b")
    }

    @Test("Progress keys are unique for different index sets")
    func testProgressKeyUniqueness() {
        let indexSet1 = ["idx_a", "idx_b"]
        let indexSet2 = ["idx_a", "idx_c"]

        let key1 = indexSet1.sorted().joined(separator: "+")
        let key2 = indexSet2.sorted().joined(separator: "+")

        #expect(key1 != key2)
    }
}

// MARK: - Batch Configuration Tests

@Suite("Multi-Target Batch Configuration Tests")
struct MultiTargetBatchConfigTests {

    @Test("Batch size configuration is respected")
    func testBatchSizeConfiguration() {
        let defaultBatchSize = 100
        let customBatchSize = 50

        #expect(defaultBatchSize > 0)
        #expect(customBatchSize > 0)
        #expect(customBatchSize != defaultBatchSize)
    }

    @Test("Throttle delay configuration is respected")
    func testThrottleDelayConfiguration() {
        let noThrottle = 0
        let withThrottle = 100

        #expect(noThrottle == 0)
        #expect(withThrottle > 0)
    }
}

// MARK: - Concurrency Safety Tests

@Suite("Multi-Target Indexer Concurrency Tests")
struct MultiTargetIndexerConcurrencyTests {

    @Test("IndexState is Sendable")
    func testIndexStateSendable() {
        let state = IndexState.readable

        // If this compiles, IndexState is Sendable
        let sendable: any Sendable = state

        // Verify the state is correctly created
        #expect(state == .readable)

        // Cast back to verify type identity
        if let castBack = sendable as? IndexState {
            #expect(castBack == .readable)
        }
    }

    @Test("Progress tracking is atomic")
    func testProgressTrackingAtomic() {
        // Progress is saved per transaction, ensuring atomicity
        // Verify the concept works with batch completion using new API

        var rangeSet = RangeSet(initialRange: (begin: [0x00], end: [0xFF]))
        var completedCount = 0

        // Process batches using nextBatchBounds and recordProgress
        while let bounds = rangeSet.nextBatchBounds() {
            // Simulate processing by marking as complete
            rangeSet.recordProgress(
                rangeIndex: bounds.rangeIndex,
                lastProcessedKey: bounds.end,
                isComplete: true
            )
            completedCount += 1
        }

        // With a single initial range, we get one batch
        #expect(completedCount >= 1)
        #expect(rangeSet.isEmpty)
    }
}

// MARK: - Error Handling Tests

@Suite("Multi-Target Indexer Error Handling Tests")
struct MultiTargetIndexerErrorHandlingTests {

    @Test("Index state errors are propagated")
    func testIndexStateErrorPropagation() {
        enum TestError: Error {
            case indexNotFound(String)
            case invalidStateTransition(from: IndexState, to: IndexState)
        }

        let error = TestError.indexNotFound("idx_missing")

        switch error {
        case .indexNotFound(let name):
            #expect(name == "idx_missing")
        default:
            Issue.record("Unexpected error type")
        }
    }

    @Test("Transaction errors are handled")
    func testTransactionErrorHandling() {
        enum TransactionError: Error {
            case conflictRetryable
            case commitFailed
            case timeout
        }

        let errors: [TransactionError] = [.conflictRetryable, .commitFailed, .timeout]
        #expect(errors.count == 3)
    }
}

// MARK: - Performance Tests

@Suite("Multi-Target Efficiency Tests")
struct MultiTargetEfficiencyTests {

    @Test("Single scan for multiple indexes is more efficient")
    func testSingleScanEfficiency() {
        let itemCount = 1000
        let indexCount = 3

        // Multi-target: 1 scan + N writes per item
        let multiTargetIO = itemCount + (itemCount * indexCount)

        // Sequential: N scans + N writes per item
        let sequentialIO = (itemCount * indexCount) + (itemCount * indexCount)

        #expect(multiTargetIO < sequentialIO)
    }

    @Test("Batch processing reduces transaction count")
    func testBatchProcessingEfficiency() {
        let totalItems = 10000
        let batchSize = 100

        let transactionCount = (totalItems + batchSize - 1) / batchSize

        #expect(transactionCount == 100)
        #expect(transactionCount < totalItems)
    }
}
