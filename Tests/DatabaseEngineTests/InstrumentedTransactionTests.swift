// InstrumentedTransactionTests.swift
// Tests for InstrumentedTransaction and metrics collection

import Testing
import Foundation
@testable import DatabaseEngine

@Suite("InstrumentedTransaction Tests")
struct InstrumentedTransactionTests {

    // MARK: - TransactionMetrics Tests

    @Test func metricsInitialization() {
        let metrics = TransactionMetrics()

        #expect(metrics.readCount == 0)
        #expect(metrics.writeCount == 0)
        #expect(metrics.bytesRead == 0)
        #expect(metrics.bytesWritten == 0)
        #expect(metrics.rangeScanCount == 0)
        #expect(metrics.emptyScanCount == 0)
        #expect(metrics.committed == false)
        #expect(metrics.rolledBack == false)
        #expect(metrics.retryCount == 0)
    }

    @Test func metricsReadTracking() {
        var metrics = TransactionMetrics()

        metrics.readCount = 5
        metrics.bytesRead = 1024

        #expect(metrics.readCount == 5)
        #expect(metrics.bytesRead == 1024)
    }

    @Test func metricsWriteTracking() {
        var metrics = TransactionMetrics()

        metrics.writeCount = 10
        metrics.bytesWritten = 2048

        #expect(metrics.writeCount == 10)
        #expect(metrics.bytesWritten == 2048)
    }

    @Test func metricsDurationCalculation() {
        var metrics = TransactionMetrics()
        metrics.startTime = Date(timeIntervalSince1970: 1000)
        metrics.endTime = Date(timeIntervalSince1970: 1001)

        #expect(metrics.duration == 1.0)
        #expect(metrics.durationNanos == 1_000_000_000)
    }

    @Test func metricsDescription() {
        var metrics = TransactionMetrics()
        metrics.readCount = 5
        metrics.writeCount = 3
        metrics.bytesRead = 1024
        metrics.bytesWritten = 512
        metrics.committed = true

        let description = metrics.description
        #expect(description.contains("Reads: 5"))
        #expect(description.contains("Writes: 3"))
        #expect(description.contains("Bytes read: 1024"))
        #expect(description.contains("Committed: true"))
    }

    @Test func metricsExportToStoreTimer() {
        var metrics = TransactionMetrics()
        metrics.readCount = 5
        metrics.writeCount = 3
        metrics.bytesRead = 1024
        metrics.bytesWritten = 512
        metrics.rangeScanCount = 2
        metrics.scannedKeyValueCount = 50
        metrics.committed = true
        metrics.retryCount = 1
        metrics.startTime = Date()
        metrics.endTime = Date()
        metrics.commitNanos = 5_000_000

        let timer = StoreTimer(emitMetrics: false)
        metrics.export(to: timer)

        #expect(timer.getCount(.recordsLoaded) == 5)
        #expect(timer.getCount(.recordsSaved) == 3)
        #expect(timer.getCount(.rangesScanned) == 2)
        #expect(timer.getCount(.rangeKeyValues) == 50)
        #expect(timer.getCount(.retries) == 1)
    }

    // MARK: - MetricsAggregator Tests

    @Test func aggregatorInitialization() {
        let aggregator = MetricsAggregator()
        let summary = aggregator.summary

        #expect(summary.totalTransactions == 0)
        #expect(summary.successfulCommits == 0)
        #expect(summary.totalRetries == 0)
    }

    @Test func aggregatorRecordsSingleTransaction() {
        let aggregator = MetricsAggregator()

        var metrics = TransactionMetrics()
        metrics.readCount = 10
        metrics.writeCount = 5
        metrics.bytesRead = 2048
        metrics.bytesWritten = 1024
        metrics.committed = true
        metrics.startTime = Date(timeIntervalSince1970: 1000)
        metrics.endTime = Date(timeIntervalSince1970: 1001)

        aggregator.record(metrics)

        let summary = aggregator.summary
        #expect(summary.totalTransactions == 1)
        #expect(summary.successfulCommits == 1)
        #expect(summary.totalReads == 10)
        #expect(summary.totalWrites == 5)
        #expect(summary.totalBytesRead == 2048)
        #expect(summary.totalBytesWritten == 1024)
    }

    @Test func aggregatorRecordsMultipleTransactions() {
        let aggregator = MetricsAggregator()

        for i in 0..<10 {
            var metrics = TransactionMetrics()
            metrics.readCount = i
            metrics.writeCount = i * 2
            metrics.committed = i % 2 == 0
            metrics.rolledBack = i % 2 != 0
            metrics.startTime = Date()
            metrics.endTime = Date()

            aggregator.record(metrics)
        }

        let summary = aggregator.summary
        #expect(summary.totalTransactions == 10)
        #expect(summary.successfulCommits == 5)
        #expect(summary.totalRollbacks == 5)
        #expect(summary.successRate == 0.5)
    }

    @Test func aggregatorSuccessRate() {
        let aggregator = MetricsAggregator()

        for _ in 0..<8 {
            var metrics = TransactionMetrics()
            metrics.committed = true
            metrics.startTime = Date()
            metrics.endTime = Date()
            aggregator.record(metrics)
        }

        for _ in 0..<2 {
            var metrics = TransactionMetrics()
            metrics.rolledBack = true
            metrics.startTime = Date()
            metrics.endTime = Date()
            aggregator.record(metrics)
        }

        let summary = aggregator.summary
        #expect(summary.successRate == 0.8)
    }

    @Test func aggregatorReset() {
        let aggregator = MetricsAggregator()

        var metrics = TransactionMetrics()
        metrics.committed = true
        metrics.startTime = Date()
        metrics.endTime = Date()
        aggregator.record(metrics)

        #expect(aggregator.summary.totalTransactions == 1)

        aggregator.reset()

        #expect(aggregator.summary.totalTransactions == 0)
    }

    @Test func aggregatorAverageReadsPerTransaction() {
        let aggregator = MetricsAggregator()

        for i in 1...10 {
            var metrics = TransactionMetrics()
            metrics.readCount = i
            metrics.startTime = Date()
            metrics.endTime = Date()
            aggregator.record(metrics)
        }

        let summary = aggregator.summary
        // Sum 1..10 = 55, average = 5.5
        #expect(summary.avgReadsPerTransaction == 5.5)
    }

    @Test func aggregatorSummaryDescription() {
        let aggregator = MetricsAggregator()

        var metrics = TransactionMetrics()
        metrics.readCount = 10
        metrics.writeCount = 5
        metrics.committed = true
        metrics.startTime = Date()
        metrics.endTime = Date()
        aggregator.record(metrics)

        let description = aggregator.summary.description
        #expect(description.contains("Transactions: 1"))
        #expect(description.contains("success: 1"))
        #expect(description.contains("Reads: 10"))
    }

    // MARK: - StoreTimerEvent Extension Tests

    @Test func storeTimerEventExtensions() {
        // Test that extended events are available
        let _ = StoreTimerEvent.transactionReads
        let _ = StoreTimerEvent.transactionWrites
        let _ = StoreTimerEvent.transactionBytesRead
        let _ = StoreTimerEvent.transactionBytesWritten
        let _ = StoreTimerEvent.emptyScans
        let _ = StoreTimerEvent.commits
        let _ = StoreTimerEvent.rollbacks

        // All events should be different
        #expect(StoreTimerEvent.transactionReads != StoreTimerEvent.transactionWrites)
        #expect(StoreTimerEvent.commits != StoreTimerEvent.rollbacks)
    }

    // MARK: - Concurrent Access Tests

    @Test func metricsAggregatorConcurrentAccess() async {
        let aggregator = MetricsAggregator()

        // Record from multiple concurrent tasks
        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<100 {
                group.addTask {
                    var metrics = TransactionMetrics()
                    metrics.readCount = 1
                    metrics.committed = true
                    metrics.startTime = Date()
                    metrics.endTime = Date()
                    aggregator.record(metrics)
                }
            }
        }

        let summary = aggregator.summary
        #expect(summary.totalTransactions == 100)
        #expect(summary.totalReads == 100)
    }

    // MARK: - Duration Tracking Tests

    @Test func aggregatorDurationTracking() {
        let aggregator = MetricsAggregator()

        // Record transaction with 10ms duration
        var metrics1 = TransactionMetrics()
        metrics1.startTime = Date(timeIntervalSince1970: 1000.000)
        metrics1.endTime = Date(timeIntervalSince1970: 1000.010)
        aggregator.record(metrics1)

        // Record transaction with 20ms duration
        var metrics2 = TransactionMetrics()
        metrics2.startTime = Date(timeIntervalSince1970: 1000.000)
        metrics2.endTime = Date(timeIntervalSince1970: 1000.020)
        aggregator.record(metrics2)

        let summary = aggregator.summary
        // Average should be 15ms
        #expect(summary.avgDurationMs > 14 && summary.avgDurationMs < 16)
        // Max should be 20ms
        #expect(summary.maxDurationMs > 19 && summary.maxDurationMs < 21)
        // Min should be 10ms
        #expect(summary.minDurationMs > 9 && summary.minDurationMs < 11)
    }
}
