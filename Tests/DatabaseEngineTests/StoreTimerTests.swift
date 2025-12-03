// StoreTimerTests.swift
// DatabaseEngine Tests - StoreTimer instrumentation tests

import Testing
import Foundation
@testable import DatabaseEngine

// MARK: - StoreTimerEvent Tests

@Suite("StoreTimerEvent Tests")
struct StoreTimerEventTests {

    @Test("Events have unique names")
    func eventUniqueNames() {
        let events: [StoreTimerEvent] = [
            .getReadVersion, .commit, .saveRecord, .loadRecord,
            .updateIndex, .scanIndex, .rangeScan, .serialize, .deserialize
        ]

        let names = events.map { $0.name }
        let uniqueNames = Set(names)

        #expect(names.count == uniqueNames.count)
    }

    @Test("Event description returns name")
    func eventDescription() {
        let event = StoreTimerEvent.saveRecord
        #expect(event.description == "save_record")
    }

    @Test("Count events are marked correctly")
    func countEvents() {
        #expect(StoreTimerEvent.recordsSaved.isCount == true)
        #expect(StoreTimerEvent.saveRecord.isCount == false)
    }

    @Test("Size events are marked correctly")
    func sizeEvents() {
        #expect(StoreTimerEvent.bytesSerialized.isSize == true)
        #expect(StoreTimerEvent.saveRecord.isSize == false)
    }
}

// MARK: - StoreTimer Tests

@Suite("StoreTimer Tests")
struct StoreTimerTests {

    @Test("Record timing event")
    func recordTimingEvent() {
        let timer = StoreTimer(emitMetrics: false)

        timer.record(.saveRecord, duration: 1_000_000) // 1ms
        timer.record(.saveRecord, duration: 2_000_000) // 2ms

        let stats = timer.getStats(.saveRecord)
        #expect(stats != nil)
        #expect(stats?.count == 2)
        #expect(stats?.totalNanos == 3_000_000)
        #expect(stats?.minNanos == 1_000_000)
        #expect(stats?.maxNanos == 2_000_000)
    }

    @Test("Increment count event")
    func incrementCountEvent() {
        let timer = StoreTimer(emitMetrics: false)

        timer.increment(.recordsSaved, by: 5)
        timer.increment(.recordsSaved, by: 10)

        let count = timer.getCount(.recordsSaved)
        #expect(count == 15)
    }

    @Test("Time synchronous operation")
    func timeSynchronousOperation() {
        let timer = StoreTimer(emitMetrics: false)

        let result = timer.time(.serialize) {
            Thread.sleep(forTimeInterval: 0.01) // 10ms
            return "done"
        }

        #expect(result == "done")

        let stats = timer.getStats(.serialize)
        #expect(stats != nil)
        #expect(stats?.count == 1)
        #expect((stats?.totalNanos ?? 0) > 9_000_000) // At least 9ms
    }

    @Test("Time async operation")
    func timeAsyncOperation() async throws {
        let timer = StoreTimer(emitMetrics: false)

        let result = try await timer.time(.loadRecord) {
            try await Task.sleep(nanoseconds: 10_000_000) // 10ms
            return 42
        }

        #expect(result == 42)

        let stats = timer.getStats(.loadRecord)
        #expect(stats != nil)
        #expect(stats?.count == 1)
    }

    @Test("Get all stats")
    func getAllStats() {
        let timer = StoreTimer(emitMetrics: false)

        timer.record(.saveRecord, duration: 1_000_000)
        timer.record(.loadRecord, duration: 2_000_000)
        timer.increment(.recordsSaved, by: 5)

        let allStats = timer.getAllStats()
        #expect(allStats.count == 3)
    }

    @Test("Reset clears all data")
    func resetClearsData() {
        let timer = StoreTimer(emitMetrics: false)

        timer.record(.saveRecord, duration: 1_000_000)
        timer.increment(.recordsSaved, by: 5)

        timer.reset()

        #expect(timer.getCount(.saveRecord) == 0)
        #expect(timer.getCount(.recordsSaved) == 0)
        #expect(timer.getAllStats().isEmpty)
    }

    @Test("Reset specific event")
    func resetSpecificEvent() {
        let timer = StoreTimer(emitMetrics: false)

        timer.record(.saveRecord, duration: 1_000_000)
        timer.record(.loadRecord, duration: 2_000_000)

        timer.reset(.saveRecord)

        #expect(timer.getStats(.saveRecord) == nil)
        #expect(timer.getStats(.loadRecord) != nil)
    }

    @Test("Add timers merges data")
    func addTimersMergesData() {
        let timer1 = StoreTimer(emitMetrics: false)
        let timer2 = StoreTimer(emitMetrics: false)

        timer1.record(.saveRecord, duration: 1_000_000)
        timer2.record(.saveRecord, duration: 2_000_000)

        timer1.add(timer2)

        let stats = timer1.getStats(.saveRecord)
        #expect(stats?.count == 2)
        #expect(stats?.totalNanos == 3_000_000)
    }

    @Test("EventStats calculations correct")
    func eventStatsCalculations() {
        let timer = StoreTimer(emitMetrics: false)

        timer.record(.saveRecord, duration: 1_000_000)
        timer.record(.saveRecord, duration: 3_000_000)

        let stats = timer.getStats(.saveRecord)!

        #expect(stats.avgNanos == 2_000_000)
        #expect(stats.totalMs == 4.0)
        #expect(stats.avgMs == 2.0)
        #expect(stats.minMs == 1.0)
        #expect(stats.maxMs == 3.0)
    }
}

// MARK: - StoreTimerSnapshot Tests

@Suite("StoreTimerSnapshot Tests")
struct StoreTimerSnapshotTests {

    @Test("Snapshot captures current state")
    func snapshotCapturesState() {
        let timer = StoreTimer(emitMetrics: false)

        timer.record(.saveRecord, duration: 1_000_000)
        timer.increment(.recordsSaved, by: 5)

        let snapshot = StoreTimerSnapshot(from: timer)

        #expect(snapshot.stats.count == 2)
        #expect(snapshot.stats[.saveRecord] != nil)
        #expect(snapshot.stats[.recordsSaved] != nil)
    }

    @Test("Snapshot difference calculation")
    func snapshotDifference() {
        let timer = StoreTimer(emitMetrics: false)

        timer.record(.saveRecord, duration: 1_000_000)
        let snapshot1 = StoreTimerSnapshot(from: timer)

        timer.record(.saveRecord, duration: 2_000_000)
        let snapshot2 = StoreTimerSnapshot(from: timer)

        let diff = snapshot2.difference(from: snapshot1)

        #expect(diff[.saveRecord] != nil)
        #expect(diff[.saveRecord]?.count == 1)
        #expect(diff[.saveRecord]?.totalNanos == 2_000_000)
    }
}
