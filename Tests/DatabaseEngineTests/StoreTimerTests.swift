#if !os(WASI)
#if FOUNDATION_DB
// StoreTimerTests.swift
// DatabaseEngine Tests - StoreTimer instrumentation tests

import Testing
import TestHeartbeat
import TestSupport
import Foundation
@testable import DatabaseEngine

// MARK: - StoreTimerEvent Tests

@Suite("StoreTimerEvent Tests", .heartbeat)
struct StoreTimerEventTests {

    @Test("Events have unique names")
    func eventUniqueNames() {
        let events: [StoreTimerEvent] = [
            .getReadVersion, .commit, .saveEntity, .loadEntity,
            .updateIndex, .scanIndex, .rangeScan, .serialize, .deserialize
        ]

        let names = events.map { $0.name }
        let uniqueNames = Set(names)

        #expect(names.count == uniqueNames.count)
    }

    @Test("Event description returns name")
    func eventDescription() {
        let event = StoreTimerEvent.saveEntity
        #expect(event.description == "save_entity")
    }

    @Test("Count events are marked correctly")
    func countEvents() {
        #expect(StoreTimerEvent.entitiesSaved.isCount == true)
        #expect(StoreTimerEvent.saveEntity.isCount == false)
    }

    @Test("Size events are marked correctly")
    func sizeEvents() {
        #expect(StoreTimerEvent.bytesSerialized.isSize == true)
        #expect(StoreTimerEvent.saveEntity.isSize == false)
    }
}

// MARK: - StoreTimer Tests

@Suite("StoreTimer Tests", .heartbeat)
struct StoreTimerTests {

    @Test("Entity timing event")
    func recordTimingEvent() {
        let timer = StoreTimer(monotonicClock: TestProcessMonotonicClock(), metrics: .disabled)

        timer.record(.saveEntity, duration: 1_000_000) // 1ms
        timer.record(.saveEntity, duration: 2_000_000) // 2ms

        let stats = timer.getStats(.saveEntity)
        #expect(stats != nil)
        #expect(stats?.count == 2)
        #expect(stats?.totalNanos == 3_000_000)
        #expect(stats?.minNanos == 1_000_000)
        #expect(stats?.maxNanos == 2_000_000)
    }

    @Test("Increment count event")
    func incrementCountEvent() {
        let timer = StoreTimer(monotonicClock: TestProcessMonotonicClock(), metrics: .disabled)

        timer.increment(.entitiesSaved, by: 5)
        timer.increment(.entitiesSaved, by: 10)

        let count = timer.getCount(.entitiesSaved)
        #expect(count == 15)
    }

    @Test("Time synchronous operation")
    func timeSynchronousOperation() {
        let timer = StoreTimer(monotonicClock: TestProcessMonotonicClock(), metrics: .disabled)

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
        let timer = StoreTimer(monotonicClock: TestProcessMonotonicClock(), metrics: .disabled)

        let result = try await timer.time(.loadEntity) {
            try await Task.sleep(nanoseconds: 10_000_000) // 10ms
            return 42
        }

        #expect(result == 42)

        let stats = timer.getStats(.loadEntity)
        #expect(stats != nil)
        #expect(stats?.count == 1)
    }

    @Test("Get all stats")
    func getAllStats() {
        let timer = StoreTimer(monotonicClock: TestProcessMonotonicClock(), metrics: .disabled)

        timer.record(.saveEntity, duration: 1_000_000)
        timer.record(.loadEntity, duration: 2_000_000)
        timer.increment(.entitiesSaved, by: 5)

        let allStats = timer.getAllStats()
        #expect(allStats.count == 3)
    }

    @Test("Reset clears all data")
    func resetClearsData() {
        let timer = StoreTimer(monotonicClock: TestProcessMonotonicClock(), metrics: .disabled)

        timer.record(.saveEntity, duration: 1_000_000)
        timer.increment(.entitiesSaved, by: 5)

        timer.reset()

        #expect(timer.getCount(.saveEntity) == 0)
        #expect(timer.getCount(.entitiesSaved) == 0)
        #expect(timer.getAllStats().isEmpty)
    }

    @Test("Reset specific event")
    func resetSpecificEvent() {
        let timer = StoreTimer(monotonicClock: TestProcessMonotonicClock(), metrics: .disabled)

        timer.record(.saveEntity, duration: 1_000_000)
        timer.record(.loadEntity, duration: 2_000_000)

        timer.reset(.saveEntity)

        #expect(timer.getStats(.saveEntity) == nil)
        #expect(timer.getStats(.loadEntity) != nil)
    }

    @Test("Add timers merges data")
    func addTimersMergesData() {
        let timer1 = StoreTimer(monotonicClock: TestProcessMonotonicClock(), metrics: .disabled)
        let timer2 = StoreTimer(monotonicClock: TestProcessMonotonicClock(), metrics: .disabled)

        timer1.record(.saveEntity, duration: 1_000_000)
        timer2.record(.saveEntity, duration: 2_000_000)

        timer1.add(timer2)

        let stats = timer1.getStats(.saveEntity)
        #expect(stats?.count == 2)
        #expect(stats?.totalNanos == 3_000_000)
    }

    @Test("EventStats calculations correct")
    func eventStatsCalculations() {
        let timer = StoreTimer(monotonicClock: TestProcessMonotonicClock(), metrics: .disabled)

        timer.record(.saveEntity, duration: 1_000_000)
        timer.record(.saveEntity, duration: 3_000_000)

        let stats = timer.getStats(.saveEntity)!

        #expect(stats.avgNanos == 2_000_000)
        #expect(stats.totalMs == 4.0)
        #expect(stats.avgMs == 2.0)
        #expect(stats.minMs == 1.0)
        #expect(stats.maxMs == 3.0)
    }
}

// MARK: - StoreTimerSnapshot Tests

@Suite("StoreTimerSnapshot Tests", .heartbeat)
struct StoreTimerSnapshotTests {

    @Test("Snapshot captures current state")
    func snapshotCapturesState() {
        let timer = StoreTimer(monotonicClock: TestProcessMonotonicClock(), metrics: .disabled)

        timer.record(.saveEntity, duration: 1_000_000)
        timer.increment(.entitiesSaved, by: 5)

        let snapshot = StoreTimerSnapshot(from: timer)

        #expect(snapshot.stats.count == 2)
        #expect(snapshot.stats[.saveEntity] != nil)
        #expect(snapshot.stats[.entitiesSaved] != nil)
    }

    @Test("Snapshot difference calculation")
    func snapshotDifference() {
        let timer = StoreTimer(monotonicClock: TestProcessMonotonicClock(), metrics: .disabled)

        timer.record(.saveEntity, duration: 1_000_000)
        let snapshot1 = StoreTimerSnapshot(from: timer)

        timer.record(.saveEntity, duration: 2_000_000)
        let snapshot2 = StoreTimerSnapshot(from: timer)

        let diff = snapshot2.difference(from: snapshot1)

        #expect(diff[.saveEntity] != nil)
        #expect(diff[.saveEntity]?.count == 1)
        #expect(diff[.saveEntity]?.totalNanos == 2_000_000)
    }
}
#endif

#endif
