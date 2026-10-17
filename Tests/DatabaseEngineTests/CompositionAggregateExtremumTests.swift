#if MultiBase
import DatabaseKit
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@testable import DatabaseEngine

@Suite("Composition selected extremum ownership")
struct CompositionAggregateExtremumTests {
    private let limits = QueryValueLimits(maximumRetainedBytes: 1_000_000,
        maximumTotalNodes: 1_000, maximumNestingDepth: 32, maximumCollectionElements: 1_000)

    private func meter(bytes: UInt64 = 1_000_000) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumWorkUnits: 1_000_000,
            maximumIntermediateBytes: bytes), monotonicClock: TestProcessMonotonicClock())
    }

    @Test("Extremum independently retains the complete selected value and descriptor")
    func completeValueAndTypeOwner() throws {
        let descriptor = try QueryCompositeType(columns: [
            QueryColumn(number: 1, name: "value", type: .sql(.text), isNullable: true)
        ], limits: limits)
        let value = QueryValue.composite(try QueryCompositeValue(type: descriptor,
            fields: [.primitive(.string("retained"))], limits: limits))
        let work = meter()
        var retained: CompositionAggregateExtremum? = try .retaining(value,
            type: .sql(.composite(descriptor)), workMeter: work)
        #expect(retained?.value == value)
        #expect(retained?.type == .sql(.composite(descriptor)))
        let exact = work.retainedIntermediateBytes
        #expect(exact > 0)
        var alias = retained
        retained = nil
        #expect(work.retainedIntermediateBytes == exact)
        #expect(alias?.value == value)
        alias = nil
        #expect(work.retainedIntermediateBytes == 0)
        #expect(work.retainedIntermediateRows == 0)
    }

    @Test("Replacement admits before retaining and preserves the old claim on failure")
    func exactBudgetAndFailedReplacement() throws {
        let first = QueryValue.numeric(try QueryNumericValue(
            value: ExactDecimal(coefficient: 125, scale: 2), displayScale: 5))
        let replacement = QueryValue.primitive(.string(String(repeating: "x", count: 4096)))
        let calibration = meter()
        var measured: CompositionAggregateExtremum? = try .retaining(first,
            type: .sql(.decimal(precision: nil, scale: nil)), workMeter: calibration)
        let exact = calibration.retainedIntermediateBytes
        #expect(measured?.value == first)
        measured = nil
        #expect(calibration.retainedIntermediateBytes == 0)
        let short = meter(bytes: exact - 1)
        #expect(throws: DatabaseWorkLimitError.self) {
            try CompositionAggregateExtremum.retaining(first,
                type: .sql(.decimal(precision: nil, scale: nil)), workMeter: short)
        }
        #expect(short.retainedIntermediateBytes == 0)
        let work = meter(bytes: exact)
        var current: CompositionAggregateExtremum? = try .retaining(first,
            type: .sql(.decimal(precision: nil, scale: nil)), workMeter: work)
        #expect(throws: DatabaseWorkLimitError.self) {
            try CompositionAggregateExtremum.retaining(replacement, type: .sql(.text), workMeter: work)
        }
        #expect(current?.value == first)
        #expect(work.retainedIntermediateBytes == exact)
        current = nil
        #expect(work.retainedIntermediateBytes == 0)
        current = try .retaining(first, type: .sql(.decimal(precision: nil, scale: nil)), workMeter: work)
        #expect(current?.value == first)
        current = nil
        #expect(work.retainedIntermediateBytes == 0)
    }

    @Test("Cancellation releases a live extremum during failed replacement")
    func cancellationReleasesTransition() async throws {
        let work = meter()
        let task = Task {
            let retained = try CompositionAggregateExtremum.retaining(.primitive(.string("old")),
                type: .sql(.text), workMeter: work)
            withUnsafeCurrentTask { $0?.cancel() }
            defer { withExtendedLifetime(retained) {} }
            return try CompositionAggregateExtremum.retaining(.primitive(.string("new")),
                type: .sql(.text), workMeter: work)
        }
        do {
            _ = try await task.value
            Issue.record("Expected cancellation")
        } catch {
            #expect(error is DatabaseWorkLimitError || error is CancellationError)
        }
        #expect(work.retainedIntermediateBytes == 0)
        #expect(work.retainedIntermediateRows == 0)
        var recovered: CompositionAggregateExtremum? = try .retaining(.primitive(.string("valid")),
            type: .sql(.text), workMeter: work)
        #expect(recovered?.value == .primitive(.string("valid")))
        recovered = nil
        #expect(work.retainedIntermediateBytes == 0)
    }
}
#endif
