import DatabaseKit
import StorageKit
import StorageKitSystemClock
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected scalar instruction ownership", .timeLimit(.minutes(1)))
struct ScalarMachineTests {
    @Test("Deep scalar control uses admitted instructions and releases unfinished storage")
    func explicitScalarStack() async throws {
        let meter = makeMeter()
        do {
            var bound = try binder(meter).bind(.int(7))
            for _ in 0..<120 {
                bound = CanonicalBoundExpression(expression: .negate(bound.expression), type: bound.type,
                    isNullable: false, children: [bound])
            }
            let result = try await DatabaseContext.evaluateCanonicalScalar(bound, on: empty(), workMeter: meter)
            #expect(result.value == .primitive(.int64(7)))
            withExtendedLifetime(result) {}
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Unselected scalar branches keep errors dormant and selected failures release every claim")
    func lazyAndFailureOwnership() async throws {
        let meter = makeMeter()
        let failure = Expression.divide(.int(1), .int(0))
        let expression = Expression.coalesce([.int(7), failure])
        do {
            let result = try await DatabaseContext.evaluateCanonicalScalar(binder(meter).bind(expression),
                on: empty(), workMeter: meter)
            #expect(result.value == .primitive(.int64(7)))
            withExtendedLifetime(result) {}
        }
        await #expect(throws: (any Error).self) {
            try await DatabaseContext.evaluateCanonicalScalar(binder(meter).bind(failure), on: empty(), workMeter: meter)
        }
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(meter.retainedIntermediateRows == 0)
    }

    @Test("Cancellation and work failure discard instruction/value state")
    func cancellationAndBudget() async throws {
        let meter = makeMeter()
        let bound = try binder(meter).bind(.add(.int(1), .int(2)))
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try await DatabaseContext.evaluateCanonicalScalar(bound, on: empty(), workMeter: meter)
        }
        await #expect(throws: (any Error).self) { try await task.value }
        #expect(meter.retainedIntermediateBytes == 0)
        let limited = makeMeter(work: 0)
        await #expect(throws: DatabaseWorkLimitError.self) {
            try await DatabaseContext.evaluateCanonicalScalar(bound, on: empty(), workMeter: limited)
        }
        #expect(limited.retainedIntermediateBytes == 0)
    }

    private func binder(_ meter: DatabaseWorkMeter) -> CanonicalQueryTypeBinder {
        CanonicalQueryTypeBinder(columns: [], resolveColumn: { _ in throw DatabaseExpressionEvaluationError.missingColumn("test") },
            wildcardSlots: { _ in [] }, workMeter: meter)
    }
    private func empty() -> CanonicalSourceRow { CanonicalSourceRow(values: [], layout: CanonicalSourceLayout(slots: [])) }
    private func makeMeter(work: UInt64 = 1000000) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 10000, maximumWorkUnits: work,
            maximumIntermediateBytes: 64 * 1024 * 1024, timeoutMilliseconds: 30000), monotonicClock: SystemStorageClock())
    }
}
