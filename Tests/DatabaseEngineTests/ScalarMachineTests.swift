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

    @Test("Extrema retain only the current selection and next computed argument")
    func boundedExtremeArguments() async throws {
        for greatest in [true, false] {
            let meter = makeMeter(work: 10000000, bytes: 256 * 1024)
            do {
                let child = Expression.function(FunctionCall(name: "repeat", arguments: [.literal(.string("x")),
                    .literal(.queryValue(.primitive(.int32(65536)), type: .sql(.integer)))]))
                let arguments = Array(repeating: child, count: 8)
                let expression = greatest ? Expression.greatest(arguments) : Expression.least(arguments)
                let result = try await DatabaseContext.evaluateCanonicalScalar(binder(meter).bind(expression),
                    on: empty(), workMeter: meter)
                #expect(result.value == .primitive(.string(String(repeating: "x", count: 65536))))
                withExtendedLifetime(result) {}
            }
            #expect(meter.retainedIntermediateBytes == 0)
            #expect(meter.retainedIntermediateRows == 0)
        }
    }

    @Test("A positional leaf reads its admitted frame without instruction storage")
    func positionalLeafBudget() async throws {
        let schema = try CanonicalRelationSchema(unscopedColumns: ["n"], types: [.sql(.bigint)], nullability: [false])
        let row = CanonicalSourceRow(values: [.primitive(.int64(7))], layout: schema.layout)
        let probe = makeMeter()
        let footprint = try DatabaseContext.prospectiveSourceCompositionFootprint(row, outer: nil, workMeter: probe)
        let meter = makeMeter(bytes: footprint.bytes)
        let bound = CanonicalBoundExpression(expression: .column(ColumnRef("n")), type: .sql(.bigint),
            isNullable: false, columnSlot: 0)
        do {
            let result = try await DatabaseContext.evaluateSelectedValue(bound, on: row,
                context: nil, databaseContext: nil, workMeter: meter)
            #expect(result.value == .primitive(.int64(7)))
            #expect(meter.consumedWorkUnits == probe.consumedWorkUnits + 1)
            withExtendedLifetime(result) {}
        }
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func binder(_ meter: DatabaseWorkMeter) -> CanonicalQueryTypeBinder {
        CanonicalQueryTypeBinder(columns: [], resolveColumn: { _ in throw DatabaseExpressionEvaluationError.missingColumn("test") },
            wildcardSlots: { _ in [] }, workMeter: meter)
    }
    private func empty() -> CanonicalSourceRow { CanonicalSourceRow(values: [], layout: CanonicalSourceLayout(slots: [])) }
    private func makeMeter(work: UInt64 = 1000000, bytes: UInt64 = 64 * 1024 * 1024) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 10000, maximumWorkUnits: work,
            maximumIntermediateBytes: bytes, timeoutMilliseconds: 30000), monotonicClock: SystemStorageClock())
    }
}
