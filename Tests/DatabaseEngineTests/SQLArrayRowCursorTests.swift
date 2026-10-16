import DatabaseKit
import DatabaseTypes
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Bounded SQL array row cursors")
struct SQLArrayRowCursorTests {
    private func cursor(_ name: String, _ operands: [QueryValue], types: [QueryColumnType]? = nil,
        meter: DatabaseWorkMeter) throws -> SQLArrayRowCursor {
        let bound = try SQLArrayFixture.bound(.function(FunctionCall(name: name, arguments: [])), operands,
            types: types, result: .integer)
        var footprint = DatabaseIntermediateFootprint()
        for value in operands { footprint = try footprint.adding(CanonicalRelationalFootprintMeter.valueFootprint(of: value, workMeter: meter, stage: .expressionEvaluation)) }
        let producer = try meter.reserveIntermediate(bytes: footprint.bytes, at: .expressionEvaluation)
        return try CanonicalSQLArrayEvaluator.makeRowCursor(bound, operands: operands,
            inputLeases: [DatabaseQueryPayloadLease(retaining: producer)], temporalEnvironment: nil,
            workMeter: meter, limits: SQLArrayFixture.limits)
    }

    @Test func unnestMetadataAndPadding() throws {
        let meter = SQLArrayFixture.meter()
        var rows = try cursor("unnest", [SQLArrayFixture.array([1, 2]), SQLArrayFixture.array([3])], meter: meter)
        let metadata = try rows.metadata()
        #expect(metadata.columns.map { $0.number } == [1, 2])
        #expect(metadata.columns.allSatisfy { $0.type == .sql(.integer) && $0.name == "unnest" && $0.isNullable })
        let firstResult = try rows.next()
        let first = try #require(firstResult)
        let secondResult = try rows.next()
        let second = try #require(secondResult)
        #expect(first.values == [1, 3].map(SQLArrayFixture.int))
        #expect(second.values == [SQLArrayFixture.int(2), SQLArrayFixture.null])
        #expect(try rows.next() == nil)
        #expect(try rows.next() == nil)
        withExtendedLifetime((metadata, first, second)) { #expect(meter.retainedIntermediateBytes > 0) }
    }

    @Test func subscriptsUseActualBoundsAndReverse() throws {
        let meter = SQLArrayFixture.meter(), array = try SQLArrayFixture.array([10, 20], lower: -2)
        var forward = try cursor("generate_subscripts", [array, SQLArrayFixture.int(1)], meter: meter)
        #expect(try forward.metadata().columns[0].type == .sql(.integer))
        #expect(try forward.metadata().columns[0].isNullable == false)
        #expect(try forward.next()?.values == [SQLArrayFixture.int(-2)])
        #expect(try forward.next()?.values == [SQLArrayFixture.int(-1)])
        #expect(try forward.next() == nil)
        var reverse = try cursor("generate_subscripts", [array, SQLArrayFixture.int(1), .primitive(.bool(true))], meter: meter)
        #expect(try reverse.next()?.values == [SQLArrayFixture.int(-1)])
        #expect(try reverse.next()?.values == [SQLArrayFixture.int(-2)])
        reverse.close()
        var missing = try cursor("generate_subscripts", [array, SQLArrayFixture.int(2)], meter: meter)
        #expect(try missing.next() == nil)
        var nullReverse = try cursor("generate_subscripts", [array, SQLArrayFixture.int(1), SQLArrayFixture.null],
            types: [.sql(.array(.integer)), .sql(.integer), .sql(.boolean)], meter: meter)
        #expect(try nullReverse.next() == nil)
    }

    @Test func emptyAndNullInputsKeepSelectedMetadata() throws {
        let meter = SQLArrayFixture.meter()
        var rows = try cursor("unnest", [SQLArrayFixture.null, SQLArrayFixture.array([])],
            types: [.sql(.array(.date)), .sql(.array(.integer))], meter: meter)
        let metadata = try rows.metadata()
        #expect(metadata.columns.map { $0.type } == [.sql(.date), .sql(.integer)])
        #expect(try rows.next() == nil)
        #expect(metadata.columns[0].type == .sql(.date))
        #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "closed SQL array cursor metadata")) {
            _ = try rows.metadata()
        }
    }

    @Test func rowsRetainFullOwnersAfterClose() throws {
        let meter = SQLArrayFixture.meter()
        var retained: (values: [QueryValue], leases: [DatabaseQueryPayloadLease])?
        do {
            let jsonb = try QueryJSONBValue(root: .array([.string(String(repeating: "x", count: 4096)), .bool(true)], limits: SQLArrayFixture.limits), limits: SQLArrayFixture.limits)
            let array = try SQLArrayFixture.value([.jsonb(jsonb)], element: .jsonb, dimensions: [1])
            var rows = try cursor("unnest", [array], meter: meter)
            retained = try rows.next()
            rows.close(); rows.close()
            #expect(try rows.next() == nil)
        }
        #expect(meter.retainedIntermediateBytes >= 4096)
        guard let value = retained?.values.first, case .jsonb(let jsonb) = value else { Issue.record("Missing retained JSONB row"); return }
        #expect(jsonb.root.storageFootprint.utf8ByteCount >= 4096)
        retained = nil
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(meter.retainedIntermediateRows == 0)
    }

    @Test func inputClaimsRequireSameMeter() throws {
        let meter = SQLArrayFixture.meter(), other = SQLArrayFixture.meter()
        let array = try SQLArrayFixture.array([1])
        let bound = try SQLArrayFixture.bound(.function(FunctionCall(name: "unnest", arguments: [])), [array], result: .integer)
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            _ = try CanonicalSQLArrayEvaluator.makeRowCursor(bound, operands: [array], inputLeases: [],
                temporalEnvironment: nil, workMeter: meter, limits: SQLArrayFixture.limits)
        }
        let claim = try other.reserveIntermediate(bytes: 1, at: .expressionEvaluation)
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            _ = try CanonicalSQLArrayEvaluator.makeRowCursor(bound, operands: [array], inputLeases: [.init(retaining: claim)],
                temporalEnvironment: nil, workMeter: meter, limits: SQLArrayFixture.limits)
        }
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test func cursorBudgetsReleaseEveryOwner() throws {
        let array = try SQLArrayFixture.array([1, 2, 3])
        func run(_ meter: DatabaseWorkMeter) throws {
            var rows = try cursor("unnest", [array], meter: meter)
            let row = try rows.next()
            withExtendedLifetime(row) { #expect(meter.retainedIntermediateRows == 1) }
            rows.close()
        }
        let measured = SQLArrayFixture.meter(); try run(measured)
        #expect(measured.retainedIntermediateBytes == 0 && measured.retainedIntermediateRows == 0)
        let exact = SQLArrayFixture.meter(bytes: measured.peakIntermediateBytes); try run(exact)
        #expect(exact.retainedIntermediateBytes == 0)
        let short = SQLArrayFixture.meter(bytes: measured.peakIntermediateBytes - 1)
        #expect(throws: DatabaseWorkLimitError.self) { try run(short) }
        #expect(short.retainedIntermediateBytes == 0 && short.retainedIntermediateRows == 0)
    }

    @Test func deadlineAndCancellationCloseCursor() async throws {
        let clock = SQLArrayTestClock(), meter = SQLArrayFixture.meter(clock: clock)
        do {
            var rows = try cursor("unnest", [SQLArrayFixture.array([1])], meter: meter)
            clock.advance()
            #expect(throws: DatabaseWorkLimitError.self) { _ = try rows.next() }
            #expect(try rows.next() == nil)
        }
        #expect(meter.retainedIntermediateBytes == 0)
        let cancelledMeter = SQLArrayFixture.meter()
        let task = Task {
            var rows = try cursor("unnest", [SQLArrayFixture.array([1])], meter: cancelledMeter)
            withUnsafeCurrentTask { $0?.cancel() }
            do { _ = try rows.next(); Issue.record("Expected cancellation") }
            catch is CancellationError { }
            #expect(try rows.next() == nil)
        }
        try await task.value
        #expect(cancelledMeter.retainedIntermediateBytes == 0)
    }
}
