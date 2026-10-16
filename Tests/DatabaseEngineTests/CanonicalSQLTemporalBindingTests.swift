import DatabaseKit
import DatabaseTypes
import StorageKitSystemClock
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected temporal signatures and identities")
struct CanonicalSQLTemporalBindingTests {
    private struct FixedClock: WallClock { let now = Timestamp(secondsSinceUnixEpoch: 0) }
    private func binder() -> CanonicalQueryTypeBinder {
        CanonicalQueryTypeBinder(columns: [], resolveColumn: { throw DatabaseExpressionEvaluationError.missingColumn($0.column) }, wildcardSlots: { _ in [] })
    }
    private func null(_ type: DataType) -> Expression { .literal(.queryValue(.primitive(.null), type: .sql(type))) }
    private func call(_ name: String, _ arguments: [Expression]) throws -> CanonicalBoundExpression {
        try binder().bind(.function(FunctionCall(name: name, arguments: arguments)))
    }
    private func meter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumWorkUnits: 1_000_000, maximumIntermediateBytes: 1_000_000), monotonicClock: SystemStorageClock())
    }

    @Test("Current expressions retain precision and reject invalid declarations before execution")
    func currentDeclarations() throws {
        let date = try binder().bind(.currentValue(kind: .date, precision: nil))
        #expect(date.type == .sql(.date) && date.children.isEmpty)
        let time = try binder().bind(.currentValue(kind: .time, precision: 3))
        #expect(time.type == .sql(.time(withTimeZone: true, precision: 3)))
        let stamp = try binder().bind(.currentValue(kind: .localTimestamp, precision: 0))
        #expect(stamp.type == .sql(.timestamp(withTimeZone: false, precision: 0)))
        for expression: Expression in [.currentValue(kind: .date, precision: 0), .currentValue(kind: .timestamp, precision: 7)] {
            #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(expression) }
        }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try call("current_date", []) }
    }

    @Test("F07 overloads select NUMERIC versus DOUBLE and static preferred datetime types")
    func functionDeclarations() throws {
        let extracted = try call("extract", [.string("epoch"), null(.interval())])
        #expect(extracted.type == .sql(.decimal(precision: nil, scale: nil)))
        #expect(try call("date_part", [.string("epoch"), null(.interval())]).type == .sql(.doublePrecision))
        #expect(try call("to_char", [null(.date), .string("YYYY")]).children[0].type == .sql(.timestamp(withTimeZone: true)))
        #expect(try call("to_char", [null(.time(withTimeZone: false)), .string("HH24")]).children[0].type == .sql(.interval()))
        #expect(throws: DatabaseExpressionEvaluationError.self) { try call("to_char", [null(.time(withTimeZone: true)), .string("HH24")]) }
        let interval = try call("make_interval", [])
        #expect(interval.children.count == 7 && interval.type == .sql(.interval()))
        #expect(try call("timezone", [.string("UTC"), null(.timestamp(withTimeZone: true))]).type == .sql(.timestamp(withTimeZone: false)))
        let truncatedTime = try call("date_trunc", [.string("day"), null(.time(withTimeZone: false))])
        #expect(truncatedTime.type == .sql(.interval()))
        #expect(truncatedTime.children[1].type == .sql(.interval()))
        #expect(throws: DatabaseExpressionEvaluationError.self) { try call("date_trunc", [.string("day"), null(.time(withTimeZone: true))]) }
    }

    @Test("Temporal arithmetic selects catalog signatures and retains strict typed NULL")
    func arithmeticDeclarations() throws {
        let date = null(.date), interval = null(.interval()), time = null(.time(withTimeZone: false))
        #expect(try binder().bind(.add(date, interval)).type == .sql(.timestamp(withTimeZone: false)))
        #expect(try binder().bind(.add(date, time)).type == .sql(.timestamp(withTimeZone: false)))
        #expect(try binder().bind(.subtract(date, date)).type == .sql(.integer))
        #expect(try binder().bind(.subtract(time, time)).type == .sql(.interval()))
        #expect(try binder().bind(.multiply(interval, .int(2))).type == .sql(.interval()))
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(.subtract(null(.time(withTimeZone: true)), null(.time(withTimeZone: true)))) }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(.modulo(interval, interval)) }
    }

    @Test("Unknown input retains runtime CAST while declared TEXT needs explicit conversion")
    func conversionContexts() throws {
        let target = DataType.timestamp(withTimeZone: true, precision: 3)
        let cast = try binder().bind(.cast(.literal(.sqlUnknownString("2001-01-01")), targetType: target))
        #expect(cast.type == .sql(target) && cast.castContext == .explicit)
        #expect(cast.children[0].type == .sql(.text))
        #expect(throws: DatabaseExpressionEvaluationError.self) { try call("age", [null(.text)]) }
        let common = try binder().bind(.coalesce([null(.date), null(.timestamp(withTimeZone: true, precision: 2))]))
        #expect(common.type == .sql(.timestamp(withTimeZone: true)))
        let typed = try binder().bind(.equal(null(.interval()), null(.interval())))
        #expect(!CanonicalSQLValueSemantics.permitsStoredKey(typed))
    }

    @Test("Pure scalar entry requires explicit temporal authority and preserves NULL short circuit")
    func explicitEnvironment() async throws {
        let workMeter = meter()
        let bound = try binder().bind(.currentValue(kind: .date, precision: nil))
        do {
            _ = try await DatabaseContext.evaluateCanonicalScalar(bound, on: CanonicalSourceRow(fields: [:]), workMeter: workMeter)
            Issue.record("Expected missing temporal authority")
        } catch let error as CanonicalReadError {
            guard case .expressionEvaluation(let cause, _) = error else { throw error }
            #expect(cause == .missingTemporalEnvironment)
        }
        let strict = try call("date_trunc", [.string("invalid"), null(.timestamp(withTimeZone: false))])
        let environment = SQLTemporalEnvironment(transactionInstant: Timestamp(secondsSinceUnixEpoch: 0), statementInstant: Timestamp(secondsSinceUnixEpoch: 0), wallClock: FixedClock())
        let baseline = workMeter.retainedIntermediateBytes
        var result: (value: QueryValue, leases: [DatabaseQueryPayloadLease])? = try await DatabaseContext.evaluateCanonicalScalar(strict, on: CanonicalSourceRow(fields: [:]), temporalEnvironment: environment, workMeter: workMeter)
        #expect(result?.value.isNull == true)
        // The returned owner retains evaluated child literals through destination admission.
        #expect(workMeter.retainedIntermediateBytes > baseline)
        result = nil
        #expect(workMeter.retainedIntermediateBytes == baseline)
        #expect(baseline == 0)
    }
}
