import DatabaseKit
import DatabaseTypes
import StorageKit
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Bounded SQL JSON row cursor")
struct SQLJSONRowCursorTests {
    private struct Clock: StorageMonotonicClock {
        var now: StorageInstant { StorageInstant(durationSinceReference: .zero) }
        func sleep(until deadline: StorageInstant) async throws(StorageClockError) { }
    }
    private let limits = QueryValueLimits(maximumRetainedBytes: 1_000_000, maximumTotalNodes: 10_000,
        maximumNestingDepth: 256, maximumCollectionElements: 10_000)
    private func meter(work: UInt64 = 1_000_000, bytes: UInt64 = 10_000_000, rows: UInt32 = 1000) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 1000, maximumWorkUnits: work,
            maximumIntermediateRows: rows, maximumIntermediateBytes: bytes, timeoutMilliseconds: 1000), monotonicClock: Clock())
    }
    private func json(_ text: String, binary: Bool = false) throws -> QueryValue {
        let value = try QueryJSONValue(bytes: ByteString(utf8: text), limits: limits)
        return binary ? .jsonb(try QueryJSONBValue(json: value, limits: limits)) : .json(value)
    }
    private func cursor(_ name: String, value: QueryValue, binary: Bool = false,
        meter: DatabaseWorkMeter, leases: [DatabaseQueryPayloadLease] = []) throws -> SQLJSONRowCursor {
        let type = QueryColumnType.sql(binary ? .jsonb : .json)
        let child = CanonicalBoundExpression(expression: .literal(.queryValue(value, type: type)), type: type, isNullable: true)
        let bound = CanonicalBoundExpression(expression: .function(FunctionCall(name: name, arguments: [child.expression])),
            type: .sql(.json), isNullable: true, children: [child])
        return try CanonicalSQLJSONEvaluator.makeRowCursor(bound, operands: [value], inputLeases: leases,
            temporalEnvironment: nil, workMeter: meter, limits: limits)
    }
    private func lexical(_ value: QueryValue) throws -> String {
        guard case .json(let json) = value else { throw QueryValueError.incompatibleType }
        return String(decoding: json.bytes, as: UTF8.self)
    }

    @Test func exactMetadataForEveryFamily() throws {
        for binary in [false, true] {
            let prefix = binary ? "jsonb_" : "json_"
            for suffix in ["each", "each_text", "object_keys", "array_elements", "array_elements_text"] {
                let object = suffix.hasPrefix("each") || suffix == "object_keys"
                var rows = try cursor(prefix + suffix, value: json(object ? "{}" : "[]", binary: binary), binary: binary, meter: meter())
                let metadata = try rows.metadata()
                try QueryColumn.validate(metadata.columns)
                #expect(metadata.columns.count == (suffix.hasPrefix("each") ? 2 : 1))
                #expect(metadata.columns[0].name == (suffix.hasPrefix("each") ? "key" : (suffix == "object_keys" ? prefix + suffix : "value")))
                if suffix.hasPrefix("each") { #expect(metadata.columns[1].name == "value") }
                let value = try #require(metadata.columns.last)
                #expect(value.isNullable == suffix.hasSuffix("_text"))
                let text = suffix.hasSuffix("_text") || suffix == "object_keys"
                #expect(value.type == .sql(text ? .text : (binary ? .jsonb : .json)))
                let exhausted = try rows.next()
                #expect(exhausted == nil)
                withExtendedLifetime(metadata.lease) { }
            }
        }
    }
    @Test func lexicalObjectIterationPreservesDuplicatesAndSourceBytes() throws {
        var rows = try cursor("json_each", value: json("{\"x\": [ 1.00 ], \"x\":2}"), meter: meter())
        let metadata = try rows.metadata()
        let firstResult = try rows.next(), secondResult = try rows.next()
        let first = try #require(firstResult)
        let second = try #require(secondResult)
        #expect(first.values[0] == .primitive(.string("x")))
        #expect(try lexical(first.values[1]) == "[ 1.00 ]")
        #expect(second.values[0] == .primitive(.string("x")))
        #expect(try lexical(second.values[1]) == "2")
        let exhausted = try rows.next()
        #expect(exhausted == nil)
        withExtendedLifetime((first.leases, second.leases, metadata.lease)) { }
    }
    @Test func canonicalObjectIterationUsesCanonicalKeyOrder() throws {
        var rows = try cursor("jsonb_each", value: json("{\"long\":0,\"b\":2,\"a\":1}", binary: true), binary: true, meter: meter())
        var keys: [QueryValue] = []
        while let row = try rows.next() { keys.append(row.values[0]) }
        #expect(keys == [.primitive(.string("a")), .primitive(.string("b")), .primitive(.string("long"))])
    }
    @Test func textRowsDistinguishJSONNullAndStrings() throws {
        for binary in [false, true] {
            var rows = try cursor(binary ? "jsonb_array_elements_text" : "json_array_elements_text",
                value: json("[null,\"null\",1.2300]", binary: binary), binary: binary, meter: meter())
            let firstResult = try rows.next(), secondResult = try rows.next(), thirdResult = try rows.next()
            let first = try #require(firstResult), second = try #require(secondResult), third = try #require(thirdResult)
            #expect(first.values[0].isNull)
            #expect(second.values[0] == .primitive(.string("null")))
            #expect(third.values[0] == .primitive(.string("1.2300")))
            let exhausted = try rows.next()
            #expect(exhausted == nil)
            withExtendedLifetime((first.leases, second.leases, third.leases)) { }
        }
    }
    @Test func sqlNullHasMetadataAndNoRows() throws {
        var rows = try cursor("json_each_text", value: .primitive(.null), meter: meter())
        let metadata = try rows.metadata()
        #expect(metadata.columns.count == 2)
        let exhausted = try rows.next()
        #expect(exhausted == nil)
        #expect(metadata.columns[1].type == .sql(.text))
        withExtendedLifetime(metadata.lease) { }
    }
    @Test func wrongRootFailsBeforeOutput() throws {
        #expect(throws: DatabaseExpressionEvaluationError.collection(.jsonObjectRootRequired(function: "json_each"))) {
            var rows = try cursor("json_each", value: json("[]"), meter: meter())
            rows.close()
        }
        #expect(throws: DatabaseExpressionEvaluationError.collection(.jsonArrayRootRequired(function: "jsonb_array_elements"))) {
            var rows = try cursor("jsonb_array_elements", value: json("{}", binary: true), binary: true, meter: meter())
            rows.close()
        }
    }
    @Test func earlyCloseDropsCursorButPriorRowAndMetadataSurvive() throws {
        let work = meter()
        do {
            var rows = try cursor("json_each", value: json("{\"x\":[1,2],\"y\":3}"), meter: work)
            let metadata = try rows.metadata()
            let rowResult = try rows.next()
            let row = try #require(rowResult)
            rows.close(); rows.close()
            let exhausted = try rows.next()
            #expect(exhausted == nil)
            do { _ = try rows.metadata(); Issue.record("Expected closed metadata rejection") }
            catch let error as DatabaseExpressionEvaluationError {
                #expect(error == .typeMismatch(operation: "closed JSON cursor metadata"))
            }
            #expect(try lexical(row.values[1]) == "[1,2]")
            #expect(metadata.columns[0].name == "key")
            #expect(work.retainedIntermediateBytes > 0)
            withExtendedLifetime((row.leases, metadata.lease)) { }
        }
        #expect(work.retainedIntermediateBytes == 0)

        let zeroRows = meter(rows: 0)
        do {
            var rows = try cursor("json_array_elements", value: json("[1]"), meter: zeroRows)
            do { _ = try rows.next(); Issue.record("A zero-row quota must reject the first retained row") }
            catch let error as DatabaseWorkLimitError {
                #expect(error == .maximumIntermediateRows(stage: .expressionEvaluation, consumed: 0, requested: 1, maximum: 0))
            }
            rows.close()
        }
        #expect(zeroRows.retainedIntermediateRows == 0)
        #expect(zeroRows.retainedIntermediateBytes == 0)

        let oneRow = meter(rows: 1)
        var rows = try cursor("json_array_elements", value: json("[1,2]"), meter: oneRow)
        var retained = try rows.next()
        #expect(retained != nil)
        #expect(oneRow.retainedIntermediateRows == 1)
        do { _ = try rows.next(); Issue.record("An outstanding row must exhaust the one-row quota") }
        catch let error as DatabaseWorkLimitError {
            #expect(error == .maximumIntermediateRows(stage: .expressionEvaluation, consumed: 1, requested: 1, maximum: 1))
        }
        rows.close()
        #expect(oneRow.retainedIntermediateRows == 1)
        if let values = retained?.values { #expect(try lexical(values[0]) == "1") }
        withExtendedLifetime(retained) { }
        retained = nil
        #expect(oneRow.retainedIntermediateRows == 0)
        #expect(oneRow.retainedIntermediateBytes == 0)
        var later = try cursor("json_array_elements", value: json("[3]"), meter: oneRow)
        var nextRow = try later.next()
        #expect(nextRow != nil)
        later.close()
        #expect(oneRow.retainedIntermediateRows == 1)
        nextRow = nil
        #expect(oneRow.retainedIntermediateRows == 0)
        #expect(oneRow.retainedIntermediateBytes == 0)
    }
    @Test func failedDecodeTerminatesAndPreservesEarlierRow() throws {
        let work = meter()
        do {
            var rows = try cursor("json_array_elements_text", value: json("[\"ok\",\"\\ud800\"]"), meter: work)
            let firstResult = try rows.next()
            let first = try #require(firstResult)
            do { _ = try rows.next(); Issue.record("Expected selected decode rejection") }
            catch let error as QueryValueError { guard case .invalidJSON = error else { throw error } }
            let exhausted = try rows.next()
            #expect(exhausted == nil)
            #expect(first.values[0] == .primitive(.string("ok")))
            withExtendedLifetime(first.leases) { }
        }
        #expect(work.retainedIntermediateBytes == 0)
    }
    @Test func unknownMeterLeaseIsRejected() throws {
        let work = meter(), other = meter()
        let claim = try other.reserveIntermediate(bytes: 1, at: .expressionEvaluation)
        #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "JSON cursor selected signature or meter")) {
            var rows = try cursor("json_each", value: json("{}"), meter: work, leases: [DatabaseQueryPayloadLease(retaining: claim)])
            rows.close()
        }
        #expect(work.retainedIntermediateBytes == 0)
    }
    @Test func workFailureClosesAndLaterCursorSucceeds() throws {
        let work = meter(work: 10)
        do {
            var rows = try cursor("json_each", value: json("{\"long\":[1,2,3,4,5]}"), meter: work)
            _ = try rows.next()
            Issue.record("Expected bounded scan failure")
        } catch { #expect(work.retainedIntermediateBytes == 0) }
        // A new operation owns a new meter; exhausted work is not reset or silently reused.
        var later = try cursor("json_each", value: json("{}"), meter: meter())
        let exhausted = try later.next()
        #expect(exhausted == nil)
    }
    @Test func cancellationClosesBeforePublication() async throws {
        let work = meter()
        let source = try json("[1,2,3]")
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            var rows = try cursor("json_array_elements", value: source, meter: work)
            return try rows.next()?.values
        }
        do { _ = try await task.value; Issue.record("Expected cancellation") }
        catch { #expect(work.retainedIntermediateBytes == 0) }
    }
}
