import DatabaseKit
import DatabaseTypes
import StorageKit
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL JSON execution")
struct CanonicalSQLJSONEvaluatorTests {
    private struct Clock: StorageMonotonicClock {
        var now: StorageInstant { StorageInstant(durationSinceReference: .zero) }
        func sleep(until deadline: StorageInstant) async throws(StorageClockError) { }
    }
    private let limits = QueryValueLimits(maximumRetainedBytes: 1_000_000, maximumTotalNodes: 10_000,
        maximumNestingDepth: 256, maximumCollectionElements: 10_000)
    private func meter(work: UInt64 = 1_000_000, bytes: UInt64 = 10_000_000) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 1000, maximumWorkUnits: work,
            maximumIntermediateRows: 1000, maximumIntermediateBytes: bytes, timeoutMilliseconds: 1000), monotonicClock: Clock())
    }
    private func json(_ text: String) throws -> QueryValue { .json(try QueryJSONValue(bytes: ByteString(utf8: text), limits: limits)) }
    private func jsonb(_ text: String) throws -> QueryValue {
        guard case .json(let value) = try json(text) else { throw QueryValueError.incompatibleType }
        return .jsonb(try QueryJSONBValue(json: value, limits: limits))
    }
    private func textArray(_ values: [QueryValue], rankTwo: Bool = false) throws -> QueryValue {
        let dimensions: [QueryArrayDimension]
        if values.isEmpty { dimensions = [] }
        else if rankTwo { dimensions = [try QueryArrayDimension(lowerBound: -2, length: 1), try QueryArrayDimension(lowerBound: 5, length: Int32(values.count))] }
        else { dimensions = [try QueryArrayDimension(lowerBound: -2, length: Int32(values.count))] }
        return .array(try QueryArrayValue(elementType: .text, dimensions: dimensions, elements: values, limits: limits))
    }
    private func call(_ name: String, _ values: [QueryValue], _ types: [DataType], result: DataType = .jsonb,
        variadic: Bool = false, meter: DatabaseWorkMeter? = nil, valueLimits: QueryValueLimits? = nil) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        let children = zip(values, types).map { value, type in
            CanonicalBoundExpression(expression: .literal(.queryValue(value, type: .sql(type))), type: .sql(type), isNullable: true)
        }
        let bound = CanonicalBoundExpression(expression: .function(FunctionCall(name: name,
            arguments: children.map { $0.expression }, finalArrayIsVariadic: variadic)),
            type: .sql(result), isNullable: true, children: children)
        return try CanonicalSQLJSONEvaluator.evaluate(bound, operands: values, temporalEnvironment: nil,
            workMeter: meter ?? self.meter(), limits: valueLimits ?? limits)
    }
    private func operation(_ operation: CollectionOperator, _ a: QueryValue, _ b: QueryValue,
        _ at: DataType, _ bt: DataType, result: DataType = .jsonb, meter: DatabaseWorkMeter? = nil) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        try expression(.collectionOperation(operation, left: .literal(.int(0)), right: .literal(.int(0))),
            [a, b], [at, bt], result: result, meter: meter)
    }
    private func expression(_ expression: DatabaseKit.Expression, _ values: [QueryValue], _ types: [DataType],
        result: DataType, meter: DatabaseWorkMeter? = nil) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        let children = zip(values, types).map { value, type in
            CanonicalBoundExpression(expression: .literal(.queryValue(value, type: .sql(type))), type: .sql(type), isNullable: true)
        }
        return try CanonicalSQLJSONEvaluator.evaluate(CanonicalBoundExpression(expression: expression, type: .sql(result),
            isNullable: true, children: children), operands: values, temporalEnvironment: nil, workMeter: meter ?? self.meter(), limits: limits)
    }
    private func lexical(_ value: QueryValue) throws -> String {
        guard case .json(let json) = value else { throw QueryValueError.incompatibleType }
        return String(decoding: json.bytes, as: UTF8.self)
    }
    private let null = QueryValue.primitive(.null)
    private func s(_ text: String) -> QueryValue { .primitive(.string(text)) }
    private func i(_ n: Int32) -> QueryValue { .primitive(.int32(n)) }

    private func composite(_ names: [String], _ values: [QueryValue], _ types: [DataType]) throws
        -> (value: QueryValue, type: DataType) {
        #expect(names.count == values.count && values.count == types.count)
        let columns = try names.indices.map { index in
            try QueryColumn(number: UInt32(index + 1), name: names[index], type: .sql(types[index]), isNullable: values[index].isNull)
        }
        let type = try QueryCompositeType(columns: columns, limits: limits)
        return (.composite(try QueryCompositeValue(type: type, fields: values, limits: limits)), .composite(type))
    }

    @Test func compositeJSONUsesNamesOrderDuplicatesAndNestedDescriptors() throws {
        let inner = try composite(["名", "名"], [i(1), i(2)], [.integer, .integer])
        let outer = try composite(["z", "child", "z"], [null, inner.value, s("last")], [.text, inner.type, .text])
        #expect(try lexical(call("row_to_json", [outer.value], [outer.type], result: .json).value)
            == "{\"z\":null,\"child\":{\"名\":1,\"名\":2},\"z\":\"last\"}")
        #expect(try call("to_jsonb", [outer.value], [outer.type]).value == jsonb("{\"z\":\"last\",\"child\":{\"名\":2}}"))
        let pretty = try call("row_to_json", [outer.value, .primitive(.bool(true))], [outer.type, .boolean], result: .json)
        #expect(try lexical(pretty.value) == "{\"z\":null,\n \"child\":{\"名\":1,\"名\":2},\n \"z\":\"last\"}")
    }

    @Test func presentAllNullCompositeIsNotStrictSQLNull() throws {
        let record = try composite(["a", "b"], [null, null], [.integer, .text])
        for name in ["row_to_json", "to_json"] {
            #expect(try lexical(call(name, [record.value], [record.type], result: .json).value) == "{\"a\":null,\"b\":null}")
            #expect(try call(name, [null], [record.type], result: .json).value.isNull)
        }
        #expect(try call("to_jsonb", [record.value], [record.type]).value == jsonb("{\"a\":null,\"b\":null}"))
        #expect(try call("to_jsonb", [null], [record.type]).value.isNull)
        #expect(try call("row_to_json", [record.value, null], [record.type, .boolean], result: .json).value.isNull)
    }

    @Test func zeroFieldAndAnonymousCompositeConversion() throws {
        let empty = try composite([], [], [])
        #expect(try call("row_to_json", [empty.value], [empty.type], result: .json).value == json("{}"))
        #expect(try call("to_jsonb", [empty.value], [empty.type]).value == jsonb("{}"))
        let anonymous = try composite(["f1", "f2"], [i(7), null], [.integer, .text])
        #expect(try lexical(call("to_json", [anonymous.value], [anonymous.type], result: .json).value) == "{\"f1\":7,\"f2\":null}")
    }

    @Test func compositesAreBuilderValuesAndCannotBecomeObjectKeys() throws {
        let record = try composite(["id"], [i(9)], [.integer])
        #expect(try call("jsonb_build_array", [record.value], [record.type]).value == jsonb("[{\"id\":9}]"))
        #expect(try call("json_build_object", [s("row"), record.value], [.text, record.type], result: .json).value
            == json("{\"row\" : {\"id\":9}}"))
        for name in ["json_build_object", "jsonb_build_object"] {
            #expect(throws: DatabaseExpressionEvaluationError.collection(.invalidJSONObjectKey(argument: 1))) {
                try call(name, [record.value, i(1)], [record.type, .integer], result: name == "json_build_object" ? .json : .jsonb)
            }
        }
    }

    @Test func arraysOfCompositesAndCompositeArrayFieldsUseOneConversionPlan() throws {
        let first = try composite(["n"], [i(1)], [.integer])
        let second = try composite(["n"], [i(2)], [.integer])
        let rows = try QueryArrayValue(elementType: first.type, dimensions: [QueryArrayDimension(lowerBound: -7, length: 2)],
            elements: [first.value, second.value], limits: limits)
        #expect(try call("to_jsonb", [.array(rows)], [.array(first.type)]).value == jsonb("[{\"n\":1},{\"n\":2}]"))
        let record = try composite(["rows"], [.array(rows)], [.array(first.type)])
        #expect(try lexical(call("row_to_json", [record.value], [record.type], result: .json).value)
            == "{\"rows\":[{\"n\":1},{\"n\":2}]}")
    }

    @Test func compositeSelectedShapeAndLowerTupleFailuresRemainTyped() throws {
        let record = try composite(["id"], [i(1)], [.integer])
        let other = try composite(["other"], [i(1)], [.integer])
        #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "selected JSON input")) {
            try call("row_to_json", [record.value], [other.type], result: .json)
        }
        #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "ROW JSON value type")) {
            try call("row_to_json", [i(1)], [.integer], result: .json)
        }
        let column = try QueryColumn(number: 1, name: "required", type: .sql(.integer), isNullable: false)
        #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "ROW JSON value type")) {
            try CanonicalSQLJSONEvaluator.convertRow(columns: [column], values: [null], resultType: .sql(.json), pretty: false,
                temporalEnvironment: nil, workMeter: meter(), limits: limits)
        }
    }

    @Test func deepCompositeConversionIsIterativeAndFailureReleasesItsDestination() throws {
        let depth = 48
        var value = i(7)
        var type = DataType.integer
        for _ in 0..<depth {
            let next = try composite(["f1"], [value], [type])
            value = next.value; type = next.type
        }
        let work = meter()
        do {
            let converted = try call("to_jsonb", [value], [type], meter: work)
            let expected = try jsonb(String(repeating: "{\"f1\":", count: depth) + "7" + String(repeating: "}", count: depth))
            #expect(converted.value == expected)
            #expect(work.retainedIntermediateBytes > 0)
            withExtendedLifetime(converted.lease) { }
        }
        #expect(work.retainedIntermediateBytes == 0)
        let small = meter(bytes: 100)
        #expect(throws: (any Error).self) { try call("to_jsonb", [value], [type], meter: small) }
        #expect(small.retainedIntermediateBytes == 0)
        let shallow = QueryValueLimits(maximumRetainedBytes: 1_000_000, maximumTotalNodes: 10_000,
            maximumNestingDepth: 8, maximumCollectionElements: 10_000)
        for result: DataType in [.json, .jsonb] {
            #expect(throws: QueryValueError.resourceLimit(resource: "depth", actual: 9, maximum: 8)) {
                try call(result == .json ? "to_json" : "to_jsonb", [value], [type], result: result,
                    meter: work, valueLimits: shallow)
            }
            #expect(work.retainedIntermediateBytes == 0)
        }
        #expect(try call("to_jsonb", [i(1)], [.integer], meter: work).value == jsonb("1"))
    }

    @Test func compositeOutputOutlivesInputOwnerAndReleasesItsMeasuredLease() throws {
        let work = meter()
        var source: (value: QueryValue, type: DataType)? = try composite(["text"], [s(String(repeating: "x", count: 4096))], [.text])
        var output: (value: QueryValue, lease: DatabaseQueryPayloadLease?)?
        if let input = source { output = try call("to_jsonb", [input.value], [input.type], meter: work) }
        source = nil
        #expect(output != nil)
        #expect(work.retainedIntermediateBytes >= 4096)
        #expect(try #require(output).value == jsonb("{\"text\":\"" + String(repeating: "x", count: 4096) + "\"}"))
        output = nil
        #expect(work.retainedIntermediateBytes == 0)
    }

    @Test func strictSQLNullAndJSONNullDiffer() throws {
        #expect(try call("to_json", [null], [.text], result: .json).value.isNull)
        #expect(try call("json_typeof", [json("null")], [.json], result: .text).value == s("null"))
        #expect(try call("jsonb_typeof", [jsonb("null")], [.jsonb], result: .text).value == s("null"))
        #expect(try call("json_build_array", [null], [.text], result: .json).value == json("[null]"))
    }
    @Test func zeroArgumentBuilders() throws {
        #expect(try call("json_build_array", [], [], result: .json).value == json("[]"))
        #expect(try call("json_build_object", [], [], result: .json).value == json("{}"))
        #expect(try call("jsonb_build_array", [], []).value == jsonb("[]"))
        #expect(try call("jsonb_build_object", [], []).value == jsonb("{}"))
    }
    @Test func heterogeneousBuildersAndBooleanKeys() throws {
        let built = try call("json_build_object", [.primitive(.bool(true)), i(3), s("v"), null], [.boolean, .integer, .text, .text], result: .json)
        #expect(try lexical(built.value) == "{\"true\" : 3, \"v\" : null}")
        #expect(try call("jsonb_build_array", [i(3), s("x"), null, .primitive(.bool(false))], [.integer, .text, .text, .boolean]).value == jsonb("[3,\"x\",null,false]"))
        // JSON conversion uses the selected CHAR output, retaining declared padding.
        let padded = s("x  ")
        let expectedArray = try json(#"["x  "]"#), expectedBinaryArray = try jsonb(#"["x  "]"#)
        let expectedObject = try json(#"{"x  " : 1}"#), expectedBinaryObject = try jsonb(#"{"x  " : 1}"#)
        #expect(try call("json_build_array", [padded], [.char(length: 3)], result: .json).value == expectedArray)
        #expect(try call("jsonb_build_array", [padded], [.char(length: 3)]).value == expectedBinaryArray)
        #expect(try call("json_build_object", [padded, i(1)], [.char(length: 3), .integer], result: .json).value == expectedObject)
        #expect(try call("jsonb_build_object", [padded, i(1)], [.char(length: 3), .integer]).value == expectedBinaryObject)
    }
    @Test func duplicateMembersLexicalVersusJSONB() throws {
        let values = [s("x"), i(1), s("x"), i(2)]
        let lexical = try call("json_build_object", values, [.text, .integer, .text, .integer], result: .json)
        #expect(try self.lexical(lexical.value) == "{\"x\" : 1, \"x\" : 2}")
        #expect(try call("jsonb_build_object", values, [.text, .integer, .text, .integer]).value == jsonb("{\"x\":2}"))
    }
    @Test func malformedObjectArgumentsAreTyped() throws {
        #expect(throws: DatabaseExpressionEvaluationError.collection(.oddJSONObjectArgumentCount(actual: 1))) {
            try call("jsonb_build_object", [s("x")], [.text])
        }
        #expect(throws: DatabaseExpressionEvaluationError.collection(.nullJSONObjectKey(argument: 1))) {
            try call("json_build_object", [null, i(1)], [.text, .integer], result: .json)
        }
        #expect(throws: DatabaseExpressionEvaluationError.collection(.invalidJSONObjectKey(argument: 1))) {
            try call("jsonb_build_object", [jsonb("{}"), i(1)], [.jsonb, .integer])
        }
    }
    @Test func explicitVariadicIsNotInferred() throws {
        let values = try textArray([s("a"), s("b")])
        #expect(try call("jsonb_build_array", [values], [.array(.text)]).value == jsonb("[[\"a\",\"b\"]]"))
        #expect(try call("jsonb_build_array", [values], [.array(.text)], variadic: true).value == jsonb("[\"a\",\"b\"]"))
        #expect(try call("jsonb_build_array", [null], [.array(.text)], variadic: true).value.isNull)
        #expect(try call("jsonb_build_array", [textArray([])], [.array(.text)], variadic: true).value == jsonb("[]"))
    }
    @Test func multidimensionalConversionOmitsBounds() throws {
        let array = try QueryArrayValue(elementType: .integer, dimensions: [QueryArrayDimension(lowerBound: -3, length: 2), QueryArrayDimension(lowerBound: 8, length: 2)],
            elements: [i(1), i(2), i(3), null], limits: limits)
        #expect(try call("to_jsonb", [.array(array)], [.array(.integer)]).value == jsonb("[[1,2],[3,null]]"))
        let pretty = try call("array_to_json", [.array(array), .primitive(.bool(true))], [.array(.integer), .boolean], result: .json)
        #expect(try lexical(pretty.value) == "[[1,2],\n [3,null]]")
    }
    @Test func lexicalSubtreeKeepsBytesAndLastDuplicate() throws {
        let source = try json(" {\"x\": 1, \"x\": [ 1.2300 , {\"z\":2} ]} ")
        let result = try operation(.jsonField, source, s("x"), .json, .text, result: .json)
        #expect(try lexical(result.value) == "[ 1.2300 , {\"z\":2} ]")
        #expect(try operation(.jsonFieldText, source, s("x"), .json, .text, result: .text).value == s("[ 1.2300 , {\"z\":2} ]"))
    }
    @Test func requestedStringDecodingAndJSONNull() throws {
        let source = try json("[\"e\\u0301\\ud83d\\ude00\",null]")
        #expect(try operation(.jsonFieldText, source, i(0), .json, .integer, result: .text).value == s("e\u{301}😀"))
        #expect(try operation(.jsonFieldText, source, i(-1), .json, .integer, result: .text).value.isNull)
        #expect(try operation(.jsonField, source, i(-1), .json, .integer, result: .json).value == json("null"))
    }
    @Test func extractionMissingWrongShapeAndInvalidIndexAreNull() throws {
        #expect(try operation(.jsonField, json("{}"), s("missing"), .json, .text, result: .json).value.isNull)
        #expect(try operation(.jsonField, jsonb("1"), i(0), .jsonb, .integer).value.isNull)
        #expect(try operation(.jsonPath, jsonb("[1]"), textArray([s("bad")]), .jsonb, .array(.text)).value.isNull)
        #expect(try operation(.jsonField, json("[1]"), i(8), .json, .integer, result: .json).value.isNull)
        #expect(try operation(.jsonField, jsonb("[1]"), s("0"), .jsonb, .text).value.isNull)
    }
    @Test func pathFlatteningEmptyAndNullCells() throws {
        let source = try jsonb("{\"a\":[10,20]}")
        #expect(try operation(.jsonPath, source, textArray([s("a"), s("-1")], rankTwo: true), .jsonb, .array(.text)).value == jsonb("20"))
        #expect(try operation(.jsonPath, source, textArray([]), .jsonb, .array(.text)).value == source)
        #expect(try operation(.jsonPath, source, textArray([null]), .jsonb, .array(.text)).value.isNull)
        #expect(try operation(.jsonPathText, jsonb("\"a\""), textArray([]), .jsonb, .array(.text), result: .text).value == s("a"))
        #expect(try call("jsonb_extract_path_text", [source, textArray([s("a"), s("0")])], [.jsonb, .array(.text)], result: .text, variadic: true).value == s("10"))
    }
    @Test func lexicalLargeNumbersStayLexical() throws {
        let number = "12345678901234567890123456789012345678901234567890"
        let source = try json("{\"n\":" + number + "}")
        #expect(try operation(.jsonFieldText, source, s("n"), .json, .text, result: .text).value == s(number))
        #expect(try lexical(operation(.jsonField, source, s("n"), .json, .text, result: .json).value) == number)
        do {
            _ = try CanonicalSQLJSONEvaluator.cast(source, from: .sql(.json), to: .sql(.jsonb), context: .explicit,
                temporalEnvironment: nil, workMeter: meter(), limits: limits)
            Issue.record("Expected JSONB numeric profile failure")
        } catch is QueryValueError { }
    }
    @Test func selectedDecodeRejectsNulAndSurrogate() throws {
        for input in ["\"\\u0000\"", "\"\\ud800\""] {
            let source = try json(input)
            #expect(try operation(.jsonPath, source, textArray([]), .json, .array(.text), result: .json).value == source)
            do {
                _ = try operation(.jsonPathText, source, textArray([]), .json, .array(.text), result: .text)
                Issue.record("Expected requested text decode failure")
            } catch let error as QueryValueError {
                guard case .invalidJSON = error else { throw error }
            }
        }
    }
    @Test func arrayLengthWrongRootIsTyped() throws {
        #expect(try call("json_array_length", [json("[1,null]")], [.json], result: .integer).value == i(2))
        #expect(throws: DatabaseExpressionEvaluationError.collection(.jsonArrayRootRequired(function: "jsonb_array_length"))) {
            try call("jsonb_array_length", [jsonb("{}")], [.jsonb], result: .integer)
        }
    }
    @Test func JSONBSemanticComparisonIgnoresDisplayScale() throws {
        let a = try CanonicalSQLJSONEvaluator.jsonb(jsonb("1.00")), b = try CanonicalSQLJSONEvaluator.jsonb(jsonb("1"))
        #expect(a != b)
        #expect(try CanonicalSQLJSONEvaluator.compare(a, b, workMeter: meter()) == .equal)
        #expect(try CanonicalSQLJSONEvaluator.compare(CanonicalSQLJSONEvaluator.jsonb(jsonb("[]")), a, workMeter: meter()) == .lessThan)
        #expect(try CanonicalSQLJSONEvaluator.compare(CanonicalSQLJSONEvaluator.jsonb(jsonb("[2]")), CanonicalSQLJSONEvaluator.jsonb(jsonb("[1,1]")), workMeter: meter()) == .lessThan)
    }
    @Test func containmentIsRecursiveAndIgnoresMultiplicity() throws {
        #expect(try operation(.contains, jsonb("[1,2]"), jsonb("[1,1]"), .jsonb, .jsonb, result: .boolean).value == .primitive(.bool(true)))
        #expect(try operation(.contains, jsonb("{\"a\":[1,2],\"b\":0}"), jsonb("{\"a\":[2]}"), .jsonb, .jsonb, result: .boolean).value == .primitive(.bool(true)))
        #expect(try operation(.contains, jsonb("[[1]]"), jsonb("[1]"), .jsonb, .jsonb, result: .boolean).value == .primitive(.bool(false)))
        #expect(try operation(.contains, jsonb("[1]"), jsonb("1"), .jsonb, .jsonb, result: .boolean).value == .primitive(.bool(true)))
        #expect(try operation(.containedBy, jsonb("1"), jsonb("[1]"), .jsonb, .jsonb, result: .boolean).value == .primitive(.bool(true)))
    }
    @Test func existenceOnlyMatchesTextAndSkipsNullKeys() throws {
        #expect(try operation(.jsonHasKey, jsonb("[1,\"x\"]"), s("1"), .jsonb, .text, result: .boolean).value == .primitive(.bool(false)))
        #expect(try operation(.jsonHasKey, jsonb("\"x\""), s("x"), .jsonb, .text, result: .boolean).value == .primitive(.bool(true)))
        #expect(try operation(.jsonHasAnyKey, jsonb("{}"), textArray([]), .jsonb, .array(.text), result: .boolean).value == .primitive(.bool(false)))
        #expect(try operation(.jsonHasAllKeys, jsonb("{\"x\":0}"), textArray([null, s("x")]), .jsonb, .array(.text), result: .boolean).value == .primitive(.bool(true)))
        #expect(try operation(.jsonHasAllKeys, jsonb("{}"), textArray([]), .jsonb, .array(.text), result: .boolean).value == .primitive(.bool(true)))
    }
    @Test func concatenationIsTopLevelOnly() throws {
        let concat = DatabaseKit.Expression.concatenate(.literal(.int(0)), .literal(.int(0)))
        #expect(try expression(concat, [jsonb("{\"a\":{\"x\":1}}"), jsonb("{\"a\":{\"y\":2}}")], [.jsonb, .jsonb], result: .jsonb).value == jsonb("{\"a\":{\"y\":2}}"))
        #expect(try expression(concat, [jsonb("1"), jsonb("[2]")], [.jsonb, .jsonb], result: .jsonb).value == jsonb("[1,2]"))
    }
    @Test func deletionOverloads() throws {
        let subtract = DatabaseKit.Expression.subtract(.literal(.int(0)), .literal(.int(0)))
        #expect(try expression(subtract, [jsonb("[\"x\",1,\"x\"]"), s("x")], [.jsonb, .text], result: .jsonb).value == jsonb("[1]"))
        #expect(try expression(subtract, [jsonb("[1,2,3]"), i(-1)], [.jsonb, .integer], result: .jsonb).value == jsonb("[1,2]"))
        #expect(try expression(subtract, [jsonb("{\"x\":1,\"y\":2}"), textArray([null, s("x")])], [.jsonb, .array(.text)], result: .jsonb).value == jsonb("{\"y\":2}"))
        #expect(throws: DatabaseExpressionEvaluationError.collection(.jsonArrayRootRequired(function: "jsonb - integer"))) {
            try expression(subtract, [jsonb("{}"), i(0)], [.jsonb, .integer], result: .jsonb)
        }
    }
    @Test func setCreatesOnlyFinalPathComponent() throws {
        let source = try jsonb("{\"a\":[]}")
        #expect(try call("jsonb_set", [source, textArray([s("a"), s("8")]), jsonb("9")], [.jsonb, .array(.text), .jsonb]).value == jsonb("{\"a\":[9]}"))
        #expect(try call("jsonb_set", [source, textArray([s("missing"), s("x")]), jsonb("9")], [.jsonb, .array(.text), .jsonb]).value == source)
        #expect(try call("jsonb_set", [source, textArray([s("a"), s("-8")]), jsonb("9"), .primitive(.bool(false))], [.jsonb, .array(.text), .jsonb, .boolean]).value == source)
    }
    @Test func insertPositionAndObjectCollision() throws {
        #expect(try call("jsonb_insert", [jsonb("[1,2]"), textArray([s("0")]), jsonb("9"), .primitive(.bool(true))], [.jsonb, .array(.text), .jsonb, .boolean]).value == jsonb("[1,9,2]"))
        #expect(throws: DatabaseExpressionEvaluationError.collection(.existingJSONInsertKey(key: "x"))) {
            try call("jsonb_insert", [jsonb("{\"x\":0}"), textArray([s("x")]), jsonb("1")], [.jsonb, .array(.text), .jsonb])
        }
    }
    @Test func reachedPathErrorsDoNotInspectUnreachableSuffix() throws {
        let missing = try textArray([s("missing"), null])
        #expect(try call("jsonb_set", [jsonb("{}"), missing, jsonb("1")], [.jsonb, .array(.text), .jsonb]).value == jsonb("{}"))
        #expect(throws: DatabaseExpressionEvaluationError.collection(.nullJSONMutationPathElement(position: 2))) {
            try call("jsonb_set", [jsonb("{\"a\":[]}"), textArray([s("a"), null]), jsonb("1")], [.jsonb, .array(.text), .jsonb])
        }
        #expect(throws: DatabaseExpressionEvaluationError.collection(.invalidJSONMutationPathIndex(position: 1, text: "x"))) {
            try call("jsonb_set", [jsonb("[]"), textArray([s("x")]), jsonb("1")], [.jsonb, .array(.text), .jsonb])
        }
        #expect(throws: DatabaseExpressionEvaluationError.collection(.multidimensionalJSONMutationPath(rank: 2))) {
            try call("jsonb_set", [jsonb("{}"), textArray([s("x")], rankTwo: true), jsonb("1")], [.jsonb, .array(.text), .jsonb])
        }
    }
    @Test func deletePathAndEmptyMutationPath() throws {
        #expect(try operation(.jsonDeletePath, jsonb("{\"x\":[1,2]}"), textArray([s("x"), s("0")]), .jsonb, .array(.text)).value == jsonb("{\"x\":[2]}"))
        #expect(try call("jsonb_set", [jsonb("[]"), textArray([]), jsonb("1")], [.jsonb, .array(.text), .jsonb]).value == jsonb("[]"))
        #expect(throws: DatabaseExpressionEvaluationError.collection(.jsonContainerRootRequired(operation: "jsonb_set"))) {
            try call("jsonb_set", [jsonb("0"), textArray([]), jsonb("1")], [.jsonb, .array(.text), .jsonb])
        }
    }
    @Test func stripNullsPreservesRootAndOptionalArrayRule() throws {
        let source = try jsonb("{\"x\":null,\"a\":[null,{\"z\":null}],\"e\":{}}")
        #expect(try call("jsonb_strip_nulls", [source], [.jsonb]).value == jsonb("{\"a\":[null,{}],\"e\":{}}"))
        #expect(try call("jsonb_strip_nulls", [source, .primitive(.bool(true))], [.jsonb, .boolean]).value == jsonb("{\"a\":[{}],\"e\":{}}"))
        #expect(try call("jsonb_strip_nulls", [jsonb("null")], [.jsonb]).value == jsonb("null"))
        #expect(try call("jsonb_strip_nulls", [source, null], [.jsonb, .boolean]).value.isNull)
    }
    @Test func jsonbScalarCastsUseSelectedNumericConversion() throws {
        func cast(_ source: String, _ target: DataType) throws -> QueryValue {
            try CanonicalSQLJSONEvaluator.cast(jsonb(source), from: .sql(.jsonb), to: .sql(target), context: .explicit,
                temporalEnvironment: nil, workMeter: meter(), limits: limits).value
        }
        #expect(try cast("1.5", .integer) == i(2))
        #expect(try cast("true", .boolean) == .primitive(.bool(true)))
        for source in ["null", "\"1\"", "[]", "{}"] {
            #expect(throws: DatabaseExpressionEvaluationError.invalidCast("JSONB scalar")) { try cast(source, .integer) }
        }
    }
    @Test func lowerRowConversionUsesOrderedDescriptor() throws {
        let columns = [try QueryColumn(number: 1, name: "z", type: .sql(.integer), isNullable: false),
            try QueryColumn(number: 2, name: "a", type: .sql(.text), isNullable: true)]
        let result = try CanonicalSQLJSONEvaluator.convertRow(columns: columns, values: [i(3), null], resultType: .sql(.json), pretty: false,
            temporalEnvironment: nil, workMeter: meter(), limits: limits)
        #expect(try lexical(result.value) == "{\"z\":3,\"a\":null}")
        #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "ROW conversion arity")) {
            try CanonicalSQLJSONEvaluator.convertRow(columns: columns, values: [i(1)], resultType: .sql(.json), pretty: false,
                temporalEnvironment: nil, workMeter: meter(), limits: limits)
        }
    }
    @Test func malformedLexicalCastPreservesIntrinsicError() throws {
        do {
            _ = try CanonicalSQLJSONEvaluator.cast(s("{bad}"), from: .sql(.text), to: .sql(.json), context: .explicit,
                temporalEnvironment: nil, workMeter: meter(), limits: limits)
            Issue.record("Expected malformed JSON")
        } catch let error as QueryValueError { guard case .invalidJSON = error else { throw error } }
    }
    @Test func tinySliceChargesWholeOwnerAndReleases() throws {
        let work = meter()
        let source = try json("{\"large\":\"" + String(repeating: "x", count: 10_000) + "\",\"small\":0}")
        do {
            let result = try operation(.jsonField, source, s("small"), .json, .text, result: .json, meter: work)
            #expect(try lexical(result.value) == "0")
            #expect(work.retainedIntermediateBytes >= 10_000)
            withExtendedLifetime(result.lease) { }
        }
        #expect(work.retainedIntermediateBytes == 0)
    }
    @Test func resourceFailureReleasesAndNextOperationSucceeds() throws {
        let work = meter(work: 1_000_000, bytes: 20_000)
        do {
            // TEST_SCALE_SEMANTICS: correctness rejects JSON construction beyond the byte budget.
            _ = try call("jsonb_build_array", [s(String(repeating: "x", count: 100_000))], [.text], meter: work)
            Issue.record("Expected resource rejection")
        } catch { #expect(work.retainedIntermediateBytes == 0) }
        #expect(try call("jsonb_build_array", [], [], meter: work).value == jsonb("[]"))
    }
    @Test func exactNodeLimitAndOnePastAreDistinct() throws {
        let work = meter()
        let restricted = QueryValueLimits(maximumRetainedBytes: 100_000, maximumTotalNodes: 1,
            maximumNestingDepth: 10, maximumCollectionElements: 10)
        func build(_ values: [QueryValue]) throws -> QueryValue {
            let children = values.map { value in CanonicalBoundExpression(expression: .literal(.queryValue(value, type: .sql(.integer))), type: .sql(.integer), isNullable: false) }
            let bound = CanonicalBoundExpression(expression: .function(FunctionCall(name: "jsonb_build_array", arguments: children.map { $0.expression })), type: .sql(.jsonb), isNullable: false, children: children)
            return try CanonicalSQLJSONEvaluator.evaluate(bound, operands: values, temporalEnvironment: nil, workMeter: work, limits: restricted).value
        }
        #expect(try build([]) == jsonb("[]"))
        #expect(throws: QueryValueError.resourceLimit(resource: "nodes", actual: 2, maximum: 1)) { try build([i(1)]) }
        #expect(work.retainedIntermediateBytes == 0)
    }
    @Test func deepMutationUsesFlatOwnerAndReleases() throws {
        let work = meter()
        let depth = 48
        let source = try jsonb(String(repeating: "[", count: depth) + "null" + String(repeating: "]", count: depth))
        do {
            let output = try call("jsonb_strip_nulls", [source, .primitive(.bool(true))], [.jsonb, .boolean], meter: work)
            let expected = try jsonb(String(repeating: "[", count: depth) + String(repeating: "]", count: depth))
            #expect(output.value == expected)
            #expect(work.retainedIntermediateBytes > 0)
            withExtendedLifetime(output.lease) { }
        }
        #expect(work.retainedIntermediateBytes == 0)
    }
    @Test func cancelledOperationPublishesNoOutput() async throws {
        let work = meter()
        let source = try jsonb("[1,2]")
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try call("jsonb_strip_nulls", [source], [.jsonb], meter: work).value
        }
        do { _ = try await task.value; Issue.record("Expected cancellation") }
        catch { #expect(work.retainedIntermediateBytes == 0) }
    }
}
