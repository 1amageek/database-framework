import DatabaseKit
import DatabaseTypes
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL collection binding")
struct CanonicalSQLCollectionBindingTests {
    private func binder(columns: [QueryColumn] = []) -> CanonicalQueryTypeBinder {
        CanonicalQueryTypeBinder(columns: columns, resolveColumn: { reference in
            guard let slot = columns.firstIndex(where: { $0.name == reference.column }) else { throw DatabaseExpressionEvaluationError.missingColumn(reference.column) }
            return slot
        }, wildcardSlots: { _ in Array(columns.indices) })
    }
    private func null(_ type: DataType) -> Expression { .literal(.queryValue(.primitive(.null), type: .sql(type))) }
    private func integer(_ value: Int32) -> Expression { .literal(.queryValue(.primitive(.int32(value)), type: .sql(.integer))) }
    private func unknown(_ value: String) -> Expression { .literal(.sqlUnknownString(value)) }
    private func call(_ name: String, _ arguments: [Expression], variadic: Bool = false) -> Expression {
        .function(FunctionCall(name: name, arguments: arguments, finalArrayIsVariadic: variadic))
    }

    @Test("ARRAY selects all scalar leaves, preserves casts and types contextual empty constructors")
    func constructorCommonTypes() throws {
        let expression = Expression.array([.array([integer(1)]), .array([.cast(integer(2), targetType: .decimal(precision: 5, scale: 2))])])
        let selected = try binder().bind(expression)
        let numeric = DataType.decimal(precision: nil, scale: nil)
        #expect(selected.type == .sql(.array(numeric)))
        #expect(selected.children.allSatisfy { $0.type == .sql(.array(numeric)) })
        #expect(selected.children[0].children[0].type == .sql(numeric))
        guard case .array(let rows) = try selected.loweredExpression,
              case .array(let cells) = rows[0], case .cast = cells[0] else {
            Issue.record("Expected rebuilt selected constructor and numeric coercion"); return
        }
        let empty = try binder().bind(.cast(.array([]), targetType: .array(.integer)))
        #expect(empty.type == .sql(.array(.integer)) && !empty.isNullable)
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(.array([])) }
        let allUnknown = try binder().bind(.array([.null, unknown("text")]))
        #expect(allUnknown.type == .sql(.array(.text)))
        let characters = try binder().bind(.array([.cast(.string("a"), targetType: .char(length: 1)), .cast(.string("b"), targetType: .char(length: 3))]))
        #expect(characters.type == .sql(.array(.char(length: nil))))
        #expect(characters.children[0].children[0].type == .sql(.char(length: 1)))
    }

    @Test("Access groups preserve index versus slice, omitted bounds and exact present-child layout")
    func accessGroups() throws {
        let array = null(.array(.integer))
        let selected = try binder().bind(.arrayAccess(array, subscripts: [.index(integer(2)), .slice(lower: nil, upper: integer(4))]))
        #expect(selected.type == .sql(.array(.integer)) && selected.isNullable)
        #expect(selected.children.map(\.type) == [.sql(.array(.integer)), .sql(.integer), .sql(.integer)])
        guard case .arrayAccess(_, let groups) = try selected.loweredExpression,
              case .slice(nil, .some) = groups[1] else { Issue.record("Omitted slice lower bound was lost"); return }
        let element = try binder().bind(.arrayAccess(array, subscripts: [.index(integer(1))]))
        #expect(element.type == .sql(.integer) && element.isNullable)
        let nested = try binder().bind(.arrayAccess(.arrayAccess(array, subscripts: [.slice(lower: nil, upper: nil)]), subscripts: [.index(integer(1))]))
        #expect(nested.children[0].type == .sql(.array(.integer)))
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(.arrayAccess(array, subscripts: [.index(.int(1))])) }
        var arena = QueryOccurrenceArena()
        let root = try arena.append()
        let base = try arena.append(parent: root, field: .operand)
        let first = try arena.append(parent: root, field: .argument, index: 0)
        let upper = try arena.append(parent: root, field: .argument, index: 3)
        let scope = CanonicalQueryTypeBinder(columns: [], occurrences: arena, root: root,
            resolveColumn: { throw DatabaseExpressionEvaluationError.missingColumn($0.column) }, wildcardSlots: { _ in [] })
        let located = try scope.bind(.arrayAccess(array, subscripts: [.index(integer(2)), .slice(lower: nil, upper: integer(4))]))
        #expect(located.children.map(\.occurrence) == [base, first, upper])
        do {
            _ = try scope.bind(.arrayAccess(.col("missing"), subscripts: [.index(integer(1))]))
            Issue.record("Expected a missing base column")
        } catch let failure as CanonicalSQLBindingFailure {
            #expect(failure.occurrence == base)
        }
    }

    @Test("F08 polymorphism and nullability select exact array, scalar and text signatures")
    func arrayFunctionSignatures() throws {
        let array = null(.array(.integer)), element = null(.integer)
        let signatures: [(String, [Expression], DataType)] = [
            ("array_length", [array, integer(1)], .integer), ("array_lower", [array, integer(1)], .integer),
            ("array_upper", [array, integer(1)], .integer), ("array_ndims", [array], .integer),
            ("array_dims", [array], .text), ("cardinality", [array], .integer),
            ("array_append", [array, element], .array(.integer)), ("array_prepend", [element, array], .array(.integer)),
            ("array_cat", [array, array], .array(.integer)), ("array_position", [array, element], .integer),
            ("array_position", [array, element, integer(2)], .integer), ("array_positions", [array, element], .array(.integer)),
            ("array_remove", [array, element], .array(.integer)), ("array_replace", [array, element, element], .array(.integer)),
            ("array_to_string", [array, .string(","), .null], .text), ("string_to_array", [.string("a"), .null, .null], .array(.text))]
        for (name, args, result) in signatures { #expect(try binder().bind(call(name, args)).type == .sql(result)) }
        #expect(!(try binder().bind(call("array_append", [array, element]))).isNullable)
        #expect(!(try binder().bind(call("string_to_array", [.string("a"), .null]))).isNullable)
        let append = try binder().bind(.concatenate(array, element))
        #expect(append.children[1].type == .sql(.integer) && !append.isNullable)
        let concatenate = try binder().bind(.concatenate(array, .null))
        #expect(concatenate.children[1].type == .sql(.array(.integer)) && concatenate.isNullable)
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(call("array_length", [.null, integer(1)])) }
    }

    @Test("Closed collection operators use selected declarations for array and JSON overloads")
    func operatorSignatures() throws {
        let array = null(.array(.integer)), json = null(.json), jsonb = null(.jsonb)
        for operation: CollectionOperator in [.contains, .containedBy, .overlaps] {
            #expect(try binder().bind(.collectionOperation(operation, left: array, right: array)).type == .sql(.boolean))
        }
        #expect(try binder().bind(.collectionOperation(.contains, left: jsonb, right: jsonb)).type == .sql(.boolean))
        #expect(try binder().bind(.collectionOperation(.jsonField, left: json, right: integer(0))).type == .sql(.json))
        #expect(try binder().bind(.collectionOperation(.jsonFieldText, left: jsonb, right: .string("key"))).type == .sql(.text))
        #expect(try binder().bind(.collectionOperation(.jsonPath, left: jsonb, right: .array([.string("key")]))).type == .sql(.jsonb))
        #expect(try binder().bind(.collectionOperation(.jsonHasAllKeys, left: jsonb, right: null(.array(.text)))).type == .sql(.boolean))
        #expect(try binder().bind(.collectionOperation(.jsonDeletePath, left: jsonb, right: null(.array(.text)))).type == .sql(.jsonb))
        for right in [integer(1), .string("key"), null(.array(.text))] {
            #expect(try binder().bind(.subtract(jsonb, right)).type == .sql(.jsonb))
        }
        #expect(try binder().bind(.concatenate(.null, jsonb)).type == .sql(.jsonb))
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(.collectionOperation(.overlaps, left: jsonb, right: jsonb)) }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(.collectionOperation(.jsonField, left: .null, right: integer(1))) }
    }

    @Test("JSON builders, path VARIADIC and optional defaults retain mode and heterogeneous types")
    func jsonFunctionSignatures() throws {
        for prefix in ["json", "jsonb"] {
            let family = prefix == "json" ? DataType.json : .jsonb
            let empty = try binder().bind(call(prefix + "_build_array", []))
            #expect(empty.type == .sql(family) && !empty.isNullable)
            let ordinary = try binder().bind(call(prefix + "_build_array", [.array([integer(1)]), .null]))
            #expect(ordinary.children[0].type == .sql(.array(.integer)) && !ordinary.isNullable)
            let variadic = try binder().bind(call(prefix + "_build_array", [null(.array(.integer))], variadic: true))
            guard case .function(let rebuilt) = try variadic.loweredExpression else { Issue.record("Expected selected function"); return }
            #expect(rebuilt.finalArrayIsVariadic && variadic.isNullable)
            let path = try binder().bind(call(prefix + "_extract_path_text", [null(family), null(.array(.text))], variadic: true))
            #expect(path.type == .sql(.text) && path.children[1].type == .sql(.array(.text)))
            for suffix in ["typeof", "array_length"] { _ = try binder().bind(call(prefix + "_" + suffix, [null(family)])) }
        }
        let set = try binder().bind(call("jsonb_set", [null(.jsonb), null(.array(.text)), null(.jsonb)]))
        #expect(set.children.count == 4 && set.children[3].type == .sql(.boolean))
        let strip = try binder().bind(call("jsonb_strip_nulls", [null(.jsonb)]))
        #expect(strip.children.count == 2)
        #expect(throws: DatabaseExpressionEvaluationError.collection(.oddJSONObjectArgumentCount(actual: 1))) { try binder().bind(call("json_build_object", [.string("key")])) }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(call("json_build_object", [null(.jsonb), integer(1)])) }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(call("lower", [null(.array(.text))], variadic: true)) }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(call("json_build_array", [integer(1)], variadic: true)) }
    }

    @Test("JSON comparison capabilities fail before reads including empty relational input")
    func jsonCapabilityGate() throws {
        for type: DataType in [.json, .array(.json)] {
            let column = try QueryColumn(number: 1, name: "value", type: .sql(type), isNullable: true)
            let selected = binder(columns: [column]), source = DataSource.values([], columnNames: ["value"])
            for query in [
                SelectQuery(projection: .distinctItems([ProjectionItem(.col("value"))]), source: source),
                SelectQuery(projection: .all, source: source, groupBy: [.col("value")]),
                SelectQuery(projection: .all, source: source, orderBy: [SortKey(.col("value"))]),
                SelectQuery(projection: .items([ProjectionItem(.aggregate(.count(.col("value"), distinct: true)))]), source: source),
                SelectQuery(projection: .items([ProjectionItem(.aggregate(.min(.col("value"))))]), source: source)] {
                #expect(throws: DatabaseExpressionEvaluationError.self) { try selected.bind(query) }
            }
            #expect(throws: DatabaseExpressionEvaluationError.self) { try selected.bind(.equal(.col("value"), .col("value"))) }
        }
        let jsonb = null(.array(.jsonb))
        #expect(try binder().bind(.equal(jsonb, jsonb)).type == .sql(.boolean))
        #expect(try binder().bind(.coalesce([null(.json), null(.json)])).type == .sql(.json))
        let extracted = Expression.collectionOperation(.jsonFieldText, left: null(.json), right: .string("key"))
        #expect(try binder().bind(.equal(extracted, .string("x"))).type == .sql(.boolean))
    }

    @Test("Rich casts validate pure constants before reads while retaining temporal runtime input")
    func castAndConstantValidation() throws {
        let array = try binder().bind(.cast(unknown("[0:1]={1,2}"), targetType: .array(.integer)))
        #expect(array.type == .sql(.array(.integer)))
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(.cast(unknown("{1,x}"), targetType: .array(.integer))) }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(.cast(unknown("[0:2147483647]={}"), targetType: .array(.integer))) }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(.coalesce([.string("selected"), .cast(.cast(unknown("{"), targetType: .jsonb), targetType: .text)])) }
        #expect(try binder().bind(.cast(unknown("{2000-01-01}"), targetType: .array(.date))).type == .sql(.array(.date)))
        #expect(try binder().bind(.cast(null(.jsonb), targetType: .integer)).type == .sql(.integer))
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(.cast(null(.json), targetType: .integer)) }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().validateCast(from: .sql(.json), to: .sql(.jsonb), context: .implicit) }
    }

    @Test("Primitive arrays preserve ANY contracts and public ROW/SRF composition stays explicit")
    func primitiveAndCompositionGates() throws {
        let primitive = Expression.literal(.array([.int(1), .int(2)]))
        let quantified = try binder().bind(.quantifiedComparison(.int(1), comparison: .equal, quantifier: .any, source: .array(primitive)))
        #expect(quantified.children[1].type == .primitive(.int64, isArray: true))
        let primitiveArray = QueryColumnType.primitive(.int32, isArray: true)
        let selectedArray = QueryColumnType.sql(.array(.integer))
        for nullable in [false, true] {
            let column = try QueryColumn(number: 7, name: "numbers", type: primitiveArray, isNullable: nullable)
            let native = binder(columns: [column])
            let access = try native.bind(.arrayAccess(.col("numbers"), subscripts: [.index(integer(1))]))
            #expect(access.type == .sql(.integer) && access.isNullable)
            #expect(access.children[0].type == selectedArray)
            #expect(access.children[0].isNullable == nullable)
            #expect(access.children[0].children.first?.type == primitiveArray)
            #expect(access.children[0].children.first?.columnSlot == 0)
            let slice = try native.bind(.arrayAccess(.col("numbers"), subscripts: [.slice(lower: nil, upper: integer(1))]))
            #expect(slice.type == selectedArray && slice.children[0].type == selectedArray)
            for name in ["json_build_array", "jsonb_build_array", "json_build_object", "jsonb_build_object"] {
                let variadic = try native.bind(call(name, [.col("numbers")], variadic: true))
                #expect(variadic.children[0].type == selectedArray)
                #expect(variadic.children[0].isNullable == nullable && variadic.isNullable == nullable)
                #expect(variadic.children[0].children.first?.type == primitiveArray)
                #expect(variadic.children[0].children.first?.columnSlot == 0)
                guard case .function(let lowered) = try variadic.loweredExpression else {
                    Issue.record("Expected a selected native-array variadic function"); continue
                }
                #expect(lowered.finalArrayIsVariadic)
            }
        }
        let sparqlScope = CanonicalQueryTypeBinder(columns: [], allowingSPARQLFunctions: true,
            resolveColumn: { throw DatabaseExpressionEvaluationError.missingColumn($0.column) }, wildcardSlots: { _ in [] })
        let variadicProducer = call("sparql", [.col("Entity"), .string("query")], variadic: true)
        #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "SPARQL arguments")) {
            try sparqlScope.bind(.inList(integer(1), values: [variadicProducer]))
        }
        for expression in [Expression.row([integer(1)]), call("unnest", [null(.array(.integer))]), call("generate_subscripts", [null(.array(.integer)), integer(1)]), call("json_each", [null(.json)]), call("row_to_json", [.row([integer(1)])])] {
            #expect(throws: DatabaseExpressionEvaluationError.self) { try binder().bind(expression) }
        }
    }
}
