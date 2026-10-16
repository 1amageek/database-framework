import DatabaseKit
import DatabaseTypes
import StorageKit
import Testing
@_spi(PolymorphicRuntime) @_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL composite values and relational keys")
struct CanonicalSQLCompositeTests {
    private struct CompositeClock: WallClock {
        let now: Timestamp
    }
    private var limits: QueryValueLimits {
        .init(maximumRetainedBytes: 4_000_000, maximumTotalNodes: 100_000,
            maximumNestingDepth: 128, maximumCollectionElements: 100_000)
    }
    private func meter(work: UInt64 = 1_000_000, bytes: UInt64 = 4_000_000) -> DatabaseWorkMeter {
        DatabaseWorkMeter(unbounded: ExecutionBudget(maximumWorkUnits: work,
            maximumIntermediateBytes: bytes, timeoutMilliseconds: UInt32.max))
    }
    private func binder(_ columns: [QueryColumn] = [], meter: DatabaseWorkMeter? = nil) -> CanonicalQueryTypeBinder {
        CanonicalQueryTypeBinder(columns: columns, resolveColumn: { reference in
            guard let index = columns.firstIndex(where: { $0.name == reference.column }) else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "test column")
            }
            return index
        }, wildcardSlots: { _ in Array(columns.indices) }, workMeter: meter)
    }
    private func descriptor(_ names: [String], types: [QueryColumnType]? = nil) throws -> QueryCompositeType {
        try QueryCompositeType(columns: names.enumerated().map { index, name in
            try QueryColumn(number: UInt32(index + 1), name: name, type: types?[index] ?? .sql(.integer), isNullable: true)
        }, limits: limits)
    }
    private func value(_ type: QueryCompositeType, _ fields: [QueryValue]) throws -> QueryValue {
        .composite(try QueryCompositeValue(type: type, fields: fields, limits: limits))
    }
    private func literal(_ value: QueryValue, _ type: QueryColumnType) -> Expression {
        .literal(.queryValue(value, type: type))
    }
    private func evaluate(_ bound: CanonicalBoundExpression, _ operands: [QueryValue], meter: DatabaseWorkMeter? = nil) throws -> QueryValue {
        try CanonicalSQLCompositeEvaluator.evaluate(bound, operands: operands, temporalEnvironment: nil,
            workMeter: meter ?? self.meter(), limits: limits).value
    }

    @Test func rowBindingAndEmptyRow() throws {
        let selected = try binder().bind(.row([.literal(.int(1)), .literal(.string("x"))]))
        guard case .sql(.composite(let type)) = selected.type else { Issue.record("ROW did not select a composite"); return }
        #expect(type.columns.map(\.name) == ["f1", "f2"])
        #expect(type.columns.map(\.type) == [.sql(.bigint), .sql(.text)])
        #expect(!selected.isNullable)
        let output = try evaluate(selected, [.primitive(.int64(1)), .primitive(.string("x"))])
        guard case .composite(let row) = output else { Issue.record("ROW value missing"); return }
        #expect(row.fields.count == 2)
        let empty = try binder().bind(.row([]))
        guard case .composite(let zero) = try evaluate(empty, []) else { Issue.record("Empty ROW missing"); return }
        #expect(zero.fields.isEmpty)
    }

    @Test func namedFieldsAndExpansion() throws {
        let type = try descriptor(["a", "b"])
        let input = try value(type, [.primitive(.int32(7)), .primitive(.null)])
        let expression = literal(input, .sql(.composite(type)))
        let field = try binder().bind(.field(expression, name: "a"))
        #expect(field.fieldOrdinal == 0)
        #expect(try evaluate(field, [input]) == .primitive(.int32(7)))
        #expect(try evaluate(field, [.primitive(.null)]).isNull)
        let query = SelectQuery(projection: [.expression(.fieldExpansion(expression))], source: .values([[]]))
        let plan = try binder().bindSelect(query)
        #expect(plan.columns.map(\.name) == ["a", "b"])
        #expect(plan.projection.map(\.fieldOrdinal) == [0, 1])
        #expect(throws: (any Error).self) { _ = try binder().bind(.field(expression, name: "missing")) }
        let duplicate = try descriptor(["x", "x"])
        let duplicateValue = try value(duplicate, [.primitive(.int32(1)), .primitive(.int32(2))])
        #expect(throws: (any Error).self) {
            _ = try binder().bind(.field(literal(duplicateValue, .sql(.composite(duplicate))), name: "x"))
        }
    }

    @Test func constructorNullAndDecisiveInequality() throws {
        let nullableBinding = try binder().bind(.equal(.row([.literal(.null)]), .row([.literal(.int(1))])))
        #expect(nullableBinding.comparisonMode == .constructor)
        #expect(nullableBinding.children[0].children[0].type == .sql(.bigint))
        #expect(nullableBinding.isNullable)
        guard case .sql(.composite(let selectedType)) = nullableBinding.children[0].type else {
            Issue.record("Selected constructor type missing"); return
        }
        let nullRow = try value(selectedType, [.primitive(.null)])
        let oneRow = try value(selectedType, [.primitive(.int64(1))])
        let unknownResult = try evaluate(nullableBinding, [nullRow, oneRow])
        #expect(try nullableBinding.normalize(unknownResult).isNull)
        let raw = Expression.row([.literal(.int(1)), .literal(.null)])
        let exactNull = try binder().bind(.equal(raw, raw))
        #expect(exactNull.isNullable)
        guard case .sql(.composite(let exactType)) = exactNull.children[0].type else {
            Issue.record("Two-field constructor type missing"); return
        }
        let exactValue = try value(exactType, [.primitive(.int64(1)), .primitive(.null)])
        let exactResult = try evaluate(exactNull, [exactValue, exactValue])
        #expect(try exactNull.normalize(exactResult).isNull)
        let nonNullRaw = Expression.row([.literal(.int(1))])
        let exactTrue = try binder().bind(.equal(nonNullRaw, nonNullRaw))
        guard case .sql(.composite(let trueType)) = exactTrue.children[0].type else {
            Issue.record("Non-null constructor type missing"); return
        }
        let trueValue = try value(trueType, [.primitive(.int64(1))])
        #expect(!exactTrue.isNullable)
        let trueResult = try evaluate(exactTrue, [trueValue, trueValue])
        #expect(try exactTrue.normalize(trueResult) == .primitive(.bool(true)))
        let between = try binder().bind(.between(raw, low: raw, high: raw))
        let membership = try binder().bind(.inList(raw, values: [raw]))
        #expect(between.isNullable)
        #expect(membership.isNullable)
        let conditional = try binder().bind(.caseWhen(operand: raw,
            cases: [CaseWhenPair(condition: raw, result: .literal(.int(1)))], elseResult: .literal(.int(0))))
        #expect(conditional.children[1].isNullable)
        #expect(conditional.children[1].comparisonMode == .constructor)
        let unknownString = try binder().bind(.equal(.row([.literal(.sqlUnknownString("1"))]), .row([.literal(.int(1))])))
        #expect(unknownString.children[0].children[0].type == .sql(.bigint))
        let type = try descriptor(["f1", "f2"])
        let a = try value(type, [.primitive(.null), .primitive(.int32(1))])
        let b = try value(type, [.primitive(.null), .primitive(.int32(2))])
        let child = CanonicalBoundExpression(expression: .row([]), type: .sql(.composite(type)), isNullable: false)
        func selected(_ expression: Expression) -> CanonicalBoundExpression {
            .init(expression: expression, type: .sql(.boolean), isNullable: true,
                children: [child, child], comparisonMode: .constructor)
        }
        #expect(try evaluate(selected(.equal(.row([]), .row([]))), [a, b]) == .primitive(.bool(false)))
        #expect(try evaluate(selected(.equal(.row([]), .row([]))), [a, a]).isNull)
        #expect(try evaluate(selected(.lessThan(.row([]), .row([]))), [a, b]).isNull)
        let c = try value(type, [.primitive(.int32(0)), .primitive(.null)])
        let d = try value(type, [.primitive(.int32(1)), .primitive(.null)])
        #expect(try evaluate(selected(.lessThan(.row([]), .row([]))), [c, d]) == .primitive(.bool(true)))
    }

    @Test func recordNullPredicatesAndDistinctness() throws {
        let type = try descriptor(["a", "b"])
        let child = CanonicalBoundExpression(expression: .literal(.null), type: .sql(.composite(type)), isNullable: true)
        let allNull = try value(type, [.primitive(.null), .primitive(.null)])
        let mixed = try value(type, [.primitive(.null), .primitive(.int32(1))])
        let present = try value(type, [.primitive(.int32(1)), .primitive(.int32(2))])
        let isNull = CanonicalBoundExpression(expression: .isNull(.literal(.null)), type: .sql(.boolean), isNullable: false, children: [child])
        let isNotNull = CanonicalBoundExpression(expression: .isNotNull(.literal(.null)), type: .sql(.boolean), isNullable: false, children: [child])
        #expect(try evaluate(isNull, [allNull]) == .primitive(.bool(true)))
        #expect(try evaluate(isNull, [mixed]) == .primitive(.bool(false)))
        #expect(try evaluate(isNotNull, [mixed]) == .primitive(.bool(false)))
        #expect(try evaluate(isNotNull, [present]) == .primitive(.bool(true)))
        #expect(try evaluate(isNotNull, [.primitive(.null)]) == .primitive(.bool(false)))
        let distinct = CanonicalBoundExpression(expression: .isDistinctFrom(.literal(.null), .literal(.null), negated: false),
            type: .sql(.boolean), isNullable: false, children: [child, child])
        #expect(try evaluate(distinct, [allNull, .primitive(.null)]) == .primitive(.bool(true)))
        #expect(try evaluate(distinct, [allNull, allNull]) == .primitive(.bool(false)))
    }

    @Test func wholeRowPresenceBinding() throws {
        let type = try descriptor(["a"])
        let selected = CanonicalBoundWholeRow(type: type, fieldSlots: [3], presenceIndex: 2)
        let binding = CanonicalQueryTypeBinder(columns: [], resolveColumn: { _ in 0 }, wildcardSlots: { _ in [] },
            resolveWholeRow: { name in
                guard name == "r" else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "source") }
                return selected
            })
        let bound = try binding.bind(.wholeRow("r"))
        #expect(bound.wholeRow?.fieldSlots == [3])
        #expect(bound.wholeRow?.presenceIndex == 2)
        #expect(bound.isNullable)
        #expect(throws: (any Error).self) { _ = try binding.bind(.wholeRow("missing")) }
        let fallback = CanonicalQueryTypeBinder(columns: [], resolveColumn: { reference in
            throw DatabaseExpressionEvaluationError.missingColumn(reference.column)
        }, wildcardSlots: { _ in [] }, resolveWholeRow: { name in
            guard name == "r" else { throw DatabaseExpressionEvaluationError.missingColumn(name) }
            return selected
        })
        let bare = try fallback.bind(.column(ColumnRef("r")))
        #expect(bare.wholeRow?.presenceIndex == 2)
        #expect(bare.columnSlot == nil)
        #expect(throws: DatabaseExpressionEvaluationError.missingColumn("r")) {
            _ = try fallback.bind(.column(ColumnRef(table: "outer", column: "r")))
        }
        let field = try QueryColumn(number: 1, name: "r", type: .sql(.integer), isNullable: false)
        let columnPriority = CanonicalQueryTypeBinder(columns: [field], resolveColumn: { _ in 0 },
            wildcardSlots: { _ in [] }, resolveWholeRow: { _ in
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "whole-row fallback must not bypass a column")
            })
        let namedField = try columnPriority.bind(.column(ColumnRef("r")))
        #expect(namedField.columnSlot == 0)
        #expect(namedField.wholeRow == nil)
        let ambiguous = CanonicalQueryTypeBinder(columns: [], resolveColumn: { _ in
            throw DatabaseExpressionEvaluationError.ambiguousColumn("r")
        }, wildcardSlots: { _ in [] }, resolveWholeRow: { _ in selected })
        #expect(throws: DatabaseExpressionEvaluationError.ambiguousColumn("r")) {
            _ = try ambiguous.bind(.column(ColumnRef("r")))
        }
    }

    @Test func nestedPositionalCast() throws {
        let origin = try descriptor(["a"], types: [.sql(.bigint)])
        let destination = try descriptor(["renamed"], types: [.sql(.decimal(precision: nil, scale: nil))])
        let outerOrigin = try descriptor(["nested"], types: [.sql(.composite(origin))])
        let outerDestination = try descriptor(["nested"], types: [.sql(.composite(destination))])
        let input = try value(outerOrigin, [value(origin, [.primitive(.int64(9))])])
        let result = try CanonicalSQLCompositeEvaluator.cast(input, from: .sql(.composite(outerOrigin)),
            to: .sql(.composite(outerDestination)), context: .explicit, temporalEnvironment: nil,
            workMeter: meter(), limits: limits)
        guard case .composite(let outer) = result.value, case .composite(let inner) = outer.fields[0],
              case .numeric(let number) = inner.fields[0] else { Issue.record("Nested cast missing"); return }
        #expect(number.value == ExactDecimal(coefficient: 9, scale: 0))
        #expect(inner.type.columns[0].name == "renamed")
        #expect(outer.storageFootprint.nodeCapacity >= 3)
        let coercionMeter = meter()
        do {
            let converted = try CanonicalSQLScalarCast.coerce(input, from: .sql(.composite(outerOrigin)),
                to: .sql(.composite(outerDestination)), temporalEnvironment: nil, workMeter: coercionMeter, limits: limits)
            #expect(converted.value == result.value)
            #expect(converted.lease != nil)
            #expect(coercionMeter.retainedIntermediateBytes > 0)
        }
        #expect(coercionMeter.retainedIntermediateBytes == 0)
        let peak = coercionMeter.peakIntermediateBytes
        #expect(peak > 0)
        let limited = meter(bytes: peak - 1)
        #expect(throws: DatabaseWorkLimitError.self) {
            _ = try CanonicalSQLScalarCast.coerce(input, from: .sql(.composite(outerOrigin)),
                to: .sql(.composite(outerDestination)), temporalEnvironment: nil, workMeter: limited, limits: limits)
        }
        #expect(limited.retainedIntermediateBytes == 0)
        let recovered = try CanonicalSQLScalarCast.coerce(.primitive(.int32(1)), from: .sql(.integer),
            to: .sql(.bigint), temporalEnvironment: nil, workMeter: limited, limits: limits)
        #expect(recovered.value == .primitive(.int64(1)))
        let array = QueryValue.array(try QueryArrayValue(elementType: .smallint,
            dimensions: [QueryArrayDimension(lowerBound: -2, length: 1)],
            elements: [.primitive(.int16(7))], limits: limits))
        let widened = try CanonicalSQLScalarCast.coerce(array, from: .sql(.array(.smallint)),
            to: .sql(.array(.bigint)), temporalEnvironment: nil, workMeter: meter(), limits: limits)
        guard case .array(let integers) = widened.value else { Issue.record("Coerced ARRAY missing"); return }
        #expect(integers.elements[0] == .primitive(.int64(7)))
        #expect(integers.dimensions[0].lowerBound == -2)
        let instant = Timestamp(secondsSinceUnixEpoch: 0)
        let environment = SQLTemporalEnvironment(transactionInstant: instant, statementInstant: instant,
            wallClock: CompositeClock(now: instant))
        let timestamp = try CanonicalSQLScalarCast.coerce(.primitive(.date(CivilDate(year: 1970, month: 1, day: 2))),
            from: .sql(.date), to: .sql(.timestamp(withTimeZone: false)), temporalEnvironment: environment,
            workMeter: meter(), limits: limits)
        let midnight = QueryValue.primitive(.dateTime(CivilDateTime(date: try CivilDate(year: 1970, month: 1, day: 2),
            time: try CivilTime(hour: 0, minute: 0, second: 0))))
        #expect(timestamp.value == midnight)
    }

    @Test func scalarAndCompositeArraySubqueries() throws {
        let m = meter()
        let scalar = try CanonicalSQLArrayEvaluator.assembleSubquery([.primitive(.int64(1)), .primitive(.null)],
            columnType: .sql(.bigint), resultType: .sql(.array(.bigint)), temporalEnvironment: nil, workMeter: m, limits: limits)
        guard case .array(let array) = scalar.value else { Issue.record("ARRAY missing"); return }
        #expect(array.elements.elementsEqual([.primitive(.int64(1)), .primitive(.null)]))
        #expect(array.dimensions.map(\.lowerBound) == [1])
        let type = try descriptor(["a"])
        let row = try value(type, [.primitive(.int32(2))])
        let composite = try CanonicalSQLArrayEvaluator.assembleSubquery([row], columnType: .sql(.composite(type)),
            resultType: .sql(.array(.composite(type))), temporalEnvironment: nil, workMeter: m, limits: limits)
        guard case .array(let rows) = composite.value else { Issue.record("Composite ARRAY missing"); return }
        #expect(rows.elements[0] == row)
        let empty = try CanonicalSQLArrayEvaluator.assembleSubquery([], columnType: .sql(.composite(type)),
            resultType: .sql(.array(.composite(type))), temporalEnvironment: nil, workMeter: m, limits: limits)
        guard case .array(let zero) = empty.value else { Issue.record("Empty ARRAY missing"); return }
        #expect(zero.dimensions.isEmpty)
        let cursorMeter = meter()
        var cursorMetadata: (columns: [QueryColumn], lease: DatabaseQueryPayloadLease)?
        do {
            let output = try CanonicalSQLArrayEvaluator.assembleSubquery([row], columnType: .sql(.composite(type)),
                resultType: .sql(.array(.composite(type))), temporalEnvironment: nil, workMeter: cursorMeter, limits: limits)
            guard let lease = output.lease else { Issue.record("Array producer lease missing"); return }
            let child = CanonicalBoundExpression(expression: .literal(.null), type: .sql(.array(.composite(type))), isNullable: false)
            let selected = CanonicalBoundExpression(expression: .function(FunctionCall(name: "unnest", arguments: [])),
                type: .sql(.composite(type)), isNullable: true, children: [child])
            var cursor = try CanonicalSQLArrayEvaluator.makeRowCursor(selected, operands: [output.value], inputLeases: [lease],
                temporalEnvironment: nil, workMeter: cursorMeter, limits: limits)
            cursorMetadata = try cursor.metadata()
            cursor.close()
        }
        #expect(cursorMetadata?.columns[0].type == .sql(.composite(type)))
        #expect(cursorMeter.retainedIntermediateBytes > 0)
        #expect(cursorMeter.retainedIntermediateRows == 0)
        cursorMetadata = nil
        #expect(cursorMeter.retainedIntermediateBytes == 0)
    }

    @Test func arraySubqueryBoundsAndMalformedRows() throws {
        let input = QueryValue.array(try QueryArrayValue(elementType: .integer,
            dimensions: [QueryArrayDimension(lowerBound: -2, length: 2)],
            elements: [.primitive(.int32(1)), .primitive(.int32(2))], limits: limits))
        let result = try CanonicalSQLArrayEvaluator.assembleSubquery([input, input], columnType: .sql(.array(.integer)),
            resultType: .sql(.array(.integer)), temporalEnvironment: nil, workMeter: meter(), limits: limits)
        guard case .array(let array) = result.value else { Issue.record("Matrix missing"); return }
        #expect(array.dimensions.map(\.lowerBound) == [1, -2])
        #expect(array.dimensions.map(\.length) == [2, 2])
        #expect(throws: DatabaseExpressionEvaluationError.collection(.invalidArrayShape)) {
            _ = try CanonicalSQLArrayEvaluator.assembleSubquery([input, .primitive(.null)], columnType: .sql(.array(.integer)),
                resultType: .sql(.array(.integer)), temporalEnvironment: nil, workMeter: meter(), limits: limits)
        }
    }

    @Test func selectedSubqueryCommonSignature() throws {
        let query = SelectQuery(projection: [.expression(.literal(.double(1.5)))], source: .values([[]]))
        let column = try QueryColumn(number: 1, name: "n", type: .sql(.decimal(precision: nil, scale: nil)), isNullable: false)
        let binding = CanonicalQueryTypeBinder(columns: [], resolveColumn: { _ in 0 }, wildcardSlots: { _ in [] },
            bindSubquery: { _ in [column] })
        let bound = try binding.bind(.inSubquery(.literal(.int(1)), subquery: query))
        #expect(bound.children[0].type == .sql(.decimal(precision: nil, scale: nil)))
        #expect(bound.subqueryComparisonTypes == [.sql(.decimal(precision: nil, scale: nil))])
        let multi = CanonicalQueryTypeBinder(columns: [], resolveColumn: { _ in 0 }, wildcardSlots: { _ in [] },
            bindSubquery: { _ in [column, try QueryColumn(number: 2, name: "b", type: .sql(.text), isNullable: true)] })
        #expect(throws: (any Error).self) { _ = try multi.bind(.subquery(query)) }
        let row = try multi.bind(.equal(.row([.literal(.int(1)), .literal(.string("x"))]), .subquery(query)))
        #expect(row.comparisonMode == .constructor)
    }

    @Test func outputKeyOccurrencesAndCollation() throws {
        let source = try QueryColumn(number: 1, name: "x", type: .sql(.integer), isNullable: false)
        let binding = binder([source])
        let query = SelectQuery(projection: [.expression(.column(ColumnRef("x")), alias: "a"),
            .expression(.column(ColumnRef("x")), alias: "b")], source: .values([]))
        let plan = try binding.bindSelect(query)
        #expect(try binding.bindOrderKey(SortKey(.column(ColumnRef("b"))), projection: plan.projection,
            output: plan.columns).projectionIndex == 1)
        #expect(try binding.bindOrderKey(SortKey(.literal(.int(1))), projection: plan.projection,
            output: plan.columns).projectionIndex == 0)
        #expect(throws: (any Error).self) {
            _ = try binding.bindOrderKey(SortKey(.literal(.int(0))), projection: plan.projection, output: plan.columns)
        }
        let collated = try binding.bind(.collate(.literal(.string("x")), collation: .posix))
        #expect(collated.collation == .posix)
        #expect(throws: (any Error).self) { _ = try binding.bind(.collate(.literal(.int(1)), collation: .c)) }
        let outputKey = try binding.bindOrderKey(SortKey(.column(ColumnRef("a"))),
            projection: plan.projection, output: plan.columns)
        let qualified = try binding.bindOrderKey(SortKey(.column(ColumnRef(table: "v", column: "x"))),
            projection: plan.projection, output: plan.columns)
        let m = meter()
        #expect(try outputKey.isSemanticallyEquivalent(to: qualified, workMeter: m, isVolatileSubquery: { _ in false }))
        #expect(outputKey.projectionIndex == 0)
        #expect(qualified.projectionIndex == nil)
        let first = try binding.bind(.add(.column(ColumnRef("x")), .literal(.int(1))))
        let second = try binding.bind(.add(.column(ColumnRef(table: "v", column: "x")), .literal(.int(1))))
        #expect(try CanonicalBoundKey(expression: first).isSemanticallyEquivalent(to: CanonicalBoundKey(expression: second),
            workMeter: m, isVolatileSubquery: { _ in false }))
        let cast = try binding.bind(.cast(.column(ColumnRef("x")), targetType: .bigint))
        #expect(try !qualified.isSemanticallyEquivalent(to: CanonicalBoundKey(expression: cast),
            workMeter: m, isVolatileSubquery: { _ in false }))
        let c = try binding.bind(.collate(.literal(.string("x")), collation: .c))
        #expect(try !CanonicalBoundKey(expression: c).isSemanticallyEquivalent(to: CanonicalBoundKey(expression: collated),
            workMeter: m, isVolatileSubquery: { _ in false }))
        let volatile = try binding.bind(.function(FunctionCall(name: "clock_timestamp", arguments: [])))
        #expect(try !CanonicalBoundKey(expression: volatile, projectionIndex: 0).isSemanticallyEquivalent(
            to: CanonicalBoundKey(expression: volatile, projectionIndex: 1), workMeter: m, isVolatileSubquery: { _ in false }))
        #expect(try CanonicalBoundKey(expression: volatile, projectionIndex: 0).isSemanticallyEquivalent(
            to: CanonicalBoundKey(expression: volatile, projectionIndex: 0), workMeter: m, isVolatileSubquery: { _ in false }))
        #expect(m.retainedIntermediateBytes == 0)
        let workLimited = meter(work: 1)
        #expect(throws: DatabaseWorkLimitError.self) {
            _ = try outputKey.isSemanticallyEquivalent(to: qualified, workMeter: workLimited, isVolatileSubquery: { _ in false })
        }
        #expect(workLimited.retainedIntermediateBytes == 0)
        var arena = QueryOccurrenceArena()
        let root = try arena.append()
        let cell = try arena.append(parent: root, field: .valuesCell, index: 0)
        let order = try arena.append(parent: root, field: .orderBy, index: 0)
        let assigned = try binding.bindAssignment(.column(ColumnRef("x")), to: .sql(.bigint), occurrence: cell)
        #expect(assigned.occurrence == cell)
        #expect(assigned.children[0].occurrence == cell)
        let reassigned = try binding.bindAssignment(assigned.children[0], to: .sql(.bigint))
        #expect(reassigned.children[0].columnSlot == 0)
        #expect(reassigned.children[0].occurrence == cell)
        let sourceOrder = try binding.bindOrderKey(SortKey(.column(ColumnRef(table: "v", column: "x"))),
            projection: plan.projection, output: plan.columns, occurrence: order)
        #expect(sourceOrder.occurrence == order)
        #expect(sourceOrder.expression.occurrence == order)
        let aliasOrder = try binding.bindOrderKey(SortKey(.column(ColumnRef("a"))),
            projection: plan.projection, output: plan.columns, occurrence: order)
        #expect(aliasOrder.occurrence == order)
        #expect(aliasOrder.expression.occurrence == plan.projection[0].occurrence)
    }

    @Test func paginationTypingAndNullOrigins() throws {
        let binding = binder()
        #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
            _ = try binding.bindPagination(QueryPagination(limit: .count(.fixed(UInt64.max), withTies: false)), localColumnCount: 0)
        }
        let unknown = try binding.bindPagination(QueryPagination(limit: .count(.expression(.literal(.null)), withTies: true)), localColumnCount: 0)
        #expect(unknown.rejectsUnknownNullLimit)
        let typed = try binding.bindPagination(QueryPagination(limit: .count(.expression(.cast(.literal(.null), targetType: .bigint)), withTies: true)), localColumnCount: 0)
        #expect(!typed.rejectsUnknownNullLimit)
        let source = try QueryColumn(number: 1, name: "n", type: .sql(.bigint), isNullable: false)
        #expect(throws: (any Error).self) {
            _ = try binder([source]).bindPagination(QueryPagination(offset: .expression(.column(ColumnRef("n")))), localColumnCount: 1)
        }
    }

    @Test func compositeIdentityAndOrdering() throws {
        let type = try descriptor(["a", "b"], types: [.sql(.char(length: nil)), .sql(.decimal(precision: nil, scale: nil))])
        let one = ExactDecimal(coefficient: 1, scale: 0)
        let a = try value(type, [.primitive(.string("x ")), .numeric(QueryNumericValue(value: one, displayScale: 0))])
        let b = try value(type, [.primitive(.string("x")), .numeric(QueryNumericValue(value: one, displayScale: 4))])
        let m = meter()
        let ka = try CanonicalSQLValueIdentity.prepare(a, type: .sql(.composite(type)), environment: nil, workMeter: m)
        let kb = try CanonicalSQLValueIdentity.prepare(b, type: .sql(.composite(type)), environment: nil, workMeter: m)
        #expect(try CanonicalSQLValueIdentity.equal(ka, kb, workMeter: m, stage: .deduplication))
        let firstHash = try CanonicalSQLValueIdentity.hash(ka, workMeter: m, stage: .deduplication)
        let secondHash = try CanonicalSQLValueIdentity.hash(kb, workMeter: m, stage: .deduplication)
        #expect(firstHash == secondHash)
        #expect(try CanonicalSQLValueSemantics.compareValues(a, b, leftType: .sql(.composite(type)),
            rightType: .sql(.composite(type)), environment: nil, workMeter: m) == .equal)
        #expect(a != b)
    }

    @Test func metadataRetainsAdmissionAfterBinderScope() throws {
        let m = meter()
        var owner: DatabaseRetainedQueryColumns?
        do {
            let binding = binder(meter: m)
            let a = try descriptor(["a"], types: [.sql(.integer)])
            let b = try descriptor(["a"], types: [.sql(.bigint)])
            let common = try binding.commonType([.sql(.composite(a)), .sql(.composite(b))])
            let query = SelectQuery(projection: [.expression(.literal(.queryValue(.primitive(.null), type: common)))], source: .values([]))
            let plan = try binding.bindSelect(query)
            var leases: [DatabaseQueryPayloadLease] = []
            if let lease = plan.metadataLease { leases.append(lease) }
            if let lease = plan.bindingMetadata?.payloadLease { leases.append(lease) }
            owner = DatabaseRetainedQueryColumns(columns: plan.columns, payloadLeases: leases)
        }
        #expect(m.retainedIntermediateBytes > 0)
        #expect(owner?.columns.count == 1)
        let admittedMetadata = m.retainedIntermediateBytes
        var page: PolymorphicQueryPage?
        do {
            guard let columns = owner else { Issue.record("Retained columns missing"); return }
            page = try PolymorphicQueryPage(columns: columns, results: [], continuation: nil, metadata: [:])
        }
        owner = nil
        #expect(page?.results.isEmpty == true)
        #expect(page?.columns.count == 1)
        #expect(m.retainedIntermediateBytes == admittedMetadata)
        #expect(m.retainedIntermediateRows == 0)
        var lastPage = page
        page = nil
        withExtendedLifetime(lastPage) { #expect(m.retainedIntermediateBytes == admittedMetadata) }
        lastPage = nil
        #expect(m.retainedIntermediateBytes == 0)
        #expect(m.retainedIntermediateRows == 0)
        let innerType = try descriptor(["x"])
        let outerType = try descriptor(["inner", "payload"], types: [.sql(.composite(innerType)), .sql(.text)])
        let root = try value(outerType, [value(innerType, [.primitive(.int32(1))]), .primitive(.string("retained sibling"))])
        guard case .composite(let outer) = root, case .composite(let child) = outer.fields[0] else {
            Issue.record("Nested retained view missing"); return
        }
        let storage = child.storageFootprint
        #expect(storage == outer.storageFootprint)
        let logical = try QueryValueValidator.measure(.composite(child))
        let retained = storage.retainedStorageByteCount + storage.leafRetainedByteCount
            + storage.typeStorageFootprint.retainedStorageByteCount + storage.typeStorageFootprint.utf8ByteCount
        #expect(logical.retainedByteCount == retained)
        let observed = try CanonicalRelationalFootprintMeter.valueFootprint(of: QueryValue.composite(child), workMeter: m, stage: .projection)
        let stride = UInt64(max(MemoryLayout<QueryValue>.stride, MemoryLayout<QueryJSONBMember>.stride) + 32)
        #expect(observed.bytes == retained + stride * (logical.nodeCount + logical.collectionElementCount * 2))
        let values = [root]
        let decoded = try CanonicalRelationalFootprintMeter.decodedRowStructureFootprint(values: values,
            annotations: [:], version: nil, payloadLeaseCount: 0, workMeter: m, stage: .projection)
        let rowBase = UInt64(64 + values.capacity * MemoryLayout<QueryValue>.stride
            + MemoryLayout<[String: FieldValue]>.stride + MemoryLayout<[DatabaseQueryPayloadLease]>.stride)
        #expect(decoded.bytes == rowBase + storage.retainedStorageByteCount
            + storage.typeStorageFootprint.retainedStorageByteCount + storage.typeStorageFootprint.utf8ByteCount)
        #expect(m.retainedIntermediateBytes == 0)
    }

    @Test func constructionFailureReleasesAndRecovers() async throws {
        let type = try descriptor(["a"])
        let constrained = meter(bytes: 1)
        #expect(throws: (any Error).self) {
            _ = try CanonicalSQLCompositeEvaluator.construct(type: type, fields: [.primitive(.int32(1))],
                workMeter: constrained, limits: limits)
        }
        #expect(constrained.retainedIntermediateBytes == 0)
        #expect(constrained.retainedIntermediateRows == 0)
        let m = meter()
        do {
            let result = try CanonicalSQLCompositeEvaluator.construct(type: type, fields: [.primitive(.int32(1))], workMeter: m, limits: limits)
            withExtendedLifetime(result) { #expect(m.retainedIntermediateBytes > 0) }
        }
        #expect(m.retainedIntermediateBytes == 0)
        #expect(throws: (any Error).self) {
            _ = try CanonicalSQLCompositeEvaluator.construct(type: type, fields: [], workMeter: m, limits: limits)
        }
        #expect(m.retainedIntermediateBytes == 0)
        let work = meter(work: 1)
        #expect(throws: DatabaseWorkLimitError.self) {
            _ = try CanonicalSQLCompositeEvaluator.construct(type: type, fields: [.primitive(.int32(1))], workMeter: work, limits: limits)
        }
        #expect(work.retainedIntermediateBytes == 0)
        let clock = SQLArrayTestClock()
        let expired = SQLArrayFixture.meter(clock: clock)
        clock.advance()
        #expect(throws: DatabaseWorkLimitError.self) {
            _ = try CanonicalSQLCompositeEvaluator.construct(type: type, fields: [.primitive(.int32(1))], workMeter: expired, limits: limits)
        }
        #expect(expired.retainedIntermediateBytes == 0)
        let cancelled = meter()
        let limits = self.limits
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try CanonicalSQLCompositeEvaluator.construct(type: type, fields: [.primitive(.int32(1))],
                workMeter: cancelled, limits: limits)
        }
        do { _ = try await task.value; Issue.record("Cancelled construction succeeded") }
        catch is CancellationError { }
        catch { throw error }
        #expect(cancelled.retainedIntermediateBytes == 0)
    }
}
