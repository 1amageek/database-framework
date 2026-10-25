import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import StorageKitSystemClock
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Executable canonical relation bindings")
struct CanonicalBoundRelationExecutionTests {
    @Persistable
    struct BindingItem {
        #Directory<BindingItem>("binding_items")
        var id: String
        var category: String
        var rank: Int64
    }

    @Test("Selected coercions reach conditional projection", .timeLimit(.minutes(1)))
    func conditionalProjectionConsumesBoundChildren() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let response = try await container.testBaseContext().query(SelectQuery(
            projection: [ProjectionItem(.coalesce([
                .literal(.sqlUnknownString("7")), .column(ColumnRef("n"))
            ]), alias: "value")], source: .aliased(.values([[.literal(.int(1))]]), name: "input_10", columns: ["n"])))
        #expect(response.columns.map(\.type) == [.sql(.bigint)])
        #expect(response.rows.map(\.values) == [[.primitive(.int64(7))]])
    }

    @Test("Filter GROUP HAVING and ORDER consume selected numeric children", .timeLimit(.minutes(1)))
    func relationalOperatorsConsumeBoundChildren() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let key = Expression.add(.column(ColumnRef("n")), .literal(.sqlUnknownString("1")))
        let count = Expression.aggregate(.count(nil, distinct: false))
        let response = try await container.testBaseContext().query(SelectQuery(
            projection: [ProjectionItem(key, alias: "next"), ProjectionItem(count, alias: "count")],
            source: .aliased(.values([[.literal(.int(2))], [.literal(.int(1))], [.literal(.int(2))]]), name: "input_9", columns: ["n"]),
            filter: .greaterThan(.column(ColumnRef("n")), .literal(.sqlUnknownString("0"))),
            groupBy: [.expression(key)], having: .greaterThan(count, .literal(.int(0))),
            orderBy: [SortKey(.column(ColumnRef("next")), direction: .descending)]))
        #expect(response.rows.map(\.values) == [
            [.primitive(.int64(3)), .primitive(.int64(2))],
            [.primitive(.int64(2)), .primitive(.int64(1))]
        ])
    }

    @Test("JOIN ON uses selected coercion and aligned input slots", .timeLimit(.minutes(1)))
    func joinConsumesBoundCondition() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let left = SelectQuery(projection: [.wildcard()], source: .aliased(.values([[.literal(.int(7))], [.literal(.int(8))]]), name: "input_8", columns: ["n"]))
        let response = try await container.testBaseContext().query(SelectQuery(
            projection: [ProjectionItem(.column(ColumnRef(table: "left_values", column: "n")))],
            source: .join(JoinClause(type: .inner, left: .aliased(.subquery(left), name: "left_values"),
                right: .aliased(.values([[.literal(.int(10))]]), name: "input_7", columns: ["right_n"]),
                condition: .on(.equal(.column(ColumnRef(table: "left_values", column: "n")),
                    .literal(.sqlUnknownString("7"))))))))
        #expect(response.rows.map(\.values) == [[.primitive(.int64(7))]])

        let context = container.testBaseContext()
        for (id, category) in [("a", "same"), ("b", "same"), ("c", "other")] {
            try context.insert(BindingItem(id: id, category: category, rank: 1))
        }
        try await context.save()
        let physical = DataSource.aliased(.table(TableRef(BindingItem.persistableType)), name: "right_items")
        let physicalLeft = DataSource.aliased(.table(TableRef(BindingItem.persistableType)), name: "left_items")
        let leftID = Expression.column(ColumnRef(table: "left_items", column: "id"))
        let rightID = Expression.column(ColumnRef(table: "right_items", column: "id"))
        let equalCategory = JoinCondition.on(.equal(
            .column(ColumnRef(table: "left_items", column: "category")),
            .column(ColumnRef(table: "right_items", column: "category"))))
        let physicalQuery = SelectQuery(projection: [ProjectionItem(leftID), ProjectionItem(rightID)],
            source: .join(JoinClause(type: .inner, left: physicalLeft, right: physical, condition: equalCategory)),
            orderBy: [SortKey(leftID), SortKey(rightID)])
        let duplicateKeys = try await context.query(physicalQuery)
        #expect(duplicateKeys.rows.map(\.values) == [
            [.primitive(.string("a")), .primitive(.string("a"))],
            [.primitive(.string("a")), .primitive(.string("b"))],
            [.primitive(.string("b")), .primitive(.string("a"))],
            [.primitive(.string("b")), .primitive(.string("b"))],
            [.primitive(.string("c")), .primitive(.string("c"))]
        ])
        let oneLeft = DataSource.aliased(.subquery(SelectQuery(projection: [.wildcard()],
            source: .table(TableRef(BindingItem.persistableType)),
            filter: .equal(.column(ColumnRef("id")), .literal(.string("a"))))), name: "left_items")
        for type in [JoinType.right, .full] {
            let outer = try await context.query(SelectQuery(projection: [ProjectionItem(leftID), ProjectionItem(rightID)],
                source: .join(JoinClause(type: type, left: oneLeft, right: physical, condition: equalCategory)),
                orderBy: [SortKey(rightID)]))
            #expect(outer.rows.map(\.values) == [
                [.primitive(.string("a")), .primitive(.string("a"))],
                [.primitive(.string("a")), .primitive(.string("b"))],
                [.primitive(.null), .primitive(.string("c"))]
            ])
        }
    }

    @Test("LEFT LATERAL keeps local before outer slots and nullable output", .timeLimit(.minutes(1)))
    func lateralFramesAndNullability() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        // VALUES metadata is conservatively nullable; the fallback makes the
        // left projection nonnullable so this fixture isolates LEFT extension.
        let left = SelectQuery(projection: [ProjectionItem(.coalesce([
            .column(ColumnRef("n")), .literal(.int(0))]), alias: "n")],
            source: .aliased(.values([[.literal(.int(1))], [.literal(.int(2))]]), name: "input_6", columns: ["n"]))
        let right = SelectQuery(projection: [ProjectionItem(.add(.column(ColumnRef("n")),
            .column(ColumnRef(table: "left_values", column: "n"))), alias: "combined")],
            source: .aliased(.values([[.literal(.int(10))]]), name: "input_5", columns: ["n"]),
            filter: .equal(.column(ColumnRef(table: "left_values", column: "n")), .literal(.int(1))))
        let response = try await container.testBaseContext().query(SelectQuery(projection: [.wildcard()],
            source: .join(JoinClause(type: .leftLateral, left: .aliased(.subquery(left), name: "left_values"),
                right: .aliased(.subquery(right), name: "right_values"), condition: .on(.literal(.bool(true)))))))
        #expect(response.columns.map(\.isNullable) == [false, true])
        #expect(response.rows.map(\.values) == [
            [.primitive(.int64(1)), .primitive(.int64(11))],
            [.primitive(.int64(2)), .primitive(.null)]
        ])
    }

    @Test("A correlated rich scalar keeps its outer owner and selected slot", .timeLimit(.minutes(1)))
    func correlatedRichScalarRetainsOuterFrame() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let json = try QueryJSONValue(bytes: ByteString(Array("{\"value\":7}".utf8)), limits: QueryValueLimits(maximumRetainedBytes: 1024, maximumTotalNodes: 16,
            maximumNestingDepth: 4, maximumCollectionElements: 8))
        let inner = SelectQuery(projection: [ProjectionItem(.column(ColumnRef("payload")))],
            source: .values([[]]))
        let query = SelectQuery(projection: [ProjectionItem(.subquery(inner), alias: "payload")],
            source: .aliased(.values([[.literal(.queryValue(.json(json), type: .sql(.json)))]]), name: "input_4", columns: ["payload"]))
        let execution = makeExecution()
        do {
            let response = try await container.testBaseContext().query(query, execution: execution)
            #expect(response.columns[0].type == .sql(.json))
            #expect(response.rows[0].values[0] == .json(json))
        }
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Output alias equivalence and one-based ordinals bind before rows", .timeLimit(.minutes(1)))
    func outputAliasAndOrdinalRules() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let source = DataSource.aliased(.values([[.literal(.int(2))], [.literal(.int(1))]]), name: "input_1", columns: ["n"])
        let identical = SelectQuery(projection: [
            ProjectionItem(.column(ColumnRef("n")), alias: "same"),
            ProjectionItem(.column(ColumnRef("n")), alias: "same")], source: source,
            orderBy: [SortKey(.column(ColumnRef("same")))])
        #expect(try await context.query(identical).rows[0].values == [.primitive(.int64(1)), .primitive(.int64(1))])
        let ambiguous = SelectQuery(projection: [
            ProjectionItem(.column(ColumnRef("n")), alias: "same"), ProjectionItem(.literal(.int(9)), alias: "same")],
            source: source, orderBy: [SortKey(.column(ColumnRef("same")))])
        await #expect(throws: CanonicalReadError.self) { _ = try await context.query(ambiguous) }
        for position: Int64 in [0, 3] {
            await #expect(throws: CanonicalReadError.self) {
                _ = try await context.query(identical.replacing(orderBy: [SortKey(.literal(.int(position)))]))
            }
        }
        let byOrdinal = try await context.query(identical.replacing(orderBy: [SortKey(.literal(.int(2)))]))
        #expect(byOrdinal.rows[0].values[0] == .primitive(.int64(1)))
        let volatile = Expression.function(FunctionCall(name: "clock_timestamp", arguments: []))
        let independent = SelectQuery(projection: [ProjectionItem(volatile, alias: "same"),
            ProjectionItem(volatile, alias: "same")], source: source, orderBy: [SortKey(.col("same"))])
        await #expect(throws: CanonicalReadError.self) { _ = try await context.query(independent) }
        let qualified = SelectQuery(projection: [ProjectionItem(.col("n"), alias: "same"),
            ProjectionItem(.column(ColumnRef(table: "input_1", column: "n")), alias: "same")], source: source,
            orderBy: [SortKey(.col("same"))])
        #expect(try await context.query(qualified).rows[0].values == [.primitive(.int64(1)), .primitive(.int64(1))])
    }

    @Test("GROUP input-name priority and invalid ungrouped columns are static", .timeLimit(.minutes(1)))
    func groupedBindingRules() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let count = Expression.aggregate(.count(nil, distinct: false))
        let query = SelectQuery(projection: [
            ProjectionItem(.add(.column(ColumnRef("n")), .literal(.int(1))), alias: "n"),
            ProjectionItem(count, alias: "count")],
            source: .aliased(.values([[.literal(.int(1))], [.literal(.int(2))]]), name: "input_3", columns: ["n"]),
            groupBy: [.expression(.column(ColumnRef("n")))], orderBy: [SortKey(.literal(.int(1)))])
        let response = try await context.query(query)
        #expect(response.rows.map(\.values) == [
            [.primitive(.int64(2)), .primitive(.int64(1))],
            [.primitive(.int64(3)), .primitive(.int64(1))]
        ])
        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.query(query.replacing(groupBy: nil))
        }
    }

    @Test("Every nonrecursive CTE binds with only earlier siblings", .timeLimit(.minutes(1)))
    func cteOrderAndUnusedValidation() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let first = NamedSubquery(name: "first", query: SelectQuery(projection: [.wildcard()],
            source: .aliased(.values([[.literal(.int(7))]]), name: "input_2", columns: ["n"])))
        let second = NamedSubquery(name: "second", query: SelectQuery(projection: [.wildcard()], source: .table(TableRef("first"))))
        let valid = SelectQuery(projection: [.wildcard()], source: .table(TableRef("second")), subqueries: [first, second])
        #expect(try await context.query(valid).rows[0].values == [.primitive(.int64(7))])
        let physicalName = BindingItem.persistableType
        let physicalCTE = NamedSubquery(name: physicalName, query: SelectQuery(projection: [.wildcard()],
            source: .table(TableRef(physicalName))))
        let physicalResponse = try await context.query(SelectQuery(projection: [.wildcard()],
            source: .table(TableRef(physicalName)), subqueries: [physicalCTE]))
        #expect(physicalResponse.rows.isEmpty)
        #expect(physicalResponse.columns.map(\.name) == ["id", "category", "rank"])
        let inheritedBody = NamedSubquery(name: "first", query: SelectQuery(projection: [.wildcard()],
            source: .table(TableRef("first"))))
        let nested = SelectQuery(projection: [.wildcard()], source: .table(TableRef("first")), subqueries: [inheritedBody])
        let inheritedResponse = try await context.query(SelectQuery(projection: [.wildcard()],
            source: .aliased(.subquery(nested), name: "nested"), subqueries: [first]))
        #expect(inheritedResponse.rows.map(\.values) == [[.primitive(.int64(7))]])
        let forward = NamedSubquery(name: "before", query: SelectQuery(projection: [.wildcard()], source: .table(TableRef("later"))))
        let later = NamedSubquery(name: "later", query: first.query)
        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.query(SelectQuery(projection: [.wildcard()], source: .values([[]]),
                subqueries: [forward, later]))
        }
        let unused = NamedSubquery(name: "unused", query: SelectQuery(projection: [
            ProjectionItem(.function(FunctionCall(name: "future_function", arguments: [])))],
            source: .values([[]])))
        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.query(SelectQuery(projection: [.wildcard()], source: .values([[]]),
                subqueries: [unused]))
        }
    }

    @Test("One parameter state is shared by nested selected plans", .timeLimit(.minutes(1)))
    func nestedParametersShareSelectedDeclaration() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let nested = SelectQuery(projection: [ProjectionItem(.parameter(.position(1)))],
            source: .values([[]]))
        let query = SelectQuery(projection: [
            ProjectionItem(.cast(.parameter(.position(1)), targetType: .integer), alias: "typed"),
            ProjectionItem(.subquery(nested), alias: "nested")], source: .values([[]]))
        let parameters = [QueryParameter(position: 1, value: .primitive(.null))]
        let response = try await container.testBaseContext().query(query, execution: makeExecution(), parameters: parameters)
        #expect(response.columns.map(\.type) == [.sql(.integer), .sql(.integer)])
        #expect(response.rows[0].values == [.primitive(.null), .primitive(.null)])
    }

    @Test("Qualified relation rejection and binding-budget failure leave the context usable", .timeLimit(.minutes(1)))
    func bindingFailureReleasesOwners() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.query(SelectQuery(projection: [.wildcard()],
                source: .table(TableRef(schema: "unresolved", table: BindingItem.persistableType))))
        }
        // A balanced expression keeps validation's pending stack shallow while
        // requiring 511 bound expression nodes, admitted at 1024 bytes each.
        // The budget accommodates validation but cannot admit that bound plan.
        var oversizedExpression = Expression.literal(.int(1))
        for _ in 0..<8 { oversizedExpression = .coalesce([oversizedExpression, oversizedExpression]) }
        let execution = makeExecution(maximumIntermediateBytes: 64 * 1024)
        do {
            _ = try await context.query(SelectQuery(projection: [ProjectionItem(oversizedExpression)],
                source: .values([[]])), execution: execution)
            Issue.record("Expected binding admission to reject an oversized selected plan")
        } catch let failure as DatabaseWorkLimitError {
            guard case .maximumIntermediateBytes(let stage, _, _, _) = failure else { throw failure }
            #expect(stage == .bindingCandidate)
        }
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
        let identityMeter = makeExecution().workMeter
        let layoutFootprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: ["first", "second"], sourceName: nil, workMeter: identityMeter, stage: .bindingCandidate)
        do {
            let owner = try identityMeter.reserveIntermediate(bytes: layoutFootprint.bytes, at: .bindingCandidate)
            let layout = CanonicalSourceLayout(slots: [.init(name: "first", scope: nil), .init(name: "second", scope: nil)],
                reservation: owner, types: [.sql(.bigint), .sql(.bigint)])
            #expect(layout.bindingSlots.usesIdentityMapping)
            #expect(layout.bindingSlots.map(\.valueIndex) == [0, 1])
            #expect(identityMeter.retainedIntermediateBytes == layoutFootprint.bytes)
        }
        #expect(identityMeter.retainedIntermediateBytes == 0)
        #expect(try await context.query(SelectQuery(projection: [.wildcard()],
            source: .table(TableRef(BindingItem.persistableType)))).rows.isEmpty)
    }

    @Test("Binding payload inference admits conversion scratch and releases owners", .timeLimit(.minutes(1)))
    func payloadInferenceUsesOperationBudget() async throws {
        let countingEngine = TransactionCountingInMemoryEngine()
        let container = try await makeContainer(storageEngine: countingEngine)
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let bindingTransactions = countingEngine.transactionCount
        let inferenceExecution = makeExecution(maximumIntermediateBytes: 64 * 1024)
        let arrayParameter = QueryParameter(position: 1,
            value: .primitive(.array(Array(repeating: .int64(7), count: 10_000))))
        let inferenceQuery = SelectQuery(projection: [ProjectionItem(.parameter(.position(1)))],
            source: .table(TableRef(BindingItem.persistableType)))
        let inferred = try context.boundQueryParameters(inferenceQuery, parameters: [arrayParameter],
            workMeter: inferenceExecution.workMeter)
        #expect(inferred[0].type == .primitive(.int64, isArray: true))
        #expect(inferenceExecution.workMeter.retainedIntermediateBytes == 0)
        let conversionExecution = makeExecution(maximumIntermediateBytes: 64 * 1024)
        // TEST_SCALE_SEMANTICS: correctness rejects conversion scratch beyond the byte budget.
        let oversizedInput = SelectQuery(projection: [ProjectionItem(.equal(
            .literal(.sqlUnknownString(String(repeating: "7", count: 100_000))), .literal(.int(7))))],
            source: .table(TableRef(BindingItem.persistableType)))
        do {
            _ = try context.boundQueryParameters(oversizedInput, parameters: [], workMeter: conversionExecution.workMeter)
            Issue.record("Expected contextual input scratch admission to fail before growth")
        } catch let failure as DatabaseWorkLimitError {
            guard case .maximumIntermediateBytes(let stage, _, _, _) = failure else { throw failure }
            #expect(stage == .bindingCandidate)
        }
        #expect(conversionExecution.workMeter.retainedIntermediateBytes == 0)
        let smallInput = SelectQuery(projection: [ProjectionItem(.equal(
            .literal(.sqlUnknownString("7")), .literal(.int(7))))],
            source: .table(TableRef(BindingItem.persistableType)))
        _ = try context.boundQueryParameters(smallInput, parameters: [], workMeter: conversionExecution.workMeter)
        #expect(conversionExecution.workMeter.retainedIntermediateBytes == 0)
        let cancelledExecution = makeExecution(maximumIntermediateBytes: 64 * 1024)
        let cancelled = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try context.boundQueryParameters(smallInput, parameters: [], workMeter: cancelledExecution.workMeter)
        }
        switch await cancelled.result {
        case .success: Issue.record("Expected cancelled binding to reject work")
        case .failure(let error): #expect(error is CancellationError)
        }
        #expect(cancelledExecution.workMeter.retainedIntermediateBytes == 0)
        #expect(countingEngine.transactionCount == bindingTransactions)
        #expect(try await context.query(SelectQuery(projection: [.wildcard()],
            source: .table(TableRef(BindingItem.persistableType)))).rows.isEmpty)
    }

    @Test("Named-index and scan paths preserve bound coercion before LIMIT", .timeLimit(.minutes(1)))
    func scanAndIndexHaveEquivalentBoundMeaning() async throws {
        let control = StorageTransactionControl()
        let container = try await makeContainer(storageEngine: ControlledStorageEngine(base: InMemoryEngine(), control: control))
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(BindingItem(id: "seven", category: "ignored", rank: 7))
        try context.insert(BindingItem(id: "eight", category: "ignored", rank: 8))
        try await context.save()
        let projection = [ProjectionItem(.column(ColumnRef("id"))),
            ProjectionItem(.add(.column(ColumnRef("rank")), .literal(.sqlUnknownString("1"))), alias: "adjusted")]
        let filter = Expression.equal(.column(ColumnRef("rank")), .literal(.sqlUnknownString("7")))
        func query(_ accessPath: AccessPath?) -> SelectQuery {
            SelectQuery(projection: projection,
            source: .table(TableRef(BindingItem.persistableType)),
            accessPath: accessPath,
            filter: filter,
            orderBy: [SortKey(.column(ColumnRef("adjusted")))],
            pagination: QueryPagination(limit: .count(.fixed(1), withTies: false)))
        }
        let scan = try await context.query(query(nil))
        let indexed = try await context.query(query(.index(IndexScanSource(indexName: "binding_items_by_rank", indexType: .ordered))))
        #expect(scan.columns == indexed.columns)
        #expect(scan.rows.map(\.values) == indexed.rows.map(\.values))
        #expect(scan.rows.map(\.values) == [[.primitive(.string("seven")), .primitive(.int64(8))]])
        let path = AccessPath.index(IndexScanSource(indexName: "binding_items_by_rank", indexType: .ordered))
        let eligible = query(nil).replacing(filter: .greaterThanOrEqual(
            .column(ColumnRef("rank")), .literal(.int(0))))
        let variants = [
            eligible.replacing(orderBy: [SortKey(.column(ColumnRef("rank")))])
                .replacing(pagination: QueryPagination(limit: .count(.expression(.literal(.int(1))), withTies: false),
                    offset: .expression(.literal(.int(1))))),
            eligible.replacing(orderBy: [SortKey(.column(ColumnRef("category")))])
                .replacing(pagination: QueryPagination(limit: .count(.fixed(1), withTies: true))),
            eligible.replacing(quantifier: .distinctOn([.column(ColumnRef("category"))]))
                .replacing(orderBy: [SortKey(.column(ColumnRef("category"))),
                    SortKey(.column(ColumnRef("rank")), direction: .descending)])
                .replacing(pagination: QueryPagination())
        ]
        for variant in variants {
            let scan = try await context.query(variant)
            let indexed = try await context.query(variant.replacing(accessPath: path))
            #expect(scan.columns == indexed.columns)
            if case .count(_, withTies: true)? = variant.pagination.limit {
                // Equal SQL peer keys do not define the order within the returned peer group.
                #expect(scan.rows.count == 2)
                var unmatched = indexed.rows.map(\.values)
                for values in scan.rows.map(\.values) {
                    if let match = unmatched.firstIndex(of: values) {
                        unmatched.remove(at: match)
                    } else {
                        Issue.record("The indexed peer bag is missing a scan row")
                    }
                }
                #expect(unmatched.isEmpty)
            } else {
                #expect(scan.rows.map(\.values) == indexed.rows.map(\.values))
            }
        }
        let cursorsBeforeZero = control.openedRangeCursorCount
        let zeroQuery = eligible.replacing(pagination: QueryPagination(
            limit: .count(.expression(.literal(.int(0))), withTies: false)))
        let zero = try await context.query(zeroQuery.replacing(accessPath: path))
        #expect(zero.rows.isEmpty && zero.columns == scan.columns)
        #expect(control.openedRangeCursorCount == cursorsBeforeZero)
        await #expect {
            _ = try await context.query(zeroQuery.replacing(accessPath: .index(
                IndexScanSource(indexName: "missing", indexType: .ordered))))
        } throws: { error in
            if case CanonicalReadError.indexHintNotFound = error { return true }
            return false
        }
        #expect(control.openedRangeCursorCount == cursorsBeforeZero)
    }

    private func makeExecution(maximumIntermediateBytes: UInt64 = 16 * 1024 * 1024) -> ReadExecutionContext {
        ReadExecutionContext(options: ReadExecutionOptions(budget: ExecutionBudget(maximumRows: 1000,
            maximumWorkUnits: 100_000, maximumIntermediateBytes: maximumIntermediateBytes,
            timeoutMilliseconds: 30_000)), monotonicClock: SystemStorageClock())
    }

    private func makeContainer(storageEngine: any StorageEngine = InMemoryEngine()) async throws -> DBContainer {
        let descriptor = try IndexDescriptor(entityName: BindingItem.persistableType,
            declaration: .ordered(name: "binding_items_by_rank", keys: [.ascending(BindingItem.fields.rank.identity)]),
            fieldSchemas: BindingItem.fieldSchemas)
        let schema = try Schema(entities: [Schema.Entity(from: BindingItem.self, including: [descriptor])])
        return try await DBContainer.open(for: schema, configuration: .testing(storageEngine: storageEngine),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(executionIdentity:
                DatabaseExecutionRuntimeIdentity(identifier: "canonical-binding-tests", revision: 1),
                entityRuntimes: [DatabaseFrameworkRuntime.entity(BindingItem.self, including: [descriptor])]),
            security: .testingDisabled)
    }
}
