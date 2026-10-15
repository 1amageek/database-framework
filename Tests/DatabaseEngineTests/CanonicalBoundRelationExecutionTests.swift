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
            projection: .items([ProjectionItem(.coalesce([
                .literal(.sqlUnknownString("7")), .column(ColumnRef("n"))
            ]), alias: "value")]), source: .values([[.int(1)]], columnNames: ["n"])))
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
            projection: .items([ProjectionItem(key, alias: "next"), ProjectionItem(count, alias: "count")]),
            source: .values([[.int(2)], [.int(1)], [.int(2)]], columnNames: ["n"]),
            filter: .greaterThan(.column(ColumnRef("n")), .literal(.sqlUnknownString("0"))),
            groupBy: [key], having: .greaterThan(count, .literal(.int(0))),
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
        let left = SelectQuery(projection: .all, source: .values([[.int(7)], [.int(8)]], columnNames: ["n"]))
        let response = try await container.testBaseContext().query(SelectQuery(
            projection: .items([ProjectionItem(.column(ColumnRef(table: "left_values", column: "n")))]),
            source: .join(JoinClause(type: .inner, left: .subquery(left, alias: "left_values"),
                right: .values([[.int(10)]], columnNames: ["right_n"]),
                condition: .on(.equal(.column(ColumnRef(table: "left_values", column: "n")),
                    .literal(.sqlUnknownString("7"))))))))
        #expect(response.rows.map(\.values) == [[.primitive(.int64(7))]])
    }

    @Test("LEFT LATERAL keeps local before outer slots and nullable output", .timeLimit(.minutes(1)))
    func lateralFramesAndNullability() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        // VALUES metadata is conservatively nullable; the fallback makes the
        // left projection nonnullable so this fixture isolates LEFT extension.
        let left = SelectQuery(projection: .items([ProjectionItem(.coalesce([
            .column(ColumnRef("n")), .literal(.int(0))]), alias: "n")]),
            source: .values([[.int(1)], [.int(2)]], columnNames: ["n"]))
        let right = SelectQuery(projection: .items([ProjectionItem(.add(.column(ColumnRef("n")),
            .column(ColumnRef(table: "left_values", column: "n"))), alias: "combined")]),
            source: .values([[.int(10)]], columnNames: ["n"]),
            filter: .equal(.column(ColumnRef(table: "left_values", column: "n")), .literal(.int(1))))
        let response = try await container.testBaseContext().query(SelectQuery(projection: .all,
            source: .join(JoinClause(type: .leftLateral, left: .subquery(left, alias: "left_values"),
                right: .subquery(right, alias: "right_values"), condition: .on(.literal(.bool(true)))))))
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
        let inner = SelectQuery(projection: .items([ProjectionItem(.column(ColumnRef("payload")))]),
            source: .values([[]], columnNames: []))
        let query = SelectQuery(projection: .items([ProjectionItem(.subquery(inner), alias: "payload")]),
            source: .values([[.queryValue(.json(json), type: .sql(.json))]], columnNames: ["payload"]))
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
        let source = DataSource.values([[.int(2)], [.int(1)]], columnNames: ["n"])
        let identical = SelectQuery(projection: .items([
            ProjectionItem(.column(ColumnRef("n")), alias: "same"),
            ProjectionItem(.column(ColumnRef("n")), alias: "same")]), source: source,
            orderBy: [SortKey(.column(ColumnRef("same")))])
        #expect(try await context.query(identical).rows[0].values == [.primitive(.int64(1)), .primitive(.int64(1))])
        let ambiguous = SelectQuery(projection: .items([
            ProjectionItem(.column(ColumnRef("n")), alias: "same"), ProjectionItem(.literal(.int(9)), alias: "same")]),
            source: source, orderBy: [SortKey(.column(ColumnRef("same")))])
        await #expect(throws: CanonicalReadError.self) { _ = try await context.query(ambiguous) }
        for position: Int64 in [0, 3] {
            await #expect(throws: CanonicalReadError.self) {
                _ = try await context.query(identical.replacing(orderBy: [SortKey(.literal(.int(position)))]))
            }
        }
        let byOrdinal = try await context.query(identical.replacing(orderBy: [SortKey(.literal(.int(2)))]))
        #expect(byOrdinal.rows[0].values[0] == .primitive(.int64(1)))
    }

    @Test("GROUP input-name priority and invalid ungrouped columns are static", .timeLimit(.minutes(1)))
    func groupedBindingRules() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let count = Expression.aggregate(.count(nil, distinct: false))
        let query = SelectQuery(projection: .items([
            ProjectionItem(.add(.column(ColumnRef("n")), .literal(.int(1))), alias: "n"),
            ProjectionItem(count, alias: "count")]),
            source: .values([[.int(1)], [.int(2)]], columnNames: ["n"]),
            groupBy: [.column(ColumnRef("n"))], orderBy: [SortKey(.literal(.int(1)))])
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
        let first = NamedSubquery(name: "first", query: SelectQuery(projection: .all,
            source: .values([[.int(7)]], columnNames: ["n"])))
        let second = NamedSubquery(name: "second", query: SelectQuery(projection: .all, source: .table(TableRef("first"))))
        let valid = SelectQuery(projection: .all, source: .table(TableRef("second")), subqueries: [first, second])
        #expect(try await context.query(valid).rows[0].values == [.primitive(.int64(7))])
        let physicalName = BindingItem.persistableType
        let physicalCTE = NamedSubquery(name: physicalName, query: SelectQuery(projection: .all,
            source: .table(TableRef(physicalName))))
        let physicalResponse = try await context.query(SelectQuery(projection: .all,
            source: .table(TableRef(physicalName)), subqueries: [physicalCTE]))
        #expect(physicalResponse.rows.isEmpty)
        #expect(physicalResponse.columns.map(\.name) == ["id", "category", "rank"])
        let inheritedBody = NamedSubquery(name: "first", query: SelectQuery(projection: .all,
            source: .table(TableRef("first"))))
        let nested = SelectQuery(projection: .all, source: .table(TableRef("first")), subqueries: [inheritedBody])
        let inheritedResponse = try await context.query(SelectQuery(projection: .all,
            source: .subquery(nested, alias: "nested"), subqueries: [first]))
        #expect(inheritedResponse.rows.map(\.values) == [[.primitive(.int64(7))]])
        let forward = NamedSubquery(name: "before", query: SelectQuery(projection: .all, source: .table(TableRef("later"))))
        let later = NamedSubquery(name: "later", query: first.query)
        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.query(SelectQuery(projection: .all, source: .values([[]], columnNames: []),
                subqueries: [forward, later]))
        }
        let unused = NamedSubquery(name: "unused", query: SelectQuery(projection: .items([
            ProjectionItem(.function(FunctionCall(name: "future_function", arguments: [])))]),
            source: .values([[]], columnNames: [])))
        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.query(SelectQuery(projection: .all, source: .values([[]], columnNames: []),
                subqueries: [unused]))
        }
    }

    @Test("One parameter state is shared by nested selected plans", .timeLimit(.minutes(1)))
    func nestedParametersShareSelectedDeclaration() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let nested = SelectQuery(projection: .items([ProjectionItem(.parameter(.position(1)))]),
            source: .values([[]], columnNames: []))
        let query = SelectQuery(projection: .items([
            ProjectionItem(.cast(.parameter(.position(1)), targetType: .integer), alias: "typed"),
            ProjectionItem(.subquery(nested), alias: "nested")]), source: .values([[]], columnNames: []))
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
            _ = try await context.query(SelectQuery(projection: .all,
                source: .table(TableRef(schema: "unresolved", table: BindingItem.persistableType))))
        }
        // A balanced expression keeps validation's pending stack shallow while
        // requiring 511 bound expression nodes, admitted at 1024 bytes each.
        // The budget accommodates validation but cannot admit that bound plan.
        var oversizedExpression = Expression.literal(.int(1))
        for _ in 0..<8 { oversizedExpression = .coalesce([oversizedExpression, oversizedExpression]) }
        let execution = makeExecution(maximumIntermediateBytes: 64 * 1024)
        do {
            _ = try await context.query(SelectQuery(projection: .items([ProjectionItem(oversizedExpression)]),
                source: .values([[]], columnNames: [])), execution: execution)
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
        #expect(try await context.query(SelectQuery(projection: .all,
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
        let inferenceQuery = SelectQuery(projection: .items([ProjectionItem(.parameter(.position(1)))]),
            source: .table(TableRef(BindingItem.persistableType)))
        let inferred = try context.boundQueryParameters(inferenceQuery, parameters: [arrayParameter],
            workMeter: inferenceExecution.workMeter)
        #expect(inferred[0].type == .primitive(.int64, isArray: true))
        #expect(inferenceExecution.workMeter.retainedIntermediateBytes == 0)
        let conversionExecution = makeExecution(maximumIntermediateBytes: 64 * 1024)
        let oversizedInput = SelectQuery(projection: .items([ProjectionItem(.equal(
            .literal(.sqlUnknownString(String(repeating: "7", count: 100_000))), .literal(.int(7))))]),
            source: .table(TableRef(BindingItem.persistableType)))
        do {
            _ = try context.boundQueryParameters(oversizedInput, parameters: [], workMeter: conversionExecution.workMeter)
            Issue.record("Expected contextual input scratch admission to fail before growth")
        } catch let failure as DatabaseWorkLimitError {
            guard case .maximumIntermediateBytes(let stage, _, _, _) = failure else { throw failure }
            #expect(stage == .bindingCandidate)
        }
        #expect(conversionExecution.workMeter.retainedIntermediateBytes == 0)
        let smallInput = SelectQuery(projection: .items([ProjectionItem(.equal(
            .literal(.sqlUnknownString("7")), .literal(.int(7))))]),
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
        #expect(try await context.query(SelectQuery(projection: .all,
            source: .table(TableRef(BindingItem.persistableType)))).rows.isEmpty)
    }

    @Test("Named-index and scan paths preserve bound coercion before LIMIT", .timeLimit(.minutes(1)))
    func scanAndIndexHaveEquivalentBoundMeaning() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(BindingItem(id: "seven", category: "ignored", rank: 7))
        try context.insert(BindingItem(id: "eight", category: "ignored", rank: 8))
        try await context.save()
        let projection = Projection.items([ProjectionItem(.column(ColumnRef("id"))),
            ProjectionItem(.add(.column(ColumnRef("rank")), .literal(.sqlUnknownString("1"))), alias: "adjusted")])
        let filter = Expression.equal(.column(ColumnRef("rank")), .literal(.sqlUnknownString("7")))
        func query(_ accessPath: AccessPath?) -> SelectQuery {
            SelectQuery(projection: projection, source: .table(TableRef(BindingItem.persistableType)),
                accessPath: accessPath, filter: filter, orderBy: [SortKey(.column(ColumnRef("adjusted")))], limit: 1)
        }
        let scan = try await context.query(query(nil))
        let indexed = try await context.query(query(.index(IndexScanSource(indexName: "binding_items_by_rank", indexType: .ordered))))
        #expect(scan.columns == indexed.columns)
        #expect(scan.rows.map(\.values) == indexed.rows.map(\.values))
        #expect(scan.rows.map(\.values) == [[.primitive(.string("seven")), .primitive(.int64(8))]])
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
