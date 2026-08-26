import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import ScalarIndex
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Schema-declared index behavior")
struct SchemaDeclaredIndexBehaviorTests {
    @Test("Application-composed index follows every mutation")
    func applicationComposedIndexFollowsMutations() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()
        let first = CatalogItem(
            id: "first",
            category: "books",
            title: "First"
        )
        let second = CatalogItem(
            id: "second",
            category: "books",
            title: "Second"
        )
        let third = CatalogItem(
            id: "third",
            category: "music",
            title: "Third"
        )

        try context.insert(first)
        try context.insert(second)
        try context.insert(third)
        try await context.save()

        #expect(try await scenario.indexEntryCount() == 3)
        #expect(try await scenario.indexEntryCount(category: "books") == 2)
        #expect(try await scenario.indexEntryCount(category: "music") == 1)

        var updated = first
        updated.category = "music"
        try context.update(updated)
        try await context.save()

        #expect(try await scenario.indexEntryCount() == 3)
        #expect(try await scenario.indexEntryCount(category: "books") == 1)
        #expect(try await scenario.indexEntryCount(category: "music") == 2)

        try context.delete(third)
        try await context.save()

        #expect(try await scenario.indexEntryCount() == 2)
        #expect(try await scenario.indexEntryCount(category: "books") == 1)
        #expect(try await scenario.indexEntryCount(category: "music") == 1)
    }

    @Test("Every read path uses the application-composed index")
    func everyReadPathUsesApplicationComposedIndex() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()
        let first = CatalogItem(
            id: "first",
            category: "books",
            title: "First"
        )
        let second = CatalogItem(
            id: "second",
            category: "books",
            title: "Second"
        )
        let unrelated = CatalogItem(
            id: "unrelated",
            category: "music",
            title: "Unrelated"
        )

        try context.insert(first)
        try context.insert(second)
        try context.insert(unrelated)
        try await context.save()
        try await scenario.corruptItems(ids: [unrelated.id])

        let automaticQuery = scenario.categoryQuery("books")
        let plan = try await QueryExecutor(
            context: scenario.container.testBaseContext(),
            query: automaticQuery
        ).executionPlan()
        guard
            case .orderedIndex(
                let name,
                let indexType,
                let indexedFields
        ) = plan.accessPath else {
            Issue.record("Expected the application-composed scalar index")
            return
        }
        #expect(name == scenario.indexName)
        #expect(indexType == .ordered)
        #expect(indexedFields == ["category"])

        let automaticResults = try await QueryExecutor(
            context: scenario.container.testBaseContext(),
            query: automaticQuery
        ).execute()
        #expect(Set(automaticResults.map(\.id)) == Set([first.id, second.id]))

        var forcedQuery = scenario.categoryQuery("books")
        forcedQuery.forcedIndex = IndexHint(indexName: scenario.indexName)
        let forcedResults = try await QueryExecutor(
            context: scenario.container.testBaseContext(),
            query: forcedQuery
        ).execute()
        #expect(Set(forcedResults.map(\.id)) == Set([first.id, second.id]))

        let canonicalResponse = try await scenario.container
            .testBaseContext()
            .query(
                SelectQuery(
                    projection: .all,
                    source: .table(TableRef(CatalogItem.persistableType)),
                    accessPath: .index(
                        IndexScanSource(
                            indexName: scenario.indexName,
                            indexType: .ordered
                        )
                    ),
                    filter: .equal(
                        .col("category"),
                        .string("books")
                    )
                )
            )
        #expect(canonicalResponse.rows.count == 2)
        #expect(
            Set(canonicalResponse.rows.compactMap {
                $0.fields["id"]?.stringValue
            }) == Set([first.id, second.id])
        )

        try await scenario.corruptItems(ids: [
            first.id,
            second.id,
            unrelated.id,
        ])
        let count = try await QueryExecutor(
            context: scenario.container.testBaseContext(),
            query: forcedQuery
        ).count()
        #expect(count == 2)
    }

    @Test("Relational cardinality operators run before result windows")
    func cardinalityOperatorsRunBeforeResultWindows() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()
        try context.insert(
            CatalogItem(id: "first", category: "books", title: "First")
        )
        try context.insert(
            CatalogItem(id: "second", category: "books", title: "Second")
        )
        try context.insert(
            CatalogItem(id: "third", category: "music", title: "Third")
        )
        try await context.save()

        let countProjection = Projection.items([
            ProjectionItem(
                .aggregate(.count(nil, distinct: false)),
                alias: "total"
            )
        ])
        let countLimitedToZero = try await context.query(
            SelectQuery(
                projection: countProjection,
                source: .table(TableRef(CatalogItem.persistableType)),
                limit: 0
            )
        )
        #expect(countLimitedToZero.rows.isEmpty)

        let countLimitedToOne = try await context.query(
            SelectQuery(
                projection: countProjection,
                source: .table(TableRef(CatalogItem.persistableType)),
                limit: 1
            )
        )
        #expect(countLimitedToOne.rows.first?.fields["total"] == .int64(3))

        let countOffsetPastAggregate = try await context.query(
            SelectQuery(
                projection: countProjection,
                source: .table(TableRef(CatalogItem.persistableType)),
                offset: 1
            )
        )
        #expect(countOffsetPastAggregate.rows.isEmpty)

        let categoryItem = ProjectionItem(
            .column(ColumnRef("category")),
            alias: "category"
        )
        let distinctResponse = try await context.query(
            SelectQuery(
                projection: .items([categoryItem]),
                source: .table(TableRef(CatalogItem.persistableType)),
                limit: 2,
                distinct: true
            )
        )
        #expect(
            Set(distinctResponse.rows.compactMap {
                $0.fields["category"]?.stringValue
            }) == Set(["books", "music"])
        )

        let distinctItemsResponse = try await context.query(
            SelectQuery(
                projection: .distinctItems([categoryItem]),
                source: .table(TableRef(CatalogItem.persistableType)),
                limit: 2
            )
        )
        #expect(
            Set(distinctItemsResponse.rows.compactMap {
                $0.fields["category"]?.stringValue
            }) == Set(["books", "music"])
        )
    }

    @Test("Canonical SELECT uses SQL three-valued and scalar expression semantics")
    func canonicalSelectUsesDatabaseExpressionSemantics() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()
        try context.insert(
            CatalogItem(id: "first", category: "books", title: "First")
        )
        try context.insert(
            CatalogItem(id: "second", category: "music", title: "Second")
        )
        try await context.save()

        let negatedNullComparison = try await context.query(
            SelectQuery(
                projection: .all,
                source: .table(TableRef(CatalogItem.persistableType)),
                filter: .not(
                    .equal(.column(ColumnRef("category")), .literal(.null))
                )
            )
        )
        #expect(negatedNullComparison.rows.isEmpty)

        let notInContainingNull = try await context.query(
            SelectQuery(
                projection: .all,
                source: .table(TableRef(CatalogItem.persistableType)),
                filter: .notInList(
                    .column(ColumnRef("category")),
                    values: [.literal(.string("other")), .literal(.null)]
                )
            )
        )
        #expect(notInContainingNull.rows.isEmpty)

        let scalarProjection = try await context.query(
            SelectQuery(
                projection: .items([
                    ProjectionItem(
                        .add(.literal(.int(2)), .literal(.int(3))),
                        alias: "sum"
                    ),
                    ProjectionItem(
                        .caseWhen(
                            cases: [
                                CaseWhenPair(
                                    condition: .equal(
                                        .column(ColumnRef("category")),
                                        .literal(.string("books"))
                                    ),
                                    result: .literal(.string("matched"))
                                )
                            ],
                            elseResult: .literal(.string("other"))
                        ),
                        alias: "classification"
                    ),
                    ProjectionItem(
                        .function(
                            FunctionCall(
                                name: "LOWER",
                                arguments: [.column(ColumnRef("title"))]
                            )
                        ),
                        alias: "lowerTitle"
                    )
                ]),
                source: .table(TableRef(CatalogItem.persistableType)),
                filter: .equal(
                    .column(ColumnRef("id")),
                    .literal(.string("first"))
                )
            )
        )
        #expect(scalarProjection.rows.first?.fields["sum"] == .int64(5))
        #expect(
            scalarProjection.rows.first?.fields["classification"]
                == .string("matched")
        )
        #expect(
            scalarProjection.rows.first?.fields["lowerTitle"]
                == .string("first")
        )

        do {
            _ = try await context.query(
                SelectQuery(
                    projection: .items([
                        ProjectionItem(
                            .divide(.literal(.int(1)), .literal(.int(0))),
                            alias: "invalid"
                        )
                    ]),
                    source: .table(TableRef(CatalogItem.persistableType)),
                    limit: 1
                )
            )
            Issue.record("Division by zero must surface as a typed read error")
        } catch CanonicalReadError.expressionEvaluation(.divisionByZero) {
            // Expected.
        }
    }

    @Test("Caller-owned reads execute CTEs on the supplied transaction")
    func callerOwnedTransactionExecutesCommonTableExpressions() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()
        let item = CatalogItem(
            id: "uncommitted",
            category: "books",
            title: "Uncommitted"
        )

        let cte = NamedSubquery(
            name: "selected_items",
            columns: ["identifier", "category", "title"],
            query: SelectQuery(
                projection: .items([
                    ProjectionItem(.column(ColumnRef("id"))),
                    ProjectionItem(.column(ColumnRef("category"))),
                    ProjectionItem(.column(ColumnRef("title"))),
                ]),
                source: .table(TableRef(CatalogItem.persistableType))
            )
        )
        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("selected_items")),
            subqueries: [cte]
        )
        let execution = ReadExecutionContext(
            options: .default,
            monotonicClock: scenario.container.monotonicClock
        )

        let response = try await context.withTransaction { writeTransaction in
            try await writeTransaction.save(item, precondition: .notExists)
            return try await context.withReadSnapshot(
                workMeter: execution.workMeter
            ) { snapshot in
                try await context.querySessionBound(
                    query,
                    execution: execution,
                    session: snapshot.session
                )
            }
        }

        #expect(response.rows.count == 1)
        #expect(
            response.rows[0].fields["identifier"]
                == .string("uncommitted")
        )
    }

    @Test("Canonical reads validate structure before reading data")
    func canonicalReadsValidateStructureBeforeDataAccess() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let scenario = try await makeScenario(storageEngine: storage)
        let context = scenario.container.testBaseContext()
        try context.insert(
            CatalogItem(id: "first", category: "books", title: "First")
        )
        try await context.save()
        let execution = ReadExecutionContext(
            options: .default,
            monotonicClock: scenario.container.monotonicClock,
            queryStructuralLimits: QueryStructuralLimits(
                maximumTotalNodes: 0
            )
        )
        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef(CatalogItem.persistableType))
        )
        let readsBeforeQuery = storage.control.dataReadOperationCount

        await #expect(throws: QueryStructuralValidationError.self) {
            _ = try await context.query(
                query,
                execution: execution
            )
        }
        #expect(storage.control.dataReadOperationCount == readsBeforeQuery)
        #expect(execution.workMeter.consumedRows == 0)
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
    }

    @Test("CTE cycles through expression subqueries fail explicitly")
    func commonTableExpressionCyclesThroughExpressionsFail() async throws {
        let scenario = try await makeScenario()
        let recursive = NamedSubquery(
            name: "recursive_items",
            query: SelectQuery(
                projection: .all,
                source: .values(
                    [[.string("candidate")]],
                    columnNames: ["identifier"]
                ),
                filter: .exists(
                    SelectQuery(
                        projection: .all,
                        source: .table(TableRef("recursive_items"))
                    )
                )
            )
        )

        await #expect(throws: CanonicalReadError.self) {
            _ = try await scenario.container.testBaseContext().query(
                SelectQuery(
                    projection: .all,
                    source: .table(TableRef("recursive_items")),
                    subqueries: [recursive]
                )
            )
        }
    }

    @Test("Outer joins retain the empty input schema for NULL extension")
    func outerJoinRetainsEmptyInputSchema() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()
        try context.insert(
            CatalogItem(id: "first", category: "books", title: "First")
        )
        try await context.save()

        let missingItems = SelectQuery(
            projection: .all,
            source: .table(TableRef(CatalogItem.persistableType)),
            filter: .equal(
                .column(ColumnRef("id")),
                .literal(.string("missing"))
            )
        )
        let response = try await context.query(
            SelectQuery(
                projection: .all,
                source: .join(
                    JoinClause(
                        type: .left,
                        left: .table(
                            TableRef(
                                schema: nil,
                                table: CatalogItem.persistableType,
                                alias: "available"
                            )
                        ),
                        right: .subquery(missingItems, alias: "missing"),
                        condition: .on(.literal(.bool(true)))
                    )
                )
            )
        )

        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields["available.id"] == .string("first"))
        #expect(response.rows[0].fields["missing.id"] == .null)
        #expect(response.rows[0].fields["missing.category"] == .null)
        #expect(response.rows[0].fields["missing.title"] == .null)
    }

    @Test("Set operations validate and align columns by position")
    func setOperationsValidateAndAlignColumns() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()

        let aligned = try await context.query(
            SelectQuery(
                projection: .all,
                source: .unionAll([
                    .values(
                        [[.string("first"), .int(1)]],
                        columnNames: ["identifier", "rank"]
                    ),
                    .values(
                        [[.string("second"), .int(2)]],
                        columnNames: ["other_identifier", "other_rank"]
                    ),
                ])
            )
        )
        #expect(aligned.rows.map { $0.fields["identifier"] } == [
            .string("first"),
            .string("second"),
        ])
        #expect(aligned.rows.map { $0.fields["rank"] } == [.int64(1), .int64(2)])

        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.query(
                SelectQuery(
                    projection: .all,
                    source: .unionAll([
                        .values([], columnNames: ["identifier"]),
                        .values([], columnNames: ["identifier", "rank"]),
                    ])
                )
            )
        }

        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.query(
                SelectQuery(
                    projection: .all,
                    source: .values(
                        [
                            [.string("first")],
                            [.string("second"), .int(2)],
                        ],
                        columnNames: nil
                    )
                )
            )
        }
    }

    @Test("JOIN USING never equates NULL values")
    func joinUsingDoesNotMatchNullValues() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()
        let left = SelectQuery(
            projection: .all,
            source: .values([[.null]], columnNames: ["identifier"])
        )
        let right = SelectQuery(
            projection: .all,
            source: .values([[.null]], columnNames: ["identifier"])
        )

        let response = try await context.query(
            SelectQuery(
                projection: .all,
                source: .join(
                    JoinClause(
                        type: .inner,
                        left: .subquery(left, alias: "left_values"),
                        right: .subquery(right, alias: "right_values"),
                        condition: .using(["identifier"])
                    )
                )
            )
        )
        #expect(response.rows.isEmpty)
    }

    @Test("JOIN USING preserves qualified inputs and one coalesced output")
    func joinUsingPreservesQualifiedInputs() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()
        let left = SelectQuery(
            projection: .all,
            source: .values([[.int(1)]], columnNames: ["identifier"])
        )
        let right = SelectQuery(
            projection: .all,
            source: .values([[.int(1)]], columnNames: ["identifier"])
        )

        let response = try await context.query(
            SelectQuery(
                projection: .items([
                    ProjectionItem(
                        .column(ColumnRef("identifier")),
                        alias: "coalesced_identifier"
                    ),
                    ProjectionItem(
                        .column(ColumnRef(
                            table: "left_values",
                            column: "identifier"
                        )),
                        alias: "left_identifier"
                    ),
                    ProjectionItem(
                        .column(ColumnRef(
                            table: "right_values",
                            column: "identifier"
                        )),
                        alias: "right_identifier"
                    ),
                ]),
                source: .join(
                    JoinClause(
                        type: .inner,
                        left: .subquery(left, alias: "left_values"),
                        right: .subquery(right, alias: "right_values"),
                        condition: .using(["identifier"])
                    )
                )
            )
        )

        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields["coalesced_identifier"] == .int64(1))
        #expect(response.rows[0].fields["left_identifier"] == .int64(1))
        #expect(response.rows[0].fields["right_identifier"] == .int64(1))

        let wildcard = try await context.query(
            SelectQuery(
                projection: .all,
                source: .join(
                    JoinClause(
                        type: .inner,
                        left: .subquery(left, alias: "left_values"),
                        right: .subquery(right, alias: "right_values"),
                        condition: .using(["identifier"])
                    )
                )
            )
        )
        #expect(Set(wildcard.rows[0].fields.keys) == ["identifier"])
    }

    @Test("Decimal aggregates remain exact across integer inputs")
    func decimalAggregatesRemainExact() async throws {
        let scenario = try await makeScenario()
        let response = try await scenario.container.testBaseContext().query(
            SelectQuery(
                projection: .items([
                    ProjectionItem(
                        .aggregate(
                            .sum(.column(ColumnRef("amount")), distinct: false)
                        ),
                        alias: "total"
                    ),
                    ProjectionItem(
                        .aggregate(
                            .avg(.column(ColumnRef("amount")), distinct: false)
                        ),
                        alias: "average"
                    ),
                ]),
                source: .values(
                    [
                        [
                            .decimal(
                                ExactDecimal(coefficient: 125, scale: 2)
                            )
                        ],
                        [.int(2)],
                    ],
                    columnNames: ["amount"]
                )
            )
        )

        #expect(
            response.rows[0].fields["total"]
                == .decimal(ExactDecimal(coefficient: 325, scale: 2))
        )
        #expect(
            response.rows[0].fields["average"]
                == .decimal(ExactDecimal(coefficient: 1_625, scale: 3))
        )
    }

    private func makeScenario(
        storageEngine: any StorageEngine = InMemoryEngine()
    ) async throws -> CatalogIndexScenario {
        let indexName = "catalog_items_by_category"
        let descriptor = try IndexDescriptor(
            entityName: CatalogItem.persistableType,
            declaration: .ordered(
                name: indexName,
                keys: [.ascending(CatalogItem.fields.category.identity)]
            ),
            fieldSchemas: try CatalogItem.fieldSchemas
        )
        let schema = try Schema(
            entities: [
                try Schema.Entity(
                    from: CatalogItem.self,
                    including: [descriptor]
                )
            ]
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: storageEngine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        CatalogItem.self,
                        including: [descriptor]
                    )
                ]
            ),
            security: .testingDisabled
        )
        let store = try await container.testBaseStore(for: CatalogItem.self)
        return CatalogIndexScenario(
            container: container,
            store: store,
            indexName: indexName
        )
    }
}

private struct CatalogIndexScenario: Sendable {
    let container: DBContainer
    let store: DatabaseDataStore
    let indexName: String

    func categoryQuery(_ category: String) -> Query<CatalogItem> {
        Query<CatalogItem>().where(
            CatalogItem.fields.category == category
        )
    }

    func indexEntryCount(category: String? = nil) async throws -> Int {
        let indexSubspace = try store.indexLifecycleStore.indexSubspace(
            for: indexName
        )
        let range: (begin: ByteString, end: ByteString)
        if let category {
            let categoryElement = try FieldValueTupleCodec.tupleElement(
                for: .string(category)
            )
            range = indexSubspace.subspace(categoryElement).range()
        } else {
            range = indexSubspace.range()
        }
        return try await container.engine.withTransaction { transaction in
            try await transaction.collectRange(
                begin: range.begin,
                end: range.end,
                snapshot: true
            ).count
        }
    }

    func corruptItems(ids: [String]) async throws {
        let itemSubspace = store.itemSubspace.subspace(
            CatalogItem.persistableType
        )
        try await container.engine.withTransaction { transaction in
            for id in ids {
                try transaction.setValue(
                    [0xFF],
                    for: itemSubspace.pack(Tuple(id))
                )
            }
        }
    }
}
