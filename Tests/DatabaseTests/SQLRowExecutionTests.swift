import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@testable import Database
@testable import DatabaseEngine

@Suite("SQL row execution")
struct SQLRowExecutionTests {
    @Persistable
    struct SQLRowItem {
        #Directory<SQLRowItem>("sql_row_items")

        var id: String
        var category: String
        var rank: Int64
    }

    @Persistable
    struct SQLRichItem {
        #Directory<SQLRichItem>("sql_rich_items")

        var id: String
        var numeric: QueryNumericValue
        var json: QueryJSONValue
        var jsonb: QueryJSONBValue
        var interval: QueryIntervalValue
        var array: QueryArray<Int32>
    }

    @Test("SELECT without FROM evaluates the singleton relation")
    func selectWithoutFrom() async throws {
        let container = try await makeContainer()
        let response = try await container.testBaseContext().executeSQL(
            "SELECT 1 + 2 AS value"
        )

        #expect(response.rows.count == 1)
        #expect(response.columns[0].type == .sql(.bigint))
        #expect(response.rows[0].values[0] == .primitive(.int64(3)))
        let cast = try await container.testBaseContext().query(SelectQuery(
            projection: .items([
                ProjectionItem(.cast(.literal(.double(0.1)), targetType: .real), alias: "rounded"),
                ProjectionItem(.cast(.literal(.int(7)), targetType: .smallint), alias: "narrow")
            ]), source: .values([[]], columnNames: [])))
        #expect(cast.columns.map(\.type) == [.sql(.real), .sql(.smallint)])
        #expect(cast.rows[0].values == [.primitive(.float32(0.1)), .primitive(.int16(7))])
    }

    @Test("SQL parameters bind before canonical row execution")
    func parametersBindBeforeExecution() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        try context.insert(SQLRowItem(id: "first", category: "books", rank: 1))
        try context.insert(SQLRowItem(id: "second", category: "music", rank: 2))
        try await context.save()

        let response = try await context.executeSQL(
            "SELECT id, rank + :increment AS adjusted FROM \"SQLRowItem\" WHERE category = :category",
            parameters: [
                QueryParameter(
                    position: 1,
                    name: "increment",
                    value: .primitive(.int64(4))
                ),
                QueryParameter(
                    position: 2,
                    name: "category",
                    value: .primitive(.string("books"))
                ),
            ]
        )

        #expect(response.rows.count == 1)
        #expect(response.rows[0].values[0] == .primitive(.string("first")))
        #expect(response.rows[0].values[1] == .primitive(.int64(5)))
    }

    @Test("SQL row execution rejects unbound parameters")
    func unboundParametersFailExplicitly() async throws {
        let container = try await makeContainer()
        await #expect(throws: QueryParameterBindingError.self) {
            _ = try await container.testBaseContext().executeSQL(
                "SELECT id FROM \"SQLRowItem\" WHERE category = :category"
            )
        }
    }

    @Test("Invalid qualifiers cannot disappear through typed pushdown")
    func invalidQualifierRemainsObservable() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()

        await #expect(throws: SQLBindingSourceFailure.self) {
            _ = try await context.executeSQL(
                "SELECT wrong.id FROM \"SQLRowItem\" item"
            )
        }
        await #expect(throws: SQLBindingSourceFailure.self) {
            _ = try await context.executeSQL(
                "SELECT (SELECT id, rank FROM \"SQLRowItem\") AS invalid FROM \"SQLRowItem\""
            )
        }
        await #expect(throws: SQLBindingSourceFailure.self) {
            _ = try await context.executeSQL(
                "SELECT * FROM \"SQLRowItem\" lhs JOIN \"SQLRowItem\" rhs ON wrong.id = rhs.id"
            )
        }
        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.query(
                SelectQuery(
                    projection: .all,
                    source: .join(
                        JoinClause(
                            type: .cross,
                            left: .table(TableRef("SQLRowItem")),
                            right: .table(
                                TableRef(
                                    table: "SQLRowItem",
                                    alias: "other"
                                )
                            ),
                            condition: .on(.bool(true))
                        )
                    )
                )
            )
        }

        try context.insert(SQLRowItem(id: "first", category: "books", rank: 1))
        try await context.save()

        await #expect(throws: SQLBindingSourceFailure.self) {
            _ = try await context.executeSQL(
                "SELECT item.id FROM \"SQLRowItem\" item WHERE wrong.category = 'books'"
            )
        }
        await #expect(throws: SQLBindingSourceFailure.self) {
            _ = try await context.executeSQL(
                "SELECT item.id FROM \"SQLRowItem\" item ORDER BY wrong.rank"
            )
        }
    }

    @Test("SQL NOT IN preserves UNKNOWN when the list contains NULL")
    func notInPreservesThreeValuedLogic() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        try context.insert(SQLRowItem(id: "first", category: "books", rank: 1))
        try context.insert(SQLRowItem(id: "second", category: "books", rank: 2))
        try await context.save()

        let response = try await context.executeSQL(
            "SELECT id FROM \"SQLRowItem\" WHERE rank NOT IN (1, NULL)"
        )

        #expect(response.rows.isEmpty)
    }

    @Test("Grouped SQL evaluates HAVING aggregates and aggregate ordering")
    func groupedAggregatesUseRelationalOrder() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        try context.insert(SQLRowItem(id: "first", category: "books", rank: 1))
        try context.insert(SQLRowItem(id: "second", category: "books", rank: 3))
        try context.insert(SQLRowItem(id: "third", category: "music", rank: 8))
        try await context.save()

        let response = try await context.executeSQL(
            "SELECT category, COUNT(*) AS item_count, SUM(rank) AS total, AVG(rank) AS average, MIN(rank) AS minimum, MAX(rank) AS maximum FROM \"SQLRowItem\" GROUP BY category HAVING COUNT(*) > 1 ORDER BY total DESC"
        )

        #expect(response.rows.count == 1)
        #expect(response.rows[0].values[0] == .primitive(.string("books")))
        #expect(response.rows[0].values[1] == .primitive(.int64(2)))
        #expect(response.rows[0].values[2] == .numeric(try QueryNumericValue(value: ExactDecimal(coefficient: 4, scale: 0), displayScale: 0)))
        #expect(response.rows[0].values[3] == .primitive(.float64(2)))
        #expect(response.rows[0].values[4] == .primitive(.int64(1)))
        #expect(response.rows[0].values[5] == .primitive(.int64(3)))
    }

    @Test("Global aggregates produce one SQL row for empty input")
    func globalAggregatesProduceEmptyInputIdentity() async throws {
        let container = try await makeContainer()
        let response = try await container.testBaseContext().executeSQL(
            "SELECT COUNT(*) AS item_count, SUM(rank) AS total, AVG(rank) AS average, MIN(rank) AS minimum, MAX(rank) AS maximum FROM \"SQLRowItem\""
        )

        #expect(response.rows.count == 1)
        #expect(response.rows[0].values[0] == .primitive(.int64(0)))
        #expect(response.rows[0].values[1] == .primitive(.null))
        #expect(response.rows[0].values[2] == .primitive(.null))
        #expect(response.rows[0].values[3] == .primitive(.null))
        #expect(response.rows[0].values[4] == .primitive(.null))
    }

    @Test("Collection aggregates preserve their declared semantics")
    func collectionAggregates() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        try context.insert(SQLRowItem(id: "first", category: "books", rank: 1))
        try context.insert(SQLRowItem(id: "second", category: "books", rank: 3))
        try await context.save()

        let response = try await context.executeSQL(
            "SELECT ARRAY_AGG(id ORDER BY rank DESC) AS ids, GROUP_CONCAT(id, '|') AS joined FROM \"SQLRowItem\""
        )

        #expect(response.rows.count == 1)
        #expect(
            response.rows[0].values[0]
                == .primitive(.array([.string("second"), .string("first")]))
        )
        guard case .primitive(.string(let joined)) = response.rows[0].values[1] else {
            Issue.record("Expected GROUP_CONCAT string output")
            return
        }
        #expect(Set(joined.split(separator: "|").map(String.init)) == [
            "first", "second",
        ])
    }

    @Test("Ungrouped columns in aggregate queries fail explicitly")
    func aggregateProjectionRejectsUngroupedColumns() async throws {
        let container = try await makeContainer()
        await #expect(throws: SQLBindingSourceFailure.self) {
            _ = try await container.testBaseContext().executeSQL(
                "SELECT category, COUNT(*) FROM \"SQLRowItem\""
            )
        }
    }

    @Test("Qualified and unqualified grouped columns share one binding")
    func groupedColumnQualificationIsEquivalent() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        try context.insert(
            SQLRowItem(id: "first", category: "books", rank: 1)
        )
        try await context.save()

        let response = try await context.executeSQL(
            "SELECT item.category, COUNT(*) AS item_count FROM \"SQLRowItem\" item GROUP BY category"
        )

        #expect(response.rows.count == 1)
        #expect(response.rows[0].values[0] == .primitive(.string("books")))
        #expect(response.rows[0].values[1] == .primitive(.int64(1)))
    }

    @Test("Grouped projection preserves duplicate labels on empty input")
    func emptyGroupedProjectionPreservesDuplicateNames() async throws {
        let container = try await makeContainer()
        let duplicate = try await container.testBaseContext().executeSQL(
            "SELECT category AS value, COUNT(*) AS value FROM \"SQLRowItem\" GROUP BY category"
        )
        #expect(duplicate.rows.isEmpty)
        #expect(duplicate.columns.map(\.name) == ["value", "value"])
        #expect(duplicate.columns.map(\.number) == [1, 2])
        await #expect(throws: SQLBindingSourceFailure.self) {
            _ = try await container.testBaseContext().executeSQL(
                "SELECT * FROM \"SQLRowItem\" GROUP BY category"
            )
        }
    }

    @Test("Grouped subqueries expose only grouped outer columns")
    func groupedSubqueriesUseGroupedOuterScope() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        try context.insert(SQLRowItem(id: "first", category: "books", rank: 1))
        try context.insert(SQLRowItem(id: "second", category: "books", rank: 3))
        try context.insert(SQLRowItem(id: "third", category: "music", rank: 8))
        try await context.save()

        let grouped = try await context.executeSQL(
            "SELECT parent.category, COUNT(*) AS item_count, (SELECT parent.category FROM \"SQLRowItem\" candidate WHERE candidate.id = 'first' LIMIT 1) AS grouped_category FROM \"SQLRowItem\" parent GROUP BY parent.category ORDER BY parent.category"
        )
        #expect(grouped.rows.count == 2)
        #expect(grouped.rows.map { $0.values[2] } == [
            .primitive(.string("books")), .primitive(.string("music")),
        ])

        let ungroupedSQL = "SELECT parent.category, COUNT(*) AS item_count, (SELECT parent.rank FROM \"SQLRowItem\" candidate WHERE candidate.id = 'first' LIMIT 1) AS arbitrary_rank FROM \"SQLRowItem\" parent GROUP BY parent.category"
        do {
            _ = try await context.executeSQL(ungroupedSQL)
            Issue.record("Expected a non-grouped correlated column failure")
        } catch let error as SQLBindingSourceFailure {
            guard case .read(.aggregateEvaluation(
                .invalidGroupedExpression(let message)
            )) = error.cause else {
                Issue.record("Unexpected grouped subquery failure: \(error)")
                return
            }
            #expect(message.contains("parent.rank"))
            let decisive = try #require(ungroupedSQL.utf8.indices.first {
                ungroupedSQL.utf8[$0...].starts(with: "parent.rank".utf8)
            })
            #expect(error.occurrence != nil)
            #expect(error.position == ungroupedSQL.utf8.distance(from: ungroupedSQL.utf8.startIndex, to: decisive))
        }

        let shadowedSQL = "SELECT parent.id, (SELECT COUNT(*) + (SELECT parent.rank FROM \"SQLRowItem\" candidate WHERE candidate.id = 'first' LIMIT 1) FROM \"SQLRowItem\" parent) AS leaked_rank FROM \"SQLRowItem\" parent"
        do {
            _ = try await context.executeSQL(shadowedSQL)
            Issue.record("Expected the current aggregate scope to shadow its ancestor")
        } catch let error as SQLBindingSourceFailure {
            guard case .read(.aggregateEvaluation(
                .invalidGroupedExpression(let message)
            )) = error.cause else {
                Issue.record("Unexpected aggregate shadowing failure: \(error)")
                return
            }
            #expect(message.contains("parent.rank"))
            let decisive = try #require(shadowedSQL.utf8.indices.first {
                shadowedSQL.utf8[$0...].starts(with: "parent.rank".utf8)
            })
            #expect(error.occurrence != nil)
            #expect(error.position == shadowedSQL.utf8.distance(from: shadowedSQL.utf8.startIndex, to: decisive))
        }
    }

    @Test("Scalar, IN, and EXISTS subqueries share correlated row scope")
    func correlatedExpressionSubqueries() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        try context.insert(SQLRowItem(id: "first", category: "books", rank: 1))
        try context.insert(SQLRowItem(id: "second", category: "books", rank: 3))
        try context.insert(SQLRowItem(id: "third", category: "music", rank: 8))
        try await context.save()

        await #expect {
            _ = try await context.executeSQL(
                "SELECT (SELECT candidate.rank FROM \"SQLRowItem\" candidate) AS invalid"
            )
        } throws: { error in
            guard case CanonicalReadError.invalidScalarSubquery(rowCount: 3, columnCount: nil) = error else {
                return false
            }
            return true
        }
        let emptyScalar = try await context.executeSQL(
            "SELECT (SELECT candidate.rank FROM \"SQLRowItem\" candidate WHERE candidate.id = 'missing') AS absent"
        )
        #expect(emptyScalar.rows[0].values[0] == .primitive(.null))

        let scalar = try await context.executeSQL(
            "SELECT parent.id, (SELECT MAX(candidate.rank) FROM \"SQLRowItem\" candidate WHERE candidate.category = parent.category) AS category_max FROM \"SQLRowItem\" parent ORDER BY parent.id"
        )
        #expect(scalar.rows.map { $0.values[1] } == [
            .primitive(.int64(3)), .primitive(.int64(3)), .primitive(.int64(8)),
        ])

        let membership = try await context.executeSQL(
            "SELECT parent.id FROM \"SQLRowItem\" parent WHERE parent.rank IN (SELECT candidate.rank FROM \"SQLRowItem\" candidate WHERE candidate.category = 'books') ORDER BY parent.id"
        )
        #expect(membership.rows.map { $0.values[0] } == [
            .primitive(.string("first")), .primitive(.string("second")),
        ])

        let unknownMembership = try await context.executeSQL(
            "SELECT parent.id FROM \"SQLRowItem\" parent WHERE parent.rank NOT IN (SELECT CASE WHEN candidate.rank = 1 THEN NULL ELSE 99 END FROM \"SQLRowItem\" candidate)"
        )
        #expect(unknownMembership.rows.isEmpty)

        let existence = try await context.executeSQL(
            "SELECT parent.id FROM \"SQLRowItem\" parent WHERE EXISTS (SELECT candidate.id FROM \"SQLRowItem\" candidate WHERE candidate.category = parent.category AND candidate.rank > parent.rank) ORDER BY parent.id"
        )
        #expect(existence.rows.map { $0.values[0] } == [.primitive(.string("first"))])

        let shadowed = try await context.executeSQL(
            "SELECT parent.id FROM \"SQLRowItem\" parent WHERE EXISTS (SELECT candidate.id FROM \"SQLRowItem\" candidate WHERE category = 'music') ORDER BY parent.id"
        )
        #expect(shadowed.rows.map { $0.values[0] } == [
            .primitive(.string("first")), .primitive(.string("second")), .primitive(.string("third")),
        ])
    }

    @Test("LATERAL evaluates its right source once per outer row")
    func lateralJoinUsesOuterRow() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        try context.insert(SQLRowItem(id: "first", category: "books", rank: 1))
        try context.insert(SQLRowItem(id: "second", category: "books", rank: 3))
        try context.insert(SQLRowItem(id: "third", category: "music", rank: 8))
        try await context.save()

        let response = try await context.executeSQL(
            "SELECT parent.id, top_item.id AS top_id FROM \"SQLRowItem\" parent JOIN LATERAL (SELECT candidate.id FROM \"SQLRowItem\" candidate WHERE candidate.category = parent.category ORDER BY candidate.rank DESC LIMIT 1) AS top_item ON TRUE ORDER BY parent.id"
        )

        #expect(response.rows.map { $0.values[1] } == [
            .primitive(.string("second")), .primitive(.string("second")), .primitive(.string("third")),
        ])

        let leftResponse = try await context.executeSQL(
            "SELECT parent.id, missing.id AS missing_id FROM \"SQLRowItem\" parent LEFT JOIN LATERAL (SELECT candidate.id FROM \"SQLRowItem\" candidate WHERE candidate.rank > parent.rank + 100) AS missing ON TRUE ORDER BY parent.id"
        )
        #expect(leftResponse.rows.map { $0.values[1] } == [
            .primitive(.null), .primitive(.null), .primitive(.null),
        ])
    }

    @Test("NATURAL JOIN coalesces wildcard columns")
    func naturalJoinCoalescesWildcardColumns() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        try context.insert(SQLRowItem(id: "first", category: "books", rank: 1))
        try context.insert(SQLRowItem(id: "second", category: "music", rank: 2))
        try await context.save()

        let response = try await context.executeSQL(
            "SELECT * FROM \"SQLRowItem\" lhs NATURAL JOIN \"SQLRowItem\" rhs ORDER BY id"
        )

        #expect(response.rows.count == 2)
        #expect(Set(response.columns.map(\.name)) == ["id", "category", "rank"])
    }

    @Test("Relational identity canonicalizes numeric representations")
    func relationalNumericIdentity() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let source = DataSource.values(
            [
                [.int(1)],
                [.uint(1)],
                [.decimal(ExactDecimal(coefficient: 10, scale: 1))],
                [.int(2)],
            ],
            columnNames: ["value"]
        )

        let distinct = try await context.query(
            SelectQuery(
                projection: .distinctItems([
                    ProjectionItem(.col("value")),
                ]),
                source: source
            )
        )
        #expect(distinct.rows.count == 2)

        let grouped = try await context.query(
            SelectQuery(
                projection: .items([
                    ProjectionItem(.col("value")),
                    ProjectionItem(
                        .aggregate(.count(nil, distinct: false)),
                        alias: "item_count"
                    ),
                ]),
                source: source,
                groupBy: [.col("value")]
            )
        )
        #expect(
            Set(grouped.rows.compactMap { row -> Int64? in
                if case .primitive(.int64(let value)) = row.values[1] { return value }; return nil
            }) == [1, 3]
        )

        let mixedNumeric = try await context.query(
            SelectQuery(
                projection: .distinctItems([
                    ProjectionItem(.col("value")),
                ]),
                source: .values(
                    [[.int(1)], [.double(1)]],
                    columnNames: ["value"]
                )
            )
        )
        #expect(mixedNumeric.rows.count == 1)

        let fractionalSource = DataSource.values(
            [
                [.decimal(ExactDecimal(coefficient: 15, scale: 1))],
                [.double(1.5)],
                [.decimal(ExactDecimal(coefficient: 16, scale: 1))],
            ],
            columnNames: ["value"]
        )
        let fractionalDistinct = try await context.query(
            SelectQuery(
                projection: .distinctItems([
                    ProjectionItem(.col("value")),
                ]),
                source: fractionalSource
            )
        )
        #expect(fractionalDistinct.rows.count == 2)

        let normalizedLargeFloat: Double = 95_367_431_640_625 * 0x1p100
        let normalizedLargeDecimal = ExactDecimal(
            coefficient: Int128(1) << 80,
            scale: -20
        )
        let normalizedLargeDistinct = try await context.query(
            SelectQuery(
                projection: .distinctItems([
                    ProjectionItem(.col("value")),
                ]),
                source: .values(
                    [
                        [.decimal(normalizedLargeDecimal)],
                        [.double(normalizedLargeFloat)],
                    ],
                    columnNames: ["value"]
                )
            )
        )
        #expect(normalizedLargeDistinct.rows.count == 1)

        let fractionalGroups = try await context.query(
            SelectQuery(
                projection: .items([
                    ProjectionItem(.col("value")),
                    ProjectionItem(
                        .aggregate(.count(nil, distinct: false)),
                        alias: "item_count"
                    ),
                ]),
                source: fractionalSource,
                groupBy: [.col("value")]
            )
        )
        #expect(
            Set(fractionalGroups.rows.compactMap { row -> Int64? in
                if case .primitive(.int64(let value)) = row.values[1] { return value }; return nil
            }) == [1, 2]
        )

        let decimalJoinInput = SelectQuery(
            projection: .all,
            source: .values(
                [[.decimal(ExactDecimal(coefficient: 15, scale: 1))]],
                columnNames: ["identifier"]
            )
        )
        let floatingJoinInput = SelectQuery(
            projection: .all,
            source: .values(
                [[.double(1.5)]],
                columnNames: ["identifier"]
            )
        )
        let joined = try await context.query(
            SelectQuery(
                projection: .all,
                source: .join(
                    JoinClause(
                        type: .inner,
                        left: .subquery(decimalJoinInput, alias: "decimal_values"),
                        right: .subquery(floatingJoinInput, alias: "float_values"),
                        condition: .using(["identifier"])
                    )
                )
            )
        )
        #expect(joined.rows.count == 1)
    }

    @Test("S01-C01 chained JOIN retains columns outside its USING pair")
    func chainedJoinKeepsUsingVisibilityLocal() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let source = "FROM (SELECT 1 AS id, 10 AS a) lhs JOIN (SELECT 1 AS id, 20 AS b) rhs USING(id) CROSS JOIN (SELECT 2 AS id, 30 AS c) third"
        let wildcard = try await context.executeSQL("SELECT * \(source)")
        #expect(wildcard.columns.map(\.name) == ["id", "a", "b", "id", "c"])
        #expect(wildcard.columns.map(\.type) == Array(repeating: .sql(.integer), count: 5))
        #expect(wildcard.rows.count == 1)
        #expect(wildcard.rows[0].values == [
            .primitive(.int32(1)), .primitive(.int32(10)), .primitive(.int32(20)),
            .primitive(.int32(2)), .primitive(.int32(30))
        ])
        let qualified = try await context.executeSQL("SELECT lhs.id, rhs.id, third.id \(source)")
        #expect(qualified.rows[0].values == [.primitive(.int32(1)), .primitive(.int32(1)), .primitive(.int32(2))])
        await #expect(throws: SQLBindingSourceFailure.self) {
            _ = try await context.executeSQL("SELECT id \(source)")
        }
    }

    @Test("S01-C01 duplicate SQL labels survive derived tables and positional set alignment")
    func duplicateSQLLabelsRemainPositional() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let derived = try await context.executeSQL("SELECT * FROM (SELECT 1 AS same, 2 AS same) nested")
        #expect(derived.columns.map(\.name) == ["same", "same"])
        #expect(derived.rows.count == 1)
        #expect(derived.columns.map(\.type) == [.sql(.integer), .sql(.integer)])
        #expect(derived.rows[0].values == [.primitive(.int32(1)), .primitive(.int32(2))])
        #expect(throws: QueryRowCodecError.duplicateColumn("same")) {
            try derived.rows[0].namedValues(columns: derived.columns)
        }
        await #expect(throws: SQLBindingSourceFailure.self) {
            _ = try await context.executeSQL("SELECT nested.same FROM (SELECT 1 AS same, 2 AS same) nested")
        }
        let left = SelectQuery(projection: .items([
            ProjectionItem(.literal(.int(1)), alias: "same"),
            ProjectionItem(.literal(.int(2)), alias: "same")
        ]), source: .values([[]], columnNames: []))
        let right = SelectQuery(projection: .items([
            ProjectionItem(.literal(.int(3)), alias: "other"),
            ProjectionItem(.literal(.int(4)), alias: "other")
        ]), source: .values([[]], columnNames: []))
        let set = try await context.query(SelectQuery(projection: .all, source: .unionAll([
            .subquery(left, alias: "left_set"), .subquery(right, alias: "right_set")
        ])))
        #expect(set.columns.map(\.name) == ["same", "same"])
        #expect(set.rows.count == 2)
        #expect(set.columns.map(\.type) == [.sql(.bigint), .sql(.bigint)])
        #expect(set.rows[1].values == [.primitive(.int64(3)), .primitive(.int64(4))])
    }

    @Test("S01-C02 bare NULL uses the strong positional set type")
    func contextualNullSetColumns() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let values = try await context.query(SelectQuery(projection: .all,
            source: .values([[.null], [.int(1)]], columnNames: ["value"])))
        #expect(values.columns[0].type == .sql(.bigint))
        #expect(values.rows.map { $0.values[0] } == [.primitive(.null), .primitive(.int64(1))])

        let set = try await context.query(SelectQuery(projection: .all, source: .unionAll([
            .values([[.null]], columnNames: ["value"]),
            .values([[.int(1)]], columnNames: ["other"])
        ])))
        #expect(set.columns[0].type == .sql(.bigint))
        #expect(set.columns[0].isNullable)
        #expect(set.rows.map { $0.values[0] } == [.primitive(.null), .primitive(.int64(1))])

        func singleton(_ expression: Expression) -> SelectQuery {
            SelectQuery(projection: .items([ProjectionItem(expression, alias: "value")]),
                        source: .values([[]], columnNames: []))
        }
        let nullQuery = singleton(.literal(.null))
        let strong = DataSource.values([[.int(1)]], columnNames: ["other"])
        let weakQueries = [
            nullQuery,
            singleton(.coalesce([.literal(.null), .literal(.null)])),
            singleton(.caseWhen(cases: [
                CaseWhenPair(condition: .literal(.bool(true)), result: .literal(.null))
            ], elseResult: .literal(.null))),
            SelectQuery(projection: .all, source: .unionAll([
                .subquery(nullQuery, alias: "first_null"),
                .subquery(nullQuery, alias: "second_null")
            ]))
        ]
        var queries = weakQueries.map { weak in
            SelectQuery(projection: .all, source: .unionAll([
                .subquery(weak, alias: "weak_values"), strong
            ]))
        }
        queries.append(SelectQuery(projection: .all, source: .unionAll([
            .table(TableRef("weak")), strong
        ]), subqueries: [NamedSubquery(name: "weak", query: nullQuery)]))
        for query in queries {
            let response = try await context.query(query)
            #expect(response.columns[0].type == .sql(.bigint))
            #expect(response.columns[0].isNullable)
            #expect(response.rows.first?.values[0] == .primitive(.null))
            #expect(response.rows.last?.values[0] == .primitive(.int64(1)))
        }
    }

    @Test("S01-C02 typed NULL and rich parameters keep bound schema on an empty result")
    func typedRichParametersKeepEmptySchema() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let numeric = QueryValue.numeric(try QueryNumericValue(
            value: ExactDecimal(coefficient: 123, scale: 2), displayScale: 6))
        let parameters = [
            QueryParameter(position: 1, name: "n", value: numeric),
            QueryParameter(position: 2, name: "j", value: .primitive(.null), type: .sql(.json))
        ]
        let result = try await context.executeSQL("SELECT :n AS same, :j AS same", parameters: parameters)
        #expect(result.columns.map(\.name) == ["same", "same"])
        #expect(result.rows[0].values == [numeric, .primitive(.null)])
        #expect(result.columns[1].type == .sql(.json))
        #expect(result.columns[1].isNullable)
        let empty = try await context.executeSQL("SELECT :n AS same, :j AS same WHERE FALSE", parameters: parameters)
        #expect(empty.rows.isEmpty)
        #expect(empty.columns == result.columns)
    }

    @Test("S01-C09 persisted rich fields keep their declared values through SQL projection")
    func richModelPersistenceAndProjection() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let limits = QueryValueLimits(maximumRetainedBytes: 16_384, maximumTotalNodes: 1_024,
            maximumNestingDepth: 32, maximumCollectionElements: 256)
        let json = try QueryJSONValue(bytes: ByteString(utf8: " {\"b\":1.00, \"a\":null, \"b\":2} "), limits: limits)
        let item = try SQLRichItem(id: "rich",
            numeric: QueryNumericValue(value: ExactDecimal(coefficient: 123, scale: 2), displayScale: 6),
            json: json, jsonb: QueryJSONBValue(json: json, limits: limits),
            interval: QueryIntervalValue(period: CalendarPeriod(months: -2, days: 3),
                time: TimeSpan(seconds: -1, nanoseconds: 500_000_000)),
            array: QueryArray(arrayValue: QueryArrayValue(elementType: .integer,
                dimensions: [QueryArrayDimension(lowerBound: -2, length: 2)],
                elements: [.primitive(.int32(1)), .primitive(.int32(2))], limits: limits), limits: limits))
        try context.insert(item)
        try await context.save()
        let projection = Projection.items(["numeric", "json", "jsonb", "interval", "array"].map {
            ProjectionItem(.column(ColumnRef(column: $0)))
        })
        let source = DataSource.table(TableRef("SQLRichItem"))
        let result = try await context.query(SelectQuery(projection: projection, source: source))
        #expect(result.rows.count == 1)
        #expect(result.rows[0].values == [.numeric(item.numeric), .json(item.json), .jsonb(item.jsonb),
            .interval(item.interval), .array(item.array.arrayValue)])
        let empty = try await context.query(SelectQuery(
            projection: projection, source: source, filter: .literal(.bool(false))))
        #expect(empty.rows.isEmpty)
        #expect(empty.columns == result.columns)
        #expect(throws: QueryRowCodecError.self) {
            try result.rows[0].primitiveFields(columns: result.columns)
        }
        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.query(SelectQuery(projection: projection, source: source,
                filter: .equal(.column(ColumnRef(column: "numeric")),
                               .column(ColumnRef(column: "numeric")))))
        }
        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.query(SelectQuery(projection: .items([
                ProjectionItem(.add(.column(ColumnRef(column: "numeric")),
                                    .column(ColumnRef(column: "numeric"))))
            ]), source: source))
        }
    }

    private func makeContainer() async throws -> DBContainer {
        let schema = try Schema(
            entities: [try SQLRowItem.schemaEntity, try SQLRichItem.schemaEntity]
        )
        return try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(SQLRowItem.self),
                    try DatabaseFrameworkRuntime.entity(SQLRichItem.self)
                ]
            ),
            security: .testingDisabled
        )
    }
}
