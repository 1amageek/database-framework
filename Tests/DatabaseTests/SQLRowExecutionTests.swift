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

    @Test("SELECT without FROM evaluates the singleton relation")
    func selectWithoutFrom() async throws {
        let container = try await makeContainer()
        let response = try await container.testBaseContext().executeSQL(
            "SELECT 1 + 2 AS value"
        )

        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields["value"] == .int64(3))
    }

    @Test("SQL parameters bind before canonical row execution")
    func parametersBindBeforeExecution() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        try context.insert(SQLRowItem(id: "first", category: "books", rank: 1))
        try context.insert(SQLRowItem(id: "second", category: "music", rank: 2))
        try await context.save()

        let response = try await context.executeSQL(
            "SELECT id, rank + :increment AS adjusted FROM SQLRowItem WHERE category = :category",
            parameters: [
                QueryParameter(
                    position: 1,
                    name: "increment",
                    value: .int64(4)
                ),
                QueryParameter(
                    position: 2,
                    name: "category",
                    value: .string("books")
                ),
            ]
        )

        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields["id"] == .string("first"))
        #expect(response.rows[0].fields["adjusted"] == .int64(5))
    }

    @Test("SQL row execution rejects unbound parameters")
    func unboundParametersFailExplicitly() async throws {
        let container = try await makeContainer()
        await #expect(throws: QueryParameterBindingError.self) {
            _ = try await container.testBaseContext().executeSQL(
                "SELECT id FROM SQLRowItem WHERE category = :category"
            )
        }
    }

    @Test("Invalid qualifiers cannot disappear through typed pushdown")
    func invalidQualifierRemainsObservable() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()

        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.executeSQL(
                "SELECT wrong.id FROM SQLRowItem item"
            )
        }
        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.executeSQL(
                "SELECT (SELECT id, rank FROM SQLRowItem) AS invalid FROM SQLRowItem"
            )
        }
        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.executeSQL(
                "SELECT * FROM SQLRowItem lhs JOIN SQLRowItem rhs ON wrong.id = rhs.id"
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

        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.executeSQL(
                "SELECT item.id FROM SQLRowItem item WHERE wrong.category = 'books'"
            )
        }
        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.executeSQL(
                "SELECT item.id FROM SQLRowItem item ORDER BY wrong.rank"
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
            "SELECT id FROM SQLRowItem WHERE rank NOT IN (1, NULL)"
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
            "SELECT category, COUNT(*) AS item_count, SUM(rank) AS total, AVG(rank) AS average, MIN(rank) AS minimum, MAX(rank) AS maximum FROM SQLRowItem GROUP BY category HAVING COUNT(*) > 1 ORDER BY total DESC"
        )

        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields["category"] == .string("books"))
        #expect(response.rows[0].fields["item_count"] == .int64(2))
        #expect(response.rows[0].fields["total"] == .int64(4))
        #expect(response.rows[0].fields["average"] == .int64(2))
        #expect(response.rows[0].fields["minimum"] == .int64(1))
        #expect(response.rows[0].fields["maximum"] == .int64(3))
    }

    @Test("Global aggregates produce one SQL row for empty input")
    func globalAggregatesProduceEmptyInputIdentity() async throws {
        let container = try await makeContainer()
        let response = try await container.testBaseContext().executeSQL(
            "SELECT COUNT(*) AS item_count, SUM(rank) AS total, AVG(rank) AS average, MIN(rank) AS minimum, MAX(rank) AS maximum FROM SQLRowItem"
        )

        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields["item_count"] == .int64(0))
        #expect(response.rows[0].fields["total"] == .null)
        #expect(response.rows[0].fields["average"] == .null)
        #expect(response.rows[0].fields["minimum"] == .null)
        #expect(response.rows[0].fields["maximum"] == .null)
    }

    @Test("Collection aggregates preserve their declared semantics")
    func collectionAggregates() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        try context.insert(SQLRowItem(id: "first", category: "books", rank: 1))
        try context.insert(SQLRowItem(id: "second", category: "books", rank: 3))
        try await context.save()

        let response = try await context.executeSQL(
            "SELECT ARRAY_AGG(id ORDER BY rank DESC) AS ids, GROUP_CONCAT(id, '|') AS joined FROM SQLRowItem"
        )

        #expect(response.rows.count == 1)
        #expect(
            response.rows[0].fields["ids"]
                == .array([.string("second"), .string("first")])
        )
        guard let joinedValue = response.rows[0].fields["joined"],
              case .string(let joined) = joinedValue else {
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
        await #expect(throws: CanonicalReadError.self) {
            _ = try await container.testBaseContext().executeSQL(
                "SELECT category, COUNT(*) FROM SQLRowItem"
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
            "SELECT item.category, COUNT(*) AS item_count FROM SQLRowItem item GROUP BY category"
        )

        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields["category"] == .string("books"))
        #expect(response.rows[0].fields["item_count"] == .int64(1))
    }

    @Test("Grouped projection names are validated for empty input")
    func emptyGroupedProjectionRejectsDuplicateNames() async throws {
        let container = try await makeContainer()
        await #expect(throws: CanonicalReadError.self) {
            _ = try await container.testBaseContext().executeSQL(
                "SELECT category AS value, COUNT(*) AS value FROM SQLRowItem GROUP BY category"
            )
        }
        await #expect(throws: CanonicalReadError.self) {
            _ = try await container.testBaseContext().executeSQL(
                "SELECT * FROM SQLRowItem GROUP BY category"
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
            "SELECT parent.category, COUNT(*) AS item_count, (SELECT parent.category FROM SQLRowItem candidate WHERE candidate.id = 'first' LIMIT 1) AS grouped_category FROM SQLRowItem parent GROUP BY parent.category ORDER BY parent.category"
        )
        #expect(grouped.rows.count == 2)
        #expect(grouped.rows.map { $0.fields["grouped_category"] } == [
            .string("books"), .string("music"),
        ])

        do {
            _ = try await context.executeSQL(
                "SELECT parent.category, COUNT(*) AS item_count, (SELECT parent.rank FROM SQLRowItem candidate WHERE candidate.id = 'first' LIMIT 1) AS arbitrary_rank FROM SQLRowItem parent GROUP BY parent.category"
            )
            Issue.record("Expected a non-grouped correlated column failure")
        } catch let error as CanonicalReadError {
            guard case .aggregateEvaluation(
                .invalidGroupedExpression(let message)
            ) = error else {
                Issue.record("Unexpected grouped subquery failure: \(error)")
                return
            }
            #expect(message.contains("parent.rank"))
        }

        do {
            _ = try await context.executeSQL(
                "SELECT parent.id, (SELECT COUNT(*) + (SELECT parent.rank FROM SQLRowItem candidate WHERE candidate.id = 'first' LIMIT 1) FROM SQLRowItem parent) AS leaked_rank FROM SQLRowItem parent"
            )
            Issue.record("Expected the current aggregate scope to shadow its ancestor")
        } catch let error as CanonicalReadError {
            guard case .aggregateEvaluation(
                .invalidGroupedExpression(let message)
            ) = error else {
                Issue.record("Unexpected aggregate shadowing failure: \(error)")
                return
            }
            #expect(message.contains("parent.rank"))
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

        await #expect(throws: CanonicalReadError.self) {
            _ = try await context.executeSQL(
                "SELECT (SELECT candidate.rank FROM SQLRowItem candidate) AS invalid"
            )
        }
        let emptyScalar = try await context.executeSQL(
            "SELECT (SELECT candidate.rank FROM SQLRowItem candidate WHERE candidate.id = 'missing') AS absent"
        )
        #expect(emptyScalar.rows[0].fields["absent"] == .null)

        let scalar = try await context.executeSQL(
            "SELECT parent.id, (SELECT MAX(candidate.rank) FROM SQLRowItem candidate WHERE candidate.category = parent.category) AS category_max FROM SQLRowItem parent ORDER BY parent.id"
        )
        #expect(scalar.rows.map { $0.fields["category_max"] } == [
            .int64(3), .int64(3), .int64(8),
        ])

        let membership = try await context.executeSQL(
            "SELECT parent.id FROM SQLRowItem parent WHERE parent.rank IN (SELECT candidate.rank FROM SQLRowItem candidate WHERE candidate.category = 'books') ORDER BY parent.id"
        )
        #expect(membership.rows.map { $0.fields["id"] } == [
            .string("first"), .string("second"),
        ])

        let unknownMembership = try await context.executeSQL(
            "SELECT parent.id FROM SQLRowItem parent WHERE parent.rank NOT IN (SELECT CASE WHEN candidate.rank = 1 THEN NULL ELSE 99 END FROM SQLRowItem candidate)"
        )
        #expect(unknownMembership.rows.isEmpty)

        let existence = try await context.executeSQL(
            "SELECT parent.id FROM SQLRowItem parent WHERE EXISTS (SELECT candidate.id FROM SQLRowItem candidate WHERE candidate.category = parent.category AND candidate.rank > parent.rank) ORDER BY parent.id"
        )
        #expect(existence.rows.map { $0.fields["id"] } == [.string("first")])

        let shadowed = try await context.executeSQL(
            "SELECT parent.id FROM SQLRowItem parent WHERE EXISTS (SELECT candidate.id FROM SQLRowItem candidate WHERE category = 'music') ORDER BY parent.id"
        )
        #expect(shadowed.rows.map { $0.fields["id"] } == [
            .string("first"), .string("second"), .string("third"),
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
            "SELECT parent.id, top_item.id AS top_id FROM SQLRowItem parent JOIN LATERAL (SELECT candidate.id FROM SQLRowItem candidate WHERE candidate.category = parent.category ORDER BY candidate.rank DESC LIMIT 1) AS top_item ON TRUE ORDER BY parent.id"
        )

        #expect(response.rows.map { $0.fields["top_id"] } == [
            .string("second"), .string("second"), .string("third"),
        ])

        let leftResponse = try await context.executeSQL(
            "SELECT parent.id, missing.id AS missing_id FROM SQLRowItem parent LEFT JOIN LATERAL (SELECT candidate.id FROM SQLRowItem candidate WHERE candidate.rank > parent.rank + 100) AS missing ON TRUE ORDER BY parent.id"
        )
        #expect(leftResponse.rows.map { $0.fields["missing_id"] } == [
            .null, .null, .null,
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
            "SELECT * FROM SQLRowItem lhs NATURAL JOIN SQLRowItem rhs ORDER BY id"
        )

        #expect(response.rows.count == 2)
        #expect(Set(response.rows[0].fields.keys) == ["id", "category", "rank"])
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
            Set(grouped.rows.compactMap {
                $0.fields["item_count"]?.int64Value
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
            Set(fractionalGroups.rows.compactMap {
                $0.fields["item_count"]?.int64Value
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

    private func makeContainer() async throws -> DBContainer {
        let schema = try Schema(
            entities: [try SQLRowItem.schemaEntity]
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
                    try DatabaseFrameworkRuntime.entity(SQLRowItem.self)
                ]
            ),
            security: .testingDisabled
        )
    }
}
