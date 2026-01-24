/// SubqueryTests.swift
/// Comprehensive tests for nested subquery operations
///
/// Coverage: FROM subqueries, WHERE IN/EXISTS, correlated subqueries, scalar subqueries, CTEs

import Testing
@testable import QueryAST

// MARK: - Nested Subquery Tests

@Suite("Nested Subquery Tests")
struct SubqueryTests {

    // MARK: - FROM Clause Subquery Tests

    @Test("Subquery in FROM clause")
    func testFromSubquery() throws {
        // SQL: SELECT * FROM (SELECT id, SUM(amount) as total FROM orders GROUP BY id) AS sub
        //      WHERE sub.total > 1000

        let innerQuery = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(column: "id"))),
                ProjectionItem(.aggregate(.sum(.column(ColumnRef(column: "amount")), distinct: false)), alias: "total")
            ]),
            source: .table(TableRef("orders")),
            groupBy: [.column(ColumnRef(column: "id"))]
        )

        let outerQuery = SelectQuery(
            projection: .all,
            source: .subquery(innerQuery, alias: "sub"),
            filter: .greaterThan(
                .column(ColumnRef(table: "sub", column: "total")),
                .literal(.int(1000))
            )
        )

        // Verify structure
        #expect(outerQuery.projection == .all)
        #expect(outerQuery.filter != nil)

        if case .subquery(let sub, let alias) = outerQuery.source {
            #expect(alias == "sub")
            #expect(sub.groupBy != nil)
            #expect(sub.groupBy?.count == 1)
            if case .items(let items) = sub.projection {
                #expect(items.count == 2)
                #expect(items[1].alias == "total")
            }
        } else {
            Issue.record("Expected subquery source")
        }
    }

    @Test("Nested subqueries in FROM (2 levels)")
    func testNestedFromSubqueries() throws {
        // SQL: SELECT * FROM (
        //        SELECT * FROM (
        //          SELECT * FROM base WHERE active = true
        //        ) AS level1 WHERE score > 50
        //      ) AS level2 WHERE rank < 10

        let base = SelectQuery(
            projection: .all,
            source: .table(TableRef("base")),
            filter: .equal(.column(ColumnRef(column: "active")), .literal(.bool(true)))
        )

        let level1 = SelectQuery(
            projection: .all,
            source: .subquery(base, alias: "level1"),
            filter: .greaterThan(.column(ColumnRef(column: "score")), .literal(.int(50)))
        )

        let level2 = SelectQuery(
            projection: .all,
            source: .subquery(level1, alias: "level2"),
            filter: .lessThan(.column(ColumnRef(column: "rank")), .literal(.int(10)))
        )

        // Verify 2 levels of nesting
        if case .subquery(let l1, let alias2) = level2.source {
            #expect(alias2 == "level2")
            if case .subquery(let l0, let alias1) = l1.source {
                #expect(alias1 == "level1")
                if case .table(let ref) = l0.source {
                    #expect(ref.table == "base")
                }
            } else {
                Issue.record("Expected inner subquery")
            }
        } else {
            Issue.record("Expected outer subquery")
        }
    }

    @Test("Deeply nested subqueries (3 levels)")
    func testDeeplyNestedSubqueries() throws {
        // SQL: SELECT * FROM (
        //   SELECT * FROM (
        //     SELECT * FROM (
        //       SELECT * FROM base WHERE active = true
        //     ) AS level1 WHERE score > 50
        //   ) AS level2 WHERE rank < 10
        // ) AS level3 WHERE status = 'approved'

        let base = SelectQuery(
            projection: .all,
            source: .table(TableRef("base")),
            filter: .equal(.column(ColumnRef(column: "active")), .literal(.bool(true)))
        )

        let level1 = SelectQuery(
            projection: .all,
            source: .subquery(base, alias: "level1"),
            filter: .greaterThan(.column(ColumnRef(column: "score")), .literal(.int(50)))
        )

        let level2 = SelectQuery(
            projection: .all,
            source: .subquery(level1, alias: "level2"),
            filter: .lessThan(.column(ColumnRef(column: "rank")), .literal(.int(10)))
        )

        let level3 = SelectQuery(
            projection: .all,
            source: .subquery(level2, alias: "level3"),
            filter: .equal(.column(ColumnRef(column: "status")), .literal(.string("approved")))
        )

        // Verify structure
        #expect(level3.filter != nil)

        var depth = 0
        var current: DataSource? = level3.source
        while case .subquery(let inner, _) = current {
            depth += 1
            current = inner.source
        }
        #expect(depth == 3)
    }

    // MARK: - WHERE IN Subquery Tests

    @Test("WHERE IN subquery")
    func testWhereInSubquery() throws {
        // SQL: SELECT * FROM users WHERE id IN (SELECT user_id FROM premium_members)

        let subquery = SelectQuery(
            projection: .items([ProjectionItem(.column(ColumnRef(column: "user_id")))]),
            source: .table(TableRef("premium_members"))
        )

        // Represent IN subquery using expression
        let inCondition = Expression.inSubquery(
            .column(ColumnRef(column: "id")),
            subquery: subquery
        )

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("users")),
            filter: inCondition
        )

        if case .inSubquery(let left, let sub) = query.filter {
            if case .column(let col) = left {
                #expect(col.column == "id")
            }
            if case .items(let items) = sub.projection {
                #expect(items.count == 1)
            }
        }
    }

    @Test("WHERE NOT IN subquery")
    func testWhereNotInSubquery() throws {
        // SQL: SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users)

        let subquery = SelectQuery(
            projection: .items([ProjectionItem(.column(ColumnRef(column: "user_id")))]),
            source: .table(TableRef("banned_users"))
        )

        let notInCondition = Expression.not(.inSubquery(
            .column(ColumnRef(column: "id")),
            subquery: subquery
        ))

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("users")),
            filter: notInCondition
        )

        if case .not(let inner) = query.filter {
            if case .inSubquery(_, _) = inner {
                // OK
            } else {
                Issue.record("Expected IN expression")
            }
        }
    }

    // MARK: - Correlated Subquery Tests

    @Test("Correlated EXISTS subquery")
    func testCorrelatedExistsSubquery() throws {
        // SQL: SELECT * FROM users u WHERE EXISTS (
        //   SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 100
        // )

        let correlatedSubquery = SelectQuery(
            projection: .items([ProjectionItem(.literal(.int(1)))]),
            source: .table(TableRef(table: "orders", alias: "o")),
            filter: .and(
                .equal(
                    .column(ColumnRef(table: "o", column: "user_id")),
                    .column(ColumnRef(table: "u", column: "id"))
                ),
                .greaterThan(
                    .column(ColumnRef(table: "o", column: "amount")),
                    .literal(.int(100))
                )
            )
        )

        let existsCondition = Expression.exists(correlatedSubquery)

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef(table: "users", alias: "u")),
            filter: existsCondition
        )

        if case .exists(let sub) = query.filter {
            #expect(sub.filter != nil)
            // Verify correlated reference (u.id from outer query)
            let columns = sub.referencedColumns
            #expect(columns.contains(ColumnRef(table: "u", column: "id")))
            #expect(columns.contains(ColumnRef(table: "o", column: "user_id")))
        }
    }

    @Test("Correlated NOT EXISTS subquery")
    func testCorrelatedNotExistsSubquery() throws {
        // SQL: SELECT * FROM products p WHERE NOT EXISTS (
        //   SELECT 1 FROM orders o WHERE o.product_id = p.id
        // )

        let correlatedSubquery = SelectQuery(
            projection: .items([ProjectionItem(.literal(.int(1)))]),
            source: .table(TableRef(table: "orders", alias: "o")),
            filter: .equal(
                .column(ColumnRef(table: "o", column: "product_id")),
                .column(ColumnRef(table: "p", column: "id"))
            )
        )

        let notExistsCondition = Expression.not(.exists(correlatedSubquery))

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef(table: "products", alias: "p")),
            filter: notExistsCondition
        )

        if case .not(let inner) = query.filter {
            if case .exists(_) = inner {
                // OK
            } else {
                Issue.record("Expected EXISTS")
            }
        }
    }

    // MARK: - Scalar Subquery Tests

    @Test("Scalar subquery in SELECT")
    func testScalarSubquery() throws {
        // SQL: SELECT name, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) AS order_count
        //      FROM users

        let scalarSubquery = SelectQuery(
            projection: .items([
                ProjectionItem(.aggregate(.count(nil, distinct: false)))
            ]),
            source: .table(TableRef("orders")),
            filter: .equal(
                .column(ColumnRef(column: "user_id")),
                .column(ColumnRef(table: "users", column: "id"))
            )
        )

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(column: "name"))),
                ProjectionItem(.subquery(scalarSubquery), alias: "order_count")
            ]),
            source: .table(TableRef("users"))
        )

        if case .items(let items) = query.projection {
            #expect(items.count == 2)
            #expect(items[1].alias == "order_count")
            if case .subquery(let sub) = items[1].expression {
                if case .items(let subItems) = sub.projection {
                    if case .aggregate(.count(nil, distinct: false)) = subItems[0].expression {
                        // OK
                    } else {
                        Issue.record("Expected COUNT aggregate")
                    }
                }
            }
        }
    }

    @Test("Multiple scalar subqueries")
    func testMultipleScalarSubqueries() throws {
        // SQL: SELECT
        //        (SELECT COUNT(*) FROM orders) AS total_orders,
        //        (SELECT SUM(amount) FROM orders) AS total_amount,
        //        (SELECT AVG(amount) FROM orders) AS avg_amount

        let countSubquery = SelectQuery(
            projection: .items([ProjectionItem(.aggregate(.count(nil, distinct: false)))]),
            source: .table(TableRef("orders"))
        )

        let sumSubquery = SelectQuery(
            projection: .items([ProjectionItem(.aggregate(.sum(.column(ColumnRef(column: "amount")), distinct: false)))]),
            source: .table(TableRef("orders"))
        )

        let avgSubquery = SelectQuery(
            projection: .items([ProjectionItem(.aggregate(.avg(.column(ColumnRef(column: "amount")), distinct: false)))]),
            source: .table(TableRef("orders"))
        )

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.subquery(countSubquery), alias: "total_orders"),
                ProjectionItem(.subquery(sumSubquery), alias: "total_amount"),
                ProjectionItem(.subquery(avgSubquery), alias: "avg_amount")
            ]),
            source: .table(TableRef("dual"))  // Dummy table
        )

        if case .items(let items) = query.projection {
            #expect(items.count == 3)
            for item in items {
                if case .subquery(_) = item.expression {
                    // OK
                } else {
                    Issue.record("Expected subquery expression")
                }
            }
        }
    }

    // MARK: - CTE (WITH Clause) Tests

    @Test("Simple CTE")
    func testSimpleCTE() throws {
        // SQL: WITH active_users AS (SELECT * FROM users WHERE active = true)
        //      SELECT * FROM active_users WHERE created_at > '2024-01-01'

        let cteQuery = SelectQuery(
            projection: .all,
            source: .table(TableRef("users")),
            filter: .equal(.column(ColumnRef(column: "active")), .literal(.bool(true)))
        )

        let cte = NamedSubquery(
            name: "active_users",
            query: cteQuery
        )

        let mainQuery = SelectQuery(
            projection: .all,
            source: .table(TableRef("active_users")),
            filter: .greaterThan(
                .column(ColumnRef(column: "created_at")),
                .literal(.string("2024-01-01"))
            ),
            subqueries: [cte]
        )

        #expect(mainQuery.subqueries?.count == 1)
        #expect(mainQuery.subqueries?[0].name == "active_users")
    }

    @Test("CTE with column names")
    func testCTEWithColumnNames() throws {
        // SQL: WITH stats(user_id, order_count, total) AS (
        //        SELECT user_id, COUNT(*), SUM(amount) FROM orders GROUP BY user_id
        //      )
        //      SELECT * FROM stats WHERE order_count > 5

        let cteQuery = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(column: "user_id"))),
                ProjectionItem(.aggregate(.count(nil, distinct: false))),
                ProjectionItem(.aggregate(.sum(.column(ColumnRef(column: "amount")), distinct: false)))
            ]),
            source: .table(TableRef("orders")),
            groupBy: [.column(ColumnRef(column: "user_id"))]
        )

        let cte = NamedSubquery(
            name: "stats",
            columns: ["user_id", "order_count", "total"],
            query: cteQuery
        )

        let mainQuery = SelectQuery(
            projection: .all,
            source: .table(TableRef("stats")),
            filter: .greaterThan(.column(ColumnRef(column: "order_count")), .literal(.int(5))),
            subqueries: [cte]
        )

        #expect(mainQuery.subqueries?[0].columns == ["user_id", "order_count", "total"])
    }

    @Test("Multiple CTEs")
    func testMultipleCTEs() throws {
        // SQL: WITH
        //        users_stats AS (SELECT user_id, COUNT(*) as cnt FROM orders GROUP BY user_id),
        //        high_volume AS (SELECT * FROM users_stats WHERE cnt > 10)
        //      SELECT * FROM high_volume

        let cte1 = NamedSubquery(
            name: "users_stats",
            query: SelectQuery(
                projection: .items([
                    ProjectionItem(.column(ColumnRef(column: "user_id"))),
                    ProjectionItem(.aggregate(.count(nil, distinct: false)), alias: "cnt")
                ]),
                source: .table(TableRef("orders")),
                groupBy: [.column(ColumnRef(column: "user_id"))]
            )
        )

        let cte2 = NamedSubquery(
            name: "high_volume",
            query: SelectQuery(
                projection: .all,
                source: .table(TableRef("users_stats")),
                filter: .greaterThan(.column(ColumnRef(column: "cnt")), .literal(.int(10)))
            )
        )

        let mainQuery = SelectQuery(
            projection: .all,
            source: .table(TableRef("high_volume")),
            subqueries: [cte1, cte2]
        )

        #expect(mainQuery.subqueries?.count == 2)
        #expect(mainQuery.subqueries?[0].name == "users_stats")
        #expect(mainQuery.subqueries?[1].name == "high_volume")
    }

    @Test("CTE with materialization hint")
    func testCTEMaterialization() throws {
        // SQL: WITH data AS MATERIALIZED (SELECT * FROM large_table)
        //      SELECT * FROM data

        let cte = NamedSubquery(
            name: "data",
            query: SelectQuery(
                projection: .all,
                source: .table(TableRef("large_table"))
            ),
            materialized: .materialized
        )

        #expect(cte.materialized == .materialized)

        let cteNotMaterialized = NamedSubquery(
            name: "data",
            query: SelectQuery(
                projection: .all,
                source: .table(TableRef("large_table"))
            ),
            materialized: .notMaterialized
        )

        #expect(cteNotMaterialized.materialized == .notMaterialized)
    }

    // MARK: - Subquery in JOIN Tests

    @Test("Subquery in JOIN")
    func testSubqueryInJoin() throws {
        // SQL: SELECT u.*, s.total
        //      FROM users u
        //      JOIN (SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id) s
        //      ON u.id = s.user_id

        let subquery = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(column: "user_id"))),
                ProjectionItem(.aggregate(.sum(.column(ColumnRef(column: "amount")), distinct: false)), alias: "total")
            ]),
            source: .table(TableRef("orders")),
            groupBy: [.column(ColumnRef(column: "user_id"))]
        )

        let join = JoinClause(
            type: .inner,
            left: .table(TableRef(table: "users", alias: "u")),
            right: .subquery(subquery, alias: "s"),
            condition: .on(.equal(
                .column(ColumnRef(table: "u", column: "id")),
                .column(ColumnRef(table: "s", column: "user_id"))
            ))
        )

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(table: "u", column: "name"))),
                ProjectionItem(.column(ColumnRef(table: "s", column: "total")))
            ]),
            source: .join(join)
        )

        if case .join(let j) = query.source {
            if case .subquery(let sub, let alias) = j.right {
                #expect(alias == "s")
                #expect(sub.groupBy != nil)
            } else {
                Issue.record("Expected subquery on right side")
            }
        }
    }

    // MARK: - Subquery in WHERE Comparison Tests

    @Test("Subquery in comparison (greater than)")
    func testSubqueryInComparison() throws {
        // SQL: SELECT * FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)

        let avgSubquery = SelectQuery(
            projection: .items([
                ProjectionItem(.aggregate(.avg(.column(ColumnRef(column: "amount")), distinct: false)))
            ]),
            source: .table(TableRef("orders"))
        )

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("orders")),
            filter: .greaterThan(
                .column(ColumnRef(column: "amount")),
                .subquery(avgSubquery)
            )
        )

        if case .greaterThan(let left, let right) = query.filter {
            if case .column(_) = left {
                // OK
            } else {
                Issue.record("Expected column on left")
            }
            if case .subquery(_) = right {
                // OK
            } else {
                Issue.record("Expected subquery on right")
            }
        }
    }

    @Test("Subquery with NOT EXISTS (equivalent to ALL)")
    func testSubqueryNotExists() throws {
        // SQL: SELECT * FROM products p WHERE NOT EXISTS
        //      (SELECT 1 FROM discounted_products d WHERE d.price >= p.price)
        // This is equivalent to: price > ALL (SELECT price FROM discounted_products)

        let subquery = SelectQuery(
            projection: .items([ProjectionItem(.literal(.int(1)))]),
            source: .table(TableRef("discounted_products"))
        )

        // Represent using NOT EXISTS
        let notExistsCondition = Expression.not(.exists(subquery))

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("products")),
            filter: notExistsCondition
        )

        if case .not(let inner) = query.filter {
            if case .exists(_) = inner {
                // OK - NOT EXISTS pattern
            } else {
                Issue.record("Expected EXISTS expression")
            }
        }
    }

    @Test("Subquery with EXISTS")
    func testSubqueryExists() throws {
        // SQL: SELECT * FROM products WHERE EXISTS (SELECT 1 FROM orders WHERE product_id = products.id)

        let subquery = SelectQuery(
            projection: .items([ProjectionItem(.literal(.int(1)))]),
            source: .table(TableRef("orders"))
        )

        let existsCondition = Expression.exists(subquery)

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("products")),
            filter: existsCondition
        )

        if case .exists(let sub) = query.filter {
            if case .table(_) = sub.source {
                // OK - EXISTS pattern with table source
            } else {
                Issue.record("Expected table source in EXISTS")
            }
        }
    }

    // MARK: - Parser Tests

    @Test("Parse subquery in FROM")
    func testParseFromSubquery() throws {
        let sql = "SELECT * FROM (SELECT id, name FROM users WHERE active = true) AS active_users"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        if case .subquery(_, let alias) = query.source {
            #expect(alias == "active_users")
        } else {
            Issue.record("Expected subquery source")
        }
    }

    @Test("Parse WHERE IN subquery")
    func testParseWhereInSubquery() throws {
        let sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM admins)"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        #expect(query.filter != nil)
        if case .inSubquery(_, _) = query.filter {
            // OK
        } else {
            Issue.record("Expected IN expression")
        }
    }

    @Test("Parse CTE")
    func testParseCTE() throws {
        let sql = "WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        #expect(query.subqueries?.count == 1)
        #expect(query.subqueries?[0].name == "active")
    }

    // MARK: - Edge Cases

    @Test("Empty subquery result handling")
    func testEmptySubqueryResult() throws {
        // Subquery that would return no rows
        let subquery = SelectQuery(
            projection: .items([ProjectionItem(.column(ColumnRef(column: "id")))]),
            source: .table(TableRef("empty_table")),
            filter: .literal(.bool(false))  // Always false
        )

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("users")),
            filter: .inSubquery(.column(ColumnRef(column: "id")), subquery: subquery)
        )

        // Should be valid AST even if subquery returns nothing
        #expect(query.filter != nil)
    }

    @Test("Subquery with LIMIT 1 for scalar result")
    func testSubqueryLimitOne() throws {
        // SQL: SELECT * FROM orders WHERE user_id = (SELECT id FROM users ORDER BY created_at DESC LIMIT 1)

        let scalarSubquery = SelectQuery(
            projection: .items([ProjectionItem(.column(ColumnRef(column: "id")))]),
            source: .table(TableRef("users")),
            orderBy: [SortKey(.column(ColumnRef(column: "created_at")), direction: .descending)],
            limit: 1
        )

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("orders")),
            filter: .equal(
                .column(ColumnRef(column: "user_id")),
                .subquery(scalarSubquery)
            )
        )

        if case .equal(_, let right) = query.filter {
            if case .subquery(let sub) = right {
                #expect(sub.limit == 1)
            }
        }
    }
}
