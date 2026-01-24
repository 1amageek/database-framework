/// MultiJoinTests.swift
/// Comprehensive tests for multi-table JOIN operations
///
/// Coverage: 3-way JOINs, mixed JOIN types, self-joins, complex conditions, USING clause, LATERAL

import Testing
@testable import QueryAST

// MARK: - Multi-Table JOIN Tests

@Suite("Multi-Table JOIN Tests")
struct MultiJoinTests {

    // MARK: - 3-way INNER JOIN Tests

    @Test("3-way INNER JOIN with transitive keys")
    func testThreeWayInnerJoin() throws {
        // SQL: SELECT * FROM users u
        //      INNER JOIN orders o ON u.id = o.user_id
        //      INNER JOIN products p ON o.product_id = p.id
        //      WHERE u.country = 'JP'

        let users = DataSource.table(TableRef(table: "users", alias: "u"))
        let orders = DataSource.table(TableRef(table: "orders", alias: "o"))
        let products = DataSource.table(TableRef(table: "products", alias: "p"))

        // First JOIN: users INNER JOIN orders
        let firstJoin = JoinClause(
            type: .inner,
            left: users,
            right: orders,
            condition: .on(.equal(
                .column(ColumnRef(table: "u", column: "id")),
                .column(ColumnRef(table: "o", column: "user_id"))
            ))
        )

        // Second JOIN: (users JOIN orders) INNER JOIN products
        let secondJoin = JoinClause(
            type: .inner,
            left: .join(firstJoin),
            right: products,
            condition: .on(.equal(
                .column(ColumnRef(table: "o", column: "product_id")),
                .column(ColumnRef(table: "p", column: "id"))
            ))
        )

        let query = SelectQuery(
            projection: .all,
            source: .join(secondJoin),
            filter: .equal(
                .column(ColumnRef(table: "u", column: "country")),
                .literal(.string("JP"))
            )
        )

        // Verify structure
        #expect(query.projection == .all)
        #expect(query.filter != nil)

        // Verify nested JOINs
        if case .join(let outerJoin) = query.source {
            #expect(outerJoin.type == .inner)
            if case .table(let productsRef) = outerJoin.right {
                #expect(productsRef.alias == "p")
            } else {
                Issue.record("Expected products table")
            }
            if case .join(let innerJoin) = outerJoin.left {
                #expect(innerJoin.type == .inner)
            } else {
                Issue.record("Expected inner join")
            }
        } else {
            Issue.record("Expected join source")
        }

        // Verify referenced columns
        let columns = query.referencedColumns
        #expect(columns.contains(ColumnRef(table: "u", column: "id")))
        #expect(columns.contains(ColumnRef(table: "u", column: "country")))
        #expect(columns.contains(ColumnRef(table: "o", column: "user_id")))
        #expect(columns.contains(ColumnRef(table: "o", column: "product_id")))
        #expect(columns.contains(ColumnRef(table: "p", column: "id")))
    }

    @Test("4-way JOIN chain")
    func testFourWayJoinChain() throws {
        // SQL: SELECT * FROM a
        //      JOIN b ON a.id = b.a_id
        //      JOIN c ON b.id = c.b_id
        //      JOIN d ON c.id = d.c_id

        let a = DataSource.table(TableRef("a"))
        let b = DataSource.table(TableRef("b"))
        let c = DataSource.table(TableRef("c"))
        let d = DataSource.table(TableRef("d"))

        let join1 = JoinClause(
            type: .inner,
            left: a,
            right: b,
            condition: .on(.equal(
                .column(ColumnRef(table: "a", column: "id")),
                .column(ColumnRef(table: "b", column: "a_id"))
            ))
        )

        let join2 = JoinClause(
            type: .inner,
            left: .join(join1),
            right: c,
            condition: .on(.equal(
                .column(ColumnRef(table: "b", column: "id")),
                .column(ColumnRef(table: "c", column: "b_id"))
            ))
        )

        let join3 = JoinClause(
            type: .inner,
            left: .join(join2),
            right: d,
            condition: .on(.equal(
                .column(ColumnRef(table: "c", column: "id")),
                .column(ColumnRef(table: "d", column: "c_id"))
            ))
        )

        let query = SelectQuery(
            projection: .all,
            source: .join(join3)
        )

        // Verify 3 levels of nesting
        if case .join(let outer) = query.source {
            #expect(outer.type == .inner)
            if case .join(let mid) = outer.left {
                #expect(mid.type == .inner)
                if case .join(let inner) = mid.left {
                    #expect(inner.type == .inner)
                } else {
                    Issue.record("Expected innermost join")
                }
            } else {
                Issue.record("Expected middle join")
            }
        } else {
            Issue.record("Expected outer join")
        }
    }

    // MARK: - Mixed JOIN Types Tests

    @Test("INNER + LEFT JOIN combination")
    func testMixedJoinTypes() throws {
        // SQL: SELECT * FROM users u
        //      INNER JOIN orders o ON u.id = o.user_id
        //      LEFT JOIN reviews r ON o.id = r.order_id

        let users = DataSource.table(TableRef(table: "users", alias: "u"))
        let orders = DataSource.table(TableRef(table: "orders", alias: "o"))
        let reviews = DataSource.table(TableRef(table: "reviews", alias: "r"))

        let innerJoin = JoinClause(
            type: .inner,
            left: users,
            right: orders,
            condition: .on(.equal(
                .column(ColumnRef(table: "u", column: "id")),
                .column(ColumnRef(table: "o", column: "user_id"))
            ))
        )

        let leftJoin = JoinClause(
            type: .left,
            left: .join(innerJoin),
            right: reviews,
            condition: .on(.equal(
                .column(ColumnRef(table: "o", column: "id")),
                .column(ColumnRef(table: "r", column: "order_id"))
            ))
        )

        let query = SelectQuery(
            projection: .all,
            source: .join(leftJoin)
        )

        if case .join(let outer) = query.source {
            #expect(outer.type == .left)
            if case .join(let inner) = outer.left {
                #expect(inner.type == .inner)
            } else {
                Issue.record("Expected inner join")
            }
        } else {
            Issue.record("Expected outer join")
        }
    }

    @Test("LEFT + RIGHT JOIN combination")
    func testLeftRightJoinCombination() throws {
        // SQL: SELECT * FROM a
        //      LEFT JOIN b ON a.id = b.a_id
        //      RIGHT JOIN c ON b.id = c.b_id

        let a = DataSource.table(TableRef("a"))
        let b = DataSource.table(TableRef("b"))
        let c = DataSource.table(TableRef("c"))

        let leftJoin = JoinClause(
            type: .left,
            left: a,
            right: b,
            condition: .on(.equal(
                .column(ColumnRef(table: "a", column: "id")),
                .column(ColumnRef(table: "b", column: "a_id"))
            ))
        )

        let rightJoin = JoinClause(
            type: .right,
            left: .join(leftJoin),
            right: c,
            condition: .on(.equal(
                .column(ColumnRef(table: "b", column: "id")),
                .column(ColumnRef(table: "c", column: "b_id"))
            ))
        )

        let query = SelectQuery(
            projection: .all,
            source: .join(rightJoin)
        )

        if case .join(let outer) = query.source {
            #expect(outer.type == .right)
            if case .join(let inner) = outer.left {
                #expect(inner.type == .left)
            }
        }
    }

    @Test("FULL OUTER JOIN")
    func testFullOuterJoin() throws {
        // SQL: SELECT * FROM a FULL OUTER JOIN b ON a.id = b.a_id

        let join = JoinClause(
            type: .full,
            left: .table(TableRef("a")),
            right: .table(TableRef("b")),
            condition: .on(.equal(
                .column(ColumnRef(table: "a", column: "id")),
                .column(ColumnRef(table: "b", column: "a_id"))
            ))
        )

        let query = SelectQuery(
            projection: .all,
            source: .join(join)
        )

        if case .join(let j) = query.source {
            #expect(j.type == .full)
        }
    }

    @Test("CROSS JOIN")
    func testCrossJoin() throws {
        // SQL: SELECT * FROM a CROSS JOIN b

        let join = JoinClause(
            type: .cross,
            left: .table(TableRef("a")),
            right: .table(TableRef("b")),
            condition: nil
        )

        let query = SelectQuery(
            projection: .all,
            source: .join(join)
        )

        if case .join(let j) = query.source {
            #expect(j.type == .cross)
            #expect(j.condition == nil)
        }
    }

    // MARK: - Self-Join Tests

    @Test("Self-join with aliases")
    func testSelfJoin() throws {
        // SQL: SELECT e.name, m.name AS manager
        //      FROM employees e
        //      LEFT JOIN employees m ON e.manager_id = m.id

        let employees = DataSource.table(TableRef(table: "employees", alias: "e"))
        let managers = DataSource.table(TableRef(table: "employees", alias: "m"))

        let selfJoin = JoinClause(
            type: .left,
            left: employees,
            right: managers,
            condition: .on(.equal(
                .column(ColumnRef(table: "e", column: "manager_id")),
                .column(ColumnRef(table: "m", column: "id"))
            ))
        )

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(table: "e", column: "name"))),
                ProjectionItem(.column(ColumnRef(table: "m", column: "name")), alias: "manager")
            ]),
            source: .join(selfJoin)
        )

        if case .items(let items) = query.projection {
            #expect(items.count == 2)
            #expect(items[1].alias == "manager")
        }

        // Verify both sides reference same table
        if case .join(let j) = query.source {
            if case .table(let leftRef) = j.left,
               case .table(let rightRef) = j.right {
                #expect(leftRef.table == "employees")
                #expect(rightRef.table == "employees")
                #expect(leftRef.alias == "e")
                #expect(rightRef.alias == "m")
            }
        }
    }

    @Test("Recursive-like self-join (hierarchy)")
    func testHierarchySelfJoin() throws {
        // SQL: SELECT c.name AS category, p.name AS parent
        //      FROM categories c
        //      LEFT JOIN categories p ON c.parent_id = p.id
        //      LEFT JOIN categories g ON p.parent_id = g.id

        let child = DataSource.table(TableRef(table: "categories", alias: "c"))
        let parent = DataSource.table(TableRef(table: "categories", alias: "p"))
        let grandparent = DataSource.table(TableRef(table: "categories", alias: "g"))

        let join1 = JoinClause(
            type: .left,
            left: child,
            right: parent,
            condition: .on(.equal(
                .column(ColumnRef(table: "c", column: "parent_id")),
                .column(ColumnRef(table: "p", column: "id"))
            ))
        )

        let join2 = JoinClause(
            type: .left,
            left: .join(join1),
            right: grandparent,
            condition: .on(.equal(
                .column(ColumnRef(table: "p", column: "parent_id")),
                .column(ColumnRef(table: "g", column: "id"))
            ))
        )

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(table: "c", column: "name")), alias: "category"),
                ProjectionItem(.column(ColumnRef(table: "p", column: "name")), alias: "parent"),
                ProjectionItem(.column(ColumnRef(table: "g", column: "name")), alias: "grandparent")
            ]),
            source: .join(join2)
        )

        if case .items(let items) = query.projection {
            #expect(items.count == 3)
            #expect(items[0].alias == "category")
            #expect(items[1].alias == "parent")
            #expect(items[2].alias == "grandparent")
        }
    }

    // MARK: - Complex JOIN Condition Tests

    @Test("JOIN with complex ON condition")
    func testComplexJoinCondition() throws {
        // SQL: SELECT * FROM a
        //      INNER JOIN b ON a.id = b.a_id AND a.type = b.type AND a.value > b.threshold

        let condition = Expression.and(
            .and(
                .equal(
                    .column(ColumnRef(table: "a", column: "id")),
                    .column(ColumnRef(table: "b", column: "a_id"))
                ),
                .equal(
                    .column(ColumnRef(table: "a", column: "type")),
                    .column(ColumnRef(table: "b", column: "type"))
                )
            ),
            .greaterThan(
                .column(ColumnRef(table: "a", column: "value")),
                .column(ColumnRef(table: "b", column: "threshold"))
            )
        )

        let join = JoinClause(
            type: .inner,
            left: .table(TableRef("a")),
            right: .table(TableRef("b")),
            condition: .on(condition)
        )

        let query = SelectQuery(
            projection: .all,
            source: .join(join)
        )

        if case .join(let j) = query.source,
           case .on(let expr) = j.condition {
            // Verify compound condition
            if case .and(let left, let right) = expr {
                // left is AND of two conditions
                if case .and(_, _) = left {
                    // OK
                } else {
                    Issue.record("Expected nested AND")
                }
                // right is greater than
                if case .greaterThan(_, _) = right {
                    // OK
                } else {
                    Issue.record("Expected greaterThan")
                }
            } else {
                Issue.record("Expected AND expression")
            }
        }
    }

    @Test("JOIN with OR condition")
    func testJoinWithOrCondition() throws {
        // SQL: SELECT * FROM a
        //      JOIN b ON a.id = b.a_id OR a.alt_id = b.a_id

        let condition = Expression.or(
            .equal(
                .column(ColumnRef(table: "a", column: "id")),
                .column(ColumnRef(table: "b", column: "a_id"))
            ),
            .equal(
                .column(ColumnRef(table: "a", column: "alt_id")),
                .column(ColumnRef(table: "b", column: "a_id"))
            )
        )

        let join = JoinClause(
            type: .inner,
            left: .table(TableRef("a")),
            right: .table(TableRef("b")),
            condition: .on(condition)
        )

        let query = SelectQuery(
            projection: .all,
            source: .join(join)
        )

        if case .join(let j) = query.source,
           case .on(let expr) = j.condition,
           case .or(_, _) = expr {
            // OK
        } else {
            Issue.record("Expected OR condition")
        }
    }

    // MARK: - USING Clause Tests

    @Test("JOIN with USING clause")
    func testJoinUsing() throws {
        // SQL: SELECT * FROM orders JOIN customers USING (customer_id)

        let join = JoinClause(
            type: .inner,
            left: .table(TableRef("orders")),
            right: .table(TableRef("customers")),
            condition: .using(["customer_id"])
        )

        let query = SelectQuery(
            projection: .all,
            source: .join(join)
        )

        if case .join(let j) = query.source,
           case .using(let columns) = j.condition {
            #expect(columns == ["customer_id"])
        } else {
            Issue.record("Expected USING clause")
        }
    }

    @Test("JOIN with multiple USING columns")
    func testJoinUsingMultipleColumns() throws {
        // SQL: SELECT * FROM a JOIN b USING (col1, col2, col3)

        let join = JoinClause(
            type: .inner,
            left: .table(TableRef("a")),
            right: .table(TableRef("b")),
            condition: .using(["col1", "col2", "col3"])
        )

        let query = SelectQuery(
            projection: .all,
            source: .join(join)
        )

        if case .join(let j) = query.source,
           case .using(let columns) = j.condition {
            #expect(columns.count == 3)
            #expect(columns.contains("col1"))
            #expect(columns.contains("col2"))
            #expect(columns.contains("col3"))
        }
    }

    // MARK: - NATURAL JOIN Tests

    @Test("NATURAL JOIN")
    func testNaturalJoin() throws {
        // SQL: SELECT * FROM a NATURAL JOIN b

        let join = JoinClause(
            type: .natural,
            left: .table(TableRef("a")),
            right: .table(TableRef("b")),
            condition: nil
        )

        let query = SelectQuery(
            projection: .all,
            source: .join(join)
        )

        if case .join(let j) = query.source {
            #expect(j.type == .natural)
            #expect(j.condition == nil)
        }
    }

    @Test("NATURAL LEFT JOIN")
    func testNaturalLeftJoin() throws {
        // SQL: SELECT * FROM a NATURAL LEFT JOIN b

        let join = JoinClause(
            type: .naturalLeft,
            left: .table(TableRef("a")),
            right: .table(TableRef("b")),
            condition: nil
        )

        let query = SelectQuery(
            projection: .all,
            source: .join(join)
        )

        if case .join(let j) = query.source {
            #expect(j.type == .naturalLeft)
        }
    }

    // MARK: - LATERAL JOIN Tests

    @Test("LATERAL JOIN with correlated subquery")
    func testLateralJoin() throws {
        // SQL: SELECT * FROM users u,
        //      LATERAL (SELECT * FROM orders o WHERE o.user_id = u.id ORDER BY created_at DESC LIMIT 3) AS recent_orders

        let correlatedSubquery = SelectQuery(
            projection: .all,
            source: .table(TableRef(table: "orders", alias: "o")),
            filter: .equal(
                .column(ColumnRef(table: "o", column: "user_id")),
                .column(ColumnRef(table: "u", column: "id"))
            ),
            orderBy: [SortKey(.column(ColumnRef(column: "created_at")), direction: .descending)],
            limit: 3
        )

        let lateralJoin = JoinClause(
            type: .lateral,
            left: .table(TableRef(table: "users", alias: "u")),
            right: .subquery(correlatedSubquery, alias: "recent_orders"),
            condition: nil
        )

        let query = SelectQuery(
            projection: .all,
            source: .join(lateralJoin)
        )

        if case .join(let j) = query.source {
            #expect(j.type == .lateral)
            if case .subquery(let sub, let alias) = j.right {
                #expect(alias == "recent_orders")
                #expect(sub.limit == 3)
            } else {
                Issue.record("Expected subquery")
            }
        }
    }

    @Test("LEFT LATERAL JOIN")
    func testLeftLateralJoin() throws {
        // SQL: SELECT * FROM users u
        //      LEFT JOIN LATERAL (SELECT COUNT(*) FROM orders WHERE user_id = u.id) AS order_count ON true

        let lateralJoin = JoinClause(
            type: .leftLateral,
            left: .table(TableRef(table: "users", alias: "u")),
            right: .subquery(
                SelectQuery(
                    projection: .items([
                        ProjectionItem(.aggregate(.count(nil, distinct: false)), alias: "cnt")
                    ]),
                    source: .table(TableRef("orders")),
                    filter: .equal(
                        .column(ColumnRef(column: "user_id")),
                        .column(ColumnRef(table: "u", column: "id"))
                    )
                ),
                alias: "order_count"
            ),
            condition: .on(.literal(.bool(true)))
        )

        let query = SelectQuery(
            projection: .all,
            source: .join(lateralJoin)
        )

        if case .join(let j) = query.source {
            #expect(j.type == .leftLateral)
        }
    }

    // MARK: - Parser Tests for JOINs

    @Test("Parse 3-way JOIN")
    func testParseThreeWayJoin() throws {
        let sql = "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id INNER JOIN products ON orders.product_id = products.id"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        // Should have nested JOIN structure
        if case .join(let outer) = query.source {
            #expect(outer.type == .inner)
            if case .join(let inner) = outer.left {
                #expect(inner.type == .inner)
            }
        }
    }

    @Test("Parse mixed JOIN types")
    func testParseMixedJoins() throws {
        let sql = "SELECT * FROM a INNER JOIN b ON a.id = b.a_id LEFT JOIN c ON b.id = c.b_id"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        if case .join(let outer) = query.source {
            #expect(outer.type == .left)
            if case .join(let inner) = outer.left {
                #expect(inner.type == .inner)
            }
        }
    }

    @Test("Parse JOIN with USING")
    func testParseJoinUsing() throws {
        let sql = "SELECT * FROM orders JOIN customers USING (customer_id)"
        let parser = SQLParser()
        let query = try parser.parseSelect(sql)

        if case .join(let j) = query.source {
            if case .using(let cols) = j.condition {
                #expect(cols == ["customer_id"])
            } else {
                Issue.record("Expected USING condition")
            }
        }
    }

    // MARK: - Edge Cases

    @Test("Empty alias handling")
    func testEmptyAliasHandling() throws {
        let ref = TableRef(table: "users", alias: nil)
        #expect(ref.effectiveName == "users")

        let refWithAlias = TableRef(table: "users", alias: "u")
        #expect(refWithAlias.effectiveName == "u")
    }

    @Test("Schema-qualified table in JOIN")
    func testSchemaQualifiedJoin() throws {
        let join = JoinClause(
            type: .inner,
            left: .table(TableRef(schema: "public", table: "users")),
            right: .table(TableRef(schema: "audit", table: "user_logs")),
            condition: .on(.equal(
                .column(ColumnRef(table: "users", column: "id")),
                .column(ColumnRef(table: "user_logs", column: "user_id"))
            ))
        )

        let query = SelectQuery(
            projection: .all,
            source: .join(join)
        )

        if case .join(let j) = query.source {
            if case .table(let left) = j.left {
                #expect(left.schema == "public")
                // description now returns properly quoted SQL identifiers
                #expect(left.description == "\"public\".\"users\"")
            }
            if case .table(let right) = j.right {
                #expect(right.schema == "audit")
            }
        }
    }
}
