import QueryAST
import DatabaseKit
import Testing

@Suite("SQL mutation parser")
struct SQLParserMutationTests {
    @Test("INSERT VALUES preserves typed rows, conflict action, and RETURNING")
    func insertValuesWithConflictAndReturning() throws {
        let statement = try SQLParser().parse("""
            INSERT INTO app.Event (id, title)
            VALUES (:firstID, 'First'), (:secondID, 'Second')
            ON CONFLICT (id) DO UPDATE SET title = 'Updated' WHERE id = :firstID
            RETURNING id, title AS eventTitle
            """)

        guard case .insert(let query) = statement else {
            Issue.record("Expected INSERT statement")
            return
        }
        #expect(query.target.schema == "app")
        #expect(query.target.table == "event")
        #expect(query.columns == ["id", "title"])
        guard case .values(let rows) = query.source else {
            Issue.record("Expected INSERT VALUES source")
            return
        }
        #expect(rows.count == 2, "Parsed row count: \(rows.count)")
        #expect(rows.allSatisfy { $0.count == 2 })
        guard case .doUpdate(let assignments, let predicate) = query.onConflict else {
            Issue.record("Expected ON CONFLICT DO UPDATE")
            return
        }
        #expect(assignments.count == 1)
        #expect(predicate != nil)
        #expect(query.returning?.count == 2)
        #expect(try projectionAlias(#require(query.returning?[1])) == "eventtitle")
    }

    @Test("INSERT SELECT and DEFAULT VALUES are canonical sources")
    func insertSelectAndDefaultValues() throws {
        let selected = try SQLParser().parse(
            "INSERT INTO Archive (id) SELECT id FROM Event"
        )
        guard case .insert(let selectInsert) = selected,
              case .select(let select) = selectInsert.source else {
            Issue.record("Expected INSERT SELECT")
            return
        }
        #expect(selectInsert.columns == ["id"])
        #expect(select.source == .table(TableRef("event")))
        for sql in [
            "INSERT INTO values VALUES (:value)",
            "INSERT INTO values DEFAULT VALUES",
            "INSERT INTO values SELECT :value",
            "INSERT INTO values WITH q AS (SELECT :value AS v) SELECT v FROM q",
        ] {
            guard case .insert(let insert) = try SQLParser().parse(sql) else { Issue.record("Expected INSERT"); continue }
            #expect(insert.target.table == "values")
            #expect(insert.target.alias == nil)
        }
        guard case .insert(let aliased) = try SQLParser().parse("INSERT INTO values AS set VALUES (:value)") else {
            Issue.record("Expected INSERT with AS alias")
            return
        }
        #expect(aliased.target.alias == "set")
        #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parse("INSERT INTO items target VALUES (:value)") }

        let defaulted = try SQLParser().parse(
            "INSERT INTO Event DEFAULT VALUES"
        )
        guard case .insert(let defaultInsert) = defaulted,
              case .defaultValues = defaultInsert.source else {
            Issue.record("Expected INSERT DEFAULT VALUES")
            return
        }
    }

    @Test("UPDATE preserves assignments, FROM, predicate, and RETURNING")
    func updateStatement() throws {
        let statement = try SQLParser().parse("""
            UPDATE Event AS event
            SET title = :title, revision = revision + 1
            FROM ImportRow AS row
            WHERE event.id = row.id
            RETURNING event.id AS id
            """)

        guard case .update(let query) = statement else {
            Issue.record("Expected UPDATE statement")
            return
        }
        #expect(query.target.table == "event")
        #expect(query.target.alias == "event")
        #expect(query.assignments.map(\.columns) == [["title"], ["revision"]])
        #expect(query.from == .aliased(.table(TableRef(table: "importrow")), name: "row"))
        #expect(query.filter != nil)
        #expect(try projectionAlias(#require(query.returning?.first)) == "id")
        let scenarios: [(String, String, String?)] = [
            ("UPDATE \"items\" SET \"value\" = :value", "items", nil),
            ("UPDATE set SET value = :value", "set", nil),
            ("UPDATE values SET value = :value", "values", nil),
            ("UPDATE items \"SET\" SET value = :value", "items", "SET"),
            ("UPDATE items AS SET SET value = :value", "items", "set"),
        ]
        for (sql, table, alias) in scenarios {
            guard case .update(let update) = try SQLParser().parse(sql) else { Issue.record("Expected UPDATE"); continue }
            #expect(update.target.table == table)
            #expect(update.target.alias == alias)
            #expect(update.assignments == [Assignment(column: "value", value: .parameter(.name("value")))])
        }
        #expect(try SQLParser().parseSelect("SELECT id FROM items SET").source == .aliased(.table(TableRef(table: "items")), name: "set"))
    }

    @Test("DELETE preserves USING, predicate, and RETURNING")
    func deleteStatement() throws {
        let statement = try SQLParser().parse("""
            DELETE FROM Event AS event
            USING ExpiredEvent AS expired
            WHERE event.id = expired.id
            RETURNING event.id AS id
            """)

        guard case .delete(let query) = statement else {
            Issue.record("Expected DELETE statement")
            return
        }
        #expect(query.target.alias == "event")
        #expect(query.using == .aliased(.table(TableRef(table: "expiredevent")), name: "expired"))
        #expect(query.filter != nil)
        #expect(query.returning?.count == 1)
        guard case .delete(let aliased) = try SQLParser().parse("DELETE FROM items SET WHERE SET.id = :id RETURNING id") else {
            Issue.record("Expected DELETE with an unreserved alias")
            return
        }
        #expect(aliased.target.alias == "set")
        guard case .delete(let unaliased) = try SQLParser().parse("DELETE FROM values WHERE id = :id RETURNING id") else {
            Issue.record("Expected DELETE without alias")
            return
        }
        #expect(unaliased.target.table == "values")
        #expect(unaliased.target.alias == nil)
    }

    @Test("Malformed mutation syntax is never rounded into a successful AST")
    func malformedMutationsFail() {
        #expect(throws: SQLParser.ParseError.self) {
            _ = try SQLParser().parse("INSERT INTO Event VALUES ()")
        }
        #expect(throws: SQLParser.ParseError.self) {
            _ = try SQLParser().parse("UPDATE Event SET")
        }
        #expect(throws: SQLParser.ParseError.self) {
            _ = try SQLParser().parse("DELETE FROM Event RETURNING")
        }
        #expect(throws: SQLParser.ParseError.self) {
            _ = try SQLParser().parse(
                "INSERT INTO Event DEFAULT VALUES trailing"
            )
        }
    }

    @Test("Mutation collection construction uses the shared structural ledger")
    func mutationCollectionLimit() {
        #expect(throws: QueryStructuralValidationError.self) {
            _ = try SQLParser(
                structuralLimits: QueryStructuralLimits(
                    maximumCollectionElements: 2
                )
            ).parse(
                "INSERT INTO Event (id, title) VALUES (1, 'First')"
            )
        }
    }
}
