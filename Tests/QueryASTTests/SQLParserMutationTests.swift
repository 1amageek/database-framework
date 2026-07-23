import QueryAST
import QueryIR
import Testing

@Suite("SQL mutation parser")
struct SQLParserMutationTests {
    @Test("INSERT VALUES preserves typed rows, conflict action, and RETURNING")
    func insertValuesWithConflictAndReturning() throws {
        let statement = try SQLParser().parse("""
            INSERT INTO app.Event (id, title)
            VALUES (:firstID, 'First'), (:secondID, 'Second')
            ON CONFLICT DO UPDATE SET title = 'Updated' WHERE id = :firstID
            RETURNING id, title AS eventTitle
            """)

        guard case .insert(let query) = statement else {
            Issue.record("Expected INSERT statement")
            return
        }
        #expect(query.target.schema == "app")
        #expect(query.target.table == "Event")
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
        #expect(query.returning?[1].alias == "eventTitle")
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
        #expect(select.source == .table(TableRef("Event")))

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
        #expect(query.target.table == "Event")
        #expect(query.target.alias == "event")
        #expect(query.assignments.map(\.column) == ["title", "revision"])
        #expect(query.from == .table(
            TableRef(table: "ImportRow", alias: "row")
        ))
        #expect(query.filter != nil)
        #expect(query.returning?.first?.alias == "id")
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
        #expect(query.using == .table(
            TableRef(table: "ExpiredEvent", alias: "expired")
        ))
        #expect(query.filter != nil)
        #expect(query.returning?.count == 1)
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
