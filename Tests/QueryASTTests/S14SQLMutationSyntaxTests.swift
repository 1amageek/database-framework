import DatabaseKit
import QueryAST
import Testing

@Suite("S14 complete SQL mutation syntax")
struct S14SQLMutationSyntaxTests {
    private func roundTrip(_ sql: String) throws -> QueryStatement {
        let statement = try SQLParser().parse(sql)
        let text = try SQLSerializer().serialize(statement)
        #expect(try SQLParser().parse(text) == statement, "Canonical SQL: \(text)")
        #expect(try SQLSerializer().serialize(SQLParser().parse(text)) == text)
        return statement
    }

    private func child(_ parsed: SQLParsedStatement, _ parent: QueryOccurrenceID,
                       _ field: QueryOccurrenceField, _ index: UInt32? = nil) throws -> QueryOccurrenceID {
        try #require(parsed.occurrences.child(of: parent, field: field, index: index))
    }

    private func text(_ source: String, _ range: SQLSourceRange) -> String {
        String(decoding: source.utf8.dropFirst(range.start).prefix(range.end - range.start), as: UTF8.self)
    }

    @Test("INSERT sources conflict targets tuple actions and transition aliases round trip")
    func insertAndConflictGrammar() throws {
        let sources = [
            "INSERT INTO app.t (a,b) VALUES (DEFAULT,1),(2,3) ON CONFLICT (b,a) WHERE b>0 DO UPDATE SET (a,b)=(EXCLUDED.a,EXCLUDED.b) WHERE t.a>0 RETURNING WITH (NEW AS n,OLD AS o) n.*,o.a",
            "INSERT INTO t DEFAULT VALUES ON CONFLICT ON CONSTRAINT \"Exact.Key\" DO NOTHING RETURNING *",
            "INSERT INTO t OVERRIDING USER VALUE SELECT a FROM source RETURNING a,a AS a",
            "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING",
        ]
        for source in sources { _ = try roundTrip(source) }
        guard case .insert(let value) = try SQLParser().parse(sources[0]) else { Issue.record("Expected INSERT"); return }
        #expect(value.returningAliases == SQLReturningAliases(old: "o", new: "n"))
        guard case .columns(let columns, let predicate) = value.conflictTarget,
              case .doUpdate(let assignments, let filter) = value.onConflict else { Issue.record("Expected separate conflict target/action"); return }
        #expect(columns == ["b", "a"])
        #expect(predicate != nil && filter != nil)
        #expect(assignments[0].columns == ["a", "b"])
    }

    @Test("UPDATE tuples DEFAULT correlated subqueries and DELETE USING retain one RHS")
    func updateDeleteGrammar() throws {
        let update = try roundTrip("UPDATE t AS dst SET (a,b)=(SELECT x,y FROM src WHERE src.id=dst.id), c=DEFAULT FROM source AS s WHERE dst.id=s.id RETURNING WITH (OLD AS prior) prior.*,NEW.a,s.id")
        guard case .update(let value) = update else { Issue.record("Expected UPDATE"); return }
        #expect(value.assignments.count == 2)
        #expect(value.assignments[0].columns == ["a", "b"])
        guard case .subquery = value.assignments[0].value else { Issue.record("Tuple RHS was flattened"); return }
        #expect(value.assignments[1].value == .defaultValue)
        _ = try roundTrip("DELETE FROM t AS dst USING source AS s WHERE dst.id=s.id RETURNING WITH (OLD AS prior,NEW AS next) prior.*,next.a,s.id")
    }

    @Test("Statement WITH preserves mixed definitions while pure SELECT retains its owner")
    func statementWithGrammar() throws {
        let sql = "WITH RECURSIVE q(a) AS MATERIALIZED (SELECT 1), w AS (DELETE FROM t RETURNING a) INSERT INTO archive SELECT a FROM w RETURNING *"
        guard case .with(let value) = try roundTrip(sql) else { Issue.record("Expected statement WITH"); return }
        #expect(value.recursive)
        #expect(value.statements.count == 2)
        #expect(value.statements[0].materialized == .materialized)
        guard case .delete = value.statements[1].statement else { Issue.record("Missing writable definition"); return }
        _ = try roundTrip("WITH w AS (UPDATE t SET a=DEFAULT) SELECT 1")
        let pure = "WITH q(update) AS (SELECT 1) SELECT update FROM q"
        guard case .select(let query) = try roundTrip(pure) else { Issue.record("Pure SELECT gained statement wrapper"); return }
        #expect(query == (try SQLParser().parseSelect(pure)))
        let parsed = try SQLParser().parseWithSource(pure)
        _ = try child(parsed, parsed.root, .cte, 0)
        #expect(parsed.occurrences.child(of: parsed.root, field: .withBody) == nil)
    }

    @Test("MERGE retains ordered match classes every action defaults and RETURNING")
    func mergeGrammar() throws {
        let statement = try roundTrip("MERGE INTO t AS dst USING source AS s ON dst.id=s.id WHEN MATCHED AND s.a>0 THEN UPDATE SET (a,b)=(s.a,s.b) WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT (id,a) OVERRIDING SYSTEM VALUE VALUES (s.id,DEFAULT) WHEN NOT MATCHED BY SOURCE AND dst.a=0 THEN DO NOTHING RETURNING WITH (OLD AS o,NEW AS n) merge_action(),o.*,n.*,s.id")
        guard case .merge(let value) = statement else { Issue.record("Expected MERGE"); return }
        #expect(value.clauses.map(\.match) == [.matched,.matched,.notMatchedByTarget,.notMatchedBySource])
        #expect(value.returning?.count == 4)
        let defaults = try roundTrip("MERGE INTO t USING source ON TRUE WHEN NOT MATCHED BY TARGET THEN INSERT DEFAULT VALUES")
        guard case .merge(let query) = defaults,
              case .insert(let columns, let values, _) = query.clauses[0].action else { Issue.record("Expected default insert"); return }
        #expect(columns == nil && values == nil)
    }

    @Test("TRUNCATE retains all tables identity reference policy and exact names")
    func truncateGrammar() throws {
        for sql in ["TRUNCATE t", "TRUNCATE TABLE app.t,\"Exact.Table\" RESTART IDENTITY CASCADE", "TRUNCATE t CONTINUE IDENTITY RESTRICT"] {
            _ = try roundTrip(sql)
        }
        guard case .truncate(let value) = try SQLParser().parse("TRUNCATE TABLE app.t,other RESTART IDENTITY CASCADE") else { Issue.record("Expected TRUNCATE"); return }
        #expect(value.tables == [TableRef(schema: "app", table: "t"), TableRef("other")])
        #expect(value.identity == .restart && value.references == .cascade)
    }

    @Test("Mutation captures retain distinct tuple conflict output and writable CTE addresses")
    func mutationSourceOccurrences() throws {
        let sql = "INSERT INTO \"é\" (a,b) VALUES ($1,$1) ON CONFLICT (a) WHERE a=$1 DO UPDATE SET (a,b)=($1,$1) WHERE a=$1 RETURNING WITH (OLD AS o,NEW AS n) o.a,n.b,$1"
        let parser = SQLParser()
        let parsed = try parser.parseWithSource(sql)
        let source = try child(parsed, parsed.root, .source)
        let row = try child(parsed, source, .valuesRow, 0)
        let first = try child(parsed, row, .valuesCell, 0)
        let second = try child(parsed, row, .valuesCell, 1)
        let conflict = try child(parsed, parsed.root, .conflictTarget)
        _ = try child(parsed, conflict, .conflictPredicate)
        let action = try child(parsed, parsed.root, .conflictAction)
        let assignment = try child(parsed, action, .assignment, 0)
        let rhs = try child(parsed, assignment, .assignmentValue)
        let tupleFirst = try child(parsed, rhs, .argument, 0)
        let tupleSecond = try child(parsed, rhs, .argument, 1)
        let output = try child(parsed, parsed.root, .returning, 2)
        _ = try child(parsed, parsed.root, .returningAlias)
        let ids = [first,second,tupleFirst,tupleSecond,output]
        #expect(Set(ids).count == 5)
        let starts = try ids.map { try #require(parsed.sourceMap[$0]).range.start }
        #expect(Set(starts).count == 5)
        for id in ids { #expect(text(sql, try #require(parsed.sourceMap[id]).range) == "$1") }
        let with = try parser.parseWithSource("WITH w AS (DELETE FROM t RETURNING a) SELECT a FROM w")
        let definition = try child(with, with.root, .cte, 0)
        let body = try child(with, definition, .cteStatement)
        _ = try child(with, body, .returning, 0)
        _ = try child(with, with.root, .withBody)
        #expect(throws: (any Error).self) { _ = try parser.parseWithSource("MERGE INTO (") }
        #expect(try parser.parseWithSource("TRUNCATE t").occurrences.count == 2)
    }

    @Test("Malformed mutations and trailing clauses fail instead of accepting prefixes")
    func malformedGrammar() throws {
        for sql in [
            "INSERT INTO t VALUES (1) ON CONFLICT DO UPDATE SET a=2",
            "INSERT INTO t VALUES (1) ON CONFLICT () DO NOTHING",
            "UPDATE t SET (a,b)=(1)", "UPDATE t SET (a,)=ROW(1,2)",
            "DELETE FROM t RETURNING WITH () *", "DELETE FROM t RETURNING WITH (OLD AS o,OLD AS p) *",
            "DELETE FROM t RETURNING WITH (NEW n) *", "DELETE FROM t RETURNING",
            "WITH w AS (DELETE FROM t RETURNING a) SELECT a FROM w junk junk",
            "MERGE INTO t USING s ON TRUE", "MERGE INTO t USING s ON TRUE WHEN MATCHED UPDATE SET a=1",
            "MERGE INTO t USING s ON TRUE WHEN NOT MATCHED THEN INSERT VALUES ()",
            "WITH w AS (TRUNCATE t) SELECT 1", "WITH w AS (SELECT 1) TRUNCATE t",
            "TRUNCATE", "TRUNCATE t AS alias", "TRUNCATE t RESTART", "TRUNCATE t CASCADE junk",
        ] {
            #expect(throws: (any Error).self) { _ = try SQLParser().parse(sql) }
        }
    }

    @Test("Complete mutation batches parser reuse and output bounds share existing admission")
    func limitsAndReuse() throws {
        let parser = SQLParser()
        let sql = "INSERT INTO t VALUES (DEFAULT) RETURNING *; UPDATE t SET a=DEFAULT; DELETE FROM t; TRUNCATE t"
        #expect(try parser.parseStatements(sql).count == 4)
        #expect(throws: (any Error).self) { _ = try SQLParser(structuralLimits: QueryStructuralLimits(maximumCollectionElements: 2)).parseStatements(sql) }
        #expect(throws: (any Error).self) { _ = try parser.parse("WITH w AS (") }
        #expect(try parser.parse("TRUNCATE t") == .truncate(TruncateQuery(tables: [TableRef("t")])))
        let statement = QueryStatement.truncate(TruncateQuery(tables: [TableRef("t")]))
        let text = try SQLSerializer().serialize(statement)
        let exact = QueryStructuralLimits(maximumRetainedBytes: UInt64(text.utf8.count))
        #expect(try SQLSerializer(limits: exact).serialize(statement) == text)
        let below = QueryStructuralLimits(maximumRetainedBytes: UInt64(text.utf8.count - 1))
        #expect(throws: SQLSerializationError.self) { _ = try SQLSerializer(limits: below).serialize(statement) }
        #expect(throws: SQLSerializationError.self) { _ = try SQLSerializer().serialize(QueryStatement.update(UpdateQuery(target: TableRef("t"), assignments: []))) }
        #expect(throws: SQLSerializationError.self) { _ = try SQLSerializer().serialize(QueryStatement.merge(MergeQuery(target: TableRef("t"), source: .values([]), condition: .bool(true), clauses: []))) }
    }
}
