import DatabaseKit
import DatabaseTypes
import QueryAST
import Testing

@Suite("S16 operational SQL block syntax")
struct S16SQLBlockSyntaxTests {
    @Test("Transaction spellings normalize to a single request envelope")
    func transactionEnvelope() throws {
        let block = try SQLParser().parseBlock("START TRANSACTION; SAVEPOINT \"é\"; SELECT 1; ROLLBACK TO SAVEPOINT \"é\"; RELEASE SAVEPOINT \"é\"; END WORK;")
        #expect(block.steps.first == .transaction(.begin))
        #expect(block.steps.last == .transaction(.commit))
        #expect(block.steps[3] == .rollbackTo("é"))
        try roundTrip("BEGIN; SELECT 1; ABORT")
        try roundTrip("COMMIT")
        for sql in ["BEGIN", "SELECT 1; BEGIN; COMMIT", "BEGIN; COMMIT; SELECT 2", "BEGIN; BEGIN; COMMIT", "COMMIT AND CHAIN", "ROLLBACK AND NO CHAIN", "BEGIN; COMMIT; BEGIN; COMMIT"] {
            #expect(throws: (any Error).self) { _ = try SQLParser().parseBlock(sql) }
        }
    }

    @Test("Constraint control and request settings retain scope and constants")
    func requestSettings() throws {
        for sql in [
            "SET CONSTRAINTS ALL DEFERRED; SET CONSTRAINTS \"A\", b IMMEDIATE",
            "SET LOCAL search_path TO app, \"Case\", '$user'; SHOW search_path; RESET SESSION search_path",
            "SET SESSION TimeZone = 'Asia/Tokyo'; SET LOCAL statement_timeout TO 250; SHOW ALL; RESET ALL",
            "SET TIME ZONE 'UTC'; SET search_path TO DEFAULT; SET LOCAL TimeZone TO DEFAULT",
            "SET search_path TO 'app, public'; SET search_path TO \"a,b\", \"x.y\", \"a\"\"b\"",
        ] { try roundTrip(sql) }
        let block = try SQLParser().parseBlock("SET CONSTRAINTS ALL DEFERRED")
        #expect(block.steps == [.setConstraints(try SQLSetConstraints(selection: .all, deferred: true))])
        for sql in ["SET unknown TO 1", "SHOW unknown", "SET LOCAL TimeZone TO", "SET CONSTRAINTS ALL", "SET search_path TO a,", "SET statement_timeout TO :timeout", "SET TimeZone TO CAST(:zone AS TEXT)"] {
            #expect(throws: (any Error).self) { _ = try SQLParser().parseBlock(sql) }
        }
        #expect(throws: (any Error).self) { _ = try SQLParser().parse("SET CONSTRAINTS ALL DEFERRED") }
    }

    @Test("Prepared ordinary statements and expression arguments use the shared grammar")
    func preparedStatements() throws {
        for sql in [
            "PREPARE \"find\" (INTEGER, TEXT) AS SELECT $1, $2; EXECUTE \"find\" (1 + 2, 'value'); DEALLOCATE PREPARE \"find\"",
            "PREPARE change AS UPDATE t SET n = $1 RETURNING n; EXECUTE change (4); DEALLOCATE ALL",
            "PREPARE add_row AS INSERT INTO t (n) VALUES ($1); EXECUTE add_row (2)",
        ] { try roundTrip(sql) }
        for sql in ["PREPARE p AS BEGIN", "PREPARE p AS CREATE TABLE t (n INT)", "EXECUTE p (1,)", "DEALLOCATE ALL extra"] {
            #expect(throws: (any Error).self) { _ = try SQLParser().parseBlock(sql) }
        }
    }

    @Test("EXPLAIN supports actual ordinary statements and explicit text or JSON options")
    func explainOptions() throws {
        for sql in ["EXPLAIN SELECT 1", "EXPLAIN ANALYZE VERBOSE SELECT 1", "EXPLAIN (ANALYZE FALSE, VERBOSE TRUE, FORMAT JSON) DELETE FROM t RETURNING n"] {
            try roundTrip(sql)
        }
        for sql in ["EXPLAIN (FORMAT XML) SELECT 1", "EXPLAIN (ANALYZE, ANALYZE) SELECT 1", "EXPLAIN (COSTS TRUE) SELECT 1", "EXPLAIN BEGIN"] {
            #expect(throws: (any Error).self) { _ = try SQLParser().parseBlock(sql) }
        }
    }

    @Test("COPY handles raw payloads without SQL lexing and preserves their positions")
    func inlineCopy() throws {
        let body = "a;'unterminated/*\t値\n"
        let prefix = "SELECT 'é'; COPY app.t (a, b) FROM STDIN WITH (FORMAT TEXT);\n"
        let sql = prefix + body + "\\.\nSELECT 3"
        let parsed = try SQLParser().parseBlockWithSource(sql)
        #expect(parsed.block.steps.count == 3)
        guard case .copy(let copy) = parsed.block.steps[1] else { Issue.record("Expected COPY"); return }
        #expect(copy.data == ByteString(utf8: body))
        let data = try #require(copy.data)
        #expect(data.retainedByteCount == body.utf8.count)
        #expect(data.isStorageSelfContained)
        #expect(parsed.copyDataRanges[1] == SQLSourceRange(start: prefix.utf8.count, end: prefix.utf8.count + body.utf8.count))
        #expect(parsed.roots[0] != nil)
        #expect(parsed.roots[1] == nil)
        #expect(parsed.roots[2] != nil)
        try roundTrip(sql)
        try roundTrip("COPY t FROM STDIN;\n\\.\n")
        try roundTrip("COPY t FROM STDIN;\r\n1\r\n\\.\r\nSHOW ALL")
        try roundTrip("COPY t FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER '|', NULL 'nil', QUOTE '\"', ESCAPE '\"');\na|b\n\\.\n")
        try roundTrip("COPY (SELECT n FROM t) TO STDOUT WITH (FORMAT CSV, HEADER)")
        for sql in ["COPY t FROM STDIN", "COPY t FROM STDIN;\nrow\n", "COPY t TO '/tmp/data'", "COPY t FROM PROGRAM 'cat'", "COPY t TO STDOUT WITH (FORMAT BINARY)", "COPY (SELECT 1) FROM STDIN;\n\\.\n", "COPY t TO STDOUT WITH (FORMAT TEXT, HEADER TRUE)"] {
            #expect(throws: (any Error).self) { _ = try SQLParser().parseBlock(sql) }
        }
    }

    @Test("Block source roots retain absolute offsets across control and wrapper steps")
    func sourceCorrespondenceAndReuse() throws {
        let parser = SQLParser()
        let sql = "SHOW ALL; PREPARE p (TEXT) AS SELECT :same; EXPLAIN SELECT :same; COPY (SELECT :same) TO STDOUT"
        let parsed = try parser.parseBlockWithSource(sql)
        #expect(parsed.roots.count == 4)
        #expect(parsed.roots[0] == nil)
        for index in 1..<4 {
            let root = try #require(parsed.roots[index])
            let projection = try #require(parsed.occurrences.child(of: root, field: .projection, index: 0))
            let range = try #require(parsed.sourceMap[projection]?.range)
            #expect(String(decoding: Array(sql.utf8)[range.start..<range.end], as: UTF8.self) == ":same")
        }
        _ = try parser.parseBlock("SET TimeZone TO 'UTC'; SELECT 2")
        #expect(parsed.roots[1] != parsed.roots[2])
        #expect(try parser.parse("SELECT 1") == SQLParser().parse("SELECT 1"))
    }

    @Test("Block limits are cumulative across steps and COPY bodies")
    func cumulativeLimits() throws {
        let tokens = SQLParser(structuralLimits: QueryStructuralLimits(maximumInputTokens: 3))
        _ = try tokens.parseBlock("SHOW ALL")
        #expect(throws: QueryStructuralValidationError.self) { _ = try tokens.parseBlock("SHOW ALL; SHOW ALL") }
        let nodes = SQLParser(structuralLimits: QueryStructuralLimits(maximumTotalNodes: 3))
        _ = try nodes.parseBlock("SHOW ALL; SHOW ALL")
        #expect(throws: QueryStructuralValidationError.self) { _ = try nodes.parseBlock("SHOW ALL; SHOW ALL; SHOW ALL") }
        let source = "COPY t FROM STDIN;\n" + String(repeating: "x", count: 128) + "\n\\.\n"
        let bytes = SQLParser(structuralLimits: QueryStructuralLimits(maximumRetainedBytes: UInt64(source.utf8.count - 1)))
        #expect(throws: QueryStructuralValidationError.self) { _ = try bytes.parseBlock(source) }
        let writer = SQLSerializer(limits: QueryStructuralLimits(maximumRetainedBytes: 15))
        let block = try SQLParser().parseBlock("SHOW ALL; SHOW ALL")
        #expect(throws: SQLSerializationError.self) { _ = try writer.serialize(block) }
    }

    private func roundTrip(_ sql: String) throws {
        let block = try SQLParser().parseBlock(sql)
        let emitted = try SQLSerializer().serialize(block)
        #expect(try SQLParser().parseBlock(emitted) == block)
    }
}
