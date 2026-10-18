import DatabaseKit
import DatabaseTypes
import StorageKit
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Bounded request-contained COPY framing", .timeLimit(.minutes(1)))
struct SQLCopyCodecTests {
    @Test("TEXT raw NULL, empty, escaped markers and controls stay distinct")
    func textFields() throws {
        let meter = makeMeter()
        var decoder = try SQLCopyDecoder(bytes: ByteString(utf8: "\\N\t\t\\\\N\tline\\nnext\t\\141\\x62\r\n"),
            options: .default, workMeter: meter)
        guard let row = try decoder.next(expectedColumnCount: 5) else { Issue.record("Expected a row"); return }
        #expect(row.fields == [nil, "", "\\N", "line\nnext", "ab"])
        if let _ = try decoder.next(expectedColumnCount: 5) { Issue.record("Expected end of input") }
    }

    @Test("CSV quoted NULL marker, empty string, multiline and doubled quote round trip")
    func csvFields() throws {
        let options = try SQLCopyOptions(format: .csv)
        let meter = makeMeter()
        let input = ",\"\",\"line\nnext\",\"a\"\"b\"\r\n"
        var decoder = try SQLCopyDecoder(bytes: ByteString(utf8: input), options: options, workMeter: meter)
        guard let row = try decoder.next(expectedColumnCount: 4) else { Issue.record("Expected a row"); return }
        #expect(row.fields == [nil, "", "line\nnext", "a\"b"])
        var encoder = try SQLCopyEncoder(options: options, workMeter: meter)
        for index in row.fields.indices {
            if index > 0 { try encoder.separator() }
            if let text = row.fields[index] { try encoder.field(text[...]) } else { try encoder.null() }
        }
        try encoder.endRecord()
        let result = encoder.finish(affectedRows: 1)
        #expect(result.0.bytes == ByteString(utf8: ",\"\",\"line\nnext\",\"a\"\"b\"\n"))
        #expect(result.0.affectedRows == 1)
    }

    @Test("Custom CSV delimiter, quote, escape and NULL preserve the ordered fields")
    func customCSV() throws {
        let options = try SQLCopyOptions(format: .csv, delimiter: "|", null: "NULL", header: true,
            quote: "'", escape: "\\")
        let meter = makeMeter()
        var decoder = try SQLCopyDecoder(bytes: ByteString(utf8: "first|second|third\r\nNULL|'NULL'|'a\\'b|c'\r\n"),
            options: options, workMeter: meter)
        guard let header = try decoder.next(expectedColumnCount: 3) else { Issue.record("Expected header"); return }
        #expect(header.fields == ["first", "second", "third"])
        guard let row = try decoder.next(expectedColumnCount: 3) else { Issue.record("Expected row"); return }
        #expect(row.fields == [nil, "NULL", "a'b|c"])
        var encoder = try SQLCopyEncoder(options: options, workMeter: meter)
        try encoder.field("NULL"[...]); try encoder.separator(); try encoder.field("a'b|c"[...]); try encoder.endRecord()
        let result = encoder.finish(affectedRows: 1)
        #expect(result.0.bytes == ByteString(utf8: "'NULL'|'a\\'b|c'\n"))
    }

    @Test("Malformed late row is reported after an independently decoded good row")
    func malformedLateRow() throws {
        var decoder = try SQLCopyDecoder(bytes: ByteString(utf8: "good,value\nlate,\"unfinished"),
            options: SQLCopyOptions(format: .csv), workMeter: makeMeter())
        guard let row = try decoder.next(expectedColumnCount: 2) else { Issue.record("Expected good row"); return }
        #expect(row.fields == ["good", "value"])
        #expect(throws: SQLCopyExecutionError.malformedInput(record: 2, field: 2)) {
            _ = try decoder.next(expectedColumnCount: 2)
        }
    }

    @Test("Malformed quoting, CRLF, escapes, UTF-8 and cardinality fail explicitly")
    func malformedInput() throws {
        for input in ["\"unterminated", "un\"closed\n"] {
            var decoder = try SQLCopyDecoder(bytes: ByteString(utf8: input), options: SQLCopyOptions(format: .csv), workMeter: makeMeter())
            #expect(throws: SQLCopyExecutionError.self) { _ = try decoder.next(expectedColumnCount: 1) }
        }
        for input in ["unfinished\\", "\\000\n"] {
            var decoder = try SQLCopyDecoder(bytes: ByteString(utf8: input), options: .default, workMeter: makeMeter())
            #expect(throws: SQLCopyExecutionError.self) { _ = try decoder.next(expectedColumnCount: 1) }
        }
        var mixed = try SQLCopyDecoder(bytes: ByteString(utf8: "a\rb\n"), options: .default, workMeter: makeMeter())
        _ = try mixed.next(expectedColumnCount: 1)
        #expect(throws: SQLCopyExecutionError.self) { _ = try mixed.next(expectedColumnCount: 1) }
        var fragments = try SQLCopyDecoder(bytes: ByteString(utf8: "pre\"quoted\"post\n"),
            options: SQLCopyOptions(format: .csv), workMeter: makeMeter())
        guard let fragment = try fragments.next(expectedColumnCount: 1) else { Issue.record("Expected quoted fragment"); return }
        #expect(fragment.fields == ["prequotedpost"])
        var invalidUTF8 = try SQLCopyDecoder(bytes: ByteString([255, 10]), options: .default, workMeter: makeMeter())
        #expect(throws: SQLCopyExecutionError.invalidUTF8(record: 1, field: 1)) { _ = try invalidUTF8.next(expectedColumnCount: 1) }
        var cardinality = try SQLCopyDecoder(bytes: ByteString(utf8: "a,b\n"), options: SQLCopyOptions(format: .csv), workMeter: makeMeter())
        #expect(throws: SQLCopyExecutionError.columnCount(record: 1, expected: 1, actual: 2)) {
            _ = try cardinality.next(expectedColumnCount: 1)
        }
    }

    @Test("Input admission measures the complete retained owner rather than the visible slice")
    func retainedInputBound() throws {
        let bytes = ByteString(utf8: "a\nretained trailing bytes")
        #expect(throws: DatabaseWorkLimitError.self) {
            _ = try SQLCopyDecoder(bytes: bytes[0..<2], options: .default,
                workMeter: makeMeter(bytes: UInt64(bytes.count - 1)))
        }
        let meter = makeMeter(bytes: UInt64(bytes.count))
        var decoder = try SQLCopyDecoder(bytes: bytes[0..<2], options: .default, workMeter: meter)
        #expect(meter.retainedIntermediateBytes == UInt64(bytes.count))
        #expect(throws: DatabaseWorkLimitError.self) { _ = try decoder.next(expectedColumnCount: 1) }
    }

    @Test("Exact and one-past row, work and output byte budgets distinguish admission")
    func resourceBounds() throws {
        func decode(using meter: DatabaseWorkMeter) throws {
            var decoder = try SQLCopyDecoder(bytes: ByteString(utf8: "some\\ntext\n"), options: .default, workMeter: meter)
            guard let row = try decoder.next(expectedColumnCount: 1) else { Issue.record("Expected bounded row"); return }
            #expect(row.fields == ["some\ntext"])
        }
        let measured = makeMeter(rows: 1)
        try decode(using: measured)
        try decode(using: makeMeter(bytes: measured.peakIntermediateBytes, rows: 1, work: measured.consumedWorkUnits))
        #expect(throws: DatabaseWorkLimitError.self) {
            try decode(using: makeMeter(bytes: measured.peakIntermediateBytes - 1))
        }
        #expect(throws: DatabaseWorkLimitError.self) {
            try decode(using: makeMeter(work: measured.consumedWorkUnits - 1))
        }
        var rows = try SQLCopyDecoder(bytes: ByteString(utf8: "a\n"), options: .default,
            workMeter: makeMeter(rows: 0))
        #expect(throws: DatabaseWorkLimitError.self) { _ = try rows.next(expectedColumnCount: 1) }
        var work = try SQLCopyDecoder(bytes: ByteString(utf8: "a\n"), options: .default,
            workMeter: makeMeter(work: 0))
        #expect(throws: DatabaseWorkLimitError.self) { _ = try work.next(expectedColumnCount: 1) }
        var encoder = try SQLCopyEncoder(options: .default, workMeter: makeMeter(bytes: 64))
        #expect(throws: DatabaseWorkLimitError.self) { try encoder.field(String(repeating: "a", count: 128)[...]) }
    }

    @Test("Cancellation is checked before reading a row")
    func cancellation() async throws {
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            var decoder = try SQLCopyDecoder(bytes: ByteString(utf8: "a\n"), options: .default, workMeter: makeMeter())
            _ = try decoder.next(expectedColumnCount: 1)
        }
        await #expect(throws: CancellationError.self) { try await task.value }
    }

    private func makeMeter(bytes: UInt64 = 1_048_576, rows: UInt32 = 128, work: UInt64 = 100_000) -> DatabaseWorkMeter {
        DatabaseWorkMeter(unbounded: ExecutionBudget(maximumRows: 128, maximumWorkUnits: work,
            maximumIntermediateRows: rows, maximumIntermediateBytes: bytes, timeoutMilliseconds: .max))
    }
}
