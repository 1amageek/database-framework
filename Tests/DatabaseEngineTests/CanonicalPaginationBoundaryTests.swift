#if !os(WASI)
import DatabaseValue
import DatabaseWire
import QueryIR
import Testing
@testable import DatabaseEngine

@Suite("Canonical pagination boundaries")
struct CanonicalPaginationBoundaryTests {
    @Test("Logical limit does not produce an unreachable continuation")
    func logicalLimitDoesNotProduceContinuation() throws {
        let page = try CanonicalQueryPagination.window(
            rows: rows(3),
            selectQuery: selectQuery(limit: 2),
            options: execution(pageSize: 2)
        )

        #expect(page.items.count == 2)
        #expect(page.continuation == nil)
    }

    @Test("Maximum page size does not overflow lookahead arithmetic")
    func maximumPageSizeDoesNotOverflow() throws {
        let page = try CanonicalQueryPagination.window(
            rows: rows(2),
            selectQuery: selectQuery(),
            options: execution(pageSize: Int.max)
        )

        #expect(page.items.count == 2)
        #expect(page.continuation == nil)
    }

    @Test("Cursor offsets outside platform capacity are rejected")
    func oversizedCursorOffsetIsRejected() throws {
        let first = try CanonicalQueryPagination.window(
            rows: rows(3),
            selectQuery: selectQuery(),
            options: execution(pageSize: 1)
        )
        let continuation = try #require(first.continuation)
        var bytes = continuation.bytes.contiguousArray()
        for index in (bytes.count - 8)..<bytes.count {
            bytes[index] = 0xff
        }

        #expect(throws: CanonicalReadError.self) {
            _ = try CanonicalQueryPagination.window(
                rows: rows(3),
                selectQuery: selectQuery(),
                options: execution(
                    pageSize: 1,
                    continuation: QueryContinuation(DatabaseBytes(bytes))
                )
            )
        }
    }

    @Test("Continuation is bound to the canonical query")
    func continuationRejectsDifferentQuery() throws {
        let first = try CanonicalQueryPagination.window(
            rows: rows(3),
            selectQuery: selectQuery(),
            options: execution(pageSize: 1)
        )
        let continuation = try #require(first.continuation)

        #expect(throws: CanonicalReadError.self) {
            _ = try CanonicalQueryPagination.window(
                rows: rows(3),
                selectQuery: selectQuery(table: "OtherEntity"),
                options: execution(
                    pageSize: 1,
                    continuation: continuation
                )
            )
        }
    }

    @Test("Continuation rejects a changed result snapshot")
    func continuationRejectsChangedResult() throws {
        let first = try CanonicalQueryPagination.window(
            rows: rows(3),
            selectQuery: selectQuery(),
            options: execution(pageSize: 1)
        )
        let continuation = try #require(first.continuation)
        var changed = rows(3)
        changed[1] = QueryRow(fields: ["id": .string("changed")])

        #expect(throws: CanonicalReadError.self) {
            _ = try CanonicalQueryPagination.window(
                rows: changed,
                selectQuery: selectQuery(),
                options: execution(
                    pageSize: 1,
                    continuation: continuation
                )
            )
        }
    }

    @Test("Pagination narrows a consumed result buffer without reallocating")
    func paginationReusesOwnedBuffer() throws {
        let source = rows(6)
        let sourceAddress = source.withUnsafeBufferPointer { buffer in
            buffer.baseAddress.map(UInt.init(bitPattern:))
        }

        let page = try CanonicalQueryPagination.window(
            rows: consume source,
            selectQuery: selectQuery(offset: 2),
            options: execution(pageSize: 2)
        )
        let pageAddress = page.items.withUnsafeBufferPointer { buffer in
            buffer.baseAddress.map(UInt.init(bitPattern:))
        }

        #expect(page.items.count == 2)
        #expect(page.items[0].fields["id"] == .int64(2))
        #expect(sourceAddress == pageAddress)
    }

    private func selectQuery(
        table: String = "PaginationEntity",
        limit: Int? = nil,
        offset: Int? = nil
    ) -> SelectQuery {
        SelectQuery(
            projection: .all,
            source: .table(TableRef(table)),
            limit: limit,
            offset: offset
        )
    }

    private func rows(_ count: Int) -> [QueryRow] {
        (0..<count).map {
            QueryRow(fields: ["id": .int64(Int64($0))])
        }
    }

    private func execution(
        pageSize: Int,
        continuation: QueryContinuation? = nil
    ) -> ReadExecutionContext {
        let budget = DatabaseExecutionBudget(
            maximumRows: UInt32.max,
            maximumWorkUnits: UInt64.max,
            timeoutMilliseconds: 30_000
        )
        return ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: pageSize,
                continuation: continuation,
                budget: budget
            ),
            workMeter: DatabaseWorkMeter(budget: budget)
        )
    }
}
#endif
