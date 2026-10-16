#if !os(WASI)
import DatabaseTypes
import DatabaseWire
import DatabaseKit
import StorageKitSystemClock
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Canonical pagination boundaries")
struct CanonicalPaginationBoundaryTests {
    @Test("Logical limit does not produce an unreachable continuation")
    func logicalLimitDoesNotProduceContinuation() throws {
        let page = try CanonicalQueryPagination.window(
            rows: try rows(3),
            columns: try paginationColumns(),
            selectQuery: selectQuery(limit: 2),
            options: execution(pageSize: 2)
        )

        #expect(page.items.count == 2)
        #expect(page.continuation == nil)
        #expect(try page.columns == paginationColumns())
        let empty = try CanonicalQueryPagination.window(
            rows: [], columns: paginationColumns(),
            selectQuery: selectQuery(), options: execution(pageSize: 2)
        )
        #expect(empty.items.isEmpty)
        #expect(empty.columns == page.columns)
    }

    @Test("Maximum page size does not overflow lookahead arithmetic")
    func maximumPageSizeDoesNotOverflow() throws {
        let page = try CanonicalQueryPagination.window(
            rows: try rows(2),
            columns: try paginationColumns(),
            selectQuery: selectQuery(),
            options: execution(pageSize: Int.max)
        )

        #expect(page.items.count == 2)
        #expect(page.continuation == nil)
    }

    @Test("Cursor offsets outside platform capacity are rejected")
    func oversizedCursorOffsetIsRejected() throws {
        let first = try CanonicalQueryPagination.window(
            rows: try rows(3),
            columns: try paginationColumns(),
            selectQuery: selectQuery(),
            options: execution(pageSize: 1)
        )
        let continuation = try #require(first.continuation)
        // A mutable copy is required only to corrupt this bounded cursor
        // fixture; production pagination retains the immutable ByteString.
        var bytes = Array(continuation.bytes)
        for index in (bytes.count - 8)..<bytes.count {
            bytes[index] = 0xff
        }

        #expect(throws: CanonicalReadError.self) {
            _ = try CanonicalQueryPagination.window(
                rows: try rows(3),
                columns: try paginationColumns(),
                selectQuery: selectQuery(),
                options: execution(
                    pageSize: 1,
                    continuation: QueryContinuation(ByteString(bytes))
                )
            )
        }
    }

    @Test("Continuation is bound to the canonical query")
    func continuationRejectsDifferentQuery() throws {
        let first = try CanonicalQueryPagination.window(
            rows: try rows(3),
            columns: try paginationColumns(),
            selectQuery: selectQuery(),
            options: execution(pageSize: 1)
        )
        let continuation = try #require(first.continuation)

        #expect(throws: CanonicalReadError.self) {
            _ = try CanonicalQueryPagination.window(
                rows: try rows(3),
                columns: try paginationColumns(),
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
            rows: try rows(3),
            columns: try paginationColumns(),
            selectQuery: selectQuery(),
            options: execution(pageSize: 1)
        )
        let continuation = try #require(first.continuation)
        var changed = try rows(3)
        changed[1] = try QueryRow(values: [.primitive(.int64(99))], columns: try paginationColumns())

        #expect(throws: CanonicalReadError.self) {
            _ = try CanonicalQueryPagination.window(
                rows: changed,
                columns: try paginationColumns(),
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
        let source = try rows(6)
        let sourceAddress = source.withUnsafeBufferPointer { buffer in
            buffer.baseAddress.map(UInt.init(bitPattern:))
        }

        let page = try CanonicalQueryPagination.window(
            rows: consume source,
            columns: try paginationColumns(),
            selectQuery: selectQuery(offset: 2),
            options: execution(pageSize: 2)
        )
        let pageAddress = page.items.withUnsafeBufferPointer { buffer in
            buffer.baseAddress.map(UInt.init(bitPattern:))
        }

        #expect(page.items.count == 2)
        #expect(page.items[0].values[0] == .primitive(.int64(2)))
        #expect(sourceAddress == pageAddress)
    }

    @Test("Internal pagination retains ownership and exposes a bounded view")
    func retainedPaginationPreservesReservation() throws {
        let execution = execution(pageSize: 2)
        do {
            var builder = try DatabaseRetainedArrayBuilder<DatabaseEngine.QueryRow>(
                workMeter: execution.workMeter,
                stage: .resultMaterialization,
                layout: try DatabaseRetainedArrayLayout.forElement(DatabaseEngine.QueryRow.self),
                expectedCount: 4
            )
            for row in try rows(4) {
                try builder.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: row,
                        workMeter: execution.workMeter
                    ),
                    make: { row }
                )
            }
            let retained = DatabaseSharedRetainedQueryRows(
                storage: try builder.finish().moveToSharedOwnership(at: .resultMaterialization),
                columns: try paginationColumns()
            )
            let page = try CanonicalQueryPagination.retainedWindow(
                rows: retained,
                selectQuery: selectQuery(offset: 1),
                options: execution
            )
            let visible = retained.boundedView(page.range)

            var visibleValues: [QueryValue] = []
            for index in 0..<visible.count {
                visible.withElement(at: index) { visibleValues.append($0.values[0]) }
            }
            #expect(visibleValues == [.primitive(.int64(1)), .primitive(.int64(2))])
            #expect(page.columns == retained.columns)
            #expect(execution.workMeter.retainedIntermediateRows == 4)
            #expect(page.continuation != nil)
        }
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
    }

    private func selectQuery(
        table: String = "PaginationEntity",
        limit: UInt64? = nil,
        offset: UInt64? = nil
    ) -> SelectQuery {
        SelectQuery(projection: [.wildcard()],
            source: .table(TableRef(table)),
            pagination: QueryPagination(limit: limit.map { .count(.fixed($0), withTies: false) }, offset: offset.map { .fixed($0) }))
    }

    private func paginationColumns() throws -> [QueryColumn] {
        try [QueryColumn(number: 1, name: "id", type: .primitive(.int64), isNullable: false)]
    }

    private func rows(_ count: Int) throws -> [DatabaseEngine.QueryRow] {
        try (0..<count).map {
            try DatabaseEngine.QueryRow(
                values: [.primitive(.int64(Int64($0)))], columns: [QueryColumn(number: 1, name: "id", type: .primitive(.int64), isNullable: false)]
            )
        }
    }

    private func execution(
        pageSize: Int,
        continuation: QueryContinuation? = nil
    ) -> ReadExecutionContext {
        let budget = ExecutionBudget(
            maximumRows: UInt32.max,
            maximumWorkUnits: UInt64.max,
            timeoutMilliseconds: 30_000
        )
        let clock = SystemStorageClock()
        return ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: pageSize,
                continuation: continuation,
                budget: budget
            ),
            monotonicClock: clock,
            workMeter: DatabaseWorkMeter(
                budget: budget,
                monotonicClock: clock
            )
        )
    }
}
#endif
