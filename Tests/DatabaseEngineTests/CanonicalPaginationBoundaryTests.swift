#if !os(WASI)
import DatabaseTypes
import DatabaseWire
import DatabaseKit
import StorageKitSystemClock
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
        // A mutable copy is required only to corrupt this bounded cursor
        // fixture; production pagination retains the immutable ByteString.
        var bytes = Array(continuation.bytes)
        for index in (bytes.count - 8)..<bytes.count {
            bytes[index] = 0xff
        }

        #expect(throws: CanonicalReadError.self) {
            _ = try CanonicalQueryPagination.window(
                rows: rows(3),
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

    @Test("Internal pagination retains ownership and exposes a bounded view")
    func retainedPaginationPreservesReservation() throws {
        let execution = execution(pageSize: 2)
        do {
            var builder = try DatabaseRetainedArrayBuilder<DatabaseEngine.QueryRow>(
                workMeter: execution.workMeter,
                stage: .resultMaterialization,
                layout: try CanonicalRelationalFootprintMeter
                    .retainedArrayLayout(for: DatabaseEngine.QueryRow.self),
                expectedCount: 4
            )
            for row in rows(4) {
                try builder.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: row,
                        workMeter: execution.workMeter
                    ),
                    make: { row }
                )
            }
            let retained = try builder.finish().moveToSharedOwnership(
                at: .resultMaterialization
            )
            let page = try CanonicalQueryPagination.retainedWindow(
                rows: retained,
                selectQuery: selectQuery(offset: 1),
                options: execution
            )
            let visible = retained.boundedView(page.range)

            #expect(visible.map { $0.fields["id"] } == [.int64(1), .int64(2)])
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
        SelectQuery(
            projection: .all,
            source: .table(TableRef(table)),
            limit: limit,
            offset: offset
        )
    }

    private func rows(_ count: Int) -> [DatabaseEngine.QueryRow] {
        (0..<count).map {
            DatabaseEngine.QueryRow(
                fields: ["id": .int64(Int64($0))]
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
