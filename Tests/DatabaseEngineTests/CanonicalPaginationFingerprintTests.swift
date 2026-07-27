#if !os(WASI)
import DatabaseTypes
import DatabaseWire
import DatabaseKit
import Testing
@testable import DatabaseEngine

@Suite("Canonical pagination fingerprint")
struct CanonicalPaginationFingerprintTests {
    @Test("streaming fingerprint preserves the canonical digest format")
    func streamingFingerprintMatchesMaterializedGoldenDigest() throws {
        let query = selectQuery()
        let scope = ByteString([9, 8, 7, 6])
        let page = try CanonicalQueryPagination.window(
            rows: rows(2),
            selectQuery: query,
            options: execution(
                budget: unlimitedBudget(),
                continuationScope: scope
            )
        )
        let continuation = try #require(page.continuation)
        var reader = try StorageFrameDecoder(continuation.bytes)
        #expect(try reader.readUInt32() == 0x4351_5031)
        let actualQueryFingerprint = try reader.readBytes()

        let encodedQuery = try QueryIRWireFormat.encode(.select(query))
        var hasher = SHA256Accumulator()
        update(UInt32(0x0151_4244), hasher: &hasher)
        updateLength(encodedQuery.count, hasher: &hasher)
        hasher.update(encodedQuery)
        updateLength(scope.count, hasher: &hasher)
        hasher.update(scope)
        let expectedQueryFingerprint = hasher.finalize()

        #expect(actualQueryFingerprint == expectedQueryFingerprint)
    }

    @Test("work budget rejection leaves the meter unconsumed")
    func workBudgetRejectsBeforeStreamingEmission() throws {
        let budget = ExecutionBudget(
            maximumRows: UInt32.max,
            maximumWorkUnits: 1,
            maximumIntermediateRows: UInt32.max,
            maximumIntermediateBytes: 1_024 * 1_024,
            timeoutMilliseconds: 30_000
        )
        let meter = DatabaseWorkMeter(budget: budget)

        do {
            _ = try CanonicalQueryPagination.window(
                rows: rows(2),
                selectQuery: selectQuery(),
                options: execution(budget: budget, workMeter: meter)
            )
            Issue.record("Expected the fingerprint work claim to fail")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumWorkUnits(
                .resultMaterialization,
                let consumed,
                let requested,
                let maximum
            ) = error else {
                Issue.record("Unexpected work limit error: \(error)")
                return
            }
            #expect(consumed == 0)
            #expect(requested > maximum)
            #expect(maximum == 1)
        }

        #expect(meter.consumedWorkUnits == 0)
    }

    @Test("intermediate byte budget rejects before work is consumed")
    func intermediateBudgetRejectsBeforeWorkClaim() throws {
        let budget = ExecutionBudget(
            maximumRows: UInt32.max,
            maximumWorkUnits: UInt64.max,
            maximumIntermediateRows: UInt32.max,
            maximumIntermediateBytes: 1,
            timeoutMilliseconds: 30_000
        )
        let meter = DatabaseWorkMeter(budget: budget)

        #expect(throws: DatabaseWorkLimitError.self) {
            _ = try CanonicalQueryPagination.window(
                rows: rows(2),
                selectQuery: selectQuery(),
                options: execution(budget: budget, workMeter: meter)
            )
        }

        #expect(meter.consumedWorkUnits == 0)
    }

    @Test("admitted depth beyond ingress wire defaults fingerprints iteratively")
    func admittedDeepQueryUsesInternalCanonicalLimits() throws {
        var expression: Expression = .bool(true)
        for _ in 0..<128 {
            expression = .not(expression)
        }
        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("events")),
            filter: expression
        )
        let structuralLimits = QueryStructuralLimits(
            maximumNestingDepth: 256
        )
        try QueryStructuralValidator.validate(query, limits: structuralLimits)

        let page = try CanonicalQueryPagination.window(
            rows: rows(2),
            selectQuery: query,
            options: execution(
                budget: unlimitedBudget(),
                structuralLimits: structuralLimits
            )
        )

        #expect(page.items.count == 1)
        #expect(page.continuation != nil)
    }

    private func selectQuery() -> SelectQuery {
        SelectQuery(
            projection: .all,
            source: .table(TableRef("events")),
            orderBy: [SortKey(.column(ColumnRef("id")))],
            limit: 10
        )
    }

    private func rows(_ count: Int) -> [DatabaseEngine.QueryRow] {
        (0..<count).map {
            DatabaseEngine.QueryRow(
                fields: ["id": .int64(Int64($0))]
            )
        }
    }

    private func unlimitedBudget() -> ExecutionBudget {
        ExecutionBudget(
            maximumRows: UInt32.max,
            maximumWorkUnits: UInt64.max,
            maximumIntermediateRows: UInt32.max,
            maximumIntermediateBytes: 16 * 1_024 * 1_024,
            timeoutMilliseconds: 30_000
        )
    }

    private func execution(
        budget: ExecutionBudget,
        workMeter: DatabaseWorkMeter? = nil,
        continuationScope: ByteString = [],
        structuralLimits: QueryStructuralLimits = .default
    ) -> ReadExecutionContext {
        ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: 1,
                budget: budget,
                continuationScope: continuationScope
            ),
            workMeter: workMeter ?? DatabaseWorkMeter(budget: budget),
            queryStructuralLimits: structuralLimits
        )
    }

    private func update(
        _ domain: UInt32,
        hasher: inout SHA256Accumulator
    ) {
        var littleEndian = domain.littleEndian
        withUnsafeBytes(of: &littleEndian) {
            hasher.update($0)
        }
    }

    private func updateLength(
        _ byteCount: Int,
        hasher: inout SHA256Accumulator
    ) {
        var littleEndian = UInt64(byteCount).littleEndian
        withUnsafeBytes(of: &littleEndian) {
            hasher.update($0)
        }
    }
}
#endif
