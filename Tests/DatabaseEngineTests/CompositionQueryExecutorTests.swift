#if MultiBase
import DatabaseKit
import DatabaseRuntime
import StorageKit
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Composition typed query execution")
struct CompositionQueryExecutorTests {
    @Persistable
    struct Item {
        var id: String = ""
        var rank: Int64 = 0
    }

    private enum DecisionTestError: Error {
        case rollbackRequested
    }

    private actor DecisionOperationProbe {
        private var didStart = false

        func markStarted() {
            didStart = true
        }

        func hasStarted() -> Bool {
            didStart
        }
    }

    private actor CompositionRowProbe {
        private var values: [CompositionQueryRow] = []

        func receive(_ event: CompositionQueryEvent) -> Bool {
            if case .row(let value) = event {
                values.append(value)
            }
            return true
        }

        func rows() -> [CompositionQueryRow] {
            values
        }
    }

    private actor CancellationRowProbe {
        private var count = 0
        private var minimumRetainedRows: UInt64?
        private var minimumRetainedBytes: UInt64?

        func receive(
            _ event: CompositionQueryEvent,
            workMeter: DatabaseWorkMeter
        ) async throws -> Bool {
            guard case .row = event else { return true }
            count += 1
            minimumRetainedRows = workMeter.retainedIntermediateRows
            minimumRetainedBytes = workMeter.retainedIntermediateBytes
            await Task.yield()
            minimumRetainedRows = min(
                minimumRetainedRows ?? .max,
                workMeter.retainedIntermediateRows
            )
            minimumRetainedBytes = min(
                minimumRetainedBytes ?? .max,
                workMeter.retainedIntermediateBytes
            )
            throw CancellationError()
        }

        func receivedRowCount() -> Int {
            count
        }

        func retainedClaimDuringCancellation() -> (
            rows: UInt64?,
            bytes: UInt64?
        ) {
            (minimumRetainedRows, minimumRetainedBytes)
        }
    }

    private actor AggregateEmissionProbe {
        private var values: [CompositionQueryRow] = []
        private var minimumRetainedRows: UInt64?
        private var minimumRetainedBytes: UInt64?

        func receive(
            _ event: CompositionQueryEvent,
            workMeter: DatabaseWorkMeter
        ) async -> Bool {
            guard case .row(let value) = event else { return true }
            minimumRetainedRows = workMeter.retainedIntermediateRows
            minimumRetainedBytes = workMeter.retainedIntermediateBytes
            await Task.yield()
            values.append(value)
            minimumRetainedRows = min(
                minimumRetainedRows ?? .max,
                workMeter.retainedIntermediateRows
            )
            minimumRetainedBytes = min(
                minimumRetainedBytes ?? .max,
                workMeter.retainedIntermediateBytes
            )
            return true
        }

        func result() -> (
            values: [CompositionQueryRow],
            rows: UInt64?,
            bytes: UInt64?
        ) {
            (values, minimumRetainedRows, minimumRetainedBytes)
        }
    }

    private final class IdentityFingerprintProbe: Sendable {
        private struct State {
            var retainedBytes: UInt64?
            var invocationCount = 0
        }

        private let state = Mutex(State())

        func record(retainedBytes: UInt64) {
            state.withLock {
                $0.retainedBytes = retainedBytes
                $0.invocationCount += 1
            }
        }

        func markInvoked() {
            state.withLock { $0.invocationCount += 1 }
        }

        var retainedBytes: UInt64? {
            state.withLock { $0.retainedBytes }
        }

        var wasInvoked: Bool {
            state.withLock { $0.invocationCount > 0 }
        }
    }

    private struct DistinctEntryLayoutProbe: Sendable {
        let fingerprint: ByteString
        let identity: ByteString
        let ownedRow: DatabaseQueryScopedQueryRowOwner
        var contributors: [Base.ID]
    }

    @Test("Global ordering, windowing, and provenance span domains")
    func globalOrderingWindowAndProvenance() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("shared", 1), ("primary", 3)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )
        try await insert(
            [("shared", 2), ("secondary", 4)],
            baseID: fixture.secondaryBaseID,
            fixture: fixture
        )

        let results = try await fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
            .query(Item.self)
            .orderBy(#field(\Item.rank))
            .offset(1)
            .limit(2)
            .execute()

        #expect(results.map(\.value.rank) == [2, 3])
        #expect(
            results.map(\.origin) == [
                .source(fixture.secondaryBaseID),
                .source(fixture.primaryBaseID),
            ]
        )
        #expect(results.allSatisfy {
            $0.composition.namedID == fixture.compositionID
                && $0.composition.generation == 1
        })

        let count = try await fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
            .query(Item.self)
            .offset(1)
            .limit(2)
            .count()
        #expect(count == 2)

        try await insert(
            (0..<12).map { index in
                ("paged-primary-\(index)", Int64(10 + index))
            },
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )

        let aggregateQuery = SelectQuery(
            projection: .items([
                ProjectionItem(
                    .aggregate(
                        .min(.column(ColumnRef(column: "id")))
                    ),
                    alias: "minimum"
                ),
                ProjectionItem(
                    .aggregate(
                        .max(.column(ColumnRef(column: "id")))
                    ),
                    alias: "maximum"
                ),
            ]),
            source: .table(TableRef(Item.persistableType))
        )
        let aggregateSource = fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
        let calibrationContext = ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: 8,
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 4,
                    maximumIntermediateBytes: 4 * 1_024 * 1_024,
                    timeoutMilliseconds: 30_000
                )
            ),
            monotonicClock: fixture.container.monotonicClock
        )
        let aggregateProbe = AggregateEmissionProbe()
        try await CompositionQueryPlanner(
            structuralLimits: calibrationContext.queryStructuralLimits
        ).execute(
            aggregateQuery,
            source: aggregateSource,
            options: CompositionQueryExecutionOptions(
                pageSize: 8,
                readContext: calibrationContext
            )
        ) { event in
            await aggregateProbe.receive(
                event,
                workMeter: calibrationContext.workMeter
            )
        }
        let aggregateEmission = await aggregateProbe.result()
        let extrema = aggregateEmission.values
        #expect(extrema.count == 1)
        #expect(
            extrema[0].row.fields["minimum"]
                == .string("paged-primary-0")
        )
        #expect(extrema[0].row.fields["maximum"] == .string("shared"))
        #expect(
            extrema[0].origin == .derived(
                contributors: [
                    fixture.primaryBaseID,
                    fixture.secondaryBaseID,
                ]
            )
        )
        #expect(aggregateEmission.rows == 1)
        #expect((aggregateEmission.bytes ?? 0) > 0)
        let aggregatePeak = calibrationContext.workMeter.peakIntermediateBytes
        #expect(aggregatePeak > 0)
        #expect(calibrationContext.workMeter.retainedIntermediateRows == 0)
        #expect(calibrationContext.workMeter.retainedIntermediateBytes == 0)

        let insufficientRowContext = ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: 8,
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 3,
                    maximumIntermediateBytes: 4 * 1_024 * 1_024,
                    timeoutMilliseconds: 30_000
                )
            ),
            monotonicClock: fixture.container.monotonicClock
        )
        var insufficientRowFailure: Error?
        do {
            try await CompositionQueryPlanner(
                structuralLimits: insufficientRowContext.queryStructuralLimits
            ).execute(
                aggregateQuery,
                source: aggregateSource,
                options: CompositionQueryExecutionOptions(
                    pageSize: 8,
                    readContext: insufficientRowContext
                )
            ) { _ in true }
            Issue.record("Expected aggregate row admission to reject a maximum of three")
        } catch {
            insufficientRowFailure = error
        }
        guard let insufficientRowLimit = insufficientRowFailure
                as? DatabaseWorkLimitError,
              case .maximumIntermediateRows(
                let insufficientRowStage,
                let insufficientRowConsumed,
                let insufficientRowRequested,
                let insufficientRowMaximum
              ) = insufficientRowLimit else {
            Issue.record(
                "Expected aggregate row-limit failure, got \(String(describing: insufficientRowFailure))"
            )
            return
        }
        #expect(insufficientRowStage == .aggregateInput)
        #expect(insufficientRowConsumed == 0)
        #expect(insufficientRowRequested == 4)
        #expect(insufficientRowMaximum == 3)
        #expect(insufficientRowContext.workMeter.retainedIntermediateRows == 0)
        #expect(insufficientRowContext.workMeter.retainedIntermediateBytes == 0)

        let stateProbeContext = ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: 8,
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 100,
                    maximumIntermediateBytes: 0,
                    timeoutMilliseconds: 30_000
                )
            ),
            monotonicClock: fixture.container.monotonicClock
        )
        var stateProbeFailure: Error?
        do {
            try await CompositionQueryPlanner(
                structuralLimits: stateProbeContext.queryStructuralLimits
            ).execute(
                aggregateQuery,
                source: aggregateSource,
                options: CompositionQueryExecutionOptions(
                    pageSize: 8,
                    readContext: stateProbeContext
                )
            ) { _ in true }
            Issue.record("Expected aggregate state admission to reject zero bytes")
        } catch {
            stateProbeFailure = error
        }
        guard let stateWorkLimit = stateProbeFailure as? DatabaseWorkLimitError,
              case .maximumIntermediateBytes(
                let stateStage,
                let stateConsumed,
                let stateRequested,
                let stateMaximum
              ) = stateWorkLimit else {
            Issue.record(
                "Expected aggregate state byte-limit failure, got \(String(describing: stateProbeFailure))"
            )
            return
        }
        #expect(stateStage == .aggregateInput)
        #expect(stateConsumed == 0)
        #expect(stateMaximum == 0)
        guard stateRequested > 0 else {
            Issue.record("Aggregate state footprint must be nonzero")
            return
        }
        #expect(stateProbeContext.workMeter.retainedIntermediateRows == 0)
        #expect(stateProbeContext.workMeter.retainedIntermediateBytes == 0)

        let oneByteShortContext = ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: 8,
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 100,
                    maximumIntermediateBytes: stateRequested - 1,
                    timeoutMilliseconds: 30_000
                )
            ),
            monotonicClock: fixture.container.monotonicClock
        )
        var oneByteShortFailure: Error?
        do {
            try await CompositionQueryPlanner(
                structuralLimits: oneByteShortContext.queryStructuralLimits
            ).execute(
                aggregateQuery,
                source: aggregateSource,
                options: CompositionQueryExecutionOptions(
                    pageSize: 8,
                    readContext: oneByteShortContext
                )
            ) { _ in true }
            Issue.record("Expected aggregate state admission one byte short")
        } catch {
            oneByteShortFailure = error
        }
        guard let oneByteShortLimit = oneByteShortFailure
                as? DatabaseWorkLimitError,
              case .maximumIntermediateBytes(
                let shortStage,
                let shortConsumed,
                let shortRequested,
                let shortMaximum
              ) = oneByteShortLimit else {
            Issue.record(
                "Expected one-byte-short state failure, got \(String(describing: oneByteShortFailure))"
            )
            return
        }
        #expect(shortStage == .aggregateInput)
        #expect(shortConsumed == 0)
        #expect(shortRequested == stateRequested)
        #expect(shortMaximum == stateRequested - 1)
        #expect(oneByteShortContext.workMeter.retainedIntermediateRows == 0)
        #expect(oneByteShortContext.workMeter.retainedIntermediateBytes == 0)

        let failingAggregateContext = ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: 1,
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 8,
                    maximumIntermediateBytes: 4 * 1_024 * 1_024,
                    timeoutMilliseconds: 30_000
                )
            ),
            monotonicClock: fixture.container.monotonicClock
        )
        var failingAggregateError: Error?
        do {
            try await CompositionQueryPlanner(
                structuralLimits: failingAggregateContext
                    .queryStructuralLimits
            ).execute(
                SelectQuery(
                    projection: .items([
                        ProjectionItem(
                            .aggregate(
                                .sum(
                                    .column(ColumnRef(column: "id")),
                                    distinct: false
                                )
                            ),
                            alias: "invalidSum"
                        )
                    ]),
                    source: .table(TableRef(Item.persistableType))
                ),
                source: aggregateSource,
                options: CompositionQueryExecutionOptions(
                    pageSize: 1,
                    readContext: failingAggregateContext
                )
            ) { _ in true }
            Issue.record("Expected nonnumeric SUM to fail")
        } catch {
            failingAggregateError = error
        }
        guard let failingAggregate = failingAggregateError
                as? CompositionQueryError,
              case .aggregateFailure = failingAggregate else {
            Issue.record(
                "Expected aggregate failure, got \(String(describing: failingAggregateError))"
            )
            return
        }
        #expect(
            failingAggregateContext.workMeter.retainedIntermediateRows == 0
        )
        #expect(
            failingAggregateContext.workMeter.retainedIntermediateBytes == 0
        )

        let streamedRows = try await aggregateSource.execute(
            SelectQuery(
                projection: .all,
                source: .table(TableRef(Item.persistableType)),
                limit: 2
            ),
            options: ReadExecutionOptions(
                pageSize: 1,
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 8,
                    maximumIntermediateBytes: 4 * 1_024 * 1_024,
                    timeoutMilliseconds: 30_000
                )
            )
        )
        #expect(streamedRows.count == 2)
    }

    @Test("Composition validates structure before opening a read snapshot")
    func compositionValidatesStructureBeforeReadSnapshot() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        let source = fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
        let readContext = ReadExecutionContext(
            options: .default,
            monotonicClock: fixture.container.monotonicClock,
            queryStructuralLimits: QueryStructuralLimits(
                maximumTotalNodes: 0
            )
        )
        let planner = CompositionQueryPlanner(
            structuralLimits: readContext.queryStructuralLimits
        )

        await #expect(throws: QueryStructuralValidationError.self) {
            try await planner.execute(
                SelectQuery(
                    projection: .all,
                    source: .table(TableRef(Item.persistableType))
                ),
                source: source,
                options: CompositionQueryExecutionOptions(
                    pageSize: 1,
                    readContext: readContext
                )
            ) { _ in
                Issue.record("Invalid query reached event emission")
                return false
            }
        }
        #expect(readContext.workMeter.consumedRows == 0)
        #expect(readContext.workMeter.retainedIntermediateRows == 0)
        #expect(readContext.workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Equal logical IDs remain Base-qualified")
    func equalLogicalIDsRemainBaseQualified() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("shared", 1)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )
        try await insert(
            [("shared", 2)],
            baseID: fixture.secondaryBaseID,
            fixture: fixture
        )

        let results = try await fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
            .query(Item.self)
            .orderBy(#field(\Item.rank))
            .execute()

        #expect(results.map(\.value.id) == ["shared", "shared"])
        #expect(Set(results.map(\.origin)).count == 2)
    }

    @Test("Every member is authorized before results are exposed")
    func everyMemberMustBeAuthorized() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: false)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("visible-only-if-composition-were-partial", 1)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )

        await #expect(throws: DatabaseCompositionAccessError.self) {
            try await fixture.container.session(
                authorization: fixture.readerAuthorization
            ).composition(fixture.compositionID)
                .query(Item.self)
                .execute()
        }
    }

    @Test("Metadata lease preserves non-authorization storage failures")
    func metadataLeasePreservesStorageFailure() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        fixture.secondaryEngine.requestShutdown()
        await fixture.secondaryEngine.waitUntilShutdown()
        let source = fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)

        do {
            _ = try await source.acquireReadMetadata()
            Issue.record("Expected the stopped storage domain to fail")
        } catch is DatabaseCompositionAccessError {
            Issue.record("Storage failure was incorrectly hidden as access denial")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
            #expect(error.operation == .beginTransaction)
        }
    }

    @Test("Derived Composition does not require a catalog record")
    func derivedCompositionExecutesWithoutCatalogRecord() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("primary", 1)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )
        try await insert(
            [("secondary", 2)],
            baseID: fixture.secondaryBaseID,
            fixture: fixture
        )

        let results = try await fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(
            bases: [fixture.secondaryBaseID, fixture.primaryBaseID]
        ).query(Item.self).orderBy(#field(\Item.rank)).execute()

        #expect(results.map(\.value.rank) == [1, 2])
        #expect(results.allSatisfy {
            $0.composition.kind == .derived
                && $0.composition.namedID == nil
                && $0.composition.generation == nil
                && $0.composition.bases == [
                    fixture.primaryBaseID,
                    fixture.secondaryBaseID,
                ].sorted()
        })
    }

    @Test("Decision reads and writes share one storage-domain transaction")
    func sameDomainDecisionTransactionCommitsAndRollsBack() async throws {
        let fixture = try await makeFixture(
            readerCanReadSecondary: true,
            secondarySharesControlDomain: true
        )
        defer { await fixture.container.shutdown() }
        try await insert(
            [("world", 7)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )
        let composition = try fixture.container.session(
            authorization: fixture.ownerAuthorization
        ).composition(
            bases: [fixture.primaryBaseID, fixture.secondaryBaseID]
        )

        try await composition.withDecisionTransaction(
            writingTo: fixture.secondaryBaseID
        ) { transaction in
            let world = try await transaction.fetch(
                Query<Item>().where(#field(\Item.id) == "world"),
                from: fixture.primaryBaseID
            )
            #expect(world.map(\.rank) == [7])
            var decision = Item()
            decision.id = "committed"
            decision.rank = world[0].rank + 1
            try await transaction.save(decision)
        }

        do {
            try await composition.withDecisionTransaction(
                writingTo: fixture.secondaryBaseID
            ) { transaction in
                var decision = Item()
                decision.id = "rolled-back"
                decision.rank = 99
                try await transaction.save(decision)
                throw DecisionTestError.rollbackRequested
            }
            Issue.record("The requested rollback unexpectedly committed")
        } catch DecisionTestError.rollbackRequested {
            // Expected typed application failure.
        }

        let written = try await fixture.container.session(
            authorization: fixture.ownerAuthorization
        ).base(fixture.secondaryBaseID).newContext()
            .fetch(Item.self)
            .orderBy(#field(\Item.rank))
            .execute()
        #expect(written.map(\.id) == ["committed"])
    }

    @Test("Decision transaction rejects multiple storage domains before work")
    func decisionTransactionRejectsMultipleDomains() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        let operationProbe = DecisionOperationProbe()
        let composition = try fixture.container.session(
            authorization: fixture.ownerAuthorization
        ).composition(
            bases: [fixture.primaryBaseID, fixture.secondaryBaseID]
        )

        do {
            try await composition.withDecisionTransaction(
                writingTo: fixture.primaryBaseID
            ) { _ in
                await operationProbe.markStarted()
            }
            Issue.record("The cross-domain decision transaction unexpectedly started")
        } catch CompositionDecisionError.multipleStorageDomains {
            // Expected typed failure before the operation begins.
        } catch {
            Issue.record("Unexpected decision transaction error: \(error)")
        }
        let operationStarted = await operationProbe.hasStarted()
        #expect(operationStarted == false)
    }

    @Test("Explicit cross-Base INNER JOIN preserves derived lineage")
    func crossBaseInnerJoinUsesCanonicalBoundedExecutor() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("shared", 1), ("left-only", 3)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )
        try await insert(
            [("shared", 2), ("right-only", 4)],
            baseID: fixture.secondaryBaseID,
            fixture: fixture
        )
        let source = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(
            bases: [fixture.primaryBaseID, fixture.secondaryBaseID]
        )
        let result = try await executeCompositionQuery(
            crossBaseQuery(fixture: fixture, joinType: .inner),
            source: source
        )
        #expect(result.first?.composition.kind == .derived)
        #expect(result.count == 1)
        #expect(result[0].value.fields["worldRank"] == .int64(1))
        #expect(result[0].value.fields["tenantRank"] == .int64(2))
        #expect(
            result[0].origin == .derived(
                contributors: [
                    fixture.primaryBaseID,
                    fixture.secondaryBaseID,
                ].sorted()
            )
        )
    }

    @Test("Cross-Base outer JOIN fails without fallback")
    func crossBaseOuterJoinIsExplicitlyUnsupported() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        let source = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(
            bases: [fixture.primaryBaseID, fixture.secondaryBaseID]
        )

        do {
            _ = try await executeCompositionQuery(
                crossBaseQuery(fixture: fixture, joinType: .left),
                source: source
            )
            Issue.record("The unsupported outer JOIN unexpectedly executed")
        } catch let error as CompositionQueryError {
            guard case .unsupportedPlan = error else {
                Issue.record("Unexpected Composition failure: \(error)")
                return
            }
        } catch {
            Issue.record("Unexpected outer JOIN failure: \(error)")
        }
    }

    @Test("Cross-Base JOIN input materialization is request-bounded")
    func crossBaseJoinHonorsIntermediateRowBudget() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("one", 1), ("two", 2)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )
        try await insert(
            [("one", 3), ("two", 4)],
            baseID: fixture.secondaryBaseID,
            fixture: fixture
        )
        let source = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(
            bases: [fixture.primaryBaseID, fixture.secondaryBaseID]
        )

        await #expect(throws: DatabaseWorkLimitError.self) {
            _ = try await executeCompositionQuery(
                crossBaseQuery(fixture: fixture, joinType: .inner),
                source: source,
                maximumIntermediateRows: 2
            )
        }
    }

    @Test("DISTINCT compares exact identity after a digest collision")
    func distinctDigestCollisionUsesExactIdentity() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }

        func meter(maximumBytes: UInt64 = 1 * 1_024 * 1_024)
            -> DatabaseWorkMeter {
            DatabaseWorkMeter(
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 16,
                    maximumIntermediateBytes: maximumBytes,
                    timeoutMilliseconds: 30_000
                ),
                monotonicClock: TestProcessMonotonicClock()
            )
        }

        func owned(
            _ row: QueryRow,
            workMeter: DatabaseWorkMeter
        ) throws -> DatabaseQueryScopedQueryRow {
            let footprint = try CanonicalRelationalFootprintMeter.footprint(
                of: row,
                workMeter: workMeter,
                stage: .resultMaterialization
            )
            return try DatabaseQueryScopedQueryRow.producing(
                exactFootprint: footprint,
                workMeter: workMeter,
                stage: .resultMaterialization
            ) { row }
        }

        let identityRow = QueryRow(fields: ["priority": .int64(1)])
        let identityCalibrationMeter = meter()
        let frameConstructionProbe = IdentityFingerprintProbe()
        let identityCalibrationWorkspace = CompositionDistinctWorkspace.create(
            maximumIntermediateBytes: 1 * 1_024 * 1_024,
            workMeter: identityCalibrationMeter,
            identityFingerprint: { _ in
                frameConstructionProbe.record(
                    retainedBytes: identityCalibrationMeter.retainedIntermediateBytes
                )
                return ByteString(repeating: 0x41, count: 32)
            }
        )
        try await identityCalibrationWorkspace.insert(
            try owned(identityRow, workMeter: identityCalibrationMeter),
            origin: .source(fixture.primaryBaseID),
            sequence: 0
        )
        let identityAdmissionPeak = try #require(
            frameConstructionProbe.retainedBytes
        )
        #expect(identityAdmissionPeak > 0)
        await identityCalibrationWorkspace.removeAll()
        #expect(identityCalibrationMeter.retainedIntermediateRows == 0)
        #expect(identityCalibrationMeter.retainedIntermediateBytes == 0)

        let identityConstrainedMeter = meter(
            maximumBytes: identityAdmissionPeak - 1
        )
        let constrainedFingerprintProbe = IdentityFingerprintProbe()
        let identityConstrainedWorkspace = CompositionDistinctWorkspace.create(
            maximumIntermediateBytes: 1 * 1_024 * 1_024,
            workMeter: identityConstrainedMeter,
            identityFingerprint: { _ in
                constrainedFingerprintProbe.markInvoked()
                return ByteString(repeating: 0x41, count: 32)
            }
        )
        await #expect(throws: DatabaseWorkLimitError.self) {
            try await identityConstrainedWorkspace.insert(
                try owned(identityRow, workMeter: identityConstrainedMeter),
                origin: .source(fixture.primaryBaseID),
                sequence: 0
            )
        }
        #expect(!constrainedFingerprintProbe.wasInvoked)
        await identityConstrainedWorkspace.removeAll()
        #expect(identityConstrainedMeter.retainedIntermediateRows == 0)
        #expect(identityConstrainedMeter.retainedIntermediateBytes == 0)

        let workMeter = meter()
        let entryCapacityProbe = IdentityFingerprintProbe()
        let workspace = CompositionDistinctWorkspace.create(
            maximumIntermediateBytes: 1 * 1_024 * 1_024,
            workMeter: workMeter,
            identityFingerprint: { _ in
                entryCapacityProbe.record(
                    retainedBytes: workMeter.retainedIntermediateBytes
                )
                return ByteString(repeating: 0x42, count: 32)
            }
        )
        let output = Mutex<[CompositionDistinctWorkspace.Result]>([])

        do {
            try await workspace.insert(
                try owned(QueryRow(
                    fields: ["priority": .int64(1)],
                    annotations: ["representative": .string("first")],
                    version: PersistableVersionToken("version-1")
                ), workMeter: workMeter),
                origin: .source(fixture.primaryBaseID),
                sequence: 0
            )
            let retainedRowsAfterNewEntry = workMeter.retainedIntermediateRows
            let retainedBytesAfterNewEntry = workMeter.retainedIntermediateBytes
            #expect(retainedRowsAfterNewEntry == 1)
            #expect(retainedBytesAfterNewEntry > 0)
            try await workspace.insert(
                try owned(QueryRow(
                    fields: ["priority": .int64(1)],
                    annotations: ["representative": .string("duplicate")]
                ), workMeter: workMeter),
                origin: .source(fixture.primaryBaseID),
                sequence: 1
            )
            #expect(
                workMeter.retainedIntermediateRows == retainedRowsAfterNewEntry
            )
            #expect(
                workMeter.retainedIntermediateBytes == retainedBytesAfterNewEntry
            )
            try await workspace.insert(
                try owned(
                    QueryRow(fields: ["priority": .int64(2)]),
                    workMeter: workMeter
                ),
                origin: .source(fixture.secondaryBaseID),
                sequence: 2
            )
            let retainedBeforeEntryReplacement = try #require(
                entryCapacityProbe.retainedBytes
            )
            let contributorLayout = try DatabaseRetainedArrayLayout
                .forElement(Base.ID.self)
            let contributorGrowth = try contributorLayout.growth(
                from: 0,
                toFit: 1
            )
            let contributorBytes = contributorLayout.containerByteCount
                + contributorGrowth.additionalByteCount
                + UInt64(fixture.secondaryBaseID.value.utf8.count)
            let entryLayout = try DatabaseRetainedArrayLayout.forElement(
                DistinctEntryLayoutProbe.self
            )
            let entryReplacement = try entryLayout.growth(
                from: 0,
                toFit: 2
            )
            let expectedIncrease = contributorBytes.addingReportingOverflow(
                entryReplacement.additionalByteCount
            )
            let expectedPeak = retainedBeforeEntryReplacement
                .addingReportingOverflow(expectedIncrease.partialValue)
            #expect(!expectedIncrease.overflow)
            #expect(!expectedPeak.overflow)
            #expect(
                workMeter.peakIntermediateBytes >= expectedPeak.partialValue
            )
            try await workspace.insert(
                try owned(QueryRow(
                    fields: ["priority": .int64(1)],
                    annotations: ["representative": .string("later")],
                    version: PersistableVersionToken("version-2")
                ), workMeter: workMeter),
                origin: .source(fixture.secondaryBaseID),
                sequence: 3
            )
            try await workspace.forEachResult(batchSize: 1) { result in
                output.withLock { $0.append(result) }
                return true
            }
            await workspace.removeAll()
        } catch {
            let operationError = error
            await workspace.removeAll()
            throw operationError
        }

        let results = output.withLock { $0 }
        // The duplicate candidate remains admitted until exact identity
        // comparison completes against both published entries.
        #expect(workMeter.peakIntermediateRows == 3)
        #expect(workMeter.peakIntermediateBytes > 0)
        #expect(workMeter.retainedIntermediateRows == 0)
        #expect(workMeter.retainedIntermediateBytes == 0)
        #expect(results.count == 2)
        #expect(results[0].row.fields["priority"] == .int64(1))
        #expect(
            results[0].origin == .derived(
                contributors: [
                    fixture.primaryBaseID,
                    fixture.secondaryBaseID,
                ].sorted()
            )
        )
        #expect(
            results[0].row.annotations["representative"] == .string("first")
        )
        #expect(results[0].row.version?.value == "version-1")
        #expect(results[1].row.fields["priority"] == .int64(2))
    }

    @Test("DISTINCT contributor replacement is admitted before publication")
    func distinctContributorReplacementAdmissionIsAtomic() async throws {
        let firstBaseID = try Base.ID("contributor-a")
        let secondBaseID = try Base.ID("contributor-b")
        let representativeRow = QueryRow(
            fields: ["priority": .int64(1)],
            annotations: ["representative": .string("first")],
            version: PersistableVersionToken("version-1")
        )
        let duplicateRow = QueryRow(
            fields: ["priority": .int64(1)],
            annotations: ["representative": .string("later")],
            version: PersistableVersionToken("version-2")
        )

        func meter(maximumBytes: UInt64) -> DatabaseWorkMeter {
            DatabaseWorkMeter(
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 16,
                    maximumIntermediateBytes: maximumBytes,
                    timeoutMilliseconds: 30_000
                ),
                monotonicClock: TestProcessMonotonicClock()
            )
        }

        func owned(
            _ row: QueryRow,
            workMeter: DatabaseWorkMeter
        ) throws -> DatabaseQueryScopedQueryRow {
            let footprint = try CanonicalRelationalFootprintMeter.footprint(
                of: row,
                workMeter: workMeter,
                stage: .resultMaterialization
            )
            return try DatabaseQueryScopedQueryRow.producing(
                exactFootprint: footprint,
                workMeter: workMeter,
                stage: .resultMaterialization
            ) { row }
        }

        let calibrationMeter = meter(maximumBytes: 1 * 1_024 * 1_024)
        let calibrationWorkspace = CompositionDistinctWorkspace.create(
            maximumIntermediateBytes: 1 * 1_024 * 1_024,
            workMeter: calibrationMeter
        )
        try await calibrationWorkspace.insert(
            try owned(representativeRow, workMeter: calibrationMeter),
            origin: .source(firstBaseID),
            sequence: 0
        )
        try await calibrationWorkspace.insert(
            try owned(duplicateRow, workMeter: calibrationMeter),
            origin: .source(secondBaseID),
            sequence: 1
        )
        let successfulPeak = calibrationMeter.peakIntermediateBytes
        let calibrationOutput = Mutex<
            [CompositionDistinctWorkspace.Result]
        >([])
        try await calibrationWorkspace.forEachResult(batchSize: 1) { result in
            calibrationOutput.withLock { $0.append(result) }
            return true
        }
        let calibrationResults = calibrationOutput.withLock { $0 }
        #expect(calibrationResults.count == 1)
        #expect(
            calibrationResults.first?.origin == .derived(
                contributors: [firstBaseID, secondBaseID]
            )
        )
        #expect(
            calibrationResults.first?.row.annotations["representative"]
                == .string("first")
        )
        #expect(calibrationResults.first?.row.version?.value == "version-1")
        await calibrationWorkspace.removeAll()
        #expect(successfulPeak > 0)
        #expect(calibrationMeter.retainedIntermediateBytes == 0)

        let constrainedMeter = meter(maximumBytes: successfulPeak - 1)
        let constrainedWorkspace = CompositionDistinctWorkspace.create(
            maximumIntermediateBytes: 1 * 1_024 * 1_024,
            workMeter: constrainedMeter
        )
        try await constrainedWorkspace.insert(
            try owned(representativeRow, workMeter: constrainedMeter),
            origin: .source(firstBaseID),
            sequence: 0
        )
        await #expect(throws: DatabaseWorkLimitError.self) {
            try await constrainedWorkspace.insert(
                try owned(duplicateRow, workMeter: constrainedMeter),
                origin: .source(secondBaseID),
                sequence: 1
            )
        }
        let output = Mutex<[CompositionDistinctWorkspace.Result]>([])
        try await constrainedWorkspace.forEachResult(batchSize: 1) { result in
            output.withLock { $0.append(result) }
            return true
        }
        let results = output.withLock { $0 }
        #expect(results.count == 1)
        #expect(results.first?.origin == .source(firstBaseID))
        #expect(
            results.first?.row.annotations["representative"]
                == .string("first")
        )
        #expect(results.first?.row.version?.value == "version-1")
        await constrainedWorkspace.removeAll()
        #expect(constrainedMeter.retainedIntermediateRows == 0)
        #expect(constrainedMeter.retainedIntermediateBytes == 0)
    }

    @Test("Cancelled DISTINCT enumeration releases every admitted claim")
    func cancelledDistinctInsertionReleasesClaims() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("shared", 1), ("primary", 3)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )
        try await insert(
            [("shared", 2), ("secondary", 4)],
            baseID: fixture.secondaryBaseID,
            fixture: fixture
        )
        let readContext = ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: 1,
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 100,
                    maximumIntermediateBytes: 4 * 1_024 * 1_024,
                    timeoutMilliseconds: 30_000
                )
            ),
            monotonicClock: fixture.container.monotonicClock
        )
        let source = fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
        let probe = CancellationRowProbe()

        await #expect(throws: CancellationError.self) {
            try await CompositionQueryPlanner(
                structuralLimits: readContext.queryStructuralLimits
            ).execute(
                SelectQuery(
                    projection: .all,
                    source: .table(TableRef(Item.persistableType)),
                    distinct: true
                ),
                source: source,
                options: CompositionQueryExecutionOptions(
                    pageSize: 1,
                    readContext: readContext
                )
            ) { event in
                try await probe.receive(
                    event,
                    workMeter: readContext.workMeter
                )
            }
        }
        #expect(await probe.receivedRowCount() == 1)
        let distinctCancellationClaim = await probe
            .retainedClaimDuringCancellation()
        #expect((distinctCancellationClaim.rows ?? 0) > 0)
        #expect((distinctCancellationClaim.bytes ?? 0) > 0)
        #expect(readContext.workMeter.retainedIntermediateRows == 0)
        #expect(readContext.workMeter.retainedIntermediateBytes == 0)

        let aggregateContext = ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: 1,
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 8,
                    maximumIntermediateBytes: 4 * 1_024 * 1_024,
                    timeoutMilliseconds: 30_000
                )
            ),
            monotonicClock: fixture.container.monotonicClock
        )
        let aggregateProbe = CancellationRowProbe()
        await #expect(throws: CancellationError.self) {
            try await CompositionQueryPlanner(
                structuralLimits: aggregateContext.queryStructuralLimits
            ).execute(
                SelectQuery(
                    projection: .items([
                        ProjectionItem(
                            .aggregate(
                                .min(.column(ColumnRef(column: "id")))
                            ),
                            alias: "minimum"
                        ),
                        ProjectionItem(
                            .aggregate(
                                .max(.column(ColumnRef(column: "id")))
                            ),
                            alias: "maximum"
                        ),
                    ]),
                    source: .table(TableRef(Item.persistableType))
                ),
                source: source,
                options: CompositionQueryExecutionOptions(
                    pageSize: 1,
                    readContext: aggregateContext
                )
            ) { event in
                try await aggregateProbe.receive(
                    event,
                    workMeter: aggregateContext.workMeter
                )
            }
        }
        #expect(await aggregateProbe.receivedRowCount() == 1)
        let aggregateCancellationClaim = await aggregateProbe
            .retainedClaimDuringCancellation()
        #expect(aggregateCancellationClaim.rows == 1)
        #expect((aggregateCancellationClaim.bytes ?? 0) > 0)
        #expect(aggregateContext.workMeter.retainedIntermediateRows == 0)
        #expect(aggregateContext.workMeter.retainedIntermediateBytes == 0)
    }

    private struct Fixture: Sendable {
        let container: DBContainer
        let primaryBaseID: Base.ID
        let secondaryBaseID: Base.ID
        let compositionID: Base.Composition.ID
        let ownerAuthorization: AuthorizationContext
        let readerAuthorization: AuthorizationContext
        let secondaryEngine: InMemoryEngine
    }

    private func makeFixture(
        readerCanReadSecondary: Bool,
        secondarySharesControlDomain: Bool = false
    ) async throws -> Fixture {
        let controlDomainID = try DatabaseStorageDomain.ID("control")
        let secondaryDomainID = try DatabaseStorageDomain.ID("secondary")
        let primaryPlacementID = try Base.Placement.ID("primary")
        // Section 14 addresses a Base Partition at `bases/<Base.ID>` below the
        // database root of its domain, so one domain has exactly one placement
        // destination. Two Bases share a domain by sharing its placement.
        let secondaryPlacementID = secondarySharesControlDomain
            ? primaryPlacementID
            : try Base.Placement.ID("secondary")
        let primaryBaseID = try Base.ID("company-a")
        let secondaryBaseID = try Base.ID("company-b")
        let compositionID = try Base.Composition.ID("shared")
        let owner = Principal(identifier: "owner")
        let reader = Principal(identifier: "reader")
        let controlEngine = InMemoryEngine()
        let secondaryEngine = secondarySharesControlDomain
            ? controlEngine
            : InMemoryEngine()
        var domains = [
            try DatabaseStorageDomain(
                id: controlDomainID,
                rootPath: ["tests", "composition", "control"],
                storageEngine: controlEngine
            )
        ]
        if !secondarySharesControlDomain {
            domains.append(
                try DatabaseStorageDomain(
                    id: secondaryDomainID,
                    rootPath: ["tests", "composition", "secondary"],
                    storageEngine: secondaryEngine
                )
            )
        }
        var placements = [
            DatabaseStoragePlacement(
                id: primaryPlacementID,
                domainID: controlDomainID
            )
        ]
        if !secondarySharesControlDomain {
            placements.append(
                DatabaseStoragePlacement(
                    id: secondaryPlacementID,
                    domainID: secondaryDomainID
                )
            )
        }
        let topology = try DatabaseStorageTopology(
            controlDomainID: controlDomainID,
            domains: domains,
            placements: placements,
            defaultPlacementID: primaryPlacementID
        )
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try Item.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(
                name: "composition-typed-query",
                storageTopology: topology,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(Item.self)
                ]
            ),
            security: .testingDisabled
        )
        let ownerAuthorization: AuthorizationContext = .authenticated(owner)
        let readerAuthorization: AuthorizationContext = .authenticated(reader)
        do {
            _ = try await container.provisionBase(
                primaryBaseID,
                placementID: primaryPlacementID,
                initialGrants: [
                    Security.Grant(
                        subject: .principal(owner.identifier),
                        resource: .base(primaryBaseID),
                        access: .all
                    ),
                    Security.Grant(
                        subject: .principal(reader.identifier),
                        resource: .base(primaryBaseID),
                        access: .read
                    ),
                ],
                expectedRevision: 0
            )
            var secondaryGrants = [
                Security.Grant(
                    subject: .principal(owner.identifier),
                    resource: .base(secondaryBaseID),
                    access: .all
                )
            ]
            if readerCanReadSecondary {
                secondaryGrants.append(
                    Security.Grant(
                        subject: .principal(reader.identifier),
                        resource: .base(secondaryBaseID),
                        access: .read
                    )
                )
            }
            _ = try await container.provisionBase(
                secondaryBaseID,
                placementID: secondaryPlacementID,
                initialGrants: secondaryGrants,
                expectedRevision: 0
            )
            _ = try await container.withControlMetadataTransaction {
                transaction in
                try await container.compositionCatalog.create(
                    try Base.Composition(
                        id: compositionID,
                        bases: [primaryBaseID, secondaryBaseID]
                    ),
                    expectedRevision: 0,
                    transaction: transaction.storageAccess
                )
            }
        } catch {
            await container.shutdown()
            throw error
        }
        return Fixture(
            container: container,
            primaryBaseID: primaryBaseID,
            secondaryBaseID: secondaryBaseID,
            compositionID: compositionID,
            ownerAuthorization: ownerAuthorization,
            readerAuthorization: readerAuthorization,
            secondaryEngine: secondaryEngine
        )
    }

    private func insert(
        _ values: [(String, Int64)],
        baseID: Base.ID,
        fixture: Fixture
    ) async throws {
        let context = fixture.container.session(
            authorization: fixture.ownerAuthorization
        ).base(baseID).newContext()
        for value in values {
            var item = Item()
            item.id = value.0
            item.rank = value.1
            try context.insert(item)
        }
        try await context.save()
    }

    private func crossBaseQuery(
        fixture: Fixture,
        joinType: JoinType
    ) -> SelectQuery {
        SelectQuery(
            projection: .items([
                ProjectionItem(
                    .column(ColumnRef(table: "world", column: "rank")),
                    alias: "worldRank"
                ),
                ProjectionItem(
                    .column(ColumnRef(table: "tenant", column: "rank")),
                    alias: "tenantRank"
                ),
            ]),
            source: .join(
                JoinClause(
                    type: joinType,
                    left: .base(
                        fixture.primaryBaseID,
                        .table(
                            TableRef(
                                table: Item.persistableType,
                                alias: "world"
                            )
                        )
                    ),
                    right: .base(
                        fixture.secondaryBaseID,
                        .table(
                            TableRef(
                                table: Item.persistableType,
                                alias: "tenant"
                            )
                        )
                    ),
                    condition: .on(
                        .equal(
                            .column(
                                ColumnRef(table: "world", column: "id")
                            ),
                            .column(
                                ColumnRef(table: "tenant", column: "id")
                            )
                        )
                    )
                )
            )
        )
    }

    private func executeCompositionQuery(
        _ query: SelectQuery,
        source: CompositionDataSource,
        maximumIntermediateRows: UInt32 = 100
    ) async throws -> [CompositionResult<QueryRow>] {
        let readOptions = ReadExecutionOptions(
            pageSize: 8,
            budget: ExecutionBudget(
                maximumRows: 100,
                maximumWorkUnits: 100_000,
                maximumIntermediateRows: maximumIntermediateRows,
                maximumIntermediateBytes: 4 * 1_024 * 1_024,
                timeoutMilliseconds: 30_000
            )
        )
        return try await source.execute(
            query,
            options: readOptions
        )
    }
}
#endif
