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
        let workMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 100,
                maximumWorkUnits: 100_000,
                maximumIntermediateRows: 16,
                maximumIntermediateBytes: 1 * 1_024 * 1_024,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        let workspace = CompositionDistinctWorkspace.create(
            maximumIntermediateBytes: 1 * 1_024 * 1_024,
            workMeter: workMeter,
            identityFingerprint: { _ in
                ByteString(repeating: 0x42, count: 32)
            }
        )
        let output = Mutex<[CompositionDistinctWorkspace.Result]>([])

        do {
            try await workspace.insert(
                QueryRow(
                    fields: ["priority": .int64(1)],
                    annotations: ["representative": .string("first")],
                    version: PersistableVersionToken("version-1")
                ),
                origin: .source(fixture.primaryBaseID),
                sequence: 0
            )
            try await workspace.insert(
                QueryRow(fields: ["priority": .int64(2)]),
                origin: .source(fixture.secondaryBaseID),
                sequence: 1
            )
            try await workspace.insert(
                QueryRow(
                    fields: ["priority": .int64(1)],
                    annotations: ["representative": .string("later")],
                    version: PersistableVersionToken("version-2")
                ),
                origin: .source(fixture.secondaryBaseID),
                sequence: 2
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
        #expect(workMeter.peakIntermediateRows == 2)
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
        let secondaryPlacementID = try Base.Placement.ID("secondary")
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
                namespacePath: ["tests", "composition", "control"],
                storageEngine: controlEngine
            )
        ]
        if !secondarySharesControlDomain {
            domains.append(
                try DatabaseStorageDomain(
                    id: secondaryDomainID,
                    namespacePath: ["tests", "composition", "secondary"],
                    storageEngine: secondaryEngine
                )
            )
        }
        let topology = try DatabaseStorageTopology(
            controlDomainID: controlDomainID,
            domains: domains,
            placements: [
                try DatabaseStoragePlacement(
                    id: primaryPlacementID,
                    domainID: controlDomainID,
                    path: ["bases"]
                ),
                try DatabaseStoragePlacement(
                    id: secondaryPlacementID,
                    domainID: secondarySharesControlDomain
                        ? controlDomainID
                        : secondaryDomainID,
                    path: secondarySharesControlDomain
                        ? ["secondary-bases"]
                        : ["bases"]
                ),
            ],
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
