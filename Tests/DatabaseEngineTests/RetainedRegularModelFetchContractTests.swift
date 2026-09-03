import DatabaseKit
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Persistable
private struct RetainedRegularFetchItem {
    var id: String
    var payload: String
}

@Suite("Retained regular model fetch contract")
struct RetainedRegularModelFetchContractTests {
    @Test("retained fetch preserves order, duplicates, missing slots, snapshot, and release")
    func retainedFetchPreservesSemanticsAndRelease() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let meter = makeMeter()
        var identifiers: RetainedRegularFetchPrimaryKeys? = try .init(
            keys: [
                Tuple("second"),
                Tuple("missing"),
                Tuple("first"),
                Tuple("second"),
            ],
            workMeter: meter
        )
        var models: DatabaseRetainedPersistedModels?

        models = try await fetch(
            fixture: fixture,
            primaryKeys: try #require(identifiers),
            snapshot: true,
            workMeter: meter
        )

        do {
            let retained = try #require(models)
            #expect(retained.count == 4)
            var observed: [FieldValue?] = []
            for index in 0..<retained.count {
                retained.withEntry(at: index) { entry in
                    var identity: FieldValue?
                    entry?.withModel { model in
                        identity = model.value(forFieldNamed: "id")
                    }
                    observed.append(identity)
                }
            }
            #expect(
                observed == [
                    .string("second"),
                    nil,
                    .string("first"),
                    .string("second"),
                ]
            )
            #expect(!fixture.control.boundedValueReadSnapshots.isEmpty)
            #expect(fixture.control.boundedValueReadSnapshots.allSatisfy { $0 })
            #expect(meter.retainedIntermediateRows > 0)
            #expect(meter.retainedIntermediateBytes > 0)
        }

        models = nil
        identifiers = nil
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("foreign source meter is rejected before authorization, storage, or destination allocation")
    func foreignMeterIsRejectedBeforeEffects() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let sessionMeter = makeMeter()
        let foreignMeter = makeMeter()
        let identifiers = try RetainedRegularFetchPrimaryKeys(
            keys: [Tuple("first")],
            workMeter: foreignMeter
        )
        let pointReadsBefore = fixture.control.boundedValueReadMaximums.count

        await #expect(throws:
            DatabaseIntermediateReservationError.workMeterMismatch
        ) {
            _ = try await fixture.context.withDataOperation {
                try await fixture.context.withReadSnapshot(
                    workMeter: sessionMeter
                ) { snapshot in
                    try await snapshot.session
                        .fetchRetainedPersistedModelsPreservingOrder(
                            entity: fixture.entity,
                            primaryKeys: identifiers,
                            partitions: FieldObject(),
                            snapshot: false
                        )
                }
            }
        }
        #expect(
            fixture.control.boundedValueReadMaximums.count == pointReadsBefore
        )
        #expect(sessionMeter.retainedIntermediateRows == 0)
        #expect(sessionMeter.retainedIntermediateBytes == 0)
        withExtendedLifetime(identifiers) {}
    }

    @Test("partial field evidence is rejected before storage")
    func incompleteAuthorizationIsRejectedBeforeStorage() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let meter = makeMeter()
        let identifiers = try RetainedRegularFetchPrimaryKeys(
            keys: [Tuple("first")],
            workMeter: meter
        )
        let pointReadsBefore = fixture.control.boundedValueReadMaximums.count

        await #expect(throws: DatabaseReadSessionError.authorizationMismatch) {
            _ = try await withAuthorizedSession(
                fixture: fixture,
                fields: DatabaseFieldReadAuthorizationPlan(
                    fieldsByEntity: [fixture.entity.name: ["id"]]
                ),
                workMeter: meter
            ) { session in
                try await session.fetchRetainedPersistedModelsPreservingOrder(
                    entity: fixture.entity,
                    primaryKeys: identifiers,
                    partitions: FieldObject(),
                    snapshot: false
                )
            }
        }
        #expect(
            fixture.control.boundedValueReadMaximums.count == pointReadsBefore
        )
        #expect(meter.retainedIntermediateRows == 1)
        #expect(meter.retainedIntermediateBytes == 32)
        withExtendedLifetime(identifiers) {}
    }

    @Test("destination allocation denial occurs before storage")
    func destinationAllocationDenialOccursBeforeStorage() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let maximumBytes = DatabaseReadSession
            .scopeCursorRegistryContainerByteCount + 32
        let meter = makeMeter(maximumIntermediateBytes: maximumBytes)
        let identifiers = try RetainedRegularFetchPrimaryKeys(
            keys: [Tuple("first")],
            workMeter: meter
        )
        let pointReadsBefore = fixture.control.boundedValueReadMaximums.count

        await #expect(throws: DatabaseWorkLimitError.self) {
            _ = try await withAuthorizedSession(
                fixture: fixture,
                workMeter: meter
            ) { session in
                try await session.fetchRetainedPersistedModelsPreservingOrder(
                    entity: fixture.entity,
                    primaryKeys: identifiers,
                    partitions: FieldObject(),
                    snapshot: false
                )
            }
        }
        #expect(
            fixture.control.boundedValueReadMaximums.count == pointReadsBefore
        )
        #expect(meter.retainedIntermediateRows == 1)
        #expect(meter.retainedIntermediateBytes == 32)
        withExtendedLifetime(identifiers) {}
    }

    @Test("later decode failure releases the partial destination")
    func laterFailureReleasesPartialDestination() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        try await writeMalformedItem(id: "malformed", fixture: fixture)
        let meter = makeMeter()

        do {
            let identifiers = try RetainedRegularFetchPrimaryKeys(
                keys: [Tuple("first"), Tuple("malformed")],
                workMeter: meter
            )
            await #expect(throws: PersistableFieldFrameError.self) {
                _ = try await withAuthorizedSession(
                    fixture: fixture,
                    workMeter: meter
                ) { session in
                    try await session
                        .fetchRetainedPersistedModelsPreservingOrder(
                            entity: fixture.entity,
                            primaryKeys: identifiers,
                            partitions: FieldObject(),
                            snapshot: false
                        )
                }
            }
            #expect(meter.retainedIntermediateRows == 2)
            #expect(meter.retainedIntermediateBytes == 64)
            withExtendedLifetime(identifiers) {}
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("cancellation retains the source through suspension and releases every claim")
    func cancellationRetainsSourceAndReleasesEveryClaim() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let meter = makeMeter()
        let itemKey = try await storageKey(id: "first", fixture: fixture)
        let barrier = fixture.control.suspendNextValueRead(for: itemKey)

        try await fixture.context.withDataOperation {
            let authorization = try fixture.context.readPolicy().authorizeRead(
                listRequirements: [],
                fields: fields(for: fixture.entity)
            )
            try await fixture.context.withReadSnapshot(
                workMeter: meter
            ) { snapshot in
                let session = try snapshot.session.authorizedSession(
                    authorization
                )
                let task = makeFetchTask(
                    session: session,
                    fixture: fixture,
                    keys: try RetainedRegularFetchPrimaryKeys(
                        keys: [Tuple("first")],
                        workMeter: meter
                    )
                )
                let monitor = try await barrier.waitUntilEntered(
                    beforeCompletionOf: task
                )
                #expect(meter.retainedIntermediateRows == 1)
                #expect(meter.retainedIntermediateBytes > 0)

                task.cancel()
                barrier.release()
                await monitor.value
                await #expect(throws: CancellationError.self) {
                    _ = try await task.value
                }
            }
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func makeFetchTask(
        session: DatabaseReadSession,
        fixture: Fixture,
        keys: RetainedRegularFetchPrimaryKeys
    ) -> Task<DatabaseRetainedPersistedModels, any Error> {
        Task {
            try await session.fetchRetainedPersistedModelsPreservingOrder(
                entity: fixture.entity,
                primaryKeys: keys,
                partitions: FieldObject(),
                snapshot: false
            )
        }
    }

    private func fetch(
        fixture: Fixture,
        primaryKeys: RetainedRegularFetchPrimaryKeys,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseRetainedPersistedModels {
        try await withAuthorizedSession(
            fixture: fixture,
            workMeter: workMeter
        ) { session in
            try await session.fetchRetainedPersistedModelsPreservingOrder(
                entity: fixture.entity,
                primaryKeys: primaryKeys,
                partitions: FieldObject(),
                snapshot: snapshot
            )
        }
    }

    private func withAuthorizedSession<Result: Sendable>(
        fixture: Fixture,
        fields: DatabaseFieldReadAuthorizationPlan? = nil,
        workMeter: DatabaseWorkMeter,
        _ operation: @Sendable @escaping (DatabaseReadSession) async throws
            -> Result
    ) async throws -> Result {
        try await fixture.context.withDataOperation {
            let authorization = try fixture.context.readPolicy().authorizeRead(
                listRequirements: [],
                fields: fields ?? self.fields(for: fixture.entity)
            )
            return try await fixture.context.withReadSnapshot(
                workMeter: workMeter
            ) { snapshot in
                try await operation(
                    try snapshot.session.authorizedSession(authorization)
                )
            }
        }
    }

    private func fields(
        for entity: Schema.Entity
    ) -> DatabaseFieldReadAuthorizationPlan {
        DatabaseFieldReadAuthorizationPlan(
            fieldsByEntity: [entity.name: Set(entity.allFields)]
        )
    }

    private func makeFixture() async throws -> Fixture {
        let entity = try RetainedRegularFetchItem.schemaEntity
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await DBContainer.open(
            for: try Schema(entities: [entity]),
            configuration: .testing(storageEngine: storage),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "retained-regular-fetch-contract-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try EntityRuntimeDefinition(
                        RetainedRegularFetchItem.self
                    ).registration()
                ]
            ),
            security: .testingDisabled
        )
        let context = container.testBaseContext()
        try context.insert(
            RetainedRegularFetchItem(id: "first", payload: "first-payload")
        )
        try context.insert(
            RetainedRegularFetchItem(id: "second", payload: "second-payload")
        )
        try await context.save()
        return Fixture(
            container: container,
            context: context,
            entity: entity,
            control: storage.control
        )
    }

    private func writeMalformedItem(
        id: String,
        fixture: Fixture
    ) async throws {
        let root = try await fixture.container.testBaseDirectory(
            for: RetainedRegularFetchItem.self
        )
        let items = root.subspace(SubspaceKey.items)
            .subspace(RetainedRegularFetchItem.persistableType)
        let blobs = root.subspace(SubspaceKey.blobs)
        try await fixture.container.engine.withTransaction { transaction in
            try await ItemStorage(
                transaction: transaction,
                blobsSubspace: blobs,
                configuration: .v1
            ).write(ByteString([0x00, 0x01, 0x02]), for: items.pack(Tuple(id)))
        }
    }

    private func storageKey(
        id: String,
        fixture: Fixture
    ) async throws -> ByteString {
        let root = try await fixture.container.testBaseDirectory(
            for: RetainedRegularFetchItem.self
        )
        return root.subspace(SubspaceKey.items)
            .subspace(RetainedRegularFetchItem.persistableType)
            .pack(Tuple(id))
    }

    private func makeMeter(
        maximumIntermediateBytes: UInt64 = 1 << 20
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumWorkUnits: 10_000,
                maximumIntermediateRows: 128,
                maximumIntermediateBytes: maximumIntermediateBytes
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private struct Fixture: Sendable {
        let container: DBContainer
        let context: DatabaseContext
        let entity: Schema.Entity
        let control: StorageTransactionControl
    }
}

private final class RetainedRegularFetchPrimaryKeys:
    DatabaseRetainedPrimaryKeyCollection,
    Sendable
{
    private let keys: [Tuple]
    private let reservation: DatabaseIntermediateReservation

    init(
        keys: [Tuple],
        workMeter: DatabaseWorkMeter
    ) throws {
        self.keys = keys
        self.reservation = try workMeter.reserveIntermediate(
            rows: UInt64(keys.count),
            bytes: UInt64(keys.count * 32),
            at: .indexScan
        )
    }

    package var count: Int { keys.count }
    package var workMeter: DatabaseWorkMeter { reservation.workMeter }

    package func withRetainedPrimaryKey(
        at position: Int,
        _ body: (borrowing Tuple) throws -> Void
    ) rethrows {
        precondition(position >= keys.startIndex && position < keys.endIndex)
        try body(keys[position])
        withExtendedLifetime(reservation) {}
    }

    package func withRetainedPrimaryKey(
        at position: Int,
        _ body: (borrowing Tuple) async throws -> Void
    ) async rethrows {
        precondition(position >= keys.startIndex && position < keys.endIndex)
        defer { withExtendedLifetime(reservation) {} }
        try await body(keys[position])
    }
}
