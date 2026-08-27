import DatabaseKit
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

@_spi(DatabaseExecution) @testable import DatabaseEngine

@Polymorphable(identifier: "PolymorphicRetainedResourceModel")
@PolymorphicDirectory("polymorphic-retained-resource-model")
private protocol PolymorphicRetainedResourceModel:
    Polymorphable<PolymorphicRetainedResourceModelPolymorphicGroup>
{
    var id: String { get }
    var payload: String { get }
}

@Persistable
private struct PolymorphicRetainedResourceItem:
    PolymorphicRetainedResourceModel
{
    #Directory<PolymorphicRetainedResourceItem>(
        "polymorphic-retained-resource-item"
    )

    let id: String
    let payload: String
}

@Suite("Polymorphic retained resource contract")
struct PolymorphicRetainedResourceContractTests {
    private enum CancellationEvent: Sendable {
        case barrierEntered
        case waiterCancelled
        case fetchCancelled
        case fetchCompleted
        case fetchFailed(String)
    }

    private struct Fixture: Sendable {
        let container: DBContainer
        let context: DatabaseContext
        let group: PolymorphicGroup
        let identifier: Tuple
        let readKey: ByteString
        let payload: String
    }

    @Test("Public scan and exact-ID output consume retained ownership")
    func publicOutputBoundariesPreserveModelsAndMetadata() async throws {
        let fixture = try await makeFixture(storageEngine: InMemoryEngine())
        defer { await fixture.container.shutdown() }
        let missing = try missingIdentifier(for: fixture)

        let scanned = try await fixture.context.fetchPolymorphic(
            PolymorphicRetainedResourceItem.self
        )
        #expect(scanned.count == 1)
        #expect(
            try scanned[0]
                .decode(as: PolymorphicRetainedResourceItem.self).payload
                == fixture.payload
        )

        let fetched = try await fixture.context.fetchPolymorphicItems(
            group: fixture.group,
            ids: [fixture.identifier, missing]
        )
        #expect(fetched.count == 1)
        #expect(
            try fetched[0].item
                .decode(as: PolymorphicRetainedResourceItem.self).payload
                == fixture.payload
        )
        #expect(
            fetched[0].typeName
                == PolymorphicRetainedResourceItem.persistableType
        )
        #expect(
            fetched[0].typeCode
                == PolymorphicTypeCode.value(
                    for: PolymorphicRetainedResourceItem.persistableType
                )
        )
        #expect(fetched[0].polymorphicIdentifier == fixture.identifier)
    }

    @Test("Retained ordered fetch preserves present and missing slots")
    func orderedFetchPreservesMissingSlots() async throws {
        let fixture = try await makeFixture(storageEngine: InMemoryEngine())
        defer { await fixture.container.shutdown() }
        let meter = makeMeter()
        let missing = try missingIdentifier(for: fixture)

        let observation = try await withAuthorizedSnapshot(
            fixture: fixture,
            workMeter: meter
        ) { session in
            var payload: FieldValue?
            var missingWasPresent = true
            do {
                let identifiers = try retainedIdentifiers(
                    [fixture.identifier, missing],
                    workMeter: meter
                )
                let fetched = try await session
                    .fetchRetainedPolymorphicItemsPreservingOrder(
                        group: fixture.group,
                        ids: identifiers,
                        snapshot: true
                    )
                #expect(fetched.count == 2)
                let result = try IndexReadResult.build(
                    workMeter: meter,
                    expectedCount: 2
                ) { rows in
                    #expect(
                        try fetched.appendIndexRow(at: 0, to: &rows)
                    )
                    missingWasPresent = try fetched.appendIndexRow(
                        at: 1,
                        to: &rows
                    )
                }
                #expect(result.count == 1)
                result.withRow(at: 0) { row in
                    payload = row.fields["payload"]
                }
                #expect(meter.retainedIntermediateRows > 0)
                #expect(meter.retainedIntermediateBytes > 0)
            }
            return (payload, missingWasPresent)
        }

        #expect(observation.0 == .string(fixture.payload))
        #expect(observation.1 == false)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Foreign identifier meter is rejected before a value read")
    func foreignIdentifierMeterIsRejectedBeforeRead() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let fixture = try await makeFixture(storageEngine: storage)
        defer { await fixture.container.shutdown() }
        let sessionMeter = makeMeter()
        let foreignMeter = makeMeter()

        try await withAuthorizedSnapshot(
            fixture: fixture,
            workMeter: sessionMeter
        ) { session in
            do {
                let identifiers = try retainedIdentifiers(
                    [fixture.identifier],
                    workMeter: foreignMeter
                )
                let readsBefore = storage.control.boundedValueReadMaximums.count
                await #expect(
                    throws:
                        DatabaseIntermediateReservationError.workMeterMismatch
                ) {
                    _ = try await session
                        .fetchRetainedPolymorphicItemsPreservingOrder(
                            group: fixture.group,
                            ids: identifiers,
                            snapshot: true
                        )
                }
                #expect(
                    storage.control.boundedValueReadMaximums.count
                        == readsBefore
                )
            }
        }
        #expect(sessionMeter.retainedIntermediateRows == 0)
        #expect(sessionMeter.retainedIntermediateBytes == 0)
        #expect(foreignMeter.retainedIntermediateRows == 0)
        #expect(foreignMeter.retainedIntermediateBytes == 0)
    }

    @Test("Incomplete field authority is rejected before a value read")
    func incompleteFieldAuthorityIsRejectedBeforeRead() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let fixture = try await makeFixture(storageEngine: storage)
        defer { await fixture.container.shutdown() }
        let meter = makeMeter()

        try await withAuthorizedSnapshot(
            fixture: fixture,
            authorization: { policy in
                try policy.authorizeRead(
                    listRequirements: [],
                    fields: DatabaseFieldReadAuthorizationPlan(
                        fieldsByEntity: [
                            PolymorphicRetainedResourceItem
                                .persistableType: []
                        ]
                    )
                )
            },
            workMeter: meter
        ) { session in
            let identifiers = try retainedIdentifiers(
                [fixture.identifier],
                workMeter: meter
            )
            let readsBefore = storage.control.boundedValueReadMaximums.count
            await #expect(
                throws: DatabaseReadSessionError.authorizationMismatch
            ) {
                _ = try await session
                    .fetchRetainedPolymorphicItemsPreservingOrder(
                        group: fixture.group,
                        ids: identifiers,
                        snapshot: true
                    )
            }
            #expect(
                storage.control.boundedValueReadMaximums.count == readsBefore
            )
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Later invalid identifier releases earlier retained results")
    func laterIdentifierFailureReleasesEarlierResults() async throws {
        let fixture = try await makeFixture(storageEngine: InMemoryEngine())
        defer { await fixture.container.shutdown() }
        let meter = makeMeter()
        let knownTypeCode = try typeCode(in: fixture.identifier)
        let unknownTypeCode = knownTypeCode == Int64.max
            ? Int64.min
            : knownTypeCode + 1

        await #expect(
            throws: PolymorphicRuntimeError.unknownTypeCode(unknownTypeCode)
        ) {
            _ = try await withAuthorizedSnapshot(
                fixture: fixture,
                workMeter: meter
            ) { session in
                let identifiers = try retainedIdentifiers(
                    [
                        fixture.identifier,
                        Tuple(unknownTypeCode, "unknown"),
                    ],
                    workMeter: meter
                )
                let entities = try await session
                    .fetchRetainedPolymorphicItemsPreservingOrder(
                        group: fixture.group,
                        ids: identifiers,
                        snapshot: true
                    )
                return entities.promoteModelsToPublicOutput()
            }
        }
        #expect(meter.peakIntermediateRows > 0)
        #expect(meter.peakIntermediateBytes > 0)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Scan admits its aggregate before opening a cursor")
    func scanAdmitsAggregateBeforeCursor() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let fixture = try await makeFixture(storageEngine: storage)
        defer { await fixture.container.shutdown() }
        let maximumBytes = DatabaseReadSession
            .scopeCursorRegistryContainerByteCount
        let meter = makeMeter(maximumIntermediateBytes: maximumBytes)
        let openedBefore = storage.control.openedRangeCursorCount

        await #expect {
            _ = try await withAuthorizedSnapshot(
                fixture: fixture,
                workMeter: meter
            ) { session in
                let entities = try await session.scanRetainedPolymorphicItems(
                    group: fixture.group,
                    selectQuery: selectQuery(for: fixture.group)
                )
                return entities.promoteModelsToPublicOutput()
            }
        } throws: { error in
            guard case DatabaseWorkLimitError.maximumIntermediateBytes(
                stage: .storageRow,
                consumed: _,
                requested: _,
                maximum: let reportedMaximum
            ) = error else {
                return false
            }
            return reportedMaximum == maximumBytes
        }
        #expect(storage.control.openedRangeCursorCount == openedBefore)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Cancellation after the point read releases every claim")
    func cancellationReleasesEveryClaim() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let fixture = try await makeFixture(storageEngine: storage)
        defer { await fixture.container.shutdown() }
        let meter = makeMeter()
        let barrier = storage.control.suspendNextValueRead(
            for: fixture.readKey
        )

        await withTaskGroup(of: CancellationEvent.self) { group in
            group.addTask {
                do {
                    try await barrier.waitUntilEnteredOrCancellation()
                    return .barrierEntered
                } catch is CancellationError {
                    return .waiterCancelled
                } catch {
                    return .fetchFailed(String(describing: error))
                }
            }
            group.addTask {
                do {
                    _ = try await withAuthorizedSnapshot(
                        fixture: fixture,
                        workMeter: meter
                    ) { session in
                        let identifiers = try retainedIdentifiers(
                            [fixture.identifier],
                            workMeter: meter
                        )
                        let entities = try await session
                            .fetchRetainedPolymorphicItemsPreservingOrder(
                                group: fixture.group,
                                ids: identifiers,
                                snapshot: true
                            )
                        return entities.promoteModelsToPublicOutput()
                    }
                    return .fetchCompleted
                } catch is CancellationError {
                    return .fetchCancelled
                } catch {
                    return .fetchFailed(String(describing: error))
                }
            }

            guard case .barrierEntered = await group.next() else {
                group.cancelAll()
                barrier.release()
                while await group.next() != nil {}
                Issue.record("Fetch did not reach the value-read barrier")
                return
            }
            #expect(meter.retainedIntermediateBytes > 0)
            group.cancelAll()
            barrier.release()
            var observedCancellation = false
            while let event = await group.next() {
                switch event {
                case .fetchCancelled:
                    observedCancellation = true
                case .fetchCompleted:
                    Issue.record("Cancelled fetch completed")
                case .fetchFailed(let description):
                    Issue.record("Cancelled fetch failed: \(description)")
                case .barrierEntered, .waiterCancelled:
                    break
                }
            }
            #expect(observedCancellation)
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func withAuthorizedSnapshot<Result: Sendable>(
        fixture: Fixture,
        workMeter: DatabaseWorkMeter,
        _ operation: @Sendable @escaping (DatabaseReadSession) async throws
            -> Result
    ) async throws -> Result {
        try await fixture.context.withDataOperation {
            let query = selectQuery(for: fixture.group)
            let authorization = try fixture.context.readPolicy()
                .authorizePolymorphicModelScan(
                    group: fixture.group,
                    selectQuery: query
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

    private func withAuthorizedSnapshot<Result: Sendable>(
        fixture: Fixture,
        authorization: @Sendable (DatabaseReadPolicy) throws
            -> DatabaseReadAuthorization,
        workMeter: DatabaseWorkMeter,
        _ operation: @Sendable @escaping (DatabaseReadSession) async throws
            -> Result
    ) async throws -> Result {
        try await fixture.context.withDataOperation {
            let admitted = try authorization(
                fixture.context.readPolicy()
            )
            return try await fixture.context.withReadSnapshot(
                workMeter: workMeter
            ) { snapshot in
                try await operation(
                    try snapshot.session.authorizedSession(admitted)
                )
            }
        }
    }

    private func retainedIdentifiers(
        _ identifiers: [Tuple],
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseSharedRetainedArray<Tuple> {
        var builder = try DatabaseRetainedArrayBuilder<Tuple>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try DatabaseRetainedArrayLayout.forElement(Tuple.self),
            expectedCount: identifiers.count
        )
        for identifier in identifiers {
            try builder.append(
                footprint: DatabaseIntermediateFootprint(
                    rows: 1,
                    bytes: UInt64(identifier.packedByteCount + 32)
                ),
                at: .indexScan,
                make: { identifier }
            )
        }
        return try builder.finish().moveToSharedOwnership(at: .indexScan)
    }

    private func makeFixture(
        storageEngine: any StorageEngine
    ) async throws -> Fixture {
        let entity = try PolymorphicRetainedResourceItem.schemaEntity
        let container = try await DBContainer.open(
            for: try Schema(entities: [entity]),
            configuration: .testing(storageEngine: storageEngine),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "polymorphic-retained-resource-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try EntityRuntimeDefinition(
                        PolymorphicRetainedResourceItem.self
                    ).registration()
                ]
            ),
            security: .testingDisabled
        )
        do {
            let context = container.testBaseContext()
            let payload = String(repeating: "x", count: 2_048)
            let item = PolymorphicRetainedResourceItem(
                id: "retained",
                payload: payload
            )
            try context.insert(item)
            try await context.save()
            let group = try container.polymorphicGroup(
                identifier: PolymorphicRetainedResourceItem.polymorphableType
            )
            let identifier = try PolymorphicIdentifierKey.tuple(
                for: entity,
                identifier: try item.persistableIdentifierTuple()
            )
            let groupSubspace = try await container
                .testBasePolymorphicDirectory(for: group.identifier)
            return Fixture(
                container: container,
                context: context,
                group: group,
                identifier: identifier,
                readKey: groupSubspace
                    .subspace(SubspaceKey.items)
                    .pack(identifier),
                payload: payload
            )
        } catch {
            await container.shutdown()
            throw error
        }
    }

    private func missingIdentifier(for fixture: Fixture) throws -> Tuple {
        Tuple(try typeCode(in: fixture.identifier), "missing")
    }

    private func selectQuery(for group: PolymorphicGroup) -> SelectQuery {
        SelectQuery(
            projection: .all,
            source: .logical(
                LogicalSourceRef(
                    kindIdentifier: LogicalSourceKind.polymorphic,
                    identifier: group.identifier
                )
            )
        )
    }

    private func typeCode(in identifier: Tuple) throws -> Int64 {
        guard case .signedInteger(let typeCode) = try identifier.value(at: 0)
        else {
            throw PolymorphicRuntimeError.invalidRequestedIdentifier
        }
        return typeCode
    }

    private func makeMeter(
        maximumIntermediateRows: UInt32 = 32,
        maximumIntermediateBytes: UInt64 = 256 * 1_024
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumWorkUnits: 20_000,
                maximumIntermediateRows: maximumIntermediateRows,
                maximumIntermediateBytes: maximumIntermediateBytes
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }
}
