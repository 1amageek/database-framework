import DatabaseKit
import StorageKit
import Synchronization
import TestSupport
import Testing

@testable import DatabaseEngine

@Persistable
private struct RetainedPersistedModelResourceItem {
    var id: String
    var payload: String
}

@Persistable
private struct RetainedPersistedModelDefaultItem {
    var id: String
    var payload: String = String(repeating: "d", count: 8_192)
}

@Persistable
private struct RetainedPersistedModelSecuredDefaultItem: SecurityPolicy {
    var id: String
    var ownerID: String = "schema-owner"

    static func permitsRead(
        of resource: borrowing RetainedPersistedModelSecuredDefaultItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        RetainedPersistedModelSecurityObservation.record(
            ownerID: resource.ownerID
        )
        return resource.ownerID == context.principal?.identifier
    }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        true
    }

    static func permitsCreate(
        _ newResource: borrowing RetainedPersistedModelSecuredDefaultItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        true
    }

    static func permitsUpdate(
        from resource: borrowing RetainedPersistedModelSecuredDefaultItem,
        to newResource: borrowing RetainedPersistedModelSecuredDefaultItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        true
    }

    static func permitsDelete(
        _ resource: borrowing RetainedPersistedModelSecuredDefaultItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        true
    }
}

private enum RetainedPersistedModelSecurityObservation {
    private static let observedOwnerIDs = Mutex<[String]>([])

    static var ownerIDs: [String] {
        observedOwnerIDs.withLock { $0 }
    }

    static func reset() {
        observedOwnerIDs.withLock { $0.removeAll(keepingCapacity: true) }
    }

    static func record(ownerID: String) {
        observedOwnerIDs.withLock { $0.append(ownerID) }
    }
}

@Suite("Retained persisted model resource accounting")
struct RetainedPersistedModelResourceTests {
    @Test("Canonical model releases the overlapping decode claim")
    func canonicalModelRetainsOneDecodedFootprint() async throws {
        let entity = try RetainedPersistedModelResourceItem.schemaEntity
        let container = try await DBContainer.open(
            for: try Schema(entities: [entity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "retained-persisted-model-resource-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try EntityRuntimeDefinition(
                        RetainedPersistedModelResourceItem.self
                    ).registration()
                ]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(
            RetainedPersistedModelResourceItem(
                id: "retained",
                payload: String(repeating: "x", count: 2_048)
            )
        )
        try await context.save()

        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumWorkUnits: 10_000,
                maximumIntermediateRows: 16,
                maximumIntermediateBytes: 64 * 1_024
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        try await context.withTransaction(requiredAccess: .read) { transaction in
            let entry = try #require(
                try await transaction.loadRetainedPersistedModel(
                    entity: entity.name,
                    id: Tuple("retained"),
                    partition: nil,
                    snapshot: false,
                    workMeter: meter
                )
            )
            var independentFootprint: UInt64 = 0
            try entry.withModel { model in
                independentFootprint = try PersistableFieldFrameCodec
                    .retainedFootprint(of: model)
            }
            #expect(entry.retainedModelFootprint.bytes == independentFootprint)
            #expect(
                meter.retainedIntermediateBytes
                    == entry.retainedModelFootprint.bytes
            )
            withExtendedLifetime(entry) {}
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Stored-model admission fills missing fields from captured Swift defaults")
    func storedModelAdmissionIsSchemaOwnedAndExact() throws {
        let id = RetainedPersistedModelDefaultItem.fields.id.identity
        let payload = RetainedPersistedModelDefaultItem.fields.payload.identity
        let source = try PersistedModel(
            entity: RetainedPersistedModelDefaultItem.persistableType,
            fields: [
                try PersistableField(
                    number: try #require(UInt32(exactly: id.number)),
                    name: id.name,
                    value: .string("stored-id")
                ),
            ]
        )
        let sourceBytes = try PersistableFieldFrameCodec.retainedFootprint(
            of: source
        )
        let runtime = try EntityRuntimeDefinition(
            RetainedPersistedModelDefaultItem.self
        ).registration()
        let schemaDefault = try #require(
            runtime.entity.fieldMapByName[payload.name]?.defaultValue
        )
        #expect(schemaDefault == .string(String(repeating: "d", count: 8_192)))
        let directlyDecoded: RetainedPersistedModelDefaultItem = try DataAccess
            .deserialize(PersistableStorageCodec.encode(source))
        #expect(directlyDecoded.id == "stored-id")
        #expect(directlyDecoded.payload == String(repeating: "d", count: 8_192))
        let canonicalBytes = try PersistableFieldFrameCodec.retainedFootprint(
            of: PersistedModel(
                entity: RetainedPersistedModelDefaultItem.persistableType,
                fields: [
                    try PersistableField(
                        number: try #require(UInt32(exactly: id.number)),
                        name: id.name,
                        value: .string("stored-id")
                    ),
                    try PersistableField(
                        number: try #require(UInt32(exactly: payload.number)),
                        name: payload.name,
                        value: schemaDefault
                    ),
                ]
            )
        )

        let insufficientMeter = makeMeter(
            maximumIntermediateBytes: sourceBytes + canonicalBytes - 1
        )
        do {
            let reservation = try insufficientMeter.reserveIntermediate(
                bytes: sourceBytes,
                at: .storageRow
            )
            #expect {
                _ = try CanonicalStoredModelAdmission.admit(
                    source,
                    runtime: runtime,
                    reservation: reservation,
                    workMeter: insufficientMeter,
                    stage: .storageRow
                )
            } throws: { error in
                error is DatabaseWorkLimitError
            }
            #expect(
                insufficientMeter.retainedIntermediateBytes == sourceBytes
            )
            reservation.release()
        }
        #expect(insufficientMeter.retainedIntermediateBytes == 0)

        let exactMeter = makeMeter(maximumIntermediateBytes: UInt64.max)
        do {
            let reservation = try exactMeter.reserveIntermediate(
                bytes: sourceBytes,
                at: .storageRow
            )
            let admitted = try CanonicalStoredModelAdmission.admit(
                source,
                runtime: runtime,
                reservation: reservation,
                workMeter: exactMeter,
                stage: .storageRow
            )
            #expect(admitted.model.value(forFieldNamed: "id") == .string("stored-id"))
            #expect(admitted.model.value(forFieldNamed: "payload") == schemaDefault)
            #expect(admitted.retainedByteCount == canonicalBytes)
            #expect(
                exactMeter.retainedIntermediateBytes
                    == sourceBytes + canonicalBytes
            )
            reservation.releaseGuaranteedPartial(bytes: sourceBytes)
            #expect(exactMeter.retainedIntermediateBytes == canonicalBytes)
            reservation.release()
        }
        #expect(exactMeter.retainedIntermediateBytes == 0)

        let requiredID = RetainedPersistedModelResourceItem.fields.id.identity
        let requiredPayload = RetainedPersistedModelResourceItem.fields.payload.identity
        let requiredRuntime = try EntityRuntimeDefinition(
            RetainedPersistedModelResourceItem.self
        ).registration()
        let missingRequired = try PersistedModel(
            entity: RetainedPersistedModelResourceItem.persistableType,
            fields: [
                try PersistableField(
                    number: try #require(UInt32(exactly: requiredID.number)),
                    name: requiredID.name,
                    value: .string("missing")
                ),
            ]
        )
        let missingRequiredBytes = try PersistableFieldFrameCodec
            .retainedFootprint(of: missingRequired)
        let rejectingMeter = makeMeter(maximumIntermediateBytes: UInt64.max)
        do {
            let reservation = try rejectingMeter.reserveIntermediate(
                bytes: missingRequiredBytes,
                at: .storageRow
            )
            #expect {
                _ = try CanonicalStoredModelAdmission.admit(
                    missingRequired,
                    runtime: requiredRuntime,
                    reservation: reservation,
                    workMeter: rejectingMeter,
                    stage: .storageRow
                )
            } throws: { error in
                error as? SchemaDrivenEntityRuntimeError
                    == .missingRequiredField(
                        entity: RetainedPersistedModelResourceItem.persistableType,
                        field: requiredPayload.name
                    )
            }
            #expect(
                rejectingMeter.retainedIntermediateBytes
                    == missingRequiredBytes
            )
            reservation.release()
        }
        #expect(rejectingMeter.retainedIntermediateBytes == 0)

        let incorrectNumber = try #require(
            UInt32(exactly: payload.number)
        )
        let mismatchedIdentity = try PersistedModel(
            entity: RetainedPersistedModelDefaultItem.persistableType,
            fields: [
                try PersistableField(
                    number: incorrectNumber,
                    name: id.name,
                    value: .string("mismatched")
                ),
            ]
        )
        let mismatchedBytes = try PersistableFieldFrameCodec
            .retainedFootprint(of: mismatchedIdentity)
        let mismatchedFrame = try PersistableStorageCodec.encode(
            mismatchedIdentity
        )
        #expect(throws: PersistableDecodingError.self) {
            let _: RetainedPersistedModelDefaultItem = try DataAccess
                .deserialize(mismatchedFrame)
        }
        let identityMeter = makeMeter(maximumIntermediateBytes: UInt64.max)
        do {
            let reservation = try identityMeter.reserveIntermediate(
                bytes: mismatchedBytes,
                at: .storageRow
            )
            #expect {
                _ = try CanonicalStoredModelAdmission.admit(
                    mismatchedIdentity,
                    runtime: runtime,
                    reservation: reservation,
                    workMeter: identityMeter,
                    stage: .storageRow
                )
            } throws: { error in
                error as? SchemaDrivenEntityRuntimeError
                    == .fieldIdentityMismatch(
                        entity: RetainedPersistedModelDefaultItem.persistableType,
                        field: id.name,
                        expectedNumber: 1,
                        actualNumber: incorrectNumber
                    )
            }
            #expect(
                identityMeter.retainedIntermediateBytes == mismatchedBytes
            )
            reservation.release()
        }
        #expect(identityMeter.retainedIntermediateBytes == 0)
    }

    @Test("Reads authorize only canonical stored models")
    func readsCanonicalizeBeforeAuthorization() async throws {
        RetainedPersistedModelSecurityObservation.reset()
        let type = RetainedPersistedModelSecuredDefaultItem.self
        let entity = try type.schemaEntity
        let runtime = try EntityRuntimeDefinition(type).registration()
        let container = try await DBContainer.open(
            for: try Schema(entities: [entity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "retained-persisted-model-security-tests",
                    revision: 1
                ),
                entityRuntimes: [runtime],
                authorizationPolicies: [AuthorizationPolicyHandler(type)]
            ),
            security: .enabled()
        )
        defer { await container.shutdown() }

        #if MultiBase
        try await container.grantTestBaseAccess(
            to: .principal("schema-owner"),
            access: .read
        )
        #endif

        let id = type.fields.id.identity
        let ownerID = type.fields.ownerID.identity
        let storedID = "canonical-security"
        let missingDefault = try PersistedModel(
            entity: type.persistableType,
            fields: [
                try PersistableField(
                    number: try #require(UInt32(exactly: id.number)),
                    name: id.name,
                    value: .string(storedID)
                ),
            ]
        )
        try await writeStoredModel(
            missingDefault,
            id: storedID,
            container: container,
            type: type
        )

        let context = container.testBaseContext(
            authorization: .authenticated(
                Principal(identifier: "schema-owner")
            )
        )
        let pointRead = try #require(
            try await context.model(for: storedID, as: type)
        )
        #expect(pointRead.ownerID == "schema-owner")
        let scanRead = try await context.fetch(type).execute()
        #expect(scanRead.map(\.ownerID) == ["schema-owner"])

        let meter = makeMeter(maximumIntermediateBytes: UInt64.max)
        try await context.withTransaction(requiredAccess: .read) { transaction in
            let entry = try #require(
                try await transaction.loadRetainedPersistedModel(
                    entity: entity.name,
                    id: Tuple(storedID),
                    partition: nil,
                    snapshot: false,
                    workMeter: meter
                )
            )
            var ownerValue: FieldValue?
            entry.withModel { model in
                ownerValue = model.value(forFieldNamed: ownerID.name)
            }
            #expect(ownerValue == .string("schema-owner"))
            withExtendedLifetime(entry) {}
        }
        #expect(
            RetainedPersistedModelSecurityObservation.ownerIDs
                == ["schema-owner", "schema-owner", "schema-owner"]
        )

        RetainedPersistedModelSecurityObservation.reset()
        let expectedNumber = try #require(UInt32(exactly: id.number))
        let incorrectNumber = try #require(UInt32(exactly: ownerID.number))
        let mismatchedIdentity = try PersistedModel(
            entity: type.persistableType,
            fields: [
                try PersistableField(
                    number: incorrectNumber,
                    name: id.name,
                    value: .string(storedID)
                ),
            ]
        )
        try await writeStoredModel(
            mismatchedIdentity,
            id: storedID,
            container: container,
            type: type
        )

        await #expect {
            _ = try await context.model(for: storedID, as: type)
        } throws: { error in
            error as? SchemaDrivenEntityRuntimeError
                == .fieldIdentityMismatch(
                    entity: type.persistableType,
                    field: id.name,
                    expectedNumber: expectedNumber,
                    actualNumber: incorrectNumber
                )
        }
        await #expect {
            _ = try await context.fetch(type).execute()
        } throws: { error in
            guard case .fieldIdentityMismatch(let number, let name) =
                    error as? PersistableDecodingError else {
                return false
            }
            return number == incorrectNumber && name == id.name
        }
        await #expect {
            try await context.withTransaction(requiredAccess: .read) { transaction in
                _ = try await transaction.loadRetainedPersistedModel(
                    entity: entity.name,
                    id: Tuple(storedID),
                    partition: nil,
                    snapshot: false,
                    workMeter: meter
                )
            }
        } throws: { error in
            error as? SchemaDrivenEntityRuntimeError
                == .fieldIdentityMismatch(
                    entity: type.persistableType,
                    field: id.name,
                    expectedNumber: expectedNumber,
                    actualNumber: incorrectNumber
                )
        }
        #expect(RetainedPersistedModelSecurityObservation.ownerIDs.isEmpty)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("retained model decode failure releases every admission")
    func retainedModelDecodeFailureReleasesEveryAdmission() async throws {
        let entity = try RetainedPersistedModelResourceItem.schemaEntity
        let container = try await makeResourceContainer()
        defer { await container.shutdown() }
        try await writeRawPayload(
            ByteString([0xFF]),
            id: "malformed-retained-model",
            container: container
        )

        let meter = makeMeter(maximumIntermediateBytes: 64 * 1_024)
        await #expect {
            try await container.testBaseContext().withTransaction(
                requiredAccess: .read
            ) { transaction in
                _ = try await transaction.loadRetainedPersistedModel(
                    entity: entity.name,
                    id: Tuple("malformed-retained-model"),
                    partition: nil,
                    snapshot: false,
                    workMeter: meter
                )
            }
        } throws: { error in
            error as? PersistableFieldFrameError == .invalidMagic
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("retained model cancellation releases every admission")
    func retainedModelCancellationReleasesEveryAdmission() async throws {
        let type = RetainedPersistedModelResourceItem.self
        let runtime = try EntityRuntimeDefinition(type).registration()
        let id = type.fields.id.identity
        let payload = type.fields.payload.identity
        let model = try PersistedModel(
            entity: type.persistableType,
            fields: [
                try PersistableField(
                    number: try #require(UInt32(exactly: id.number)),
                    name: id.name,
                    value: .string("cancelled-retained-model")
                ),
                try PersistableField(
                    number: try #require(UInt32(exactly: payload.number)),
                    name: payload.name,
                    value: .string("decoded-before-cancellation")
                ),
            ]
        )
        let bytes = try PersistableStorageCodec.encode(model)
        let meter = makeMeter(maximumIntermediateBytes: 64 * 1_024)
        let barrier = StorageOperationBarrier()
        let task = Task {
            let retained = try DatabaseRetainedStoredModel.decode(
                bytes,
                entity: type.persistableType,
                runtime: runtime,
                workMeter: meter,
                stage: .storageRow
            )
            await barrier.enterAndWait()
            try Task.checkCancellation()
            withExtendedLifetime(retained) {}
        }
        let completionMonitor = try await barrier.waitUntilEntered(
            beforeCompletionOf: task
        )
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes > 0)

        task.cancel()
        barrier.release()
        await completionMonitor.value
        await #expect {
            try await task.value
        } throws: { error in
            error is CancellationError
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func writeStoredModel<Model: Persistable>(
        _ model: PersistedModel,
        id: String,
        container: DBContainer,
        type: Model.Type
    ) async throws {
        let root = try await container.testBaseDirectory(for: type)
        let items = root.subspace(SubspaceKey.items)
            .subspace(Model.persistableType)
        let blobs = root.subspace(SubspaceKey.blobs)
        let bytes = try PersistableStorageCodec.encode(model)
        try await container.engine.withTransaction { transaction in
            try await ItemStorage(
                transaction: transaction,
                blobsSubspace: blobs,
                configuration: .v1
            ).write(bytes, for: items.pack(Tuple(id)))
        }
    }

    private func writeRawPayload(
        _ bytes: ByteString,
        id: String,
        container: DBContainer
    ) async throws {
        let root = try await container.testBaseDirectory(
            for: RetainedPersistedModelResourceItem.self
        )
        let items = root.subspace(SubspaceKey.items)
            .subspace(RetainedPersistedModelResourceItem.persistableType)
        let blobs = root.subspace(SubspaceKey.blobs)
        try await container.engine.withTransaction { transaction in
            try await ItemStorage(
                transaction: transaction,
                blobsSubspace: blobs,
                configuration: .v1
            ).write(bytes, for: items.pack(Tuple(id)))
        }
    }

    private func makeResourceContainer() async throws -> DBContainer {
        let entity = try RetainedPersistedModelResourceItem.schemaEntity
        return try await DBContainer.open(
            for: try Schema(entities: [entity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "retained-model-resource-contract-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try EntityRuntimeDefinition(
                        RetainedPersistedModelResourceItem.self
                    ).registration()
                ]
            ),
            security: .testingDisabled
        )
    }

    private func makeMeter(
        maximumIntermediateBytes: UInt64
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumWorkUnits: 10_000,
                maximumIntermediateRows: 16,
                maximumIntermediateBytes: maximumIntermediateBytes
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }
}
