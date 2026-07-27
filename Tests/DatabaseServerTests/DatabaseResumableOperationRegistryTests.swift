import DatabaseServer
import DatabaseTypes
import DatabaseWire
import Testing

@Suite("Database resumable operation registry")
struct DatabaseResumableOperationRegistryTests {
    @Test("same family and different kinds resolve independently")
    func sameFamilyDifferentKindsResolveIndependently() throws {
        let first = try AnyDatabaseResumableOperation(
            EmptyResumableOperation<FirstJob>()
        )
        let second = try AnyDatabaseResumableOperation(
            EmptyResumableOperation<SecondJob>()
        )
        let registry = try DatabaseResumableOperationRegistry(
            operations: [second, first]
        )

        #expect(registry.identifiers == [first.operation, second.operation])
        #expect(
            try registry.resolve(first.operation).operation == first.operation
        )
        #expect(
            try registry.resolve(second.operation).operation == second.operation
        )
    }

    @Test("exact duplicates are rejected")
    func exactDuplicateIsRejected() throws {
        let operation = try AnyDatabaseResumableOperation(
            EmptyResumableOperation<FirstJob>()
        )

        do {
            _ = try DatabaseResumableOperationRegistry(
                operations: [operation, operation]
            )
            Issue.record("Expected an exact duplicate error")
        } catch DatabaseResumableOperationRegistryError
            .duplicateOperation(let duplicate) {
            #expect(duplicate == operation.operation)
        }
    }

    @Test("an unregistered kind in a registered family is rejected")
    func unsupportedKindInRegisteredFamilyIsRejected() throws {
        let operation = try AnyDatabaseResumableOperation(
            EmptyResumableOperation<FirstJob>()
        )
        let registry = try DatabaseResumableOperationRegistry(
            operations: [operation]
        )
        let missing = try DatabaseOperations.maintenanceExecute.resumableJob(
            kind: MissingJob.kind
        ).identifier

        do {
            _ = try registry.resolve(missing)
            Issue.record("Expected an unsupported operation error")
        } catch DatabaseResumableOperationRegistryError
            .unsupportedOperation(let unsupported) {
            #expect(unsupported == missing)
        }
    }
}

private protocol EmptyJobKind {
    static var kind: String { get }
}

private enum FirstJob: EmptyJobKind {
    static let kind = "calendar.import.first"
}

private enum SecondJob: EmptyJobKind {
    static let kind = "calendar.import.second"
}

private enum MissingJob: EmptyJobKind {
    static let kind = "calendar.import.missing"
}

private struct EmptyPersistentJobPayload: PersistentJobPayload {
    func persistentJobValue()
        throws(PersistentJobPayloadError) -> FieldValue {
        .null
    }

    init() {}

    init(
        persistentJobValue: FieldValue
    ) throws(PersistentJobPayloadError) {
        guard persistentJobValue == .null else {
            throw .invalidValue("Expected null")
        }
    }
}

private struct EmptyResumableOperation<Job: EmptyJobKind>:
    DatabaseUnsuccessfulOutcomeIndependentOperation {
    typealias Request = MaintenanceExecuteOperation.Request
    typealias Response = MaintenanceExecuteOperation.Response
    typealias Plan = EmptyPersistentJobPayload
    typealias State = EmptyPersistentJobPayload

    static func job()
        throws(DatabaseWireError)
        -> JobOperation<Request, Response> {
        try DatabaseOperations.maintenanceExecute.resumableJob(kind: Job.kind)
    }

    func compile(
        _ request: Request,
        context: DatabaseResumableOperationStartContext
    ) async throws -> DatabasePreparedResumableJob<Plan, State> {
        _ = request
        _ = context
        return DatabasePreparedResumableJob(
            plan: EmptyPersistentJobPayload(),
            initialState: EmptyPersistentJobPayload(),
            sliceTimeoutMilliseconds: 1
        )
    }

    func runSlice(
        plan: Plan,
        state: State,
        maximumWorkUnits: UInt64,
        context: DatabaseResumableOperationContext
    ) async throws -> DatabaseResumableOperationSlice<State, Response> {
        _ = plan
        _ = state
        _ = maximumWorkUnits
        _ = context
        return .complete(
            completedWorkUnits: 0,
            result: .execution(
                MaintenanceExecuteOperation.ExecutionResult(
                    kind: .migrations,
                    completedWorkUnits: 0,
                    isComplete: true
                )
            )
        )
    }
}
