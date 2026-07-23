import DatabaseServer
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
        #expect(try registry.resolve(first.operation).operation == first.operation)
        #expect(try registry.resolve(second.operation).operation == second.operation)
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
        } catch {
            Issue.record("Unexpected error: \(error)")
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
        let missing = try MissingJob.jobOperationIdentifier()

        do {
            _ = try registry.resolve(missing)
            Issue.record("Expected an unsupported operation error")
        } catch DatabaseResumableOperationRegistryError
            .unsupportedOperation(let unsupported) {
            #expect(unsupported == missing)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    private struct FirstJob: EmptyJobDescriptor {
        static let kind = "calendar.import.first"
    }

    private struct SecondJob: EmptyJobDescriptor {
        static let kind = "calendar.import.second"
    }

    private struct MissingJob: EmptyJobDescriptor {
        static let kind = "calendar.import.missing"
    }
}

private protocol EmptyJobDescriptor: DatabaseJobDescriptor
where Request == DatabaseEmpty, Response == DatabaseEmpty {
    static var kind: String { get }
}

private extension EmptyJobDescriptor {
    static func jobOperationIdentifier()
        throws(DatabaseWireError) -> DatabaseJobOperationIdentifier {
        try DatabaseJobOperationIdentifier(
            family: .commandWrite,
            kind: kind
        )
    }
}

private struct EmptyResumableOperation<Job: EmptyJobDescriptor>:
    DatabaseResumableOperation {
    typealias Plan = DatabaseEmpty
    typealias State = DatabaseEmpty

    func compile(
        _ request: DatabaseEmpty,
        context: DatabaseResumableOperationStartContext
    ) async throws -> DatabasePreparedResumableJob<Plan, State> {
        _ = request
        _ = context
        return DatabasePreparedResumableJob(
            plan: DatabaseEmpty(),
            initialState: DatabaseEmpty(),
            sliceTimeoutMilliseconds: 1
        )
    }

    func runSlice(
        plan: DatabaseEmpty,
        state: DatabaseEmpty,
        maximumWorkUnits: UInt64,
        context: DatabaseResumableOperationContext
    ) async throws -> DatabaseResumableOperationSlice<State, Job.Response> {
        _ = plan
        _ = state
        _ = maximumWorkUnits
        _ = context
        return .complete(
            completedWorkUnits: 0,
            result: DatabaseEmpty()
        )
    }
}
