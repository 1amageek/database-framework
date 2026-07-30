import DatabaseKit
import TestSupport
import DatabaseRuntime
import DatabaseEngine
@testable import DatabaseServer
import DatabaseServerFoundation
import DatabaseTypes
import DatabaseWire
import StorageKit
import Testing

@Suite("Database server runtime", .serialized)
struct DatabaseServerRuntimeTests {
    @Test("runtime registers every canonical operation handler")
    func registersEveryOperationHandler() async throws {
        let container = try await makeContainer()
        let runtime = try await makeRuntime(container: container)

        #expect(
            try await container.getCurrentSchemaVersion()
                == container.schema.version
        )
        let response = try await invoke(
            DatabaseOperations.capabilitiesDescribe,
            request: EmptyOperationPayload(),
            requestID: 1,
            runtime: runtime
        )

        #expect(response.runtimeVersion == "test-runtime")
        #expect(
            response.jobOperations == [
                try JobOperationIdentifier(
                    family: .commandExecute,
                    kind: "database.test.runtime-job"
                ),
            ]
        )
    }

    @Test("write commands are atomic and replay idempotently")
    func writeCommandUsesSharedTransactionalCoordinator() async throws {
        let container = try await makeContainer()
        let command = try CountingCommand(stateID: "command-count")
        let runtime = try await makeRuntime(
            container: container,
            writeCommands: [AnyDatabaseWriteCommand(command)]
        )
        let request = try commandRequest(
            declaration: command.declaration,
            value: "same-input"
        )
        let metadata = OperationRequestMetadata(
            idempotencyKey: "command-key"
        )

        let first = try await invoke(
            DatabaseOperations.commandExecute,
            request: request,
            requestID: 2,
            metadata: metadata,
            runtime: runtime
        )
        let second = try await invoke(
            DatabaseOperations.commandExecute,
            request: request,
            requestID: 3,
            metadata: metadata,
            runtime: runtime
        )
        let storedCount = try await container.newContext().model(
            for: command.stateID,
            as: DatabaseEndpointEntity.self
        )

        guard case .write(
            let firstOutput,
            let firstCommitVersion,
            nil
        ) = first,
        case .write(
            let secondOutput,
            let secondCommitVersion,
            nil
        ) = second else {
            Issue.record("Expected successful write command responses")
            return
        }
        #expect(firstOutput == .uint8(1))
        #expect(firstCommitVersion == 1)
        #expect(secondOutput == .uint8(1))
        #expect(secondCommitVersion == 1)
        #expect(storedCount?.priority == 1)
    }

    @Test("an idempotency key cannot be reused with a different payload")
    func idempotencyConflictIsTyped() async throws {
        let container = try await makeContainer()
        let command = try CountingCommand(stateID: "conflict-count")
        let runtime = try await makeRuntime(
            container: container,
            writeCommands: [AnyDatabaseWriteCommand(command)]
        )
        let metadata = OperationRequestMetadata(
            idempotencyKey: "conflict-key"
        )
        let first = try makeRequest(
            operation: DatabaseOperations.commandExecute,
            requestID: 3,
            metadata: metadata,
            request: commandRequest(
                declaration: command.declaration,
                value: "first"
            )
        )
        let conflicting = try makeRequest(
            operation: DatabaseOperations.commandExecute,
            requestID: 4,
            metadata: metadata,
            request: commandRequest(
                declaration: command.declaration,
                value: "second"
            )
        )

        _ = try await runtime.execute(first)
        let responseBytes = try await runtime.execute(conflicting)
        let response = try DatabaseWireDecoder().decodeResponse(
            DatabaseOperations.commandExecute,
            from: responseBytes,
            matching: 4
        )

        guard case .failure(let error) = response else {
            Issue.record("Expected an idempotency conflict")
            return
        }
        #expect(error.category == .conflict)
        #expect(error.code == "IDEMPOTENCY_KEY_CONFLICT")
    }

    @Test("oversized final responses roll back mutations and idempotency state")
    func oversizedResponseRollsBackTransaction() async throws {
        let limits = try DatabaseWireLimits(
            maximumFrameBytes: 512,
            maximumStringBytes: 256,
            maximumByteStringBytes: 1_024,
            maximumCollectionCount: 100,
            maximumNestingDepth: 16,
            maximumObjectCount: 100
        )
        let container = try await makeContainer()
        let command = try OversizedResponseCommand(
            stateID: "oversized-response"
        )
        let runtime = try await makeRuntime(
            container: container,
            writeCommands: [AnyDatabaseWriteCommand(command)],
            wireLimits: limits
        )
        let request = try makeRequest(
            operation: DatabaseOperations.commandExecute,
            requestID: 5,
            metadata: OperationRequestMetadata(
                idempotencyKey: "oversized-response"
            ),
            request: CommandRequest(command: command.declaration),
            limits: limits
        )

        let response = try DatabaseWireDecoder(limits: limits).decodeResponse(
            DatabaseOperations.commandExecute,
            from: try await runtime.execute(request),
            matching: 5
        )
        guard case .failure(let error) = response else {
            Issue.record("Expected response resource limit failure")
            return
        }
        #expect(error.category == .resourceLimit)
        #expect(error.code == "RESPONSE_RESOURCE_LIMIT")

        let stateStore = try await DatabaseMutationStateStore(
            container: container
        )
        let commandState = try await container.newContext().model(
            for: command.stateID,
            as: DatabaseEndpointEntity.self
        )
        let mutationState = try await StorageTransactionExecutor(
            engine: container.engine
        ).withTransaction(
            configuration: .readOnly,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            (
                try await stateStore.currentLogicalVersion(
                    transaction: transaction
                ),
                try await stateStore.idempotencyEntry(
                    for: "oversized-response",
                    transaction: transaction,
                    limits: limits
                )
            )
        }
        #expect(commandState == nil)
        #expect(mutationState.0 == 0)
        #expect(mutationState.1 == nil)
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            for: try Schema(
                entities: [
                    try DatabaseEndpointEntity.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration.testing(
                backend: .custom(InMemoryEngine())
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DatabaseEndpointEntity.self)]
            ),
            security: .disabled
        )
    }

    private func makeRuntime(
        container: DBContainer,
        readCommands: [AnyDatabaseReadCommand] = [],
        writeCommands: [AnyDatabaseWriteCommand] = [],
        wireLimits: DatabaseWireLimits = .default
    ) async throws -> DatabaseServerRuntime {
        try await DatabaseServerRuntime(
            container: container,
            configuration: DatabaseServerRuntimeConfiguration(
                identity: DatabaseRuntimeIdentity(version: "test-runtime"),
                serviceFactory: AnyDatabaseServerServiceFactory(
                    ConfiguredCommandServiceFactory(
                        readCommands: readCommands,
                        writeCommands: writeCommands
                    )
                ),
                admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                    UnrestrictedDatabaseOperationAdmissionPolicy()
                ),
                clock: RealtimeDatabaseWallClock(),
                wireLimits: wireLimits
            )
        )
    }

    private func makeRequest<Request, Response>(
        operation: DatabaseOperation<Request, Response>,
        requestID: UInt64,
        metadata: OperationRequestMetadata = OperationRequestMetadata(),
        request: Request,
        limits: DatabaseWireLimits = .default
    ) throws -> ByteString {
        try DatabaseWireEncoder(limits: limits).encodeRequest(
            operation,
            requestID: requestID,
            metadata: metadata,
            request: request
        )
    }

    private func invoke<Request, Response>(
        _ operation: DatabaseOperation<Request, Response>,
        request: Request,
        requestID: UInt64,
        metadata: OperationRequestMetadata = OperationRequestMetadata(),
        runtime: DatabaseServerRuntime
    ) async throws -> Response {
        let requestBytes = try makeRequest(
            operation: operation,
            requestID: requestID,
            metadata: metadata,
            request: request
        )
        let response = try DatabaseWireDecoder().decodeResponse(
            operation,
            from: try await runtime.execute(requestBytes),
            matching: requestID
        )
        switch response {
        case .success(let value):
            return value
        case .failure(let error):
            throw error
        }
    }

    private func commandRequest(
        declaration: CommandDeclaration,
        value: String
    ) throws -> CommandRequest {
        CommandRequest(
            command: declaration,
            input: try FieldObject([
                (key: "value", value: .string(value)),
            ])
        )
    }

    private struct CountingCommand: DatabaseWriteCommand {
        let declaration: CommandDeclaration
        let stateID: String

        init(stateID: String) throws {
            self.declaration = CommandDeclaration(
                identifier: try CommandIdentifier("test.increment"),
                access: .readWrite
            )
            self.stateID = stateID
        }

        func execute(
            input: FieldObject,
            context: DatabaseWriteCommandContext
        ) async throws -> DatabaseCommandResult {
            guard case .string(let value) = input["value"] else {
                throw CountingCommandError.invalidInput
            }
            let stored = try await context.transaction.fetch(
                DatabaseEndpointEntity.self,
                identifiedBy: stateID,
                consistency: .serializable
            )
            let current = stored?.priority ?? 0
            guard current < Int(UInt8.max) else {
                throw CountingCommandError.overflow
            }
            let next = current + 1
            var nextState = DatabaseEndpointEntity()
            nextState.id = stateID
            nextState.title = value
            nextState.priority = next
            try await context.transaction.save(
                nextState,
                precondition: stored == nil ? .notExists : .exists
            )
            return DatabaseCommandResult(output: .uint8(UInt8(next)))
        }
    }

    private enum CountingCommandError: Error {
        case invalidInput
        case overflow
    }

    private struct OversizedResponseCommand: DatabaseWriteCommand {
        let declaration: CommandDeclaration
        let stateID: String

        init(stateID: String) throws {
            self.declaration = CommandDeclaration(
                identifier: try CommandIdentifier(
                    "test.oversized.response"
                ),
                access: .readWrite
            )
            self.stateID = stateID
        }

        func execute(
            input: FieldObject,
            context: DatabaseWriteCommandContext
        ) async throws -> DatabaseCommandResult {
            guard input.isEmpty else {
                throw CountingCommandError.invalidInput
            }
            var state = DatabaseEndpointEntity()
            state.id = stateID
            state.title = "must-roll-back"
            state.priority = 1
            try await context.transaction.save(
                state,
                precondition: .notExists
            )
            return DatabaseCommandResult(
                output: .bytes(
                    ByteString([UInt8](repeating: 0xa5, count: 600))
                )
            )
        }
    }

    private final class ConfiguredCommandServiceFactory:
        DatabaseServerServiceFactory {
        let readCommands: [AnyDatabaseReadCommand]
        let writeCommands: [AnyDatabaseWriteCommand]

        init(
            readCommands: [AnyDatabaseReadCommand],
            writeCommands: [AnyDatabaseWriteCommand]
        ) {
            self.readCommands = readCommands
            self.writeCommands = writeCommands
        }

        func makeServices(
            context: DatabaseServerServiceContext
        ) async throws -> DatabaseServerServices {
            let unavailable = try UnavailableServices()
            return DatabaseServerServices(
                statementExecutor: AnyDatabaseStatementMutationExecutor(
                    CanonicalDatabaseStatementMutationExecutor(
                        runtimeLimits: context.runtimeLimits
                    )
                ),
                graphAlgorithmService: AnyDatabaseGraphAlgorithmService(
                    unavailable
                ),
                ontologyService: AnyDatabaseOntologyService(unavailable),
                shaclService: AnyDatabaseSHACLService(unavailable),
                readCommandRegistry: try DatabaseReadCommandRegistry(
                    commands: readCommands
                ),
                writeCommandRegistry: try DatabaseWriteCommandRegistry(
                    commands: writeCommands
                ),
                maintenanceService: AnyDatabaseMaintenanceService(unavailable),
                jobService: AnyDatabaseJobService(unavailable)
            )
        }
    }

    private struct UnavailableServices:
        DatabaseGraphAlgorithmService,
        DatabaseOntologyService,
        DatabaseSHACLService,
        DatabaseMaintenanceService,
        DatabaseJobService {
        let jobOperations: [JobOperationIdentifier]

        init() throws {
            self.jobOperations = [
                try JobOperationIdentifier(
                    family: .commandExecute,
                    kind: "database.test.runtime-job"
                ),
            ]
        }

        func execute(
            _ request: GraphAlgorithmOperation.Request,
            context: DatabaseOperationContext
        ) async throws -> GraphAlgorithmOperation.Response {
            throw UnavailableError()
        }

        func execute(
            _ request: OntologyExecuteOperation.Request,
            context: DatabaseOperationContext
        ) async throws -> OntologyExecutionResult {
            throw UnavailableError()
        }

        func execute(
            _ request: SHACLExecuteOperation.Request,
            context: DatabaseOperationContext
        ) async throws -> SHACLExecutionResult {
            throw UnavailableError()
        }

        func execute(
            _ request: MaintenanceExecuteOperation.Request,
            context: DatabaseOperationContext
        ) async throws -> MaintenanceExecutionResult {
            throw UnavailableError()
        }

        func start(
            _ request: JobStartOperation.Request,
            context: DatabaseOperationContext
        ) async throws -> JobStartExecutionResult {
            throw UnavailableError()
        }

        func status(
            _ request: JobStatusOperation.Request,
            context: DatabaseOperationContext
        ) async throws -> JobStatusOperation.Response {
            throw UnavailableError()
        }

        func result(
            _ request: JobResultOperation.Request,
            context: DatabaseOperationContext
        ) async throws -> JobResultOperation.Response {
            throw UnavailableError()
        }

        func cancel(
            _ request: JobCancelOperation.Request,
            context: DatabaseOperationContext
        ) async throws -> JobCancellationExecutionResult {
            throw UnavailableError()
        }

        func runScheduledWork() async throws {
            throw UnavailableError()
        }
    }

    private struct UnavailableError: Error {}
}
