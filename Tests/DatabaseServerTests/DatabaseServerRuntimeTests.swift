import Core
import DatabaseRuntime
import DatabaseEngine
@testable import DatabaseServer
import DatabaseValue
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
        let response: CapabilitiesDescribeOperation.Response = try await invoke(
            CapabilitiesDescribeOperation.self,
            request: DatabaseEmpty(),
            requestID: 1,
            runtime: runtime
        )

        #expect(response.runtimeVersion == "test-runtime")
        #expect(
            response.jobOperations == [
                try DatabaseJobOperationIdentifier(
                    family: .commandWrite,
                    kind: "database.test.runtime-job"
                ),
            ]
        )
    }

    @Test("write commands are atomic and replay idempotently")
    func writeCommandUsesSharedTransactionalCoordinator() async throws {
        let container = try await makeContainer()
        let command = CountingCommand(key: Bytes("command-count".utf8))
        let runtime = try await makeRuntime(
            container: container,
            writeCommands: [AnyDatabaseWriteCommand(command)]
        )
        let request = DatabaseTypedCommandRequest<CountingCommandDescriptor>(
            input: CountingCommandInput(value: "same-input")
        )
        let firstRequestBytes = try makeRequest(
            operation: CountingCommandOperation.self,
            requestID: 2,
            metadata: DatabaseRequestMetadata(idempotencyKey: "command-key"),
            payload: request
        )
        let secondRequestBytes = try makeRequest(
            operation: CountingCommandOperation.self,
            requestID: 3,
            metadata: DatabaseRequestMetadata(idempotencyKey: "command-key"),
            payload: request
        )

        let firstEnvelope = try DatabaseEnvelopeCodec.decodeResponse(
            try await runtime.execute(firstRequestBytes)
        )
        let secondEnvelope = try DatabaseEnvelopeCodec.decodeResponse(
            try await runtime.execute(secondRequestBytes)
        )
        guard case .success(let firstPayload) = firstEnvelope.payload,
              case .success(let secondPayload) = secondEnvelope.payload else {
            Issue.record("Expected successful command responses")
            return
        }
        let first = try DatabaseEnvelopeCodec.decode(
            CountingCommandOperation.Response.self,
            from: firstPayload
        )
        let second = try DatabaseEnvelopeCodec.decode(
            CountingCommandOperation.Response.self,
            from: secondPayload
        )
        let storedCount = try await container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            try await transaction.getValue(for: command.key)
        }

        #expect(first.output == CountingCommandOutput(count: 1))
        #expect(first.commitVersion == 1)
        #expect(second.output == CountingCommandOutput(count: 1))
        #expect(second.commitVersion == 1)
        #expect(firstEnvelope.requestID == 2)
        #expect(secondEnvelope.requestID == 3)
        #expect(firstPayload == secondPayload)
        #expect(storedCount == Bytes([1]))
    }

    @Test("an idempotency key cannot be reused with a different payload")
    func idempotencyConflictIsTyped() async throws {
        let container = try await makeContainer()
        let command = CountingCommand(key: Bytes("conflict-count".utf8))
        let runtime = try await makeRuntime(
            container: container,
            writeCommands: [AnyDatabaseWriteCommand(command)]
        )
        let metadata = DatabaseRequestMetadata(idempotencyKey: "conflict-key")
        let first = try makeRequest(
            operation: CountingCommandOperation.self,
            requestID: 3,
            metadata: metadata,
            payload: DatabaseTypedCommandRequest<CountingCommandDescriptor>(
                input: CountingCommandInput(value: "first")
            )
        )
        let conflicting = try makeRequest(
            operation: CountingCommandOperation.self,
            requestID: 4,
            metadata: metadata,
            payload: DatabaseTypedCommandRequest<CountingCommandDescriptor>(
                input: CountingCommandInput(value: "second")
            )
        )

        _ = try await runtime.execute(first)
        let responseBytes = try await runtime.execute(conflicting)
        let response = try DatabaseEnvelopeCodec.decodeResponse(responseBytes)

        switch response.payload {
        case .success:
            Issue.record("Expected an idempotency conflict")
        case .failure(let error):
            #expect(error.category == .conflict)
            #expect(error.code == "MUTATION_CONFLICT")
        }
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
        let command = OversizedResponseCommand(
            key: Bytes("oversized-response".utf8)
        )
        let runtime = try await makeRuntime(
            container: container,
            writeCommands: [AnyDatabaseWriteCommand(command)],
            wireLimits: limits
        )
        let request = try makeRequest(
            operation: OversizedResponseOperation.self,
            requestID: 5,
            metadata: DatabaseRequestMetadata(
                idempotencyKey: "oversized-response"
            ),
            payload: DatabaseTypedCommandRequest<
                OversizedResponseCommandDescriptor
            >(input: DatabaseEmpty()),
            limits: limits
        )

        let response = try DatabaseEnvelopeCodec.decodeResponse(
            try await runtime.execute(request),
            limits: limits
        )
        guard case .failure(let error) = response.payload else {
            Issue.record("Expected response resource limit failure")
            return
        }
        #expect(error.category == .resourceLimit)
        #expect(error.code == "RESPONSE_RESOURCE_LIMIT")

        let stateStore = try await DatabaseMutationStateStore(
            container: container
        )
        let state = try await container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            (
                try await transaction.getValue(for: command.key),
                try await stateStore.currentLogicalVersion(
                    transaction: transaction
                ),
                try await stateStore.idempotencyRecord(
                    for: "oversized-response",
                    transaction: transaction,
                    limits: limits
                )
            )
        }
        #expect(state.0 == nil)
        #expect(state.1 == 0)
        #expect(state.2 == nil)
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer(
            for: Schema(
                [DatabaseEndpointRecord.self],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
    }

    private func makeRuntime(
        container: DBContainer,
        readCommands: [AnyDatabaseReadCommand] = [],
        writeCommands: [AnyDatabaseWriteCommand] = [],
        wireLimits: DatabaseWireLimits = .default
    ) async throws -> DatabaseServerRuntime {
        return try await DatabaseServerRuntime(
            container: container,
            configuration: DatabaseServerRuntimeConfiguration(
                identity: DatabaseRuntimeIdentity(version: "test-runtime"),
                serviceFactory: AnyDatabaseServerServiceFactory { context in
                    try await ConfiguredCommandServiceFactory(
                        readCommands: readCommands,
                        writeCommands: writeCommands
                    )
                        .makeServices(context: context)
                },
                authorizationPolicy: AnyDatabaseOperationAuthorizationPolicy(
                    UnrestrictedDatabaseOperationAuthorizationPolicy()
                ),
                wireLimits: wireLimits
            )
        )
    }

    private func makeRequest<Operation: DatabaseOperation>(
        operation: Operation.Type,
        requestID: UInt64,
        metadata: DatabaseRequestMetadata = DatabaseRequestMetadata(),
        payload: Operation.Request,
        limits: DatabaseWireLimits = .default
    ) throws -> DatabaseBytes {
        try DatabaseEnvelopeCodec.encodeRequest(
            operation,
            requestID: requestID,
            metadata: metadata,
            request: payload,
            limits: limits
        )
    }

    private func invoke<Operation: DatabaseOperation>(
        _ operation: Operation.Type,
        request: Operation.Request,
        requestID: UInt64,
        runtime: DatabaseServerRuntime
    ) async throws -> Operation.Response {
        try await invoke(
            makeRequest(
                operation: operation,
                requestID: requestID,
                payload: request
            ),
            as: operation,
            runtime: runtime
        )
    }

    private func invoke<Operation: DatabaseOperation>(
        _ request: DatabaseBytes,
        as operation: Operation.Type,
        runtime: DatabaseServerRuntime
    ) async throws -> Operation.Response {
        let responseBytes = try await runtime.execute(request)
        let response = try DatabaseEnvelopeCodec.decodeResponse(responseBytes)
        switch response.payload {
        case .success(let payload):
            return try DatabaseEnvelopeCodec.decode(
                Operation.Response.self,
                from: payload
            )
        case .failure(let error):
            throw error
        }
    }

    private typealias CountingCommandOperation =
        DatabaseTypedWriteCommandOperation<CountingCommandDescriptor>

    private enum CountingCommandDescriptor: DatabaseWriteCommandDescriptor {
        typealias Input = CountingCommandInput
        typealias Output = CountingCommandOutput

        static let identifier = "test.increment"
    }

    private struct CountingCommandInput: DatabaseWireValue, Equatable {
        let value: String

        func encode(
            into writer: inout DatabaseWireWriter
        ) throws(DatabaseWireError) {
            try writer.writeString(value)
        }

        init(value: String) {
            self.value = value
        }

        init(
            from reader: inout DatabaseWireReader
        ) throws(DatabaseWireError) {
            self.init(value: try reader.readString())
        }
    }

    private struct CountingCommandOutput: DatabaseWireValue, Equatable {
        let count: UInt8

        func encode(
            into writer: inout DatabaseWireWriter
        ) throws(DatabaseWireError) {
            writer.writeUInt8(count)
        }

        init(count: UInt8) {
            self.count = count
        }

        init(
            from reader: inout DatabaseWireReader
        ) throws(DatabaseWireError) {
            self.init(count: try reader.readUInt8())
        }
    }

    private struct CountingCommand: DatabaseWriteCommand {
        typealias Descriptor = CountingCommandDescriptor

        let key: Bytes

        func execute(
            input: CountingCommandInput,
            context: DatabaseWriteCommandContext
        ) async throws -> DatabaseCommandResult<CountingCommandOutput> {
            let stored = try await context.transaction.getValue(for: key)
            let current = stored?.first ?? 0
            guard current < UInt8.max else {
                throw CountingCommandError.overflow
            }
            let next = current + 1
            try context.transaction.setValue(Bytes([next]), for: key)
            return DatabaseCommandResult(
                output: CountingCommandOutput(count: next)
            )
        }
    }

    private enum CountingCommandError: Error {
        case overflow
    }

    private typealias OversizedResponseOperation =
        DatabaseTypedWriteCommandOperation<
            OversizedResponseCommandDescriptor
        >

    private enum OversizedResponseCommandDescriptor:
        DatabaseWriteCommandDescriptor {
        typealias Input = DatabaseEmpty
        typealias Output = OversizedResponseOutput

        static let identifier = "test.oversized-response"
    }

    private struct OversizedResponseOutput: DatabaseWireValue {
        let bytes: DatabaseBytes

        func encode(
            into writer: inout DatabaseWireWriter
        ) throws(DatabaseWireError) {
            try writer.writeBytes(bytes)
        }

        init(bytes: DatabaseBytes) {
            self.bytes = bytes
        }

        init(
            from reader: inout DatabaseWireReader
        ) throws(DatabaseWireError) {
            self.init(bytes: try reader.readBytes())
        }
    }

    private struct OversizedResponseCommand: DatabaseWriteCommand {
        typealias Descriptor = OversizedResponseCommandDescriptor

        let key: Bytes

        func execute(
            input: DatabaseEmpty,
            context: DatabaseWriteCommandContext
        ) async throws -> DatabaseCommandResult<OversizedResponseOutput> {
            _ = input
            try context.transaction.setValue([1], for: key)
            return DatabaseCommandResult(
                output: OversizedResponseOutput(
                    bytes: DatabaseBytes(
                        [UInt8](repeating: 0xa5, count: 600)
                    )
                )
            )
        }
    }

    private final class ConfiguredCommandServiceFactory: DatabaseServerServiceFactory {
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

        let jobOperations: [DatabaseJobOperationIdentifier]

        init() throws {
            self.jobOperations = [
                try DatabaseJobOperationIdentifier(
                    family: .commandWrite,
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
        ) async throws
            -> DatabasePreparedOperationResponse<OntologyExecuteOperation> {
            throw UnavailableError()
        }

        func execute(
            _ request: SHACLExecuteOperation.Request,
            context: DatabaseOperationContext
        ) async throws
            -> DatabasePreparedOperationResponse<SHACLExecuteOperation> {
            throw UnavailableError()
        }

        func execute(
            _ request: MaintenanceExecuteOperation.Request,
            context: DatabaseOperationContext
        ) async throws
            -> DatabasePreparedOperationResponse<MaintenanceExecuteOperation> {
            throw UnavailableError()
        }

        func start(
            _ request: JobStartOperation.Request,
            context: DatabaseOperationContext
        ) async throws
            -> DatabasePreparedOperationResponse<JobStartOperation> {
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
        ) async throws
            -> DatabasePreparedOperationResponse<JobCancelOperation> {
            throw UnavailableError()
        }

        func runScheduledWork() async throws {
            throw UnavailableError()
        }
    }

    private struct UnavailableError: Error {}
}
