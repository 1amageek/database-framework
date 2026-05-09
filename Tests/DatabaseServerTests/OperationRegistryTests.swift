#if SQLITE
import Database
import DatabaseClientProtocol
import DatabaseEngine
import DatabaseServer
import Foundation
import Testing
import Core
import Synchronization

@Persistable
private struct OperationRegistryTestRecord {
    #Directory<OperationRegistryTestRecord>("test", "operation-registry")

    var id: String = UUID().uuidString
    var name: String = ""
}

private struct EchoOperationHandler: ServiceOperationHandler {
    let operationID = "test.echo"

    func handle(
        _ envelope: ServiceEnvelope,
        context: ServiceOperationContext
    ) async throws -> ServiceEnvelope {
        ServiceEnvelope(
            responseTo: envelope.requestID,
            operationID: envelope.operationID,
            payload: envelope.payload,
            metadata: ["entityCount": "\(context.container.schema.entities.count)"]
        )
    }
}

private final class CommandInvocationCounter: Sendable {
    private let count = Mutex(0)

    func increment() -> Int {
        count.withLock { value in
            value += 1
            return value
        }
    }

    var value: Int {
        count.withLock { $0 }
    }
}

private struct EchoCommandRequest: Sendable, Codable {
    let message: String
}

private struct EchoCommandResponse: Sendable, Codable, Equatable {
    let message: String
    let invocationCount: Int
    let tenantID: String?
}

private struct EchoCommandHandler: CommandHandler {
    let commandID = "test.echoCommand"
    let counter: CommandInvocationCounter

    func execute(
        _ request: EchoCommandRequest,
        context: CommandExecutionContext
    ) async throws -> CommandResult<EchoCommandResponse> {
        let count = counter.increment()
        if request.message == "fail-check" {
            context.addCommitCheck(
                ClosureCommitCheck { _ in
                    throw ServiceError(code: "DOMAIN_CHECK_FAILED", message: "Domain check failed")
                },
                name: "domain-check"
            )
        }
        if request.message == "fail-hook" {
            context.addHook(
                ClosureTransactionHook { _ in
                    throw ServiceError(code: "HOOK_FAILED", message: "Hook failed")
                }
            )
        }
        return CommandResult(
            response: EchoCommandResponse(
                message: request.message,
                invocationCount: count,
                tenantID: context.metadata["tenantID"]
            ),
            effects: [
                CommandEffect(kind: "echo", metadata: ["count": .int64(Int64(count))])
            ]
        )
    }
}

@Suite("OperationRegistry Tests", .serialized)
struct OperationRegistryTests {
    @Test("registry resolves registered handlers")
    func registryResolvesRegisteredHandlers() {
        let registry = OperationRegistry()
        let handler = EchoOperationHandler()

        #expect(registry.resolve(handler.operationID) == nil)

        registry.register(handler)

        #expect(registry.resolve(handler.operationID) != nil)
    }

    @Test("endpoint routes unknown builtin operations through registry")
    func endpointRoutesUnknownBuiltinOperationsThroughRegistry() async throws {
        let registry = OperationRegistry(handlers: [EchoOperationHandler()])
        let schema = Schema([OperationRegistryTestRecord.self], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(for: schema, security: .disabled)
        let endpoint = DatabaseEndpoint(container: container, operationRegistry: registry)

        let payload = try JSONEncoder().encode(["message": "hello"])
        let request = ServiceEnvelope(operationID: "test.echo", payload: payload)
        let responseData = await endpoint.handleRequest(try JSONEncoder().encode(request))
        let response = try JSONDecoder().decode(ServiceEnvelope.self, from: responseData)
        let decodedPayload = try JSONDecoder().decode([String: String].self, from: response.payload)

        #expect(response.isError == false)
        #expect(response.operationID == "test.echo")
        #expect(response.metadata["entityCount"] == "1")
        #expect(decodedPayload["message"] == "hello")
    }

    @Test("command operation executes registered typed command")
    func commandOperationExecutesRegisteredTypedCommand() async throws {
        let counter = CommandInvocationCounter()
        let commandRegistry = CommandRegistry()
        commandRegistry.register(EchoCommandHandler(counter: counter))

        let schema = Schema([OperationRegistryTestRecord.self], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(for: schema, security: .disabled)
        let endpoint = DatabaseEndpoint(container: container, commandRegistry: commandRegistry)

        let response = try await sendCommand(
            EchoCommandRequest(message: "hello"),
            idempotencyKey: nil,
            endpoint: endpoint
        )
        let decoded = try JSONDecoder().decode(EchoCommandResponse.self, from: response.payload)

        #expect(response.status == "applied")
        #expect(response.replayed == false)
        #expect(response.effects.first?.kind == "echo")
        #expect(decoded == EchoCommandResponse(message: "hello", invocationCount: 1, tenantID: "tenant-a"))
        #expect(counter.value == 1)
    }

    @Test("command operation replays idempotent result")
    func commandOperationReplaysIdempotentResult() async throws {
        let counter = CommandInvocationCounter()
        let commandRegistry = CommandRegistry()
        commandRegistry.register(EchoCommandHandler(counter: counter))

        let schema = Schema([OperationRegistryTestRecord.self], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(for: schema, security: .disabled)
        let endpoint = DatabaseEndpoint(container: container, commandRegistry: commandRegistry)

        let first = try await sendCommand(
            EchoCommandRequest(message: "hello"),
            idempotencyKey: "echo-1",
            endpoint: endpoint,
            envelopeMetadata: ["traceID": "first"]
        )
        let second = try await sendCommand(
            EchoCommandRequest(message: "hello"),
            idempotencyKey: "echo-1",
            endpoint: endpoint,
            envelopeMetadata: ["traceID": "retry"]
        )

        let firstPayload = try JSONDecoder().decode(EchoCommandResponse.self, from: first.payload)
        let secondPayload = try JSONDecoder().decode(EchoCommandResponse.self, from: second.payload)

        #expect(first.replayed == false)
        #expect(second.replayed == true)
        #expect(firstPayload.invocationCount == 1)
        #expect(secondPayload.invocationCount == 1)
        #expect(counter.value == 1)
    }

    @Test("command operation rejects idempotency key reuse with different payload")
    func commandOperationRejectsIdempotencyKeyReuseWithDifferentPayload() async throws {
        let counter = CommandInvocationCounter()
        let commandRegistry = CommandRegistry()
        commandRegistry.register(EchoCommandHandler(counter: counter))

        let schema = Schema([OperationRegistryTestRecord.self], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(for: schema, security: .disabled)
        let endpoint = DatabaseEndpoint(container: container, commandRegistry: commandRegistry)

        _ = try await sendCommand(
            EchoCommandRequest(message: "hello"),
            idempotencyKey: "echo-1",
            endpoint: endpoint
        )

        let error = try await sendCommandEnvelope(
            EchoCommandRequest(message: "changed"),
            idempotencyKey: "echo-1",
            endpoint: endpoint
        )

        #expect(error.isError == true)
        #expect(error.errorCode == "IDEMPOTENCY_CONFLICT")
        #expect(counter.value == 1)
    }

    @Test("command operation evaluates existence preconditions")
    func commandOperationEvaluatesExistencePreconditions() async throws {
        let counter = CommandInvocationCounter()
        let commandRegistry = CommandRegistry()
        commandRegistry.register(EchoCommandHandler(counter: counter))

        let schema = Schema([OperationRegistryTestRecord.self], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(for: schema, security: .disabled)
        let context = container.newContext()
        var record = OperationRegistryTestRecord()
        record.id = "record-1"
        record.name = "existing"
        context.insert(record)
        try await context.save()

        let endpoint = DatabaseEndpoint(container: container, commandRegistry: commandRegistry)

        let success = try await sendCommand(
            EchoCommandRequest(message: "hello"),
            idempotencyKey: nil,
            endpoint: endpoint,
            preconditions: [
                WritePreconditionEntry(
                    key: RecordKey(entityName: OperationRegistryTestRecord.persistableType, id: .string("record-1")),
                    precondition: .exists
                ),
                WritePreconditionEntry(
                    key: RecordKey(entityName: OperationRegistryTestRecord.persistableType, id: .string("missing")),
                    precondition: .notExists
                )
            ]
        )
        let failure = try await sendCommandEnvelope(
            EchoCommandRequest(message: "blocked"),
            idempotencyKey: nil,
            endpoint: endpoint,
            preconditions: [
                WritePreconditionEntry(
                    key: RecordKey(entityName: OperationRegistryTestRecord.persistableType, id: .string("missing")),
                    precondition: .exists
                )
            ]
        )

        #expect(success.status == "applied")
        #expect(failure.isError == true)
        #expect(failure.errorCode == "PRECONDITION_FAILED")
        #expect(counter.value == 1)
    }

    @Test("command operation executes commit checks before recording idempotency")
    func commandOperationExecutesCommitChecksBeforeRecordingIdempotency() async throws {
        let counter = CommandInvocationCounter()
        let commandRegistry = CommandRegistry()
        commandRegistry.register(EchoCommandHandler(counter: counter))

        let schema = Schema([OperationRegistryTestRecord.self], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(for: schema, security: .disabled)
        let endpoint = DatabaseEndpoint(container: container, commandRegistry: commandRegistry)

        let first = try await sendCommandEnvelope(
            EchoCommandRequest(message: "fail-check"),
            idempotencyKey: "check-1",
            endpoint: endpoint
        )
        let second = try await sendCommandEnvelope(
            EchoCommandRequest(message: "fail-check"),
            idempotencyKey: "check-1",
            endpoint: endpoint
        )

        #expect(first.isError == true)
        #expect(first.errorCode == "COMMIT_CHECK_FAILED")
        #expect(second.isError == true)
        #expect(second.errorCode == "COMMIT_CHECK_FAILED")
        #expect(counter.value == 2)
    }

    @Test("command operation executes transaction hooks before recording idempotency")
    func commandOperationExecutesTransactionHooksBeforeRecordingIdempotency() async throws {
        let counter = CommandInvocationCounter()
        let commandRegistry = CommandRegistry()
        commandRegistry.register(EchoCommandHandler(counter: counter))

        let schema = Schema([OperationRegistryTestRecord.self], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(for: schema, security: .disabled)
        let endpoint = DatabaseEndpoint(container: container, commandRegistry: commandRegistry)

        let first = try await sendCommandEnvelope(
            EchoCommandRequest(message: "fail-hook"),
            idempotencyKey: "hook-1",
            endpoint: endpoint
        )
        let second = try await sendCommandEnvelope(
            EchoCommandRequest(message: "fail-hook"),
            idempotencyKey: "hook-1",
            endpoint: endpoint
        )

        #expect(first.isError == true)
        #expect(first.errorCode == "TRANSACTION_HOOK_FAILED")
        #expect(second.isError == true)
        #expect(second.errorCode == "TRANSACTION_HOOK_FAILED")
        #expect(counter.value == 2)
    }

    @Test("save operation enforces changed-record preconditions")
    func saveOperationEnforcesChangedRecordPreconditions() async throws {
        let schema = Schema([OperationRegistryTestRecord.self], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(for: schema, security: .disabled)
        let endpoint = DatabaseEndpoint(container: container)

        let create = ChangeSet.Change(
            entityName: OperationRegistryTestRecord.persistableType,
            id: "record-1",
            operation: .insert,
            fields: ["id": .string("record-1"), "name": .string("created")]
        )
        let first = try await sendSave(
            changes: [create],
            preconditions: [
                WritePreconditionEntry(
                    key: RecordKey(entityName: OperationRegistryTestRecord.persistableType, id: .string("record-1")),
                    precondition: .notExists
                )
            ],
            endpoint: endpoint
        )
        let duplicate = try await sendSave(
            changes: [create],
            preconditions: [
                WritePreconditionEntry(
                    key: RecordKey(entityName: OperationRegistryTestRecord.persistableType, id: .string("record-1")),
                    precondition: .notExists
                )
            ],
            endpoint: endpoint
        )

        #expect(first.isError == false)
        #expect(duplicate.isError == true)
        #expect(duplicate.errorCode == "PRECONDITION_FAILED")
        #expect(duplicate.errorMessage?.contains("Precondition") == true)
    }

    private func sendCommand(
        _ request: EchoCommandRequest,
        idempotencyKey: IdempotencyKey?,
        endpoint: DatabaseEndpoint,
        envelopeMetadata: [String: String] = [:],
        preconditions: [WritePreconditionEntry] = []
    ) async throws -> CommandResponse {
        let envelope = try await sendCommandEnvelope(
            request,
            idempotencyKey: idempotencyKey,
            endpoint: endpoint,
            envelopeMetadata: envelopeMetadata,
            preconditions: preconditions
        )
        if envelope.isError == true {
            throw ServiceError(
                code: envelope.errorCode ?? "UNKNOWN",
                message: envelope.errorMessage ?? "Unknown service error"
            )
        }
        return try JSONDecoder().decode(CommandResponse.self, from: envelope.payload)
    }

    private func sendCommandEnvelope(
        _ request: EchoCommandRequest,
        idempotencyKey: IdempotencyKey?,
        endpoint: DatabaseEndpoint,
        envelopeMetadata: [String: String] = [:],
        preconditions: [WritePreconditionEntry] = []
    ) async throws -> ServiceEnvelope {
        let command = CommandRequest(
            commandID: "test.echoCommand",
            idempotencyKey: idempotencyKey,
            payload: try JSONEncoder().encode(request),
            preconditions: preconditions,
            metadata: ["tenantID": "tenant-a"]
        )
        let envelope = ServiceEnvelope(
            operationID: "command",
            payload: try JSONEncoder().encode(command),
            metadata: envelopeMetadata
        )
        let responseData = await endpoint.handleRequest(try JSONEncoder().encode(envelope))
        return try JSONDecoder().decode(ServiceEnvelope.self, from: responseData)
    }

    private func sendSave(
        changes: [ChangeSet.Change],
        preconditions: [WritePreconditionEntry],
        endpoint: DatabaseEndpoint
    ) async throws -> ServiceEnvelope {
        let request = SaveRequest(
            changes: changes,
            preconditions: preconditions
        )
        let envelope = ServiceEnvelope(
            operationID: "save",
            payload: try JSONEncoder().encode(request)
        )
        let responseData = await endpoint.handleRequest(try JSONEncoder().encode(envelope))
        return try JSONDecoder().decode(ServiceEnvelope.self, from: responseData)
    }
}
#endif
