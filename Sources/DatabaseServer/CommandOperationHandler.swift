import Core
import DatabaseClientProtocol
import DatabaseEngine
import Foundation
import StorageKit

/// Service operation that executes registered typed commands.
public final class CommandOperationHandler: ServiceOperationHandler {
    public let operationID = "command"

    private let commandRegistry: CommandRegistry

    public init(commandRegistry: CommandRegistry) {
        self.commandRegistry = commandRegistry
    }

    public func handle(
        _ envelope: ServiceEnvelope,
        context: ServiceOperationContext
    ) async throws -> ServiceEnvelope {
        let request = try JSONDecoder().decode(CommandRequest.self, from: envelope.payload)
        guard let handler = commandRegistry.resolve(request.commandID) else {
            throw ServiceError(
                code: "COMMAND_NOT_FOUND",
                message: "Command '\(request.commandID)' is not registered"
            )
        }

        let idempotencyStore = try await CommandIdempotencyStore(container: context.container)
        let fingerprint = try CommandFingerprint.make(request: request)
        let tenantID = request.metadata["tenantID"] ?? envelope.metadata["tenantID"] ?? "_global"

        let response = try await context.container.engine.withTransaction(configuration: .default) { transaction in
            if let key = request.idempotencyKey {
                if let existing = try await idempotencyStore.load(
                    tenantID: tenantID,
                    commandID: request.commandID,
                    key: key,
                    transaction: transaction
                ) {
                    guard existing.fingerprint == fingerprint else {
                        throw ServiceError(
                            code: "IDEMPOTENCY_CONFLICT",
                            message: "Idempotency key '\(key.value)' was already used with a different request"
                        )
                    }
                    return existing.response.asReplay()
                }
            }

            try await CommandPreconditionEvaluator.evaluate(
                request.preconditions,
                container: context.container,
                transaction: transaction
            )

            let executionContext = CommandExecutionContext(
                container: context.container,
                transaction: transaction,
                metadata: request.metadata.merging(envelope.metadata) { current, _ in current },
                preconditions: request.preconditions
            )
            let result = try await handler.execute(
                request: request,
                context: executionContext
            )

            do {
                try await executionContext.executeCommitChecks()
            } catch {
                throw ServiceError(
                    code: "COMMIT_CHECK_FAILED",
                    message: "\(error)"
                )
            }

            do {
                try await executionContext.executeHooks()
            } catch {
                throw ServiceError(
                    code: "TRANSACTION_HOOK_FAILED",
                    message: "\(error)"
                )
            }

            if let key = request.idempotencyKey {
                try idempotencyStore.save(
                    CommandIdempotencyRecord(
                        fingerprint: fingerprint,
                        response: result
                    ),
                    tenantID: tenantID,
                    commandID: request.commandID,
                    key: key,
                    transaction: transaction
                )
            }

            return result
        }

        let payload = try JSONEncoder().encode(response)
        return ServiceEnvelope(
            responseTo: envelope.requestID,
            operationID: envelope.operationID,
            payload: payload
        )
    }
}

private enum CommandPreconditionEvaluator {
    static func evaluate(
        _ entries: [WritePreconditionEntry],
        container: DBContainer,
        transaction: any Transaction
    ) async throws {
        for entry in entries {
            try await evaluate(entry, container: container, transaction: transaction)
        }
    }

    private static func evaluate(
        _ entry: WritePreconditionEntry,
        container: DBContainer,
        transaction: any Transaction
    ) async throws {
        switch entry.precondition.kind {
        case .none:
            return
        case .exists:
            let exists = try await recordExists(
                entry.key,
                container: container,
                transaction: transaction
            )
            guard exists else {
                throw preconditionError("Record '\(entry.key.entityName)' with id '\(entry.key.id)' must exist")
            }
        case .notExists:
            let exists = try await recordExists(
                entry.key,
                container: container,
                transaction: transaction
            )
            guard !exists else {
                throw preconditionError("Record '\(entry.key.entityName)' with id '\(entry.key.id)' must not exist")
            }
        case .matchesStored, .matchesStoredOrAbsent:
            throw preconditionError("Version preconditions require record version metadata and are not yet supported")
        }
    }

    private static func recordExists(
        _ key: RecordKey,
        container: DBContainer,
        transaction: any Transaction
    ) async throws -> Bool {
        guard let type = container.schema.entity(named: key.entityName)?.persistableType else {
            throw preconditionError("Entity '\(key.entityName)' is not registered in the runtime schema")
        }

        let path = try directoryPath(for: type, partitionValues: key.partitionValues)
        let directory = try await container.resolveDirectory(for: type, path: path)
        let storageKey = directory
            .subspace(SubspaceKey.items)
            .subspace(type.persistableType)
            .pack(try idTuple(from: key.id))

        return try await transaction.getValue(for: storageKey, snapshot: false) != nil
    }

    private static func directoryPath(
        for type: any Persistable.Type,
        partitionValues: [String: String]?
    ) throws -> AnyDirectoryPath {
        guard let partitionValues else {
            return AnyDirectoryPath(for: type)
        }

        let fieldValues: [(keyPath: AnyKeyPath, value: any Sendable)] = type.directoryPathComponents.compactMap { component in
            guard let dynamicElement = component as? any DynamicDirectoryElement else { return nil }
            let fieldName = type.fieldName(for: dynamicElement.anyKeyPath)
            guard let value = partitionValues[fieldName] else { return nil }
            return (dynamicElement.anyKeyPath, value)
        }
        return AnyDirectoryPath(fieldValues: fieldValues, type: type)
    }

    private static func idTuple(from value: FieldValue) throws -> Tuple {
        switch value {
        case .int64(let value):
            return Tuple([value])
        case .double(let value):
            return Tuple([value])
        case .string(let value):
            return Tuple([value])
        case .bool(let value):
            return Tuple([value])
        case .data(let value):
            return Tuple([Array(value)])
        case .array(let values):
            return Tuple(try values.map { try tupleElement(from: $0) })
        case .null:
            throw preconditionError("Record id cannot be null")
        }
    }

    private static func tupleElement(from value: FieldValue) throws -> any TupleElement {
        switch value {
        case .int64(let value):
            return value
        case .double(let value):
            return value
        case .string(let value):
            return value
        case .bool(let value):
            return value
        case .data(let value):
            return Array(value)
        case .array, .null:
            throw preconditionError("Composite record ids cannot contain nested arrays or null values")
        }
    }

    private static func preconditionError(_ message: String) -> ServiceError {
        ServiceError(code: "PRECONDITION_FAILED", message: message)
    }
}

private struct CommandFingerprintPayload: Encodable {
    let commandID: String
    let payloadBase64: String
    let preconditions: [WritePreconditionEntry]
    let requestMetadata: [String: String]
}

private enum CommandFingerprint {
    static func make(request: CommandRequest) throws -> String {
        let payload = CommandFingerprintPayload(
            commandID: request.commandID,
            payloadBase64: request.payload.base64EncodedString(),
            preconditions: request.preconditions,
            requestMetadata: request.metadata
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        return try encoder.encode(payload).base64EncodedString()
    }
}

private struct CommandIdempotencyRecord: Sendable, Codable {
    let fingerprint: String
    let response: CommandResponse
}

private struct CommandIdempotencyStore: Sendable {
    private let subspace: Subspace

    init(container: DBContainer) async throws {
        self.subspace = try await container.engine.directoryService.createOrOpen(
            path: ["_database_server", "idempotency"]
        )
    }

    func load(
        tenantID: String,
        commandID: String,
        key: IdempotencyKey,
        transaction: any Transaction
    ) async throws -> CommandIdempotencyRecord? {
        let storageKey = storageKey(tenantID: tenantID, commandID: commandID, key: key)
        guard let data = try await transaction.getValue(for: storageKey, snapshot: false) else {
            return nil
        }
        return try JSONDecoder().decode(CommandIdempotencyRecord.self, from: Data(data))
    }

    func save(
        _ record: CommandIdempotencyRecord,
        tenantID: String,
        commandID: String,
        key: IdempotencyKey,
        transaction: any Transaction
    ) throws {
        let storageKey = storageKey(tenantID: tenantID, commandID: commandID, key: key)
        let data = try JSONEncoder().encode(record)
        transaction.setValue(Array(data), for: storageKey)
    }

    private func storageKey(
        tenantID: String,
        commandID: String,
        key: IdempotencyKey
    ) -> [UInt8] {
        subspace.pack(Tuple([tenantID, commandID, key.value]))
    }
}

private extension CommandResponse {
    func asReplay() -> CommandResponse {
        CommandResponse(
            status: status,
            payload: payload,
            effects: effects,
            replayed: true
        )
    }
}
