import Foundation
import Core
import DatabaseEngine
import DatabaseClientProtocol

/// Routes ServiceEnvelope requests to the appropriate entity handler
///
/// Creates an FDBContext per request (stateless) for horizontal scalability.
/// Handlers are registered per entity type during initialization.
final class OperationRouter: Sendable {

    private let container: FDBContainer
    private let handlers: [String: EntityHandler]

    init(container: FDBContainer) {
        self.container = container

        // Build handlers for each entity type in the schema
        var handlers: [String: EntityHandler] = [:]
        for entity in container.schema.entities {
            guard let type = entity.persistableType else { continue }
            handlers[entity.name] = Self.buildHandler(for: type)
        }
        self.handlers = handlers
    }

    /// Build a handler by opening the existential Persistable type
    ///
    /// Swift 5.7+ implicit existential opening: `any Persistable.Type` → `T.Type`
    private static func buildHandler(for type: any Persistable.Type) -> EntityHandler {
        _buildHandler(type)
    }

    private static func _buildHandler<T: Persistable>(_ type: T.Type) -> EntityHandler {
        EntityHandler.build(for: type)
    }

    /// Handle an incoming request
    func handle(_ envelope: ServiceEnvelope) async throws -> ServiceEnvelope {
        let decoder = JSONDecoder()

        switch envelope.operationID {
        case "fetch":
            let request = try decoder.decode(FetchRequest.self, from: envelope.payload)
            guard let handler = handlers[request.entityName] else {
                return ServiceEnvelope(
                    responseTo: envelope.requestID,
                    operationID: envelope.operationID,
                    errorCode: "UNKNOWN_ENTITY",
                    errorMessage: "Entity '\(request.entityName)' not found in schema"
                )
            }
            let context = container.newContext()
            let response = try await handler.fetch(context, request)
            let payload = try JSONEncoder().encode(response)
            return ServiceEnvelope(
                responseTo: envelope.requestID,
                operationID: envelope.operationID,
                payload: payload
            )

        case "get":
            let request = try decoder.decode(GetRequest.self, from: envelope.payload)
            guard let handler = handlers[request.entityName] else {
                return ServiceEnvelope(
                    responseTo: envelope.requestID,
                    operationID: envelope.operationID,
                    errorCode: "UNKNOWN_ENTITY",
                    errorMessage: "Entity '\(request.entityName)' not found in schema"
                )
            }
            let context = container.newContext()
            let response = try await handler.get(context, request)
            let payload = try JSONEncoder().encode(response)
            return ServiceEnvelope(
                responseTo: envelope.requestID,
                operationID: envelope.operationID,
                payload: payload
            )

        case "save":
            let request = try decoder.decode(SaveRequest.self, from: envelope.payload)

            // Group changes by entity name
            let grouped = Dictionary(grouping: request.changes, by: \.entityName)

            // Create a single context for all changes (one transaction)
            let context = container.newContext()

            for (entityName, changes) in grouped {
                guard let handler = handlers[entityName] else {
                    return ServiceEnvelope(
                        responseTo: envelope.requestID,
                        operationID: envelope.operationID,
                        errorCode: "UNKNOWN_ENTITY",
                        errorMessage: "Entity '\(entityName)' not found in schema"
                    )
                }
                try await handler.applyChanges(context, changes)
            }

            // Commit all changes in one transaction
            try await context.save()

            return ServiceEnvelope(
                responseTo: envelope.requestID,
                operationID: envelope.operationID
            )

        case "count":
            let request = try decoder.decode(CountRequest.self, from: envelope.payload)
            guard let handler = handlers[request.entityName] else {
                return ServiceEnvelope(
                    responseTo: envelope.requestID,
                    operationID: envelope.operationID,
                    errorCode: "UNKNOWN_ENTITY",
                    errorMessage: "Entity '\(request.entityName)' not found in schema"
                )
            }
            let context = container.newContext()
            let response = try await handler.count(context, request)
            let payload = try JSONEncoder().encode(response)
            return ServiceEnvelope(
                responseTo: envelope.requestID,
                operationID: envelope.operationID,
                payload: payload
            )

        case "schema":
            let entities = container.schema.entities
            let response = SchemaResponse(entities: entities)
            let payload = try JSONEncoder().encode(response)
            return ServiceEnvelope(
                responseTo: envelope.requestID,
                operationID: envelope.operationID,
                payload: payload
            )

        default:
            return ServiceEnvelope(
                responseTo: envelope.requestID,
                operationID: envelope.operationID,
                errorCode: "UNKNOWN_OPERATION",
                errorMessage: "Operation '\(envelope.operationID)' is not supported"
            )
        }
    }
}
