import DatabaseClientProtocol
import DatabaseEngine
import Foundation
import Synchronization

/// Context passed to service operation handlers.
public struct ServiceOperationContext: Sendable {
    public let container: DBContainer

    public init(container: DBContainer) {
        self.container = container
    }
}

/// Handler for a single service operation.
///
/// `operationID` is the transport-level route carried by `ServiceEnvelope`.
/// Domain-specific command IDs should live inside the handler payload.
public protocol ServiceOperationHandler: Sendable {
    var operationID: String { get }

    func handle(
        _ envelope: ServiceEnvelope,
        context: ServiceOperationContext
    ) async throws -> ServiceEnvelope
}

/// Thread-safe registry for extension service operations.
public final class OperationRegistry: Sendable {
    private let handlers: Mutex<[String: any ServiceOperationHandler]>

    public init(handlers: [any ServiceOperationHandler] = []) {
        var mapped: [String: any ServiceOperationHandler] = [:]
        for handler in handlers {
            mapped[handler.operationID] = handler
        }
        self.handlers = Mutex(mapped)
    }

    public func register(_ handler: any ServiceOperationHandler) {
        handlers.withLock { handlers in
            handlers[handler.operationID] = handler
        }
    }

    public func resolve(_ operationID: String) -> (any ServiceOperationHandler)? {
        handlers.withLock { handlers in
            handlers[operationID]
        }
    }
}
