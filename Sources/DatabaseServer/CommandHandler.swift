import DatabaseClientProtocol
import DatabaseEngine
import Foundation
import StorageKit
import Synchronization

/// Transaction-scoped context passed to command handlers.
public struct CommandExecutionContext: Sendable {
    public let container: DBContainer
    public let transaction: any Transaction
    public let metadata: [String: String]
    public let preconditions: [WritePreconditionEntry]
    private let commitChecks: CommitCheckRegistry
    private let transactionHooks: TransactionHookRegistry

    public init(
        container: DBContainer,
        transaction: any Transaction,
        metadata: [String: String],
        preconditions: [WritePreconditionEntry],
        commitChecks: CommitCheckRegistry = CommitCheckRegistry(),
        transactionHooks: TransactionHookRegistry = TransactionHookRegistry()
    ) {
        self.container = container
        self.transaction = transaction
        self.metadata = metadata
        self.preconditions = preconditions
        self.commitChecks = commitChecks
        self.transactionHooks = transactionHooks
    }

    public func addCommitCheck(
        _ check: any CommitCheck,
        name: String? = nil,
        priority: Int = 100
    ) {
        commitChecks.add(check, name: name, priority: priority)
    }

    func executeCommitChecks() async throws {
        try await commitChecks.executeAll(transaction: transaction)
    }

    public func addHook(
        _ hook: any TransactionHook,
        priority: Int = 100
    ) {
        transactionHooks.add(hook, priority: priority)
    }

    func executeHooks() async throws {
        try await transactionHooks.executeAll(context: self)
    }
}

/// Hook executed inside a command transaction after commit checks and before commit.
public protocol TransactionHook: Sendable {
    func beforeCommit(context: CommandExecutionContext) async throws
}

/// Closure-based transaction hook for simple command-side audit or outbox writes.
public struct ClosureTransactionHook: TransactionHook {
    private let closure: @Sendable (CommandExecutionContext) async throws -> Void

    public init(_ closure: @escaping @Sendable (CommandExecutionContext) async throws -> Void) {
        self.closure = closure
    }

    public func beforeCommit(context: CommandExecutionContext) async throws {
        try await closure(context)
    }
}

public final class TransactionHookRegistry: Sendable {
    private struct Entry: Sendable {
        let hook: any TransactionHook
        let priority: Int
    }

    private let hooks = Mutex<[Entry]>([])

    public init() {}

    public func add(_ hook: any TransactionHook, priority: Int = 100) {
        hooks.withLock { $0.append(Entry(hook: hook, priority: priority)) }
    }

    func executeAll(context: CommandExecutionContext) async throws {
        let sorted = hooks.withLock { $0.sorted { $0.priority < $1.priority } }
        for entry in sorted {
            try await entry.hook.beforeCommit(context: context)
        }
    }
}

/// Typed command execution result.
public struct CommandResult<Response: Encodable & Sendable>: Sendable {
    public let status: String
    public let response: Response
    public let effects: [CommandEffect]

    public init(
        status: String = "applied",
        response: Response,
        effects: [CommandEffect] = []
    ) {
        self.status = status
        self.response = response
        self.effects = effects
    }
}

/// Typed domain command handler.
public protocol CommandHandler: Sendable {
    associatedtype Request: Decodable & Sendable
    associatedtype Response: Encodable & Sendable

    var commandID: String { get }

    func execute(
        _ request: Request,
        context: CommandExecutionContext
    ) async throws -> CommandResult<Response>
}

/// Type-erased command handler stored by `CommandRegistry`.
public struct AnyCommandHandler: Sendable {
    public let commandID: String

    private let executeClosure: @Sendable (
        CommandRequest,
        CommandExecutionContext
    ) async throws -> CommandResponse

    public init<Handler: CommandHandler>(_ handler: Handler) {
        self.commandID = handler.commandID
        self.executeClosure = { request, context in
            let decoded = try JSONDecoder().decode(Handler.Request.self, from: request.payload)
            let result = try await handler.execute(decoded, context: context)
            let payload = try JSONEncoder().encode(result.response)
            return CommandResponse(
                status: result.status,
                payload: payload,
                effects: result.effects
            )
        }
    }

    public func execute(
        request: CommandRequest,
        context: CommandExecutionContext
    ) async throws -> CommandResponse {
        try await executeClosure(request, context)
    }
}

/// Thread-safe registry for typed command handlers.
public final class CommandRegistry: Sendable {
    private let handlers: Mutex<[String: AnyCommandHandler]>

    public init(handlers: [AnyCommandHandler] = []) {
        var mapped: [String: AnyCommandHandler] = [:]
        for handler in handlers {
            mapped[handler.commandID] = handler
        }
        self.handlers = Mutex(mapped)
    }

    public func register<Handler: CommandHandler>(_ handler: Handler) {
        register(AnyCommandHandler(handler))
    }

    public func register(_ handler: AnyCommandHandler) {
        handlers.withLock { handlers in
            handlers[handler.commandID] = handler
        }
    }

    public func resolve(_ commandID: String) -> AnyCommandHandler? {
        handlers.withLock { handlers in
            handlers[commandID]
        }
    }
}
