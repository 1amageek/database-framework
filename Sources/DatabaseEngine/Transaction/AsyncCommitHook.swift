// AsyncCommitHook.swift
// DatabaseEngine - Enhanced async commit hooks with validation and post-commit actions
//
// Reference: FDB Record Layer CommitCheckAsync, PostCommit
// Provides structured pre-commit validation and post-commit processing.

import Foundation
import Synchronization
import Core

// MARK: - PreCommitCheck Protocol

/// Protocol for pre-commit validation checks
///
/// Pre-commit checks are executed before the transaction commits.
/// If any check fails (throws an error), the transaction is aborted.
///
/// **Use Cases**:
/// - Business rule validation
/// - Constraint checking
/// - Permission verification
/// - Quota enforcement
///
/// **Usage**:
/// ```swift
/// struct QuotaCheck: PreCommitCheck {
///     func validate(transaction: any TransactionProtocol, context: CommitContext) async throws {
///         let currentCount = try await countRecords(transaction: transaction)
///         guard currentCount + context.insertedRecords.count <= quota else {
///             throw QuotaExceededError()
///         }
///     }
/// }
/// ```
public protocol PreCommitCheck: Sendable {
    /// Unique identifier for this check
    var identifier: String { get }

    /// Priority (higher runs first)
    var priority: Int { get }

    /// Whether this check can run concurrently with other checks
    var supportsConcurrentExecution: Bool { get }

    /// Validate the transaction before commit
    ///
    /// - Parameters:
    ///   - context: Information about pending changes
    /// - Throws: If validation fails, transaction will be aborted
    func validate(context: CommitContext) async throws
}

// MARK: - Default Implementation

extension PreCommitCheck {
    public var identifier: String { String(describing: type(of: self)) }
    public var priority: Int { 0 }
    public var supportsConcurrentExecution: Bool { true }
}

// MARK: - PostCommitAction Protocol

/// Protocol for post-commit actions
///
/// Post-commit actions are executed after the transaction successfully commits.
/// They run asynchronously and their failures do not affect the committed transaction.
///
/// **Use Cases**:
/// - Cache invalidation
/// - Event publishing
/// - Notification sending
/// - Audit logging
/// - Webhook triggering
///
/// **Usage**:
/// ```swift
/// struct EventPublisher: PostCommitAction {
///     func execute(commitVersion: Int64, context: CommitContext) async throws {
///         for record in context.insertedRecords {
///             await eventBus.publish(.recordCreated(record))
///         }
///     }
/// }
/// ```
public protocol PostCommitAction: Sendable {
    /// Unique identifier for this action
    var identifier: String { get }

    /// Priority (higher runs first)
    var priority: Int { get }

    /// Whether failures should be logged as errors (vs warnings)
    var failureIsError: Bool { get }

    /// Execute the post-commit action
    ///
    /// - Parameters:
    ///   - commitVersion: The committed transaction version
    ///   - context: Information about committed changes
    /// - Throws: Failures are logged but don't affect the transaction
    func execute(commitVersion: Int64, context: CommitContext) async throws
}

// MARK: - Default Implementation

extension PostCommitAction {
    public var identifier: String { String(describing: type(of: self)) }
    public var priority: Int { 0 }
    public var failureIsError: Bool { false }
}

// MARK: - AsyncCommitHooks

/// Container for managing async commit hooks
///
/// Provides a structured way to manage pre-commit checks and post-commit actions
/// with support for priority ordering, concurrent execution, and error handling.
///
/// **Usage**:
/// ```swift
/// let hooks = AsyncCommitHooks()
///
/// // Add pre-commit validation
/// hooks.addPreCommitCheck(QuotaCheck())
/// hooks.addPreCommitCheck(PermissionCheck())
///
/// // Add post-commit actions
/// hooks.addPostCommitAction(CacheInvalidator())
/// hooks.addPostCommitAction(EventPublisher())
///
/// // During transaction commit:
/// try await hooks.runPreCommitChecks(context: context)
/// // ... commit transaction ...
/// await hooks.runPostCommitActions(commitVersion: version, context: context)
/// ```
public final class AsyncCommitHooks: Sendable {
    // MARK: - State

    private struct State: Sendable {
        var preCommitChecks: [PreCommitCheckEntry] = []
        var postCommitActions: [PostCommitActionEntry] = []
    }

    private struct PreCommitCheckEntry: Sendable {
        let id: UUID
        let check: any PreCommitCheck
    }

    private struct PostCommitActionEntry: Sendable {
        let id: UUID
        let action: any PostCommitAction
    }

    private let state: Mutex<State>

    /// Configuration for hook execution
    public let configuration: HookExecutionConfiguration

    // MARK: - Initialization

    public init(configuration: HookExecutionConfiguration = .default) {
        self.state = Mutex(State())
        self.configuration = configuration
    }

    // MARK: - Pre-Commit Check Management

    /// Add a pre-commit check
    ///
    /// - Parameter check: The check to add
    /// - Returns: An identifier for removing the check
    @discardableResult
    public func addPreCommitCheck(_ check: any PreCommitCheck) -> UUID {
        let id = UUID()
        state.withLock { state in
            state.preCommitChecks.append(PreCommitCheckEntry(id: id, check: check))
            state.preCommitChecks.sort { $0.check.priority > $1.check.priority }
        }
        return id
    }

    /// Remove a pre-commit check by ID
    public func removePreCommitCheck(id: UUID) {
        state.withLock { state in
            state.preCommitChecks.removeAll { $0.id == id }
        }
    }

    /// Remove all pre-commit checks matching a predicate
    public func removePreCommitChecks(where predicate: (any PreCommitCheck) -> Bool) {
        state.withLock { state in
            state.preCommitChecks.removeAll { predicate($0.check) }
        }
    }

    // MARK: - Post-Commit Action Management

    /// Add a post-commit action
    ///
    /// - Parameter action: The action to add
    /// - Returns: An identifier for removing the action
    @discardableResult
    public func addPostCommitAction(_ action: any PostCommitAction) -> UUID {
        let id = UUID()
        state.withLock { state in
            state.postCommitActions.append(PostCommitActionEntry(id: id, action: action))
            state.postCommitActions.sort { $0.action.priority > $1.action.priority }
        }
        return id
    }

    /// Remove a post-commit action by ID
    public func removePostCommitAction(id: UUID) {
        state.withLock { state in
            state.postCommitActions.removeAll { $0.id == id }
        }
    }

    /// Remove all post-commit actions matching a predicate
    public func removePostCommitActions(where predicate: (any PostCommitAction) -> Bool) {
        state.withLock { state in
            state.postCommitActions.removeAll { predicate($0.action) }
        }
    }

    // MARK: - Execution

    /// Run all pre-commit checks
    ///
    /// Checks are run in priority order. If `supportsConcurrentExecution` is true
    /// for all checks at the same priority level, they run concurrently.
    ///
    /// - Parameter context: The commit context
    /// - Throws: If any check fails
    public func runPreCommitChecks(context: CommitContext) async throws {
        let checks = state.withLock { $0.preCommitChecks.map { $0.check } }

        guard !checks.isEmpty else { return }

        // Group checks by priority for potential concurrent execution
        let grouped = Dictionary(grouping: checks) { $0.priority }
        let priorities = grouped.keys.sorted(by: >)

        for priority in priorities {
            guard let priorityChecks = grouped[priority] else { continue }

            let allSupportConcurrent = priorityChecks.allSatisfy { $0.supportsConcurrentExecution }

            if allSupportConcurrent && configuration.enableConcurrentPreCommitChecks {
                // Run concurrently
                try await withThrowingTaskGroup(of: Void.self) { group in
                    for check in priorityChecks {
                        group.addTask {
                            try await check.validate(context: context)
                        }
                    }
                    try await group.waitForAll()
                }
            } else {
                // Run sequentially
                for check in priorityChecks {
                    try await check.validate(context: context)
                }
            }
        }
    }

    /// Run all post-commit actions
    ///
    /// Actions run after the transaction has committed. Failures are logged
    /// but do not affect the committed transaction.
    ///
    /// - Parameters:
    ///   - commitVersion: The committed transaction version
    ///   - context: The commit context
    /// - Returns: Results of all actions
    @discardableResult
    public func runPostCommitActions(
        commitVersion: Int64,
        context: CommitContext
    ) async -> [PostCommitResult] {
        let actions = state.withLock { $0.postCommitActions.map { $0.action } }

        guard !actions.isEmpty else { return [] }

        var results: [PostCommitResult] = []

        if configuration.enableConcurrentPostCommitActions {
            // Run concurrently
            results = await withTaskGroup(of: PostCommitResult.self) { group in
                for action in actions {
                    group.addTask {
                        await self.executePostCommitAction(
                            action,
                            commitVersion: commitVersion,
                            context: context
                        )
                    }
                }

                var collected: [PostCommitResult] = []
                for await result in group {
                    collected.append(result)
                }
                return collected
            }
        } else {
            // Run sequentially
            for action in actions {
                let result = await executePostCommitAction(
                    action,
                    commitVersion: commitVersion,
                    context: context
                )
                results.append(result)
            }
        }

        return results
    }

    /// Execute a single post-commit action with error handling
    private func executePostCommitAction(
        _ action: any PostCommitAction,
        commitVersion: Int64,
        context: CommitContext
    ) async -> PostCommitResult {
        let startTime = Date()
        do {
            try await action.execute(commitVersion: commitVersion, context: context)
            return PostCommitResult(
                identifier: action.identifier,
                succeeded: true,
                duration: Date().timeIntervalSince(startTime),
                error: nil
            )
        } catch {
            return PostCommitResult(
                identifier: action.identifier,
                succeeded: false,
                duration: Date().timeIntervalSince(startTime),
                error: error
            )
        }
    }

    // MARK: - Statistics

    /// Number of registered pre-commit checks
    public var preCommitCheckCount: Int {
        state.withLock { $0.preCommitChecks.count }
    }

    /// Number of registered post-commit actions
    public var postCommitActionCount: Int {
        state.withLock { $0.postCommitActions.count }
    }
}

// MARK: - HookExecutionConfiguration

/// Configuration for hook execution
public struct HookExecutionConfiguration: Sendable, Equatable {
    /// Whether to run pre-commit checks concurrently when possible
    public let enableConcurrentPreCommitChecks: Bool

    /// Whether to run post-commit actions concurrently
    public let enableConcurrentPostCommitActions: Bool

    /// Timeout for pre-commit checks in seconds
    public let preCommitCheckTimeoutSeconds: Double

    /// Timeout for post-commit actions in seconds
    public let postCommitActionTimeoutSeconds: Double

    /// Default configuration
    public static let `default` = HookExecutionConfiguration(
        enableConcurrentPreCommitChecks: true,
        enableConcurrentPostCommitActions: true,
        preCommitCheckTimeoutSeconds: 5.0,
        postCommitActionTimeoutSeconds: 30.0
    )

    /// Sequential execution (for debugging)
    public static let sequential = HookExecutionConfiguration(
        enableConcurrentPreCommitChecks: false,
        enableConcurrentPostCommitActions: false,
        preCommitCheckTimeoutSeconds: 5.0,
        postCommitActionTimeoutSeconds: 30.0
    )

    public init(
        enableConcurrentPreCommitChecks: Bool = true,
        enableConcurrentPostCommitActions: Bool = true,
        preCommitCheckTimeoutSeconds: Double = 5.0,
        postCommitActionTimeoutSeconds: Double = 30.0
    ) {
        self.enableConcurrentPreCommitChecks = enableConcurrentPreCommitChecks
        self.enableConcurrentPostCommitActions = enableConcurrentPostCommitActions
        self.preCommitCheckTimeoutSeconds = preCommitCheckTimeoutSeconds
        self.postCommitActionTimeoutSeconds = postCommitActionTimeoutSeconds
    }
}

// MARK: - PostCommitResult

/// Result of a post-commit action execution
public struct PostCommitResult: Sendable {
    /// Action identifier
    public let identifier: String

    /// Whether the action succeeded
    public let succeeded: Bool

    /// Execution duration in seconds
    public let duration: TimeInterval

    /// Error if failed
    public let error: Error?

    public init(
        identifier: String,
        succeeded: Bool,
        duration: TimeInterval,
        error: Error?
    ) {
        self.identifier = identifier
        self.succeeded = succeeded
        self.duration = duration
        self.error = error
    }
}

// MARK: - Common Pre-Commit Checks

/// Pre-commit check that validates record counts don't exceed a limit
public struct RecordCountLimitCheck: PreCommitCheck {
    public let identifier = "RecordCountLimitCheck"
    public let priority: Int = 100  // Run early
    public let supportsConcurrentExecution = true

    private let typeName: String
    private let maxCount: Int
    private let getCurrentCount: @Sendable () async throws -> Int

    public init<T: Persistable>(
        for type: T.Type,
        maxCount: Int,
        getCurrentCount: @escaping @Sendable () async throws -> Int
    ) {
        self.typeName = T.persistableType
        self.maxCount = maxCount
        self.getCurrentCount = getCurrentCount
    }

    public func validate(context: CommitContext) async throws {
        let insertCount = context.insertedRecords.filter { $0.typeName == typeName }.count
        let deleteCount = context.deletedRecords.filter { $0.typeName == typeName }.count
        let netChange = insertCount - deleteCount

        if netChange > 0 {
            let currentCount = try await getCurrentCount()
            if currentCount + netChange > maxCount {
                throw PreCommitValidationError.recordCountLimitExceeded(
                    typeName: typeName,
                    current: currentCount,
                    adding: netChange,
                    limit: maxCount
                )
            }
        }
    }
}

/// Pre-commit check that ensures required fields are present
public struct RequiredFieldsCheck: PreCommitCheck {
    public let identifier = "RequiredFieldsCheck"
    public let priority: Int = 90
    public let supportsConcurrentExecution = true

    private let typeName: String
    private let requiredFields: Set<String>

    public init<T: Persistable>(
        for type: T.Type,
        requiredFields: Set<String>
    ) {
        self.typeName = T.persistableType
        self.requiredFields = requiredFields
    }

    public func validate(context: CommitContext) async throws {
        let recordsToCheck = context.insertedRecords.filter { $0.typeName == typeName }

        for record in recordsToCheck {
            let presentFields = Set(record.values.keys)
            let missingFields = requiredFields.subtracting(presentFields)

            if !missingFields.isEmpty {
                throw PreCommitValidationError.requiredFieldsMissing(
                    typeName: typeName,
                    recordId: record.id,
                    missingFields: Array(missingFields)
                )
            }
        }
    }
}

// MARK: - Common Post-Commit Actions

/// Post-commit action that invalidates a cache
public struct CacheInvalidationAction: PostCommitAction {
    public let identifier = "CacheInvalidationAction"
    public let priority: Int = 100  // Run early
    public let failureIsError = false

    private let invalidate: @Sendable (CommitContext) async throws -> Void

    public init(invalidate: @escaping @Sendable (CommitContext) async throws -> Void) {
        self.invalidate = invalidate
    }

    public func execute(commitVersion: Int64, context: CommitContext) async throws {
        try await invalidate(context)
    }
}

/// Post-commit action that logs changes for auditing
public struct AuditLogAction: PostCommitAction {
    public let identifier = "AuditLogAction"
    public let priority: Int = 50
    public let failureIsError = true  // Audit failures should be errors

    private let log: @Sendable (Int64, CommitContext) async throws -> Void

    public init(log: @escaping @Sendable (Int64, CommitContext) async throws -> Void) {
        self.log = log
    }

    public func execute(commitVersion: Int64, context: CommitContext) async throws {
        try await log(commitVersion, context)
    }
}

// MARK: - PreCommitValidationError

/// Errors from pre-commit validation
public enum PreCommitValidationError: Error, CustomStringConvertible, Sendable {
    case recordCountLimitExceeded(typeName: String, current: Int, adding: Int, limit: Int)
    case requiredFieldsMissing(typeName: String, recordId: String, missingFields: [String])
    case customValidationFailed(String)

    public var description: String {
        switch self {
        case .recordCountLimitExceeded(let typeName, let current, let adding, let limit):
            return "Record count limit exceeded for \(typeName): current=\(current), adding=\(adding), limit=\(limit)"
        case .requiredFieldsMissing(let typeName, let recordId, let missingFields):
            return "Required fields missing for \(typeName)[\(recordId)]: \(missingFields.joined(separator: ", "))"
        case .customValidationFailed(let message):
            return "Validation failed: \(message)"
        }
    }
}
