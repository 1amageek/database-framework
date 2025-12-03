// CommitHook.swift
// Transaction - Pre/post commit hooks for extensibility

import Foundation
import Synchronization
import Core

// MARK: - Commit Hook Protocol

/// Protocol for transaction commit hooks
///
/// Hooks are called before and after transaction commit, allowing for:
/// - Validation before commit
/// - Audit logging after commit
/// - Cache invalidation
/// - Event publishing
/// - Custom side effects
///
/// **Usage**:
/// ```swift
/// struct AuditHook: CommitHook {
///     func beforeCommit(context: CommitContext) async throws {
///         // Validate changes
///         for record in context.insertedRecords {
///             guard isValid(record) else {
///                 throw ValidationError.invalidRecord
///             }
///         }
///     }
///
///     func afterCommit(context: CommitContext) async {
///         // Log changes
///         await auditLogger.log(context)
///     }
/// }
///
/// container.registerHook(AuditHook())
/// ```
public protocol CommitHook: Sendable {
    /// Called before the transaction commits
    ///
    /// Throwing an error will abort the transaction.
    ///
    /// - Parameter context: Information about the pending changes
    /// - Throws: Any error to abort the commit
    func beforeCommit(context: CommitContext) async throws

    /// Called after the transaction successfully commits
    ///
    /// This is called after the transaction is durably committed.
    /// Errors here do not affect the transaction.
    ///
    /// - Parameter context: Information about the committed changes
    func afterCommit(context: CommitContext) async
}

// MARK: - Default Implementation

extension CommitHook {
    /// Default implementation does nothing
    public func beforeCommit(context: CommitContext) async throws {}

    /// Default implementation does nothing
    public func afterCommit(context: CommitContext) async {}
}

// MARK: - Commit Context

/// Information about changes in a transaction
public struct CommitContext: Sendable {
    /// Unique transaction identifier
    public let transactionId: UUID

    /// Records being inserted
    public let insertedRecords: [AnyPersistable]

    /// Records being updated (old and new values)
    public let updatedRecords: [UpdatedRecord]

    /// Records being deleted
    public let deletedRecords: [AnyPersistable]

    /// Timestamp of the commit
    public let timestamp: Date

    /// Custom metadata attached to the transaction
    public let metadata: [String: any Sendable]

    public init(
        transactionId: UUID = UUID(),
        insertedRecords: [AnyPersistable] = [],
        updatedRecords: [UpdatedRecord] = [],
        deletedRecords: [AnyPersistable] = [],
        timestamp: Date = Date(),
        metadata: [String: any Sendable] = [:]
    ) {
        self.transactionId = transactionId
        self.insertedRecords = insertedRecords
        self.updatedRecords = updatedRecords
        self.deletedRecords = deletedRecords
        self.timestamp = timestamp
        self.metadata = metadata
    }

    /// Total number of affected records
    public var totalAffectedRecords: Int {
        insertedRecords.count + updatedRecords.count + deletedRecords.count
    }

    /// Check if any records of a specific type are affected
    public func hasChanges<T: Persistable>(for type: T.Type) -> Bool {
        let typeName = T.persistableType

        let hasInserts = insertedRecords.contains { $0.typeName == typeName }
        let hasUpdates = updatedRecords.contains { $0.typeName == typeName }
        let hasDeletes = deletedRecords.contains { $0.typeName == typeName }

        return hasInserts || hasUpdates || hasDeletes
    }
}

// MARK: - Updated Record

/// Represents an updated record with old and new values
public struct UpdatedRecord: @unchecked Sendable {
    /// Type name of the record
    public let typeName: String

    /// Record ID
    public let id: String

    /// Old values (before update)
    public let oldValues: [String: any Sendable]

    /// New values (after update)
    public let newValues: [String: any Sendable]

    /// Fields that changed
    public var changedFields: Set<String> {
        var changed: Set<String> = []
        for (key, newValue) in newValues {
            if let oldValue = oldValues[key] {
                if "\(oldValue)" != "\(newValue)" {
                    changed.insert(key)
                }
            } else {
                changed.insert(key)
            }
        }
        return changed
    }

    public init(
        typeName: String,
        id: String,
        oldValues: [String: any Sendable],
        newValues: [String: any Sendable]
    ) {
        self.typeName = typeName
        self.id = id
        self.oldValues = oldValues
        self.newValues = newValues
    }
}

// MARK: - Any Persistable

/// Type-erased Persistable for storage in CommitContext
public struct AnyPersistable: @unchecked Sendable {
    /// Type name
    public let typeName: String

    /// Record ID as string
    public let id: String

    /// Field values
    public let values: [String: any Sendable]

    public init<T: Persistable>(_ item: T) {
        self.typeName = T.persistableType
        self.id = "\(item.id)"
        self.values = Self.extractValues(from: item)
    }

    private static func extractValues<T: Persistable>(from item: T) -> [String: any Sendable] {
        var values: [String: any Sendable] = [:]
        for field in T.allFields {
            if let value = item[dynamicMember: field] {
                // Convert to Sendable types
                if let str = value as? String {
                    values[field] = str
                } else if let int = value as? Int {
                    values[field] = int
                } else if let int64 = value as? Int64 {
                    values[field] = int64
                } else if let double = value as? Double {
                    values[field] = double
                } else if let bool = value as? Bool {
                    values[field] = bool
                } else {
                    values[field] = String(describing: value)
                }
            }
        }
        return values
    }
}

// MARK: - Commit Hook Manager

/// Manages commit hooks for a container
public final class CommitHookManager: Sendable {
    /// Registered hooks
    private let hooks: Mutex<[HookEntry]>

    /// Hook entry with identifier
    private struct HookEntry: Sendable {
        let id: UUID
        let hook: any CommitHook
        let priority: Int
    }

    public init() {
        self.hooks = Mutex([])
    }

    /// Register a hook
    ///
    /// - Parameters:
    ///   - hook: The hook to register
    ///   - priority: Higher priority hooks run first (default: 0)
    /// - Returns: An identifier that can be used to unregister the hook
    @discardableResult
    public func register(_ hook: any CommitHook, priority: Int = 0) -> UUID {
        let id = UUID()
        hooks.withLock { hooks in
            hooks.append(HookEntry(id: id, hook: hook, priority: priority))
            hooks.sort { $0.priority > $1.priority }
        }
        return id
    }

    /// Unregister a hook by its identifier
    public func unregister(id: UUID) {
        hooks.withLock { hooks in
            hooks.removeAll { $0.id == id }
        }
    }

    /// Execute all beforeCommit hooks
    ///
    /// - Parameter context: The commit context
    /// - Throws: If any hook throws, the commit should be aborted
    internal func executeBeforeCommit(context: CommitContext) async throws {
        let currentHooks = hooks.withLock { Array($0) }

        for entry in currentHooks {
            try await entry.hook.beforeCommit(context: context)
        }
    }

    /// Execute all afterCommit hooks
    ///
    /// - Parameter context: The commit context
    internal func executeAfterCommit(context: CommitContext) async {
        let currentHooks = hooks.withLock { Array($0) }

        for entry in currentHooks {
            await entry.hook.afterCommit(context: context)
        }
    }

    /// Number of registered hooks
    public var hookCount: Int {
        hooks.withLock { $0.count }
    }
}

// MARK: - Common Hook Implementations

/// A simple hook that runs closures
public struct ClosureHook: CommitHook {
    private let onBeforeCommit: (@Sendable (CommitContext) async throws -> Void)?
    private let onAfterCommit: (@Sendable (CommitContext) async -> Void)?

    public init(
        beforeCommit: (@Sendable (CommitContext) async throws -> Void)? = nil,
        afterCommit: (@Sendable (CommitContext) async -> Void)? = nil
    ) {
        self.onBeforeCommit = beforeCommit
        self.onAfterCommit = afterCommit
    }

    public func beforeCommit(context: CommitContext) async throws {
        try await onBeforeCommit?(context)
    }

    public func afterCommit(context: CommitContext) async {
        await onAfterCommit?(context)
    }
}

/// A hook that validates records before commit
public struct ValidationHook<T: Persistable>: CommitHook {
    private let validator: @Sendable (T) throws -> Void

    public init(for type: T.Type, validator: @escaping @Sendable (T) throws -> Void) {
        self.validator = validator
    }

    public func beforeCommit(context: CommitContext) async throws {
        // Validation would need access to the actual records
        // This is a placeholder showing the pattern
    }

    public func afterCommit(context: CommitContext) async {}
}
