// CommitCheck.swift
// DatabaseEngine - Pre-commit validation hooks
//
// Reference: FDB Record Layer CommitCheckAsync
// Provides extensible pre-commit validation for transactions.

import Foundation
import FoundationDB

// MARK: - CommitCheck Protocol

/// Protocol for synchronous pre-commit validation
///
/// CommitChecks are executed before a transaction commits, allowing validation
/// of constraints that span multiple operations within the transaction.
///
/// **Use Cases**:
/// - Uniqueness constraint validation
/// - Referential integrity checks
/// - Business rule validation
/// - Quota/limit enforcement
///
/// **Important**: CommitChecks run within the transaction, so they should be
/// efficient and avoid long-running operations.
///
/// **Usage**:
/// ```swift
/// struct UniquenessCheck: CommitCheck {
///     let field: String
///     let value: String
///     let indexSubspace: Subspace
///
///     func check(transaction: any TransactionProtocol) async throws {
///         let key = indexSubspace.pack(Tuple([value]))
///         if let existing = try await transaction.getValue(for: key) {
///             throw UniquenessViolation(field: field, value: value)
///         }
///     }
/// }
///
/// context.addCommitCheck(UniquenessCheck(
///     field: "email",
///     value: user.email,
///     indexSubspace: emailIndexSubspace
/// ))
/// ```
///
/// **Reference**: FDB Record Layer `CommitCheckAsync`
public protocol CommitCheck: Sendable {
    /// Execute the check within the transaction
    ///
    /// - Parameter transaction: The transaction to validate against
    /// - Throws: Error if validation fails (transaction will be aborted)
    func check(transaction: any TransactionProtocol) async throws
}

// MARK: - CommitCheckResult

/// Result of a commit check execution
public enum CommitCheckResult: Sendable {
    /// Check passed successfully
    case passed

    /// Check failed with reason
    case failed(reason: String)

    /// Check was skipped (e.g., condition not met)
    case skipped(reason: String)
}

// MARK: - CommitCheckError

/// Error thrown when a commit check fails
public enum CommitCheckError: Error, CustomStringConvertible, Sendable {
    /// A commit check failed validation
    case validationFailed(checkName: String, reason: String)

    /// Multiple commit checks failed
    case multipleFailures(failures: [(checkName: String, reason: String)])

    /// Commit check execution timed out
    case timeout(checkName: String, duration: TimeInterval)

    /// Commit check threw an unexpected error
    case checkError(checkName: String, underlying: Error)

    public var description: String {
        switch self {
        case .validationFailed(let name, let reason):
            return "CommitCheck '\(name)' failed: \(reason)"
        case .multipleFailures(let failures):
            let details = failures.map { "'\($0.checkName)': \($0.reason)" }.joined(separator: ", ")
            return "Multiple commit checks failed: \(details)"
        case .timeout(let name, let duration):
            return "CommitCheck '\(name)' timed out after \(String(format: "%.2f", duration))s"
        case .checkError(let name, let underlying):
            return "CommitCheck '\(name)' threw error: \(underlying)"
        }
    }
}

// MARK: - Named CommitCheck

/// A commit check with an associated name for logging and error reporting
public struct NamedCommitCheck: Sendable {
    /// Name of the check (for logging/errors)
    public let name: String

    /// The actual check implementation
    public let check: any CommitCheck

    /// Priority (lower runs first)
    public let priority: Int

    public init(name: String, check: any CommitCheck, priority: Int = 100) {
        self.name = name
        self.check = check
        self.priority = priority
    }
}

// MARK: - Built-in CommitChecks

/// Closure-based commit check for simple validations
public struct ClosureCommitCheck: CommitCheck {
    private let closure: @Sendable (any TransactionProtocol) async throws -> Void

    public init(_ closure: @escaping @Sendable (any TransactionProtocol) async throws -> Void) {
        self.closure = closure
    }

    public func check(transaction: any TransactionProtocol) async throws {
        try await closure(transaction)
    }
}

/// Uniqueness constraint check
///
/// Verifies that no other record has the same value for a given field.
///
/// **Usage**:
/// ```swift
/// let check = UniquenessCommitCheck(
///     indexSubspace: emailIndex,
///     value: user.email,
///     excludeID: user.id  // Exclude current record for updates
/// )
/// ```
public struct UniquenessCommitCheck: CommitCheck {
    /// Subspace of the unique index
    public let indexSubspace: Subspace

    /// Value to check for uniqueness
    public let value: any TupleElement & Sendable

    /// ID to exclude (for updates)
    public let excludeID: (any TupleElement & Sendable)?

    /// Field name (for error messages)
    public let fieldName: String

    public init(
        indexSubspace: Subspace,
        value: any TupleElement & Sendable,
        excludeID: (any TupleElement & Sendable)? = nil,
        fieldName: String = "field"
    ) {
        self.indexSubspace = indexSubspace
        self.value = value
        self.excludeID = excludeID
        self.fieldName = fieldName
    }

    public func check(transaction: any TransactionProtocol) async throws {
        let valueSubspace = indexSubspace.subspace(Tuple([value]))
        let (begin, end) = valueSubspace.range()

        // Scan for existing entries with this value
        for try await (key, _) in transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: false
        ) {
            // Extract ID from key
            if let tuple = try? valueSubspace.unpack(key) {
                // If excludeID is provided, skip if it matches
                if let excludeID = excludeID,
                   tuple.count >= 1,
                   let existingID = tuple[0] {
                    // Compare IDs (simplified - assumes same type)
                    if String(describing: existingID) == String(describing: excludeID) {
                        continue
                    }
                }

                // Found a duplicate
                throw SimpleUniquenessError(
                    field: fieldName,
                    value: String(describing: value)
                )
            }
        }
    }
}

/// Simple uniqueness violation error for CommitCheck
///
/// Distinct from `UniquenessViolationError` in the index module which has more context.
public struct SimpleUniquenessError: Error, CustomStringConvertible, Sendable {
    public let field: String
    public let value: String

    public init(field: String, value: String) {
        self.field = field
        self.value = value
    }

    public var description: String {
        "Uniqueness violation: '\(field)' value '\(value)' already exists"
    }
}

/// Condition-based commit check
///
/// Only executes the inner check if the condition is true.
public struct ConditionalCommitCheck: CommitCheck {
    private let condition: @Sendable () -> Bool
    private let innerCheck: any CommitCheck

    public init(
        if condition: @escaping @Sendable () -> Bool,
        then check: any CommitCheck
    ) {
        self.condition = condition
        self.innerCheck = check
    }

    public func check(transaction: any TransactionProtocol) async throws {
        if condition() {
            try await innerCheck.check(transaction: transaction)
        }
    }
}

/// Composite commit check that runs multiple checks
public struct CompositeCommitCheck: CommitCheck {
    private let checks: [any CommitCheck]
    private let failFast: Bool

    /// Create a composite check
    ///
    /// - Parameters:
    ///   - checks: Checks to run
    ///   - failFast: If true, stop on first failure. If false, collect all failures.
    public init(checks: [any CommitCheck], failFast: Bool = true) {
        self.checks = checks
        self.failFast = failFast
    }

    public func check(transaction: any TransactionProtocol) async throws {
        if failFast {
            for check in checks {
                try await check.check(transaction: transaction)
            }
        } else {
            var errors: [Error] = []
            for check in checks {
                do {
                    try await check.check(transaction: transaction)
                } catch {
                    errors.append(error)
                }
            }
            if !errors.isEmpty {
                if errors.count == 1 {
                    throw errors[0]
                } else {
                    throw CommitCheckError.multipleFailures(
                        failures: errors.enumerated().map { ("check_\($0.offset)", "\($0.element)") }
                    )
                }
            }
        }
    }
}

// MARK: - CommitCheckRegistry

/// Registry for managing commit checks
///
/// Maintains a collection of commit checks that will be executed before commit.
/// Checks are executed in priority order (lower priority first).
public final class CommitCheckRegistry: Sendable {
    private let checks: Mutex<[NamedCommitCheck]>

    public init() {
        self.checks = Mutex([])
    }

    /// Add a commit check
    public func add(_ check: any CommitCheck, name: String? = nil, priority: Int = 100) {
        let named = NamedCommitCheck(
            name: name ?? "check_\(UUID().uuidString.prefix(8))",
            check: check,
            priority: priority
        )
        checks.withLock { $0.append(named) }
    }

    /// Add a closure-based check
    public func add(
        name: String? = nil,
        priority: Int = 100,
        _ closure: @escaping @Sendable (any TransactionProtocol) async throws -> Void
    ) {
        add(ClosureCommitCheck(closure), name: name, priority: priority)
    }

    /// Remove all checks
    public func clear() {
        checks.withLock { $0.removeAll() }
    }

    /// Execute all registered checks
    ///
    /// - Parameter transaction: Transaction to validate against
    /// - Throws: CommitCheckError if any check fails
    public func executeAll(transaction: any TransactionProtocol) async throws {
        let sortedChecks = checks.withLock { checks in
            checks.sorted { $0.priority < $1.priority }
        }

        for named in sortedChecks {
            do {
                try await named.check.check(transaction: transaction)
            } catch {
                if error is CommitCheckError {
                    throw error
                }
                throw CommitCheckError.checkError(checkName: named.name, underlying: error)
            }
        }
    }

    /// Number of registered checks
    public var count: Int {
        checks.withLock { $0.count }
    }
}

// MARK: - Synchronization Import

import Synchronization
