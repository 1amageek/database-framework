// PostCommit.swift
// DatabaseEngine - Post-commit actions
//
// Reference: FDB Record Layer PostCommit
// Provides callbacks that execute after successful transaction commit.

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import StorageKit
import Synchronization

// MARK: - PostCommit Protocol

/// Protocol for post-commit callbacks
///
/// PostCommit hooks are executed after a transaction successfully commits.
/// They are useful for:
/// - Cache invalidation
/// - Sending notifications
/// - Updating external systems
/// - Logging/auditing
///
/// **Important**:
/// - PostCommit hooks run AFTER the transaction is committed
/// - They are NOT part of the transaction (cannot be rolled back)
/// - Failures in PostCommit do not affect the committed transaction
/// - Multiple PostCommits may run concurrently
///
/// **Usage**:
/// ```swift
/// struct CacheInvalidation: PostCommit {
///     let keys: [String]
///
///     func run() async {
///         for key in keys {
///             await cache.invalidate(key)
///         }
///     }
/// }
///
/// context.addPostCommit(CacheInvalidation(keys: ["user:\(userID)"]))
/// ```
///
/// **Reference**: FDB Record Layer `PostCommit`
public protocol PostCommit: Sendable {
    /// Execute the post-commit action
    ///
    /// This runs after the transaction has successfully committed.
    /// Errors thrown here are logged but do not affect the committed transaction.
    func run() async throws
}

// MARK: - PostCommitResult

/// Result of post-commit execution
public struct PostCommitResult: Sendable {
    /// Name of the post-commit hook
    public let name: String

    /// Whether execution succeeded
    public let success: Bool

    /// Duration of execution
    public let duration: TimeInterval

    /// Error if failed
    public let error: Error?

    public init(name: String, success: Bool, duration: TimeInterval, error: Error? = nil) {
        self.name = name
        self.success = success
        self.duration = duration
        self.error = error
    }
}

// MARK: - Named PostCommit

/// A post-commit hook with an associated name
public struct NamedPostCommit: Sendable {
    /// Name of the hook (for logging)
    public let name: String

    /// The actual hook implementation
    public let hook: any PostCommit

    /// Priority (lower runs first)
    public let priority: Int

    /// Whether to run concurrently with other hooks
    public let runConcurrently: Bool

    public init(
        name: String,
        hook: any PostCommit,
        priority: Int = 100,
        runConcurrently: Bool = true
    ) {
        self.name = name
        self.hook = hook
        self.priority = priority
        self.runConcurrently = runConcurrently
    }
}

// MARK: - Post-Commit Actions

/// Executes an application-provided action after a successful commit.
public struct PostCommitAction: PostCommit {
    private let performAction: @Sendable () async throws -> Void

    public init(_ performAction: @escaping @Sendable () async throws -> Void) {
        self.performAction = performAction
    }

    public func run() async throws {
        try await performAction()
    }
}

/// Fire-and-forget post-commit (errors are ignored)
public struct FireAndForgetPostCommit: PostCommit {
    private let inner: any PostCommit

    public init(_ inner: any PostCommit) {
        self.inner = inner
    }

    public func run() async throws {
        do {
            try await inner.run()
        } catch {
            // Silently ignore errors
        }
    }
}

/// Delayed post-commit that waits before executing
public struct DelayedPostCommit: PostCommit {
    private let inner: any PostCommit
    private let delay: Duration
    private let clock: any StorageMonotonicClock

    public init(
        _ inner: any PostCommit,
        delay: Duration,
        clock: any StorageMonotonicClock = SystemStorageClock()
    ) {
        precondition(delay >= .zero, "Post-commit delay must not be negative")
        self.inner = inner
        self.delay = delay
        self.clock = clock
    }

    public func run() async throws {
        try await clock.sleep(until: clock.now.advanced(by: delay))
        try await inner.run()
    }
}

/// Retry post-commit that retries on failure
public struct RetryingPostCommit: PostCommit {
    private let inner: any PostCommit
    private let maxAttempts: Int
    private let backoffMs: Int
    private let clock: any StorageMonotonicClock

    public init(
        _ inner: any PostCommit,
        maxAttempts: Int = 3,
        backoffMs: Int = 100,
        clock: any StorageMonotonicClock = SystemStorageClock()
    ) {
        precondition(maxAttempts > 0, "Post-commit retry count must be positive")
        precondition(backoffMs >= 0, "Post-commit backoff must not be negative")
        precondition(
            maxAttempts <= Int.bitWidth,
            "Post-commit retry count exceeds the backoff exponent range"
        )
        self.inner = inner
        self.maxAttempts = maxAttempts
        self.backoffMs = backoffMs
        self.clock = clock
    }

    public func run() async throws {
        var lastError: Error?

        for attempt in 0..<maxAttempts {
            do {
                try await inner.run()
                return
            } catch {
                lastError = error
                if attempt < maxAttempts - 1 {
                    let (delay, overflow) = backoffMs.multipliedReportingOverflow(
                        by: 1 << attempt
                    )
                    precondition(!overflow, "Post-commit backoff overflow")
                    try await clock.sleep(
                        until: clock.now.advanced(
                            by: .milliseconds(Int64(delay))
                        )
                    )
                }
            }
        }

        if let error = lastError {
            throw error
        }
    }
}

/// Composite post-commit that runs multiple hooks.
///
/// Two execution modes with different error semantics:
///
/// | Mode | Ordering | Error behavior |
/// |------|----------|----------------|
/// | `runConcurrently: false` (default) | Sequential, in declaration order | First throw propagates; remaining hooks are skipped |
/// | `runConcurrently: true` | Parallel via `TaskGroup` | Each hook's failure is **logged and swallowed** — other hooks still run, and `run()` always succeeds |
///
/// Concurrent mode is intentionally best-effort: post-commit hooks are observational
/// (cache invalidation, notifications, etc.) and one failing hook should not cancel
/// peers that might be doing unrelated work. Callers that require strict failure
/// propagation across multiple hooks must use sequential mode.
public struct CompositePostCommit: PostCommit {
    private let hooks: [any PostCommit]
    private let runConcurrently: Bool
    private let logger: DatabaseLogger

    public init(
        hooks: [any PostCommit],
        runConcurrently: Bool = false,
        logger: DatabaseLogger = .disabled
    ) {
        self.hooks = hooks
        self.runConcurrently = runConcurrently
        self.logger = logger
    }

    public func run() async throws {
        if runConcurrently {
            // Best-effort: log per-hook failures but never propagate them, so that
            // one failing observational hook does not cancel its peers. See type
            // docstring for the full contract.
            let logger = logger
            await withTaskGroup(of: Void.self) { group in
                for hook in hooks {
                    group.addTask {
                        do {
                            try await hook.run()
                        } catch {
                            logger.error("post-commit hook failed: \(error)")
                        }
                    }
                }
            }
        } else {
            for hook in hooks {
                try await hook.run()
            }
        }
    }
}

// MARK: - PostCommitRegistry

/// Registry for managing post-commit hooks
///
/// Maintains a collection of post-commit hooks that will be executed
/// after successful transaction commit.
public final class PostCommitRegistry: Sendable {
    private let hooks: Mutex<[NamedPostCommit]>

    public init() {
        self.hooks = Mutex([])
    }

    /// Add a post-commit hook
    public func add(
        _ hook: any PostCommit,
        name: String? = nil,
        priority: Int = 100,
        runConcurrently: Bool = true
    ) {
        let named = NamedPostCommit(
            name: name ?? "postcommit_\(UUID().uuidString.prefix(8))",
            hook: hook,
            priority: priority,
            runConcurrently: runConcurrently
        )
        hooks.withLock { $0.append(named) }
    }

    /// Add an application-defined post-commit action.
    public func add(
        name: String? = nil,
        priority: Int = 100,
        runConcurrently: Bool = true,
        _ action: @escaping @Sendable () async throws -> Void
    ) {
        add(PostCommitAction(action), name: name, priority: priority, runConcurrently: runConcurrently)
    }

    /// Remove all hooks
    public func clear() {
        hooks.withLock { $0.removeAll() }
    }

    /// Execute all registered hooks
    ///
    /// - Returns: Results of each hook execution
    @discardableResult
    public func executeAll() async -> [PostCommitResult] {
        let sortedHooks = hooks.withLock { hooks in
            hooks.sorted { $0.priority < $1.priority }
        }

        // Separate sequential and concurrent hooks
        let sequential = sortedHooks.filter { !$0.runConcurrently }
        let concurrent = sortedHooks.filter { $0.runConcurrently }

        var results: [PostCommitResult] = []

        // Run sequential hooks first (in priority order)
        for named in sequential {
            let result = await executeHook(named)
            results.append(result)
        }

        // Run concurrent hooks in parallel
        if !concurrent.isEmpty {
            let concurrentResults = await withTaskGroup(of: PostCommitResult.self) { group in
                for named in concurrent {
                    group.addTask {
                        await self.executeHook(named)
                    }
                }

                var collected: [PostCommitResult] = []
                for await result in group {
                    collected.append(result)
                }
                return collected
            }
            results.append(contentsOf: concurrentResults)
        }

        return results
    }

    private func executeHook(_ named: NamedPostCommit) async -> PostCommitResult {
        let startTime = Date()

        do {
            try await named.hook.run()
            let duration = Date().timeIntervalSince(startTime)
            return PostCommitResult(name: named.name, success: true, duration: duration)
        } catch {
            let duration = Date().timeIntervalSince(startTime)
            return PostCommitResult(name: named.name, success: false, duration: duration, error: error)
        }
    }

    /// Number of registered hooks
    public var count: Int {
        hooks.withLock { $0.count }
    }
}

// MARK: - Common PostCommit Factories

extension PostCommit where Self == PostCommitAction {
    /// Create a cache invalidation post-commit
    public static func invalidateCache<Cache: Sendable>(
        _ cache: Cache,
        keys: [String],
        invalidate: @escaping @Sendable (Cache, String) async -> Void
    ) -> some PostCommit {
        PostCommitAction {
            for key in keys {
                await invalidate(cache, key)
            }
        }
    }

    /// Create a notification post-commit
    public static func notify(
        _ sendNotification: @escaping @Sendable () async -> Void
    ) -> some PostCommit {
        PostCommitAction {
            await sendNotification()
        }
    }
}
