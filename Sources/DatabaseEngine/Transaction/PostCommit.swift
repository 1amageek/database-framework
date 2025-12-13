// PostCommit.swift
// DatabaseEngine - Post-commit callback hooks
//
// Reference: FDB Record Layer PostCommit
// Provides callbacks that execute after successful transaction commit.

import Foundation
import FoundationDB
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

// MARK: - Built-in PostCommit Implementations

/// Closure-based post-commit hook
public struct ClosurePostCommit: PostCommit {
    private let closure: @Sendable () async throws -> Void

    public init(_ closure: @escaping @Sendable () async throws -> Void) {
        self.closure = closure
    }

    public func run() async throws {
        try await closure()
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
    private let delay: TimeInterval

    public init(_ inner: any PostCommit, delay: TimeInterval) {
        self.inner = inner
        self.delay = delay
    }

    public func run() async throws {
        try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
        try await inner.run()
    }
}

/// Retry post-commit that retries on failure
public struct RetryingPostCommit: PostCommit {
    private let inner: any PostCommit
    private let maxAttempts: Int
    private let backoffMs: Int

    public init(_ inner: any PostCommit, maxAttempts: Int = 3, backoffMs: Int = 100) {
        self.inner = inner
        self.maxAttempts = maxAttempts
        self.backoffMs = backoffMs
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
                    let delay = backoffMs * (1 << attempt)
                    try await Task.sleep(nanoseconds: UInt64(delay) * 1_000_000)
                }
            }
        }

        if let error = lastError {
            throw error
        }
    }
}

/// Composite post-commit that runs multiple hooks
public struct CompositePostCommit: PostCommit {
    private let hooks: [any PostCommit]
    private let runConcurrently: Bool

    public init(hooks: [any PostCommit], runConcurrently: Bool = false) {
        self.hooks = hooks
        self.runConcurrently = runConcurrently
    }

    public func run() async throws {
        if runConcurrently {
            await withTaskGroup(of: Void.self) { group in
                for hook in hooks {
                    group.addTask {
                        try? await hook.run()
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

    /// Add a closure-based hook
    public func add(
        name: String? = nil,
        priority: Int = 100,
        runConcurrently: Bool = true,
        _ closure: @escaping @Sendable () async throws -> Void
    ) {
        add(ClosurePostCommit(closure), name: name, priority: priority, runConcurrently: runConcurrently)
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

extension PostCommit where Self == ClosurePostCommit {
    /// Create a cache invalidation post-commit
    public static func invalidateCache<Cache: Sendable>(
        _ cache: Cache,
        keys: [String],
        invalidate: @escaping @Sendable (Cache, String) async -> Void
    ) -> some PostCommit {
        ClosurePostCommit {
            for key in keys {
                await invalidate(cache, key)
            }
        }
    }

    /// Create a notification post-commit
    public static func notify(
        _ closure: @escaping @Sendable () async -> Void
    ) -> some PostCommit {
        ClosurePostCommit {
            await closure()
        }
    }
}
