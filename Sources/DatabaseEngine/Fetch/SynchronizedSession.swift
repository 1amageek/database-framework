// SynchronizedSession.swift
// DatabaseEngine - Synchronized session for coordinating distributed work
//
// Reference: FDB Record Layer SynchronizedSession
// Provides coordination between multiple processes working on shared data.

import Foundation
import FoundationDB
import Synchronization

// MARK: - SessionConfiguration

/// Configuration for synchronized sessions
public struct SessionConfiguration: Sendable, Equatable {
    /// Unique identifier for this session
    public let sessionId: UUID

    /// Session name for debugging
    public let sessionName: String

    /// Maximum time to hold the lock (seconds)
    public let lockTimeoutSeconds: Double

    /// How often to renew the lock (seconds)
    public let renewalIntervalSeconds: Double

    /// Whether to allow stealing expired locks
    public let allowLockStealing: Bool

    /// Grace period before considering lock stale (seconds)
    public let staleThresholdSeconds: Double

    /// Default configuration
    public static func `default`(name: String) -> SessionConfiguration {
        SessionConfiguration(
            sessionId: UUID(),
            sessionName: name,
            lockTimeoutSeconds: 30.0,
            renewalIntervalSeconds: 10.0,
            allowLockStealing: true,
            staleThresholdSeconds: 60.0
        )
    }

    /// Long-running session configuration
    public static func longRunning(name: String) -> SessionConfiguration {
        SessionConfiguration(
            sessionId: UUID(),
            sessionName: name,
            lockTimeoutSeconds: 300.0,
            renewalIntervalSeconds: 60.0,
            allowLockStealing: true,
            staleThresholdSeconds: 600.0
        )
    }

    public init(
        sessionId: UUID = UUID(),
        sessionName: String,
        lockTimeoutSeconds: Double = 30.0,
        renewalIntervalSeconds: Double = 10.0,
        allowLockStealing: Bool = true,
        staleThresholdSeconds: Double = 60.0
    ) {
        precondition(lockTimeoutSeconds > 0, "lockTimeoutSeconds must be positive")
        precondition(renewalIntervalSeconds > 0, "renewalIntervalSeconds must be positive")
        precondition(renewalIntervalSeconds < lockTimeoutSeconds, "renewal must be less than timeout")

        self.sessionId = sessionId
        self.sessionName = sessionName
        self.lockTimeoutSeconds = lockTimeoutSeconds
        self.renewalIntervalSeconds = renewalIntervalSeconds
        self.allowLockStealing = allowLockStealing
        self.staleThresholdSeconds = staleThresholdSeconds
    }
}

// MARK: - SynchronizedSession

/// Synchronized session for coordinating distributed work
///
/// Provides mutual exclusion and coordination between multiple processes
/// that need to work on shared data. Uses FDB's transaction isolation
/// to implement a distributed lock.
///
/// **Use Cases**:
/// - Online index building (only one process at a time)
/// - Schema migrations
/// - Exclusive batch operations
/// - Leader election
///
/// **Reference**: FDB Record Layer SynchronizedSession.java
///
/// **Usage**:
/// ```swift
/// let session = SynchronizedSession(
///     container: container,
///     lockSubspace: metadataSubspace.subspace("locks"),
///     configuration: .default(name: "online-indexer")
/// )
///
/// // Acquire exclusive lock
/// guard try await session.acquire() else {
///     print("Another session has the lock")
///     return
/// }
///
/// defer { Task { try? await session.release() } }
///
/// // Do exclusive work
/// try await performExclusiveOperation()
/// ```
public final class SynchronizedSession: Sendable {
    // MARK: - Properties

    private let container: FDBContainer
    private let lockSubspace: Subspace
    public let configuration: SessionConfiguration

    // MARK: - State

    private struct State: Sendable {
        var isHeld: Bool = false
        var renewalTask: Task<Void, Error>?
        var acquiredAt: Date?
        var lastRenewal: Date?
    }

    private let state: Mutex<State>

    // MARK: - Lock Keys

    private var lockKey: FDB.Bytes {
        lockSubspace.pack(Tuple("lock", configuration.sessionName))
    }

    private var heartbeatKey: FDB.Bytes {
        lockSubspace.pack(Tuple("heartbeat", configuration.sessionName))
    }

    // MARK: - Initialization

    public init(
        container: FDBContainer,
        lockSubspace: Subspace,
        configuration: SessionConfiguration
    ) {
        self.container = container
        self.lockSubspace = lockSubspace
        self.configuration = configuration
        self.state = Mutex(State())
    }

    deinit {
        state.withLock { state in
            state.renewalTask?.cancel()
        }
    }

    // MARK: - Lock Acquisition

    /// Attempt to acquire the lock
    ///
    /// - Returns: True if lock was acquired, false if held by another session
    public func acquire() async throws -> Bool {
        let now = Date()

        let acquired = try await container.database.withTransaction(configuration: .interactive) { transaction in
            // Read current lock holder
            let currentHolder = try await self.readLockHolder(transaction: transaction)

            if let holder = currentHolder {
                // Check if it's us
                if holder.sessionId == self.configuration.sessionId {
                    // We already hold the lock, just update heartbeat
                    try self.writeLockHolder(LockHolder(
                        sessionId: self.configuration.sessionId,
                        sessionName: self.configuration.sessionName,
                        acquiredAt: holder.acquiredAt,
                        lastHeartbeat: now,
                        expiresAt: now.addingTimeInterval(self.configuration.lockTimeoutSeconds)
                    ), transaction: transaction)
                    return true
                }

                // Check if lock is stale and can be stolen
                if self.configuration.allowLockStealing && holder.isStale(threshold: self.configuration.staleThresholdSeconds) {
                    // Steal the lock
                    try self.writeLockHolder(LockHolder(
                        sessionId: self.configuration.sessionId,
                        sessionName: self.configuration.sessionName,
                        acquiredAt: now,
                        lastHeartbeat: now,
                        expiresAt: now.addingTimeInterval(self.configuration.lockTimeoutSeconds)
                    ), transaction: transaction)
                    return true
                }

                // Lock held by another active session
                return false
            }

            // No current holder, acquire lock
            try self.writeLockHolder(LockHolder(
                sessionId: self.configuration.sessionId,
                sessionName: self.configuration.sessionName,
                acquiredAt: now,
                lastHeartbeat: now,
                expiresAt: now.addingTimeInterval(self.configuration.lockTimeoutSeconds)
            ), transaction: transaction)

            return true
        }

        if acquired {
            state.withLock { state in
                state.isHeld = true
                state.acquiredAt = now
                state.lastRenewal = now
            }

            // Start renewal task
            startRenewalTask()
        }

        return acquired
    }

    /// Release the lock
    public func release() async throws {
        // Stop renewal
        state.withLock { state in
            state.renewalTask?.cancel()
            state.renewalTask = nil
        }

        try await container.database.withTransaction(configuration: .interactive) { transaction in
            // Verify we hold the lock
            let currentHolder = try await self.readLockHolder(transaction: transaction)

            if let holder = currentHolder, holder.sessionId == self.configuration.sessionId {
                // Clear the lock
                transaction.clear(key: self.lockKey)
                transaction.clear(key: self.heartbeatKey)
            }
        }

        state.withLock { state in
            state.isHeld = false
            state.acquiredAt = nil
            state.lastRenewal = nil
        }
    }

    /// Check if this session holds the lock
    public var isHeld: Bool {
        state.withLock { $0.isHeld }
    }

    /// Get lock status
    public func getLockStatus() async throws -> LockStatus {
        let holder = try await container.database.withTransaction(configuration: .interactive) { transaction in
            try await self.readLockHolder(transaction: transaction)
        }

        guard let holder = holder else {
            return LockStatus(isHeld: false, holder: nil, isOurs: false, isStale: false)
        }

        return LockStatus(
            isHeld: true,
            holder: holder,
            isOurs: holder.sessionId == configuration.sessionId,
            isStale: holder.isStale(threshold: configuration.staleThresholdSeconds)
        )
    }

    // MARK: - Work Execution

    /// Execute work within the synchronized session
    ///
    /// Automatically acquires, renews, and releases the lock.
    /// The lock is guaranteed to be released before this method returns.
    ///
    /// - Parameters:
    ///   - work: The work to execute
    /// - Returns: The result of the work
    /// - Throws: `SessionError.lockNotAcquired` if lock cannot be obtained
    public func execute<T: Sendable>(
        work: @Sendable () async throws -> T
    ) async throws -> T {
        guard try await acquire() else {
            throw SessionError.lockNotAcquired(sessionName: configuration.sessionName)
        }

        do {
            let result = try await work()
            try await release()
            return result
        } catch {
            // Ensure lock is released even on error
            try? await release()
            throw error
        }
    }

    // MARK: - Lock Renewal

    private func startRenewalTask() {
        // Capture configuration values to avoid capturing self in the sleep calculation
        let renewalInterval = UInt64(configuration.renewalIntervalSeconds * 1_000_000_000)

        let task = Task { [weak self] in
            while !Task.isCancelled {
                try await Task.sleep(nanoseconds: renewalInterval)

                if Task.isCancelled { break }

                // Check if self still exists
                guard let self = self else { break }

                do {
                    try await self.renewLock()
                } catch {
                    // Renewal failed, mark as not held
                    self.state.withLock { state in
                        state.isHeld = false
                    }
                    throw error
                }
            }
        }

        state.withLock { state in
            state.renewalTask = task
        }
    }

    private func renewLock() async throws {
        let now = Date()

        try await container.database.withTransaction(configuration: .interactive) { transaction in
            // Verify we still hold the lock
            let currentHolder = try await self.readLockHolder(transaction: transaction)

            guard let holder = currentHolder, holder.sessionId == self.configuration.sessionId else {
                throw SessionError.lockLost(sessionName: self.configuration.sessionName)
            }

            // Update heartbeat
            try self.writeLockHolder(LockHolder(
                sessionId: self.configuration.sessionId,
                sessionName: self.configuration.sessionName,
                acquiredAt: holder.acquiredAt,
                lastHeartbeat: now,
                expiresAt: now.addingTimeInterval(self.configuration.lockTimeoutSeconds)
            ), transaction: transaction)
        }

        state.withLock { state in
            state.lastRenewal = now
        }
    }

    // MARK: - Storage Helpers

    private func readLockHolder(transaction: any TransactionProtocol) async throws -> LockHolder? {
        guard let data = try await transaction.getValue(for: lockKey) else {
            return nil
        }
        return try LockHolder.decode(from: Data(data))
    }

    private func writeLockHolder(_ holder: LockHolder, transaction: any TransactionProtocol) throws {
        let data = try holder.encode()
        transaction.setValue(Array(data), for: lockKey)
    }
}

// MARK: - LockHolder

/// Information about the current lock holder
public struct LockHolder: Sendable, Codable {
    /// Session ID of the lock holder
    public let sessionId: UUID

    /// Session name
    public let sessionName: String

    /// When the lock was acquired
    public let acquiredAt: Date

    /// Last heartbeat time
    public let lastHeartbeat: Date

    /// When the lock expires
    public let expiresAt: Date

    /// Check if the lock is stale
    public func isStale(threshold: TimeInterval) -> Bool {
        Date().timeIntervalSince(lastHeartbeat) > threshold
    }

    /// Check if the lock has expired
    public var isExpired: Bool {
        Date() > expiresAt
    }

    /// Encode to data
    func encode() throws -> Data {
        try JSONEncoder().encode(self)
    }

    /// Decode from data
    static func decode(from data: Data) throws -> LockHolder {
        try JSONDecoder().decode(LockHolder.self, from: data)
    }
}

// MARK: - LockStatus

/// Status of a synchronized session lock
public struct LockStatus: Sendable {
    /// Whether the lock is held by anyone
    public let isHeld: Bool

    /// Information about the current holder
    public let holder: LockHolder?

    /// Whether this session holds the lock
    public let isOurs: Bool

    /// Whether the lock is stale (holder not renewing)
    public let isStale: Bool
}

// MARK: - SessionError

/// Errors from synchronized sessions
public enum SessionError: Error, CustomStringConvertible, Sendable {
    /// Lock could not be acquired
    case lockNotAcquired(sessionName: String)

    /// Lock was lost (not renewed)
    case lockLost(sessionName: String)

    /// Invalid lock data
    case invalidLockData(String)

    public var description: String {
        switch self {
        case .lockNotAcquired(let name):
            return "Could not acquire lock for session '\(name)'"
        case .lockLost(let name):
            return "Lock lost for session '\(name)' (renewal failed)"
        case .invalidLockData(let reason):
            return "Invalid lock data: \(reason)"
        }
    }
}

// MARK: - SessionLeaderElection

/// Leader election using synchronized sessions
///
/// Provides a simple leader election mechanism where one process
/// is elected as leader and others wait.
///
/// **Usage**:
/// ```swift
/// let election = SessionLeaderElection(
///     container: container,
///     lockSubspace: subspace.subspace("leader"),
///     electionName: "worker-leader"
/// )
///
/// // Try to become leader
/// if try await election.tryBecomeLeader() {
///     print("I am the leader!")
///     defer { Task { try? await election.resign() } }
///     try await performLeaderWork()
/// } else {
///     print("Following leader: \(try await election.currentLeader())")
/// }
/// ```
public final class SessionLeaderElection: Sendable {
    private let session: SynchronizedSession

    public init(
        container: FDBContainer,
        lockSubspace: Subspace,
        electionName: String,
        configuration: SessionConfiguration? = nil
    ) {
        let config = configuration ?? .default(name: "leader-\(electionName)")
        self.session = SynchronizedSession(
            container: container,
            lockSubspace: lockSubspace,
            configuration: config
        )
    }

    /// Try to become the leader
    ///
    /// - Returns: True if this process is now the leader
    public func tryBecomeLeader() async throws -> Bool {
        try await session.acquire()
    }

    /// Resign from leadership
    public func resign() async throws {
        try await session.release()
    }

    /// Check if this process is the leader
    public var isLeader: Bool {
        session.isHeld
    }

    /// Get information about the current leader
    public func currentLeader() async throws -> LockHolder? {
        let status = try await session.getLockStatus()
        return status.holder
    }

    /// Execute work as leader
    ///
    /// The leadership is guaranteed to be released before this method returns.
    ///
    /// - Parameter work: The leader work to execute
    /// - Returns: The result if leadership was obtained, nil otherwise
    public func executeAsLeader<T: Sendable>(
        work: @Sendable () async throws -> T
    ) async throws -> T? {
        guard try await tryBecomeLeader() else {
            return nil
        }

        do {
            let result = try await work()
            try await resign()
            return result
        } catch {
            // Ensure leadership is released even on error
            try? await resign()
            throw error
        }
    }
}
