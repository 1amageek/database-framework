import Synchronization
import StorageKit

package struct DatabaseReadScopeCleanupError: Error, Sendable {
    package let operationError: (any Error)?
    package let cursorCleanupErrors: [any Error]

    package init(cursorCleanupErrors: [any Error]) {
        self.operationError = nil
        self.cursorCleanupErrors = cursorCleanupErrors
    }

    private init(
        operationError: any Error,
        cursorCleanupErrors: [any Error]
    ) {
        self.operationError = operationError
        self.cursorCleanupErrors = cursorCleanupErrors
    }

    package func preserving(
        operationError: any Error
    ) -> DatabaseReadScopeCleanupError {
        DatabaseReadScopeCleanupError(
            operationError: operationError,
            cursorCleanupErrors: cursorCleanupErrors
        )
    }
}

/// Revokes a request-scoped read capability and drains every operation that
/// was admitted before the owning transaction reaches its terminal boundary.
package final class DatabaseReadScopeGate: Sendable {
    private struct State: Sendable {
        var acceptsReads = true
        var nextReadIdentifier: UInt64 = 0
        var activeReadIdentifiers: Set<UInt64> = []
        var cursors: [UInt64: DatabaseReadScopeCursorRegistration] = [:]
        var closeStarted = false
        var closeResult: Result<Void, DatabaseReadScopeCleanupError>?
        var readWaiters: [CheckedContinuation<Void, Never>] = []
        var closeWaiters: [
            CheckedContinuation<
                Result<Void, DatabaseReadScopeCleanupError>,
                Never
            >
        ] = []
    }

    private let state = Mutex(State())

    package init() {}

    package func beginRead() throws -> DatabaseReadScopeLease {
        let identifier = try state.withLock { state in
            guard state.acceptsReads else {
                throw DatabaseReadTransactionError.snapshotClosed
            }
            let next = state.nextReadIdentifier.addingReportingOverflow(1)
            guard !next.overflow else {
                throw DatabaseReadTransactionError.readOperationCountOverflow
            }
            state.nextReadIdentifier = next.partialValue
            state.activeReadIdentifiers.insert(next.partialValue)
            return next.partialValue
        }
        return DatabaseReadScopeLease(gate: self, identifier: identifier)
    }

    package func validateOpen() throws {
        let lease = try beginRead()
        lease.finish()
    }

    package func scope(
        _ cursor: consuming KeyValueCursor,
        admittedBy lease: DatabaseReadScopeLease
    ) throws -> KeyValueCursor {
        guard lease.belongs(to: self) else {
            throw DatabaseReadTransactionError.snapshotClosed
        }
        let ownedCursor = cursor
        let registration = try state.withLock { state in
            guard state.activeReadIdentifiers.contains(lease.identifier) else {
                throw DatabaseReadTransactionError.snapshotClosed
            }
            precondition(
                state.cursors[lease.identifier] == nil,
                "A database read admission can register only one cursor"
            )
            let registration = DatabaseReadScopeCursorRegistration(
                identifier: lease.identifier,
                cursor: ownedCursor
            )
            state.cursors[lease.identifier] = registration
            return registration
        }
        return KeyValueCursor(
            consuming: DatabaseReadScopeRangeResult(
                registration: registration,
                readScope: self
            )
        )
    }

    package func closeAndWait() async throws {
        let isLeader = state.withLock { state in
            state.acceptsReads = false
            guard !state.closeStarted else { return false }
            state.closeStarted = true
            return true
        }
        guard isLeader else {
            return try await waitForCloseResult()
        }

        // A read admitted before close may still be constructing and
        // registering its cursor. Wait for that admission to finish before
        // freezing the cleanup set.
        await waitForActiveReads()
        let registrations = state.withLock { state in
            Array(state.cursors.values)
        }
        var cleanupErrors: [any Error] = []
        cleanupErrors.reserveCapacity(registrations.count)
        for registration in registrations {
            do {
                try await registration.finish()
            } catch {
                cleanupErrors.append(error)
            }
            unregisterCursor(registration.identifier)
        }

        let result: Result<Void, DatabaseReadScopeCleanupError> =
            cleanupErrors.isEmpty
            ? .success(())
            : .failure(
                DatabaseReadScopeCleanupError(
                    cursorCleanupErrors: cleanupErrors
                )
            )
        let waiters = state.withLock { state in
            state.closeResult = result
            let waiters = state.closeWaiters
            state.closeWaiters.removeAll(keepingCapacity: false)
            state.cursors.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in waiters { waiter.resume(returning: result) }
        try result.get()
    }

    fileprivate func unregisterCursor(_ identifier: UInt64) {
        _ = state.withLock { state in
            state.cursors.removeValue(forKey: identifier)
        }
    }

    private func waitForActiveReads() async {
        await withCheckedContinuation { continuation in
            let resumesImmediately = state.withLock { state in
                guard !state.activeReadIdentifiers.isEmpty else { return true }
                state.readWaiters.append(continuation)
                return false
            }
            if resumesImmediately { continuation.resume() }
        }
    }

    private func waitForCloseResult() async throws {
        let result = await withCheckedContinuation { continuation in
            let completed: Result<
                Void,
                DatabaseReadScopeCleanupError
            >? = state.withLock { state in
                if let result = state.closeResult { return result }
                state.closeWaiters.append(continuation)
                return nil
            }
            if let completed { continuation.resume(returning: completed) }
        }
        try result.get()
    }

    fileprivate func finishRead(identifier: UInt64) {
        let waiters = state.withLock { state in
            precondition(
                state.activeReadIdentifiers.remove(identifier) != nil,
                "A database read admission must finish exactly once"
            )
            guard !state.acceptsReads,
                  state.activeReadIdentifiers.isEmpty else {
                return [CheckedContinuation<Void, Never>]()
            }
            let waiters = state.readWaiters
            state.readWaiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in waiters { waiter.resume() }
    }
}

private final class DatabaseReadScopeCursorRegistration: Sendable {
    let identifier: UInt64
    private let cursor: KeyValueCursor

    init(identifier: UInt64, cursor: consuming KeyValueCursor) {
        self.identifier = identifier
        self.cursor = cursor
    }

    func next() async throws -> KeyValueCursor.Element? {
        var cursor = cursor
        return try await cursor.next()
    }

    func finish() async throws {
        var cursor = cursor
        try await cursor.finish()
    }
}

private struct DatabaseReadScopeRangeResult: TransactionRangeResult {
    let registration: DatabaseReadScopeCursorRegistration
    let readScope: DatabaseReadScopeGate

    func makeCursor() -> DatabaseReadScopeRangeCursor {
        DatabaseReadScopeRangeCursor(
            registration: registration,
            readScope: readScope
        )
    }
}

private struct DatabaseReadScopeRangeCursor: TransactionRangeCursor {
    private var registration: DatabaseReadScopeCursorRegistration?
    private let readScope: DatabaseReadScopeGate

    init(
        registration: DatabaseReadScopeCursorRegistration,
        readScope: DatabaseReadScopeGate
    ) {
        self.registration = registration
        self.readScope = readScope
    }

    mutating func next() async throws -> KeyValueCursor.Element? {
        let scopeLease = try readScope.beginRead()
        defer { scopeLease.finish() }
        guard let registration else { return nil }
        do {
            let element = try await registration.next()
            if element == nil { finishRegistration(registration) }
            return element
        } catch let cleanupError as StorageRangeCleanupError {
            finishRegistration(registration)
            throw cleanupError
        } catch {
            let iterationError = error
            do {
                try await registration.finish()
            } catch {
                finishRegistration(registration)
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            finishRegistration(registration)
            throw iterationError
        }
    }

    mutating func finish(
        isolation actor: isolated (any Actor)?
    ) async throws {
        guard let registration else { return }
        self.registration = nil
        defer { readScope.unregisterCursor(registration.identifier) }
        try await registration.finish()
    }

    private mutating func finishRegistration(
        _ registration: DatabaseReadScopeCursorRegistration
    ) {
        self.registration = nil
        readScope.unregisterCursor(registration.identifier)
    }
}

package final class DatabaseReadScopeLease: Sendable {
    private let gate: DatabaseReadScopeGate
    fileprivate let identifier: UInt64
    private let didFinish = Mutex(false)

    fileprivate init(gate: DatabaseReadScopeGate, identifier: UInt64) {
        self.gate = gate
        self.identifier = identifier
    }

    fileprivate func belongs(to gate: DatabaseReadScopeGate) -> Bool {
        self.gate === gate
    }

    package func finish() {
        let shouldFinish = didFinish.withLock { didFinish in
            guard !didFinish else { return false }
            didFinish = true
            return true
        }
        if shouldFinish { gate.finishRead(identifier: identifier) }
    }

    deinit { finish() }
}
