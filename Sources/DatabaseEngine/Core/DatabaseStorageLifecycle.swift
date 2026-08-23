import StorageKit
import Synchronization

/// Owns one storage engine from configuration creation through the terminal
/// lifecycle of exactly one database container.
final class DatabaseStorageLifecycle: Sendable {
    private enum Phase: Sendable, Equatable {
        case available
        case opening
        case open
        case closing
        case stopping
        case closed
    }

    private struct State: Sendable {
        var phase = Phase.available
        var operationCount = 0
        var shutdownWaiters: [CheckedContinuation<Void, Never>] = []
    }

    private enum ShutdownAction {
        case none
        case perform
        case resume
    }

    private let storageEngine: any StorageEngine
    private let state = Mutex(State())

    init(storageEngine: any StorageEngine) {
        self.storageEngine = storageEngine
    }

    func claimStorageEngine() throws(DatabaseContainerLifecycleError) -> any StorageEngine {
        let result: Result<any StorageEngine, DatabaseContainerLifecycleError> =
            state.withLock { state in
                switch state.phase {
                case .available:
                    state.phase = .opening
                    return .success(ContainerStorageEngine(lifecycle: self))
                case .opening, .open:
                    return .failure(.configurationAlreadyUsed)
                case .closing, .stopping:
                    return .failure(.shuttingDown)
                case .closed:
                    return .failure(.shutdown)
                }
            }
        switch result {
        case .success(let storageEngine):
            return storageEngine
        case .failure(let error):
            throw error
        }
    }

    func finishOpening() throws(DatabaseContainerLifecycleError) {
        let result: Result<Void, DatabaseContainerLifecycleError> =
            state.withLock { state in
                switch state.phase {
                case .opening:
                    state.phase = .open
                    return .success(())
                case .closing, .stopping:
                    return .failure(.shuttingDown)
                case .closed:
                    return .failure(.shutdown)
                case .available, .open:
                    return .failure(.configurationAlreadyUsed)
                }
            }
        switch result {
        case .success:
            return
        case .failure(let error):
            throw error
        }
    }

    func beginOperation() throws(DatabaseContainerLifecycleError) -> DatabaseStorageOperationLease {
        let result: Result<DatabaseStorageOperationLease, DatabaseContainerLifecycleError> =
            state.withLock { state in
                switch state.phase {
                case .opening, .open:
                    let (operationCount, overflow) = state.operationCount
                        .addingReportingOverflow(1)
                    guard !overflow else {
                        return .failure(.operationLimitExceeded)
                    }
                    state.operationCount = operationCount
                    return .success(DatabaseStorageOperationLease(lifecycle: self))
                case .available:
                    return .failure(.configurationAlreadyUsed)
                case .closing, .stopping:
                    return .failure(.shuttingDown)
                case .closed:
                    return .failure(.shutdown)
                }
            }
        switch result {
        case .success(let lease):
            return lease
        case .failure(let error):
            throw error
        }
    }

    func finishOperation() {
        let shouldStop = state.withLock { state in
            precondition(
                state.operationCount > 0,
                "A database storage operation lease must finish exactly once"
            )
            state.operationCount -= 1
            guard state.operationCount == 0, state.phase == .closing else {
                return false
            }
            state.phase = .stopping
            return true
        }
        if shouldStop {
            startStorageEngineShutdown()
        }
    }

    func shutdown() async {
        await withCheckedContinuation { continuation in
            let action = state.withLock { state -> ShutdownAction in
                switch state.phase {
                case .available, .opening, .open:
                    state.shutdownWaiters.append(continuation)
                    if state.operationCount == 0 {
                        state.phase = .stopping
                        return .perform
                    }
                    state.phase = .closing
                    return .none
                case .closing, .stopping:
                    state.shutdownWaiters.append(continuation)
                    return .none
                case .closed:
                    return .resume
                }
            }
            switch action {
            case .none:
                break
            case .perform:
                startStorageEngineShutdown()
            case .resume:
                continuation.resume()
            }
        }
    }

    func shutdownIfUnclaimed() async {
        await withCheckedContinuation { continuation in
            let action = state.withLock { state -> ShutdownAction in
                guard state.phase == .available else {
                    return .resume
                }
                state.shutdownWaiters.append(continuation)
                state.phase = .stopping
                return .perform
            }
            switch action {
            case .none:
                break
            case .perform:
                startStorageEngineShutdown()
            case .resume:
                continuation.resume()
            }
        }
    }

    func requestShutdown() {
        let shouldStop = state.withLock { state in
            switch state.phase {
            case .available, .opening, .open:
                if state.operationCount == 0 {
                    state.phase = .stopping
                    return true
                }
                state.phase = .closing
                return false
            case .closing, .stopping, .closed:
                return false
            }
        }
        if shouldStop {
            startStorageEngineShutdown()
        }
    }

    var underlyingStorageEngine: any StorageEngine {
        storageEngine
    }

    private func startStorageEngineShutdown() {
        storageEngine.requestShutdown()
        Task { [self] in
            await storageEngine.waitUntilShutdown()
            finishStorageEngineShutdown()
        }
    }

    private func finishStorageEngineShutdown() {
        let waiters = state.withLock { state in
            precondition(
                state.phase == .stopping,
                "Storage shutdown must have one authoritative performer"
            )
            state.phase = .closed
            let waiters = state.shutdownWaiters
            state.shutdownWaiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in waiters {
            waiter.resume()
        }
    }

    deinit {
        storageEngine.requestShutdown()
    }
}
