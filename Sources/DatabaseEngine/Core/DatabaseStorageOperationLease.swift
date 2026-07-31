import Synchronization

/// Keeps the container's storage engine alive for one admitted operation.
final class DatabaseStorageOperationLease: Sendable {
    private let lifecycle: DatabaseStorageLifecycle
    private let didFinish = Mutex(false)

    init(lifecycle: DatabaseStorageLifecycle) {
        self.lifecycle = lifecycle
    }

    func finish() {
        let shouldFinish = didFinish.withLock { didFinish in
            guard !didFinish else { return false }
            didFinish = true
            return true
        }
        if shouldFinish {
            lifecycle.finishOperation()
        }
    }

    deinit {
        finish()
    }
}
