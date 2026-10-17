import Synchronization
@testable import DatabaseEngine

/// Keeps retry evidence under one retained synchronization owner.
final class DatabaseTransactionAttemptCapture: Sendable {
    private let storage = Mutex<[(DatabaseTransaction, DatabaseTransactionCheckpoint)]>([])
    private let returned = Mutex(false)
    private let unknownAttempts = Mutex(0)

    func append(_ transaction: DatabaseTransaction, checkpoint: DatabaseTransactionCheckpoint) {
        storage.withLock { $0.append((transaction, checkpoint)) }
    }

    var attempts: [(DatabaseTransaction, DatabaseTransactionCheckpoint)] {
        storage.withLock { $0 }
    }

    func markReturned() { returned.withLock { $0 = true } }
    var didReturn: Bool { returned.withLock { $0 } }

    func recordUnknownAttempt() { unknownAttempts.withLock { $0 += 1 } }
    var unknownAttemptCount: Int { unknownAttempts.withLock { $0 } }
}
