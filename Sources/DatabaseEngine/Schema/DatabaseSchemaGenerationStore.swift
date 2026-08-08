import Synchronization

/// Atomically publishes immutable schema generations without invalidating old
/// request leases.
final class DatabaseSchemaGenerationStore: Sendable {
    private let currentLease: Mutex<DatabaseSchemaLease>

    init(initial: DatabaseSchemaGeneration) {
        self.currentLease = Mutex(DatabaseSchemaLease(initial))
    }

    func acquire() -> DatabaseSchemaLease {
        currentLease.withLock { $0 }
    }

    func publish(_ generation: DatabaseSchemaGeneration) {
        currentLease.withLock { current in
            current = DatabaseSchemaLease(generation)
        }
    }
}
