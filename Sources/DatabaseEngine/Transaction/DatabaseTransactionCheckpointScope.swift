import Synchronization

/// Revocable dynamic admission retained until a compound scope has drained.
package final class DatabaseTransactionCheckpointScope: Sendable {
    @TaskLocal package static var current: DatabaseTransactionCheckpointScope?

    package let owner: ObjectIdentifier
    package let identifier: UInt64
    private let gate: TransactionOperationGate
    private let active = Mutex(true)

    package init(owner: ObjectIdentifier, identifier: UInt64, gate: TransactionOperationGate) {
        self.owner = owner
        self.identifier = identifier
        self.gate = gate
    }

    package var isActive: Bool { active.withLock { $0 } }

    package func end() {
        let ends = active.withLock { active in
            guard active else { return false }
            active = false
            return true
        }
        if ends { gate.leave() }
    }

    deinit { end() }
}
