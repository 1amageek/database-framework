import StorageKit
import DatabaseEngine

/// One explicit storage snapshot shared by every read in a graph operation.
///
/// The owner of the transaction controls retry, timeout, commit, and cancel.
/// Algorithms never create nested transactions and therefore cannot mix read
/// versions while traversing a graph.
package final class GraphReadSnapshot: Sendable {
    package let transaction: any TransactionAccess
    package let monotonicClock: any StorageMonotonicClock
    package let workBudget: GraphAlgorithmWorkBudget?
    package let identityPool: GraphIdentityPool
    let clock: MonotonicClock

    package init(
        transaction: any TransactionAccess,
        monotonicClock: any StorageMonotonicClock,
        workBudget: GraphAlgorithmWorkBudget? = nil
    ) {
        self.transaction = transaction
        self.monotonicClock = monotonicClock
        self.workBudget = workBudget
        self.identityPool = GraphIdentityPool()
        self.clock = MonotonicClock(source: monotonicClock)
    }
}

extension IndexQueryContext {
    var graphClock: MonotonicClock {
        MonotonicClock(source: context.container.monotonicClock)
    }
}
