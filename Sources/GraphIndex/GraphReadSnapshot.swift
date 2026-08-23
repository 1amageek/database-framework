import StorageKit
import DatabaseEngine

/// One explicit storage snapshot shared by every read in a graph operation.
///
/// The owner of the transaction controls retry, timeout, commit, and cancel.
/// Algorithms never create nested transactions and therefore cannot mix read
/// versions while traversing a graph.
@_spi(DatabaseExecution)
public final class GraphReadSnapshot: Sendable {
    package let transaction: any TransactionReadAccess
    public let monotonicClock: any StorageMonotonicClock
    public let workBudget: GraphAlgorithmWorkBudget?
    package let identityPool: GraphIdentityPool
    let clock: MonotonicClock

    public init(
        transaction: any IndexReadAccess,
        monotonicClock: any StorageMonotonicClock,
        workBudget: GraphAlgorithmWorkBudget? = nil
    ) {
        self.transaction = transaction
        self.monotonicClock = monotonicClock
        self.workBudget = workBudget
        self.identityPool = GraphIdentityPool()
        self.clock = MonotonicClock(source: monotonicClock)
    }

    package init(
        transaction: any TransactionReadAccess,
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
