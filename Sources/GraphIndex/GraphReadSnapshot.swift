import StorageKit

/// One explicit storage snapshot shared by every read in a graph operation.
///
/// The owner of the transaction controls retry, timeout, commit, and cancel.
/// Algorithms never create nested transactions and therefore cannot mix read
/// versions while traversing a graph.
package struct GraphReadSnapshot: Sendable {
    package let transaction: any Transaction
    package let workBudget: GraphAlgorithmWorkBudget?
    package let identityPool: GraphIdentityPool

    package init(
        transaction: any Transaction,
        workBudget: GraphAlgorithmWorkBudget? = nil
    ) {
        self.transaction = transaction
        self.workBudget = workBudget
        self.identityPool = GraphIdentityPool()
    }
}
