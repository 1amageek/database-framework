import DatabaseServer
import DatabaseValue
import DatabaseWire
import GraphIndex
import StorageKit

actor MutableSnapshotSHACLDataSourceResolver: DatabaseSHACLDataSourceResolver {
    private let executor: SPARQLQueryExecutor
    private let graphScope: SHACLDataGraphScope
    private var snapshotFingerprint: DatabaseBytes

    init(
        executor: SPARQLQueryExecutor,
        graphScope: SHACLDataGraphScope,
        snapshotFingerprint: DatabaseBytes
    ) {
        self.executor = executor
        self.graphScope = graphScope
        self.snapshotFingerprint = snapshotFingerprint
    }

    func updateSnapshotFingerprint(_ value: DatabaseBytes) {
        snapshotFingerprint = value
    }

    func resolve(
        data: SHACLExecuteOperation.DataSource,
        focus: SHACLExecuteOperation.Focus,
        entailment: SHACLExecuteOperation.Entailment,
        workBudget: SHACLValidationWorkBudget,
        transaction: any TransactionAccess
    ) async throws -> DatabaseSHACLResolvedDataSource {
        _ = workBudget
        _ = transaction
        return DatabaseSHACLResolvedDataSource(
            data: data,
            focus: focus,
            entailment: entailment,
            executor: executor,
            graphScope: graphScope,
            selectedFocusNodes: selectedNodes(for: focus),
            snapshotFingerprint: snapshotFingerprint
        )
    }

    private func selectedNodes(
        for focus: SHACLExecuteOperation.Focus
    ) -> [DatabaseRDFTerm]? {
        switch focus {
        case .targets:
            return nil
        case .nodes(let nodes):
            return nodes
        case .entities:
            return []
        }
    }
}
