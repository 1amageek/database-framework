import DatabaseServer
import DatabaseTypes
import DatabaseWire
import GraphIndex
import OntologyIndex
import StorageKit

actor MutableSnapshotSHACLDataSourceResolver: DatabaseSHACLDataSourceResolver {
    private let executor: SPARQLQueryExecutor
    private let graphScope: SHACLDataGraphScope
    private let entailmentContext: (any SHACLEntailmentContext)?
    private var snapshotFingerprint: ByteString

    init(
        executor: SPARQLQueryExecutor,
        graphScope: SHACLDataGraphScope,
        entailmentContext: (any SHACLEntailmentContext)? = nil,
        snapshotFingerprint: ByteString
    ) {
        self.executor = executor
        self.graphScope = graphScope
        self.entailmentContext = entailmentContext
        self.snapshotFingerprint = snapshotFingerprint
    }

    func updateSnapshotFingerprint(_ value: ByteString) {
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
            entailmentContext: entailmentContext,
            selectedFocusNodes: selectedNodes(for: focus),
            snapshotFingerprint: snapshotFingerprint
        )
    }

    private func selectedNodes(
        for focus: SHACLExecuteOperation.Focus
    ) -> [RDFTerm]? {
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
