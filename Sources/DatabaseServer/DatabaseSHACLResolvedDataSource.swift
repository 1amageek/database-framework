import DatabaseValue
import DatabaseWire
import GraphIndex
import OntologyIndex

public struct DatabaseSHACLResolvedDataSource: Sendable {
    public let data: SHACLExecuteOperation.DataSource
    public let focus: SHACLExecuteOperation.Focus
    public let entailment: SHACLExecuteOperation.Entailment
    public let executor: SPARQLQueryExecutor
    public let graphScope: SHACLDataGraphScope
    public let reasoner: OWLReasoner?
    public let selectedFocusNodes: [DatabaseRDFTerm]?
    public let snapshotFingerprint: DatabaseBytes

    public init(
        data: SHACLExecuteOperation.DataSource,
        focus: SHACLExecuteOperation.Focus,
        entailment: SHACLExecuteOperation.Entailment,
        executor: SPARQLQueryExecutor,
        graphScope: SHACLDataGraphScope,
        reasoner: OWLReasoner? = nil,
        selectedFocusNodes: [DatabaseRDFTerm]? = nil,
        snapshotFingerprint: DatabaseBytes
    ) {
        self.data = data
        self.focus = focus
        self.entailment = entailment
        self.executor = executor
        self.graphScope = graphScope
        self.reasoner = reasoner
        self.selectedFocusNodes = selectedFocusNodes
        self.snapshotFingerprint = snapshotFingerprint
    }
}
