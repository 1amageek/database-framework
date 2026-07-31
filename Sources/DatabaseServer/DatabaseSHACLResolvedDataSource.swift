#if DATABASE_SERVER_GRAPH_INDEXES
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import GraphIndex

public struct DatabaseSHACLResolvedDataSource: Sendable {
    public let data: SHACLExecuteOperation.DataSource
    public let focus: SHACLExecuteOperation.Focus
    public let entailment: SHACLExecuteOperation.Entailment
    public let executor: SPARQLQueryExecutor
    public let graphScope: SHACLDataGraphScope
    public let entailmentContext: (any SHACLEntailmentContext)?
    public let selectedFocusNodes: [RDFTerm]?
    public let snapshotFingerprint: ByteString

    public init(
        data: SHACLExecuteOperation.DataSource,
        focus: SHACLExecuteOperation.Focus,
        entailment: SHACLExecuteOperation.Entailment,
        executor: SPARQLQueryExecutor,
        graphScope: SHACLDataGraphScope,
        entailmentContext: (any SHACLEntailmentContext)? = nil,
        selectedFocusNodes: [RDFTerm]? = nil,
        snapshotFingerprint: ByteString
    ) {
        self.data = data
        self.focus = focus
        self.entailment = entailment
        self.executor = executor
        self.graphScope = graphScope
        self.entailmentContext = entailmentContext
        self.selectedFocusNodes = selectedFocusNodes
        self.snapshotFingerprint = snapshotFingerprint
    }
}
#endif
