import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_GRAPH_OPERATIONS_ENABLED
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire
import GraphIndex

public struct DatabaseSHACLResolvedDataSource: Sendable {
    public let data: SHACLExecuteOperation.DataSource
    public let focus: SHACLExecuteOperation.Focus
    public let entailment: SHACLExecuteOperation.Entailment
    public let executor: SPARQLQueryExecutor
    public let dataGraph: SHACLDataGraphTarget
    public let entailmentContext: (any SHACLEntailmentContext)?
    public let selectedFocusNodes: [RDFTerm]?
    public let snapshotFingerprint: ByteString

    public init(
        data: SHACLExecuteOperation.DataSource,
        focus: SHACLExecuteOperation.Focus,
        entailment: SHACLExecuteOperation.Entailment,
        executor: SPARQLQueryExecutor,
        dataGraph: SHACLDataGraphTarget,
        entailmentContext: (any SHACLEntailmentContext)? = nil,
        selectedFocusNodes: [RDFTerm]? = nil,
        snapshotFingerprint: ByteString
    ) {
        self.data = data
        self.focus = focus
        self.entailment = entailment
        self.executor = executor
        self.dataGraph = dataGraph
        self.entailmentContext = entailmentContext
        self.selectedFocusNodes = selectedFocusNodes
        self.snapshotFingerprint = snapshotFingerprint
    }
}
#endif
