import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_MUTATION_OPERATIONS_GRAPH_INDEXES
import DatabaseKit

package enum PreparedSPARQLUpdateOperation: Sendable {
    case insertData(InsertDataQuery)
    case deleteData(DeleteDataQuery)
    case modify(SPARQLModifyOperation)
    case deleteWhere(DeleteWhereQuery)
    case load(PreparedSPARQLLoad)
    case silentLoadNoOp
    case clear(ClearQuery)
    case createGraph(CreateSPARQLGraphQuery)
    case drop(DropQuery)
    case graphTransfer(GraphTransferQuery)
}

#endif
