import DatabaseTypes
import DatabaseKit
import StorageKit

public protocol GraphTableSourceExecutor: Sendable {
    @_spi(DatabaseExecution)
    func execute(
        context: DatabaseContext,
        graphTableSource: GraphTableSource,
        authorization: IndexReadAuthorization,
        options: ReadExecutionContext,
        partitions: FieldObject,
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseRetainedQueryRows
}

public protocol SPARQLSourceExecutor: Sendable {
    @_spi(DatabaseExecution)
    func executeInTransaction(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitions: FieldObject,
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseRetainedQueryResponse

    func executeAskInTransaction(
        context: DatabaseContext,
        askQuery: AskQuery,
        options: ReadExecutionContext,
        partitions: FieldObject,
        transaction: any TransactionReadAccess
    ) async throws -> Bool

    func executeConstructInTransaction(
        context: DatabaseContext,
        constructQuery: ConstructQuery,
        nodeNamespace: GraphResultNodeNamespace,
        options: ReadExecutionContext,
        partitions: FieldObject,
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseRetainedRDFGraph

    func executeDescribeInTransaction(
        context: DatabaseContext,
        describeQuery: DescribeQuery,
        options: ReadExecutionContext,
        partitions: FieldObject,
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseRetainedRDFGraph

}

public struct LogicalSourceExecutorRegistry: Sendable {
    public let graphTableExecutor: (any GraphTableSourceExecutor)?
    public let sparqlExecutor: (any SPARQLSourceExecutor)?

    public init(
        graphTableExecutor: (any GraphTableSourceExecutor)? = nil,
        sparqlExecutor: (any SPARQLSourceExecutor)? = nil
    ) {
        self.graphTableExecutor = graphTableExecutor
        self.sparqlExecutor = sparqlExecutor
    }
}
