import DatabaseTypes
import DatabaseKit
import StorageKit

public protocol GraphTableSourceExecutor: Sendable {
    func executeInTransaction(
        session: DatabaseReadSession,
        graphTableSource: GraphTableSource,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedQueryRows
}

public protocol SPARQLSourceExecutor: Sendable {
    func executeInTransaction(
        session: DatabaseReadSession,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedQueryRows

    func executeAskInTransaction(
        session: DatabaseReadSession,
        askQuery: AskQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> Bool

    func executeConstructInTransaction(
        session: DatabaseReadSession,
        constructQuery: ConstructQuery,
        nodeNamespace: GraphResultNodeNamespace,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedRDFGraph

    func executeDescribeInTransaction(
        session: DatabaseReadSession,
        describeQuery: DescribeQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
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
