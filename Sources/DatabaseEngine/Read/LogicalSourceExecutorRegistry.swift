#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseValue
import DatabaseWire
import QueryIR
import StorageKit

public protocol GraphTableSourceExecutor: Sendable {
    func execute(
        context: FDBContext,
        graphTableSource: GraphTableSource,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> [QueryRow]
}

public protocol SPARQLSourceExecutor: Sendable {
    func execute(
        context: FDBContext,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> QueryResponse

    func executeInTransaction(
        context: FDBContext,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField],
        transaction: any Transaction
    ) async throws -> QueryResponse

    func executeAskInTransaction(
        context: FDBContext,
        askQuery: AskQuery,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField],
        transaction: any Transaction
    ) async throws -> Bool

    func executeConstructInTransaction(
        context: FDBContext,
        constructQuery: ConstructQuery,
        resultScope: DatabaseGraphResultScope,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField],
        transaction: any Transaction
    ) async throws -> DatabaseRetainedRDFGraph

    func executeDescribeInTransaction(
        context: FDBContext,
        describeQuery: DescribeQuery,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField],
        transaction: any Transaction
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
