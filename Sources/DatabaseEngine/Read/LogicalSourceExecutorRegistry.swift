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
        context: DatabaseContext,
        graphTableSource: GraphTableSource,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> [QueryRow]
}

public protocol SPARQLSourceExecutor: Sendable {
    func execute(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> QueryResponse

    func executeInTransaction(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField],
        transaction: any TransactionAccess
    ) async throws -> QueryResponse

    func executeAskInTransaction(
        context: DatabaseContext,
        askQuery: AskQuery,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField],
        transaction: any TransactionAccess
    ) async throws -> Bool

    func executeConstructInTransaction(
        context: DatabaseContext,
        constructQuery: ConstructQuery,
        resultScope: DatabaseGraphResultScope,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField],
        transaction: any TransactionAccess
    ) async throws -> DatabaseRetainedRDFGraph

    func executeDescribeInTransaction(
        context: DatabaseContext,
        describeQuery: DescribeQuery,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField],
        transaction: any TransactionAccess
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
