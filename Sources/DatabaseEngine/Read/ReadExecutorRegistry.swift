import DatabaseKit
import DatabaseTypes

public protocol IndexReadExecutor: Sendable {
    var indexType: IndexType { get }

    /// Produce an index-native row set.
    ///
    /// Executors must not apply SQL `WHERE` / `ORDER BY` / projection /
    /// `DISTINCT` / `LIMIT` / `OFFSET` — the dispatcher does. Executors are
    /// responsible only for producing the candidate rows ordered in index-native
    /// form (e.g. distance ascending, rank descending) together with any
    /// per-row annotations (`distance`, `score`, `rank`, …).
    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        index: IndexDescriptor,
        indexScan: IndexScanSource,
        entity: Schema.Entity,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult
}

public protocol PolymorphicIndexReadExecutor: Sendable {
    var indexType: IndexType { get }

    /// Produce an index-native row set for a polymorphic group. Same contract
    /// as `IndexReadExecutor.executeRows` — no SQL post-processing in executors.
    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        index: IndexDeclaration<String>,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult
}

public struct ReadExecutorRegistry: Sendable {
    private let polymorphicIndexExecutors: [IndexType: any PolymorphicIndexReadExecutor]

    public init(
        polymorphicIndexExecutors: [any PolymorphicIndexReadExecutor] = []
    ) throws(DatabaseRuntimeConfigurationError) {
        self.polymorphicIndexExecutors = try Self.polymorphicExecutorsByIdentifier(
            polymorphicIndexExecutors
        )
    }

    public func polymorphicIndexExecutor(for indexType: IndexType) -> (any PolymorphicIndexReadExecutor)? {
        polymorphicIndexExecutors[indexType]
    }

    private static func polymorphicExecutorsByIdentifier(
        _ executors: [any PolymorphicIndexReadExecutor]
    ) throws(DatabaseRuntimeConfigurationError) -> [IndexType: any PolymorphicIndexReadExecutor] {
        var result: [IndexType: any PolymorphicIndexReadExecutor] = [:]
        for executor in executors {
            guard result[executor.indexType] == nil else {
                throw .duplicatePolymorphicIndexReadExecutor(executor.indexType)
            }
            result[executor.indexType] = executor
        }
        return result
    }

}
