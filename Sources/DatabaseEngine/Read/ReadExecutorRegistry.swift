import DatabaseKit
import DatabaseTypes

public protocol IndexReadExecutor: Sendable {
    var kindIdentifier: String { get }

    /// Produce an index-native row set.
    ///
    /// Executors must not apply SQL `WHERE` / `ORDER BY` / projection /
    /// `DISTINCT` / `LIMIT` / `OFFSET` — the dispatcher does. Executors are
    /// responsible only for producing the candidate rows ordered in index-native
    /// form (e.g. distance ascending, rank descending) together with any
    /// per-row annotations (`distance`, `score`, `rank`, …).
    func executeRows<T: Persistable>(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult
}

public protocol PolymorphicIndexReadExecutor: Sendable {
    var kindIdentifier: String { get }

    /// Produce an index-native row set for a polymorphic group. Same contract
    /// as `IndexReadExecutor.executeRows` — no SQL post-processing in executors.
    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult
}

public struct ReadExecutorRegistry: Sendable {
    private let polymorphicIndexExecutors: [String: any PolymorphicIndexReadExecutor]

    public init(
        polymorphicIndexExecutors: [any PolymorphicIndexReadExecutor] = []
    ) throws(DatabaseRuntimeConfigurationError) {
        self.polymorphicIndexExecutors = try Self.polymorphicExecutorsByIdentifier(
            polymorphicIndexExecutors
        )
    }

    public func polymorphicIndexExecutor(for kindIdentifier: String) -> (any PolymorphicIndexReadExecutor)? {
        polymorphicIndexExecutors[kindIdentifier]
    }

    private static func polymorphicExecutorsByIdentifier(
        _ executors: [any PolymorphicIndexReadExecutor]
    ) throws(DatabaseRuntimeConfigurationError) -> [String: any PolymorphicIndexReadExecutor] {
        var result: [String: any PolymorphicIndexReadExecutor] = [:]
        for executor in executors {
            guard result[executor.kindIdentifier] == nil else {
                throw .duplicatePolymorphicIndexReadExecutor(executor.kindIdentifier)
            }
            result[executor.kindIdentifier] = executor
        }
        return result
    }

}
