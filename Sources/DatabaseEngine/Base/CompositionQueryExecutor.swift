#if DATABASE_MULTI_BASE
import DatabaseKit

/// Executes a type-safe, read-only query through the canonical Composition
/// planner shared by local applications and remote adapters.
public struct CompositionQueryExecutor<Model: Persistable>: Sendable {
    private let source: CompositionDataSource
    private var query: Query<Model>

    package init(
        source: CompositionDataSource,
        query: Query<Model>
    ) {
        self.source = source
        self.query = query
    }

    public func `where`(
        _ predicate: Predicate<Model>
    ) -> CompositionQueryExecutor<Model> {
        var copy = self
        copy.query = query.where(predicate)
        return copy
    }

    public func orderBy<Value: Comparable & Sendable>(
        _ field: Field<Model, Value>
    ) -> CompositionQueryExecutor<Model> {
        var copy = self
        copy.query = query.orderBy(field)
        return copy
    }

    public func orderBy<Value: Comparable & Sendable>(
        _ field: Field<Model, Value>,
        _ order: SortOrder
    ) -> CompositionQueryExecutor<Model> {
        var copy = self
        copy.query = query.orderBy(field, order)
        return copy
    }

    public func limit(_ count: Int) -> CompositionQueryExecutor<Model> {
        var copy = self
        copy.query = query.limit(count)
        return copy
    }

    public func offset(_ count: Int) -> CompositionQueryExecutor<Model> {
        var copy = self
        copy.query = query.offset(count)
        return copy
    }

    public func partition<Value: Sendable & Equatable & FieldValueRepresentable>(
        _ field: Field<Model, Value>,
        equals value: Value
    ) -> CompositionQueryExecutor<Model> {
        var copy = self
        copy.query = query.partition(field, equals: value)
        return copy
    }

    /// Executes with a bounded request-wide budget. Local applications and
    /// remote hosts therefore use the same plan, merge, and failure semantics.
    public func execute(
        options: ReadExecutionOptions = .default
    ) async throws -> [CompositionResult<Model>] {
        let selectQuery = try query.toSelectQuery()
        return try await source.execute(
            selectQuery,
            options: options
        ).map { result in
            CompositionResult(
                composition: result.composition,
                origin: result.origin,
                value: try QueryRowCodec.decode(result.value, as: Model.self)
            )
        }
    }

    public func count(
        options: ReadExecutionOptions = .default
    ) async throws -> Int {
        let selected = try query.toSelectQuery()
        let aggregate = SelectQuery(
            projection: .items([
                ProjectionItem(
                    .aggregate(.count(nil, distinct: false)),
                    alias: "count"
                ),
            ]),
            source: selected.source,
            accessPath: selected.accessPath,
            filter: selected.filter,
            groupBy: nil,
            having: nil,
            orderBy: nil,
            limit: nil,
            offset: nil,
            distinct: false,
            subqueries: selected.subqueries,
            reduced: false,
            dataset: selected.dataset
        )
        let results = try await source.execute(
            aggregate,
            options: options
        )
        guard results.count == 1,
              case .int64(let value)? = results[0].value.fields["count"],
              value >= 0,
              let total = Int(exactly: value) else {
            throw CompositionQueryError.aggregateFailure(
                "COUNT result is missing or exceeds the current runtime range"
            )
        }
        return QueryResultWindow.resultCount(
            totalCount: total,
            limit: query.fetchLimit,
            offset: query.fetchOffset ?? 0
        )
    }

    public func first(
        options: ReadExecutionOptions = .default
    ) async throws -> CompositionResult<Model>? {
        try await limit(1).execute(options: options).first
    }

}

#endif
