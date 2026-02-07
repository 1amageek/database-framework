import Foundation
import Core
import DatabaseEngine
import DatabaseClientProtocol

/// Type-erased handler for entity operations
///
/// Built at initialization time with concrete type knowledge, then stored
/// in a dictionary keyed by entity name for dynamic dispatch.
struct EntityHandler: Sendable {

    /// Fetch records matching a request
    let fetch: @Sendable (FDBContext, FetchRequest) async throws -> FetchResponse

    /// Get a single record by ID
    let get: @Sendable (FDBContext, GetRequest) async throws -> GetResponse

    /// Apply a batch of changes (insert/update/delete)
    let applyChanges: @Sendable (FDBContext, [ChangeSet.Change]) async throws -> Void

    /// Count records matching a request
    let count: @Sendable (FDBContext, CountRequest) async throws -> CountResponse

    // MARK: - Builder

    /// Build a handler for a concrete Persistable type
    ///
    /// Uses implicit existential opening: called with `any Persistable.Type`,
    /// Swift opens the existential and binds T to the concrete type.
    static func build<T: Persistable>(for type: T.Type) -> EntityHandler {
        let encoder = JSONEncoder()
        let decoder = JSONDecoder()

        return EntityHandler(
            fetch: { context, request in
                // Fetch all items of this type
                var executor = context.fetch(T.self)

                // Apply partition if provided
                if let partitionValues = request.partitionValues, !partitionValues.isEmpty {
                    for (fieldName, value) in partitionValues {
                        executor = applyPartition(executor, fieldName: fieldName, value: value)
                    }
                }

                // Apply limit (oversize to allow post-filtering)
                // For now fetch all and filter in memory
                let items = try await executor.execute()

                // Convert to [String: FieldValue] records
                var records: [[String: FieldValue]] = try items.map { item in
                    let data = try encoder.encode(item)
                    return try decoder.decode([String: FieldValue].self, from: data)
                }

                // Apply predicate filter
                if let predicate = request.predicate {
                    records = records.filter { PredicateEvaluator.evaluate(predicate, on: $0) }
                }

                // Apply sort
                records = PredicateEvaluator.sort(records, by: request.sortDescriptors)

                // Apply limit
                if let limit = request.limit, limit < records.count {
                    records = Array(records.prefix(limit))
                }

                return FetchResponse(records: records, continuation: nil)
            },

            get: { context, request in
                let item: T? = try await context.model(for: request.id, as: T.self)
                guard let item else {
                    return GetResponse(record: nil)
                }
                let data = try encoder.encode(item)
                let dict = try decoder.decode([String: FieldValue].self, from: data)
                return GetResponse(record: dict)
            },

            applyChanges: { context, changes in
                for change in changes {
                    switch change.operation {
                    case .insert:
                        guard let fields = change.fields else { continue }
                        let data = try encoder.encode(fields)
                        let item = try decoder.decode(T.self, from: data)
                        context.insert(item)

                    case .update:
                        guard let fields = change.fields else { continue }
                        let data = try encoder.encode(fields)
                        let item = try decoder.decode(T.self, from: data)
                        context.insert(item)

                    case .delete:
                        // Reconstruct a minimal item with just the ID for deletion
                        guard let fields = change.fields else {
                            // Create from ID only - encode {"id": "..."} as JSON
                            let idDict: [String: FieldValue] = ["id": .string(change.id)]
                            let data = try encoder.encode(idDict)
                            let item = try decoder.decode(T.self, from: data)
                            context.delete(item)
                            continue
                        }
                        let data = try encoder.encode(fields)
                        let item = try decoder.decode(T.self, from: data)
                        context.delete(item)
                    }
                }
            },

            count: { context, request in
                var executor = context.fetch(T.self)

                if let partitionValues = request.partitionValues, !partitionValues.isEmpty {
                    for (fieldName, value) in partitionValues {
                        executor = applyPartition(executor, fieldName: fieldName, value: value)
                    }
                }

                let items = try await executor.execute()

                var records: [[String: FieldValue]]
                if let predicate = request.predicate {
                    let encoder = JSONEncoder()
                    let decoder = JSONDecoder()
                    records = try items.map { item in
                        let data = try encoder.encode(item)
                        return try decoder.decode([String: FieldValue].self, from: data)
                    }
                    records = records.filter { PredicateEvaluator.evaluate(predicate, on: $0) }
                    return CountResponse(count: records.count)
                }

                return CountResponse(count: items.count)
            }
        )
    }

    /// Apply partition binding to a QueryExecutor using field name string
    ///
    /// Note: This is a simplified version that works with string field names.
    /// Full KeyPath-based partition requires the concrete type at compile time.
    private static func applyPartition<T: Persistable>(
        _ executor: QueryExecutor<T>,
        fieldName: String,
        value: String
    ) -> QueryExecutor<T> {
        // For now, partition is handled at the directory level
        // The QueryExecutor.partition() requires a KeyPath which we don't have
        // This will be enhanced later with dynamic partition resolution
        return executor
    }
}
