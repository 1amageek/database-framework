import Foundation
import FoundationDB

/// Handler for aggregation indexes (COUNT, SUM, AVG, MIN, MAX)
///
/// Storage layout:
/// - values/<groupKey> = aggregated value
/// - metadata/<groupKey> = count (for AVG computation)
public struct AggregationIndexHandler: IndexHandler, Sendable {
    public let indexDefinition: IndexDefinition
    public let schemaName: String

    public init(indexDefinition: IndexDefinition, schemaName: String) {
        self.indexDefinition = indexDefinition
        self.schemaName = schemaName
    }

    public func updateIndex(
        oldItem: [String: Any]?,
        newItem: [String: Any]?,
        id: String,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws {
        guard let config = indexDefinition.config,
              case .aggregation(let aggConfig) = config else {
            return
        }

        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .aggregation,
            indexName: indexDefinition.name
        )

        // Extract group keys and values
        let oldGroupKey = extractGroupKey(from: oldItem, config: aggConfig)
        let newGroupKey = extractGroupKey(from: newItem, config: aggConfig)
        let oldValue = extractValue(from: oldItem, config: aggConfig)
        let newValue = extractValue(from: newItem, config: aggConfig)

        // Update aggregation for old group (subtract)
        if let groupKey = oldGroupKey {
            try await updateAggregation(
                subspace: indexSubspace,
                groupKey: groupKey,
                value: oldValue,
                isAdd: false,
                config: aggConfig,
                transaction: transaction
            )
        }

        // Update aggregation for new group (add)
        if let groupKey = newGroupKey {
            try await updateAggregation(
                subspace: indexSubspace,
                groupKey: groupKey,
                value: newValue,
                isAdd: true,
                config: aggConfig,
                transaction: transaction
            )
        }
    }

    public func scan(
        query: Any,
        limit: Int,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws -> [String] {
        guard let config = indexDefinition.config,
              case .aggregation(let aggConfig) = config else {
            return []
        }

        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .aggregation,
            indexName: indexDefinition.name
        )

        guard let aggQuery = query as? AggregationQuery else {
            return []
        }

        switch aggQuery {
        case .get(let groupKey):
            // Return the aggregated value for a specific group
            var tupleElements: [any TupleElement] = ["values"]
            tupleElements.append(contentsOf: groupKey.map { "\($0)" })
            let key = indexSubspace.pack(Tuple(tupleElements))
            if let bytes = try await transaction.getValue(for: key, snapshot: true) {
                let value = unpackDouble(bytes)
                return [formatValue(value, config: aggConfig)]
            }
            return ["0"]

        case .all:
            // Return all group aggregations
            let valuesSubspace = indexSubspace.subspace(Tuple(["values"]))
            let (begin, end) = valuesSubspace.range()

            var results: [String] = []
            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

            for try await (key, value) in sequence {
                guard results.count < limit else { break }

                if let tuple = try? valuesSubspace.unpack(key) {
                    // Convert tuple elements to strings
                    var groupKeyParts: [String] = []
                    for i in 0..<tuple.count {
                        if let element = tuple[i] {
                            groupKeyParts.append("\(element)")
                        }
                    }
                    let groupKey = groupKeyParts.joined(separator: ",")
                    let aggValue = unpackDouble(value)
                    results.append("\(groupKey): \(formatValue(aggValue, config: aggConfig))")
                }
            }

            return results
        }
    }

    // MARK: - Aggregation Logic

    private func updateAggregation(
        subspace: Subspace,
        groupKey: [String],
        value: Double?,
        isAdd: Bool,
        config: AggregationIndexConfig,
        transaction: any TransactionProtocol
    ) async throws {
        let valueKey = subspace.pack(Tuple(["values"] + groupKey))
        let countKey = subspace.pack(Tuple(["count"] + groupKey))

        switch config.aggregationType {
        case .count:
            let delta: Int64 = isAdd ? 1 : -1
            // Read-modify-write for count since atomicAdd may not be available
            let currentBytes = try await transaction.getValue(for: valueKey, snapshot: false)
            let currentCount = currentBytes.map(unpackInt64) ?? 0
            transaction.setValue(packInt64(currentCount + delta), for: valueKey)

        case .sum:
            if let v = value {
                let delta = isAdd ? v : -v
                // For sum, we need to read-modify-write since atomic add only works for integers
                let current = try await transaction.getValue(for: valueKey, snapshot: false)
                let currentValue = current.map(unpackDouble) ?? 0.0
                transaction.setValue(packDouble(currentValue + delta), for: valueKey)
            }

        case .avg:
            if let v = value {
                // Update sum
                let current = try await transaction.getValue(for: valueKey, snapshot: false)
                let currentSum = current.map(unpackDouble) ?? 0.0
                let delta = isAdd ? v : -v
                transaction.setValue(packDouble(currentSum + delta), for: valueKey)

                // Update count using read-modify-write
                let countDelta: Int64 = isAdd ? 1 : -1
                let currentCountBytes = try await transaction.getValue(for: countKey, snapshot: false)
                let currentCount = currentCountBytes.map(unpackInt64) ?? 0
                transaction.setValue(packInt64(currentCount + countDelta), for: countKey)
            }

        case .min:
            if let v = value, isAdd {
                let current = try await transaction.getValue(for: valueKey, snapshot: false)
                let currentMin = current.map(unpackDouble) ?? Double.infinity
                if v < currentMin {
                    transaction.setValue(packDouble(v), for: valueKey)
                }
            }

        case .max:
            if let v = value, isAdd {
                let current = try await transaction.getValue(for: valueKey, snapshot: false)
                let currentMax = current.map(unpackDouble) ?? -Double.infinity
                if v > currentMax {
                    transaction.setValue(packDouble(v), for: valueKey)
                }
            }

        case .minmax:
            if let v = value, isAdd {
                let minKey = subspace.pack(Tuple(["min"] + groupKey as [any TupleElement]))
                let maxKey = subspace.pack(Tuple(["max"] + groupKey as [any TupleElement]))

                let currentMin = try await transaction.getValue(for: minKey, snapshot: false)
                    .map(unpackDouble) ?? Double.infinity
                let currentMax = try await transaction.getValue(for: maxKey, snapshot: false)
                    .map(unpackDouble) ?? -Double.infinity

                if v < currentMin {
                    transaction.setValue(packDouble(v), for: minKey)
                }
                if v > currentMax {
                    transaction.setValue(packDouble(v), for: maxKey)
                }
            }

        case .distinct:
            // For distinct, we store each unique value
            if let v = value {
                var elements: [any TupleElement] = ["distinct"]
                elements.append(contentsOf: groupKey)
                elements.append(v)
                let distinctKey = subspace.pack(Tuple(elements))
                if isAdd {
                    transaction.setValue([], for: distinctKey)
                } else {
                    transaction.clear(key: distinctKey)
                }
            }

        case .percentile:
            // Percentile requires storing all values or using approximation
            // For simplicity, store values in sorted order
            if let v = value {
                var elements: [any TupleElement] = ["percentile"]
                elements.append(contentsOf: groupKey)
                elements.append(v)
                let percentileKey = subspace.pack(Tuple(elements))
                if isAdd {
                    transaction.setValue([], for: percentileKey)
                } else {
                    transaction.clear(key: percentileKey)
                }
            }
        }
    }

    // MARK: - Helpers

    private func extractGroupKey(from item: [String: Any]?, config: AggregationIndexConfig) -> [String]? {
        guard let item = item else { return nil }

        var keys: [String] = []
        for field in config.groupByFields {
            guard let value = item[field] else { return nil }
            // Convert value to string for tuple key
            keys.append("\(value)")
        }
        return keys
    }

    private func extractValue(from item: [String: Any]?, config: AggregationIndexConfig) -> Double? {
        guard let item = item,
              let field = config.valueField,
              let value = item[field] else {
            return nil
        }

        if let d = value as? Double { return d }
        if let i = value as? Int { return Double(i) }
        if let i = value as? Int64 { return Double(i) }
        return nil
    }

    private func packDouble(_ value: Double) -> FDB.Bytes {
        var v = value
        return withUnsafeBytes(of: &v) { Array($0) }
    }

    private func unpackDouble(_ bytes: FDB.Bytes) -> Double {
        guard bytes.count >= 8 else { return 0 }
        return bytes.withUnsafeBytes { $0.load(as: Double.self) }
    }

    private func packInt64(_ value: Int64) -> FDB.Bytes {
        var v = value
        return withUnsafeBytes(of: &v) { Array($0) }
    }

    private func unpackInt64(_ bytes: FDB.Bytes) -> Int64 {
        guard bytes.count >= 8 else { return 0 }
        return bytes.withUnsafeBytes { $0.load(as: Int64.self) }
    }

    private func formatValue(_ value: Double, config: AggregationIndexConfig) -> String {
        switch config.aggregationType {
        case .count:
            return String(Int(value))
        default:
            return String(format: "%.2f", value)
        }
    }
}

// MARK: - Aggregation Query

public enum AggregationQuery {
    case get([Any])
    case all
}
