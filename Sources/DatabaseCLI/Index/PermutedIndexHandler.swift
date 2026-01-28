import Foundation
import FoundationDB
import DatabaseEngine

/// Handler for permuted indexes (alternative field ordering for composite indexes)
///
/// Storage layout:
/// - entries/<permuted-values>/<id> = empty
public struct PermutedIndexHandler: IndexHandler, Sendable {
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
              case .permuted(let permutedConfig) = config else {
            return
        }

        // Get the source index configuration to know the fields
        // For now, use the fields from the index definition
        let fields = indexDefinition.fields

        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .permuted,
            indexName: indexDefinition.name
        )
        let entriesSubspace = indexSubspace.subspace(Tuple(["entries"]))

        // Extract values in permuted order
        let oldValues = extractPermutedValues(from: oldItem, fields: fields, permutation: permutedConfig.permutation)
        let newValues = extractPermutedValues(from: newItem, fields: fields, permutation: permutedConfig.permutation)

        // If values haven't changed, no update needed
        if valuesEqual(oldValues, newValues) {
            return
        }

        // Remove old entry
        if let values = oldValues {
            let key = makeKey(subspace: entriesSubspace, values: values, id: id)
            transaction.clear(key: key)
        }

        // Add new entry
        if let values = newValues {
            let key = makeKey(subspace: entriesSubspace, values: values, id: id)
            transaction.setValue([], for: key)
        }
    }

    public func scan(
        query: Any,
        limit: Int,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws -> [String] {
        guard let permutedQuery = query as? PermutedQuery else {
            return []
        }

        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .permuted,
            indexName: indexDefinition.name
        )
        let entriesSubspace = indexSubspace.subspace(Tuple(["entries"]))

        switch permutedQuery {
        case .prefix(let values):
            let tupleValues = values.map(Self.toTupleElement)
            let prefixSubspace = entriesSubspace.subspace(Tuple(tupleValues))
            let (begin, end) = prefixSubspace.range()

            var ids: [String] = []
            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await (key, _) in sequence {
                guard ids.count < limit else { break }
                if let tuple = try? prefixSubspace.unpack(key),
                   tuple.count > 0,
                   let id = tuple[tuple.count - 1] as? String {
                    ids.append(id)
                }
            }
            return ids

        case .range(let prefix, let lowerBound, let upperBound):
            var beginKey: FDB.Bytes
            var endKey: FDB.Bytes

            let prefixTuple = prefix.map(Self.toTupleElement)

            if let lower = lowerBound {
                var lowerTuple = prefixTuple
                lowerTuple.append(Self.toTupleElement(lower))
                let lowerSubspace = entriesSubspace.subspace(Tuple(lowerTuple))
                beginKey = lowerSubspace.range().0
            } else {
                let prefixSubspace = entriesSubspace.subspace(Tuple(prefixTuple))
                beginKey = prefixSubspace.range().0
            }

            if let upper = upperBound {
                var upperTuple = prefixTuple
                upperTuple.append(Self.toTupleElement(upper))
                let upperSubspace = entriesSubspace.subspace(Tuple(upperTuple))
                endKey = upperSubspace.range().1
            } else {
                let prefixSubspace = entriesSubspace.subspace(Tuple(prefixTuple))
                endKey = prefixSubspace.range().1
            }

            var ids: [String] = []
            let sequence = transaction.getRange(begin: beginKey, end: endKey, snapshot: true)
            for try await (key, _) in sequence {
                guard ids.count < limit else { break }
                if let tuple = try? entriesSubspace.unpack(key),
                   tuple.count > 0,
                   let id = tuple[tuple.count - 1] as? String {
                    ids.append(id)
                }
            }
            return ids
        }
    }

    // MARK: - Helpers

    private func extractPermutedValues(from item: [String: Any]?, fields: [String], permutation: [Int]) -> [Any]? {
        guard let item = item else { return nil }

        var values: [Any] = []
        for index in permutation {
            guard index < fields.count else { return nil }
            let field = fields[index]
            guard let value = item[field] else { return nil }
            values.append(value)
        }
        return values
    }

    private func valuesEqual(_ a: [Any]?, _ b: [Any]?) -> Bool {
        guard let a = a, let b = b else {
            return a == nil && b == nil
        }
        guard a.count == b.count else { return false }

        for (va, vb) in zip(a, b) {
            if !areEqual(va, vb) { return false }
        }
        return true
    }

    private func areEqual(_ a: Any, _ b: Any) -> Bool {
        switch (a, b) {
        case (let a as String, let b as String): return a == b
        case (let a as Int, let b as Int): return a == b
        case (let a as Double, let b as Double): return a == b
        case (let a as Bool, let b as Bool): return a == b
        default: return false
        }
    }

    private func makeKey(subspace: Subspace, values: [Any], id: String) -> FDB.Bytes {
        var tupleElements: [any TupleElement] = values.map(Self.toTupleElement)
        tupleElements.append(id)
        return subspace.pack(Tuple(tupleElements))
    }

    /// Convert Any to TupleElement for index storage
    ///
    /// Uses TupleEncoder for consistent type conversion.
    /// Falls back to string representation for unsupported types.
    private static func toTupleElement(_ value: Any) -> any TupleElement {
        do {
            return try TupleEncoder.encode(value)
        } catch {
            // Fallback for unsupported types in dynamic schema context
            return "\(value)"
        }
    }
}

// MARK: - Permuted Query

public enum PermutedQuery {
    case prefix([Any])
    case range(prefix: [Any], lowerBound: Any?, upperBound: Any?)
}
