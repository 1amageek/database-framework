import Foundation
import FoundationDB

/// Handler for scalar indexes (equality, range queries)
public struct ScalarIndexHandler: IndexHandler, Sendable {
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
        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .scalar,
            indexName: indexDefinition.name
        )

        // Get field values
        let oldValues = extractValues(from: oldItem)
        let newValues = extractValues(from: newItem)

        // If values haven't changed, no update needed
        if valuesEqual(oldValues, newValues) {
            return
        }

        // Remove old index entry
        if let oldVals = oldValues {
            let oldKey = makeIndexKey(indexSubspace: indexSubspace, values: oldVals, id: id)
            transaction.clear(key: oldKey)
        }

        // Add new index entry
        if let newVals = newValues {
            let newKey = makeIndexKey(indexSubspace: indexSubspace, values: newVals, id: id)
            transaction.setValue([], for: newKey)
        }
    }

    public func scan(
        query: Any,
        limit: Int,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws -> [String] {
        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .scalar,
            indexName: indexDefinition.name
        )

        var ids: [String] = []

        guard let operation = query as? ScalarOperation else {
            return ids
        }

        let (begin, end) = getRange(for: operation, indexSubspace: indexSubspace)

        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
        for try await (key, _) in sequence {
            guard ids.count < limit else { break }

            // Extract ID from key: indexSubspace/<values...>/<id>
            if let tuple = try? indexSubspace.unpack(key) {
                let idIndex = indexDefinition.fields.count
                if tuple.count > idIndex, let id = tuple[idIndex] as? String {
                    ids.append(id)
                }
            }
        }

        return ids
    }

    // MARK: - Private Helpers

    private func extractValues(from item: [String: Any]?) -> [Any]? {
        guard let item = item else { return nil }

        var values: [Any] = []
        for field in indexDefinition.fields {
            guard let value = item[field] else {
                return nil // Missing field means can't index
            }
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

    private func makeIndexKey(indexSubspace: Subspace, values: [Any], id: String) -> FDB.Bytes {
        var tupleElements: [any TupleElement] = values.map(Self.toTupleElement)
        tupleElements.append(id)
        return indexSubspace.pack(Tuple(tupleElements))
    }

    private func getRange(for operation: ScalarOperation, indexSubspace: Subspace) -> (FDB.Bytes, FDB.Bytes) {
        switch operation {
        case .equals(let value):
            let valueSubspace = indexSubspace.subspace(Tuple([Self.toTupleElement(value)]))
            return valueSubspace.range()
        case .range(let lower, let upper):
            let beginKey = lower.map { indexSubspace.pack(Tuple([Self.toTupleElement($0)])) } ?? indexSubspace.range().0
            let endKey = upper.map { indexSubspace.pack(Tuple([Self.toTupleElement($0)])) } ?? indexSubspace.range().1
            return (beginKey, endKey)
        case .greaterThan(let value):
            let beginKey = indexSubspace.pack(Tuple([Self.toTupleElement(value)]))
            return (beginKey, indexSubspace.range().1)
        case .lessThan(let value):
            let endKey = indexSubspace.pack(Tuple([Self.toTupleElement(value)]))
            return (indexSubspace.range().0, endKey)
        }
    }

    /// Convert Any to TupleElement for index storage
    private static func toTupleElement(_ value: Any) -> any TupleElement {
        switch value {
        case let s as String:
            return s
        case let i as Int:
            return Int64(i)
        case let i as Int64:
            return i
        case let d as Double:
            // Encode as string to preserve ordering
            return String(format: "%020.6f", d)
        case let b as Bool:
            return b ? Int64(1) : Int64(0)
        default:
            return "\(value)"
        }
    }
}
