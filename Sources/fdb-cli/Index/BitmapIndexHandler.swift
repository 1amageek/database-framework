import Foundation
import FoundationDB

/// Handler for bitmap indexes (low cardinality fields, set operations)
public struct BitmapIndexHandler: IndexHandler, Sendable {
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
              case .bitmap(let bitmapConfig) = config else {
            return
        }

        let field = bitmapConfig.field
        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .bitmap,
            indexName: indexDefinition.name
        )

        let oldValue = oldItem?[field] as? String
        let newValue = newItem?[field] as? String

        // If value hasn't changed, no update needed
        if oldValue == newValue {
            return
        }

        // Remove from old bitmap
        if let oldVal = oldValue {
            let oldKey = indexSubspace.pack(Tuple([oldVal, id]))
            transaction.clear(key: oldKey)
        }

        // Add to new bitmap
        if let newVal = newValue {
            let newKey = indexSubspace.pack(Tuple([newVal, id]))
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
            kind: .bitmap,
            indexName: indexDefinition.name
        )

        var ids: [String] = []

        guard let bitmapQuery = query as? BitmapQuery else {
            return ids
        }

        switch bitmapQuery {
        case .equals(let value):
            let valueSubspace = indexSubspace.subspace(Tuple([value]))
            let (begin, end) = valueSubspace.range()

            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await (key, _) in sequence {
                guard ids.count < limit else { break }
                if let tuple = try? valueSubspace.unpack(key),
                   let id = tuple[0] as? String {
                    ids.append(id)
                }
            }

        case .inSet(let values):
            var idSet = Set<String>()
            for value in values {
                let valueSubspace = indexSubspace.subspace(Tuple([value]))
                let (begin, end) = valueSubspace.range()

                let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
                for try await (key, _) in sequence {
                    if let tuple = try? valueSubspace.unpack(key),
                       let id = tuple[0] as? String {
                        idSet.insert(id)
                    }
                }
            }
            ids = Array(idSet.prefix(limit))

        case .count:
            // Return count as a single "id"
            let (begin, end) = indexSubspace.range()
            var count = 0
            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await _ in sequence {
                count += 1
            }
            ids = ["\(count)"]
        }

        return ids
    }
}

// MARK: - Bitmap Query

public enum BitmapQuery {
    case equals(String)
    case inSet([String])
    case count
}
