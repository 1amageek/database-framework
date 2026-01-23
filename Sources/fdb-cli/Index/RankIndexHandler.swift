import Foundation
import FoundationDB

/// Handler for rank indexes (leaderboards, Top-K queries)
public struct RankIndexHandler: IndexHandler, Sendable {
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
              case .rank(let rankConfig) = config else {
            return
        }

        let scoreField = rankConfig.scoreField
        let descending = rankConfig.descending
        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .rank,
            indexName: indexDefinition.name
        )

        let oldScore = extractScore(from: oldItem, field: scoreField)
        let newScore = extractScore(from: newItem, field: scoreField)

        // If score hasn't changed, no update needed
        if oldScore == newScore {
            return
        }

        // Remove old entry
        if let score = oldScore {
            let sortKey = descending ? Int64.max - score : score
            let oldKey = indexSubspace.pack(Tuple([sortKey, id]))
            transaction.clear(key: oldKey)
        }

        // Add new entry
        if let score = newScore {
            let sortKey = descending ? Int64.max - score : score
            let newKey = indexSubspace.pack(Tuple([sortKey, id]))
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
            kind: .rank,
            indexName: indexDefinition.name
        )

        var ids: [String] = []

        guard let rankQuery = query as? RankQuery else {
            // Default: top N
            let (begin, end) = indexSubspace.range()
            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await (key, _) in sequence {
                guard ids.count < limit else { break }
                if let tuple = try? indexSubspace.unpack(key),
                   tuple.count >= 2,
                   let id = tuple[1] as? String {
                    ids.append(id)
                }
            }
            return ids
        }

        switch rankQuery {
        case .top(let count):
            let (begin, end) = indexSubspace.range()
            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await (key, _) in sequence {
                guard ids.count < count else { break }
                if let tuple = try? indexSubspace.unpack(key),
                   tuple.count >= 2,
                   let id = tuple[1] as? String {
                    ids.append(id)
                }
            }

        case .of(let targetId):
            // Find rank of a specific ID
            var rank = 0
            let (begin, end) = indexSubspace.range()
            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await (key, _) in sequence {
                rank += 1
                if let tuple = try? indexSubspace.unpack(key),
                   tuple.count >= 2,
                   let id = tuple[1] as? String,
                   id == targetId {
                    ids = ["\(rank)"]
                    break
                }
            }

        case .range(let start, let endRank):
            var rank = 0
            let (begin, end) = indexSubspace.range()
            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await (key, _) in sequence {
                rank += 1
                if rank >= start && rank <= endRank {
                    if let tuple = try? indexSubspace.unpack(key),
                       tuple.count >= 2,
                       let id = tuple[1] as? String {
                        ids.append(id)
                    }
                }
                if rank > endRank { break }
            }

        case .count:
            var count = 0
            let (begin, end) = indexSubspace.range()
            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await _ in sequence {
                count += 1
            }
            ids = ["\(count)"]
        }

        return ids
    }

    private func extractScore(from item: [String: Any]?, field: String) -> Int64? {
        guard let item = item, let value = item[field] else { return nil }
        if let i = value as? Int { return Int64(i) }
        if let i = value as? Int64 { return i }
        if let d = value as? Double { return Int64(d) }
        return nil
    }
}

// MARK: - Rank Query

public enum RankQuery {
    case top(Int)
    case of(String)
    case range(Int, Int)
    case count
}
