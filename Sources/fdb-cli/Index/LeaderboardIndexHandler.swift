import Foundation
import FoundationDB

/// Handler for leaderboard indexes (time-windowed rankings)
///
/// Storage layout:
/// - windows/<windowKey>/<score>/<id> = empty
/// - metadata/windows = list of active windows
public struct LeaderboardIndexHandler: IndexHandler, Sendable {
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
              case .leaderboard(let lbConfig) = config else {
            return
        }

        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .leaderboard,
            indexName: indexDefinition.name
        )
        let windowsSubspace = indexSubspace.subspace(Tuple(["windows"]))

        // Get current window key
        let currentWindow = getCurrentWindowKey(config: lbConfig)

        // Extract scores
        let oldScore = extractScore(from: oldItem, field: lbConfig.scoreField)
        let newScore = extractScore(from: newItem, field: lbConfig.scoreField)

        // Extract group key (if any)
        let groupKey = extractGroupKey(from: newItem ?? oldItem, config: lbConfig)

        // Build window-specific subspace
        var windowTuple: [any TupleElement] = [currentWindow]
        if let gk = groupKey {
            windowTuple.append(contentsOf: gk.map(Self.toTupleElement))
        }
        let currentWindowSubspace = windowsSubspace.subspace(Tuple(windowTuple))

        // Remove old entry
        if let score = oldScore {
            let sortKey = Int64.max - score // Descending order
            let key = currentWindowSubspace.pack(Tuple([sortKey, id]))
            transaction.clear(key: key)
        }

        // Add new entry
        if let score = newScore {
            let sortKey = Int64.max - score // Descending order
            let key = currentWindowSubspace.pack(Tuple([sortKey, id]))
            transaction.setValue([], for: key)
        }

        // Record active window
        let metadataKey = indexSubspace.pack(Tuple(["metadata", "windows", currentWindow]))
        transaction.setValue([], for: metadataKey)
    }

    public func scan(
        query: Any,
        limit: Int,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws -> [String] {
        guard let config = indexDefinition.config,
              case .leaderboard(let lbConfig) = config else {
            return []
        }

        guard let lbQuery = query as? LeaderboardQuery else {
            return []
        }

        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .leaderboard,
            indexName: indexDefinition.name
        )
        let windowsSubspace = indexSubspace.subspace(Tuple(["windows"]))

        switch lbQuery {
        case .top(let count, let window, let groupKey):
            let windowKey = window ?? getCurrentWindowKey(config: lbConfig)
            var windowTuple: [any TupleElement] = [windowKey]
            if let gk = groupKey {
                windowTuple.append(contentsOf: gk.map(Self.toTupleElement))
            }
            let windowSubspace = windowsSubspace.subspace(Tuple(windowTuple))

            return try await getTopEntries(
                subspace: windowSubspace,
                count: count,
                transaction: transaction
            )

        case .rank(let id, let window, let groupKey):
            let windowKey = window ?? getCurrentWindowKey(config: lbConfig)
            var windowTuple: [any TupleElement] = [windowKey]
            if let gk = groupKey {
                windowTuple.append(contentsOf: gk.map(Self.toTupleElement))
            }
            let windowSubspace = windowsSubspace.subspace(Tuple(windowTuple))

            return try await getRank(
                id: id,
                subspace: windowSubspace,
                transaction: transaction
            )

        case .listWindows:
            let metadataSubspace = indexSubspace.subspace(Tuple(["metadata", "windows"]))
            let (begin, end) = metadataSubspace.range()

            var windows: [String] = []
            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await (key, _) in sequence {
                guard windows.count < limit else { break }
                if let tuple = try? metadataSubspace.unpack(key),
                   let window = tuple[0] as? String {
                    windows.append(window)
                }
            }
            return windows
        }
    }

    // MARK: - Helpers

    private func extractScore(from item: [String: Any]?, field: String) -> Int64? {
        guard let item = item, let value = item[field] else { return nil }
        if let i = value as? Int { return Int64(i) }
        if let i = value as? Int64 { return i }
        if let d = value as? Double { return Int64(d) }
        return nil
    }

    private func extractGroupKey(from item: [String: Any]?, config: LeaderboardIndexConfig) -> [Any]? {
        guard !config.groupByFields.isEmpty, let item = item else { return nil }

        var keys: [Any] = []
        for field in config.groupByFields {
            if let value = item[field] {
                keys.append(value)
            } else {
                return nil
            }
        }
        return keys
    }

    private func getCurrentWindowKey(config: LeaderboardIndexConfig) -> String {
        let now = Date()
        let calendar = Calendar.current

        switch config.windowType {
        case .hourly:
            let components = calendar.dateComponents([.year, .month, .day, .hour], from: now)
            return String(format: "%04d%02d%02d%02d",
                         components.year!, components.month!, components.day!, components.hour!)
        case .daily:
            let components = calendar.dateComponents([.year, .month, .day], from: now)
            return String(format: "%04d%02d%02d",
                         components.year!, components.month!, components.day!)
        case .weekly:
            let components = calendar.dateComponents([.yearForWeekOfYear, .weekOfYear], from: now)
            return String(format: "%04dW%02d",
                         components.yearForWeekOfYear!, components.weekOfYear!)
        case .monthly:
            let components = calendar.dateComponents([.year, .month], from: now)
            return String(format: "%04d%02d",
                         components.year!, components.month!)
        }
    }

    private func getTopEntries(
        subspace: Subspace,
        count: Int,
        transaction: any TransactionProtocol
    ) async throws -> [String] {
        let (begin, end) = subspace.range()
        var entries: [String] = []

        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
        for try await (key, _) in sequence {
            guard entries.count < count else { break }
            if let tuple = try? subspace.unpack(key),
               tuple.count >= 2,
               let id = tuple[1] as? String {
                entries.append(id)
            }
        }

        return entries
    }

    private func getRank(
        id: String,
        subspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [String] {
        let (begin, end) = subspace.range()
        var rank = 0

        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
        for try await (key, _) in sequence {
            rank += 1
            if let tuple = try? subspace.unpack(key),
               tuple.count >= 2,
               let entryId = tuple[1] as? String,
               entryId == id {
                return ["\(rank)"]
            }
        }

        return ["Not ranked"]
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
            return String(format: "%020.6f", d)
        case let b as Bool:
            return b ? Int64(1) : Int64(0)
        default:
            return "\(value)"
        }
    }
}

// MARK: - Leaderboard Query

public enum LeaderboardQuery {
    case top(count: Int, window: String?, groupKey: [Any]?)
    case rank(id: String, window: String?, groupKey: [Any]?)
    case listWindows
}
