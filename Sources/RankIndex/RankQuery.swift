// RankQuery.swift
// RankIndex - Query extension for ranking operations
//
// Follows GraphIndex pattern: execute() uses the actual index, not in-memory processing.

import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

// MARK: - Rank Query Builder

/// Builder for ranking queries (leaderboards, top-N, percentiles)
///
/// Executes against the RankIndex selected for the requested field. A missing
/// or ambiguous index is a typed error; execution never falls back to an
/// in-memory full scan.
///
/// **Usage**:
/// ```swift
/// import RankIndex
///
/// let leaderboard = try await context.rank(Player.self)
///     .by(Player.fields.score)
///     .top(100)
///     .execute()
/// // Returns: [(item: Player, rank: Int)]
///
/// let median = try await context.rank(Player.self)
///     .by(Player.fields.score)
///     .percentile(0.5)
///     .executeOne()
/// // Returns: Player?
/// ```
public struct RankQueryBuilder<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let fieldName: String
    private let selectedIndexName: String?
    private var queryMode: RankQueryMode = .top(10)

    /// Query mode for ranking
    internal enum RankQueryMode: Sendable {
        case top(Int)
        case bottom(Int)
        case range(from: Int, to: Int)
        case percentile(Double)
    }

    internal init(
        queryContext: IndexQueryContext,
        fieldName: String,
        selectedIndexName: String? = nil
    ) {
        self.queryContext = queryContext
        self.fieldName = fieldName
        self.selectedIndexName = selectedIndexName
    }

    /// Get top N items (highest values).
    ///
    /// Invalid `n` (≤ 0) is stored as-is; validation is deferred to execute
    /// time so callers receive an error rather than a silently swapped-out mode.
    ///
    /// - Parameter n: Number of items to return (must be > 0 at execute time)
    /// - Returns: Updated query builder
    public func top(_ n: Int) -> Self {
        var copy = self
        copy.queryMode = .top(n)
        return copy
    }

    /// Get bottom N items (lowest values).
    ///
    /// Validation is deferred to execute time; see `top(_:)`.
    ///
    /// - Parameter n: Number of items to return (must be > 0 at execute time)
    /// - Returns: Updated query builder
    public func bottom(_ n: Int) -> Self {
        var copy = self
        copy.queryMode = .bottom(n)
        return copy
    }

    /// Get items in a specific rank range.
    ///
    /// Validation is deferred to execute time; see `top(_:)`.
    ///
    /// - Parameters:
    ///   - from: Start rank (0-based, inclusive; must be ≥ 0 at execute time)
    ///   - to: End rank (exclusive; must be > `from` at execute time)
    /// - Returns: Updated query builder
    public func range(from: Int, to: Int) -> Self {
        var copy = self
        copy.queryMode = .range(from: from, to: to)
        return copy
    }

    /// Get items at a specific percentile.
    ///
    /// Validation is deferred to execute time; see `top(_:)`.
    ///
    /// - Parameter p: Percentile value (must be in [0.0, 1.0] at execute time)
    /// - Returns: Updated query builder
    public func percentile(_ p: Double) -> Self {
        var copy = self
        copy.queryMode = .percentile(p)
        return copy
    }

    /// Validate the current query mode. Called at every execute entry point so
    /// invalid arguments surface as typed errors rather than silently reverting
    /// to the default mode.
    private func validateMode() throws {
        switch queryMode {
        case .top(let n), .bottom(let n):
            guard n > 0 else { throw RankQueryError.invalidCount(n) }
        case .range(let from, let to):
            guard from >= 0, to > from else {
                throw RankQueryError.invalidRange(from: from, to: to)
            }
        case .percentile(let p):
            guard p >= 0.0, p <= 1.0 else { throw RankQueryError.invalidPercentile(p) }
        }
    }

    /// Execute the ranking query using the index
    ///
    /// Uses RankIndex for efficient queries:
    /// - top(k): O(K) bounded reverse storage range scan
    /// - bottom(k): O(K) bounded forward storage range scan
    /// - range: O(to) reverse scan then drop first `from`
    /// - percentile: O(1) atomic count + O(targetRank) reverse scan
    ///
    /// - Returns: Array of (item, rank) tuples in the requested score order
    /// - Throws: Error if execution fails
    public func execute() async throws -> [(item: T, rank: Int)] {
        try validateMode()

        let response = try await queryContext.context.query(
            try toSelectQuery(),
            as: T.self,
            options: .default
        )

        return try response.rows.map { row in
            let item = try QueryRowCodec.decode(row, as: T.self)
            guard let rank = row.annotations["rank"]?.int64Value,
                  let exactRank = Int(exactly: rank) else {
                throw RankQueryError.invalidResponse("Missing rank annotation")
            }
            return (item: item, rank: exactRank)
        }
    }

    internal func executeDirect(
        configuration: TransactionConfiguration = .default
    ) async throws -> [(item: T, rank: Int)] {
        try validateMode()

        let indexName = try resolvedIndexName()

        // Execute query using index
        return try await queryContext.withReadableIndex(
            named: indexName,
            indexType: .rank,
            for: T.self,
            configuration: configuration
        ) { readableIndex, transaction in
            guard let readableIndex else {
                return []
            }
            return try await self.executeWithIndex(
                indexSubspace: readableIndex.subspace,
                transaction: transaction
            )
        }
    }

    /// Execute query using the rank index
    private func executeWithIndex(
        indexSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> [(item: T, rank: Int)] {
        let scoresSubspace = indexSubspace.subspace("scores")
        let scanner = RankScanner(scoresSubspace: scoresSubspace, transaction: transaction)

        switch queryMode {
        case .top(let k):
            return try await scanTop(
                scanner: scanner,
                k: k,
                transaction: transaction
            )

        case .bottom(let k):
            return try await scanBottom(
                scanner: scanner,
                indexSubspace: indexSubspace,
                k: k,
                transaction: transaction
            )

        case .range(let from, let to):
            return try await scanRange(
                scanner: scanner,
                from: from,
                to: to,
                transaction: transaction
            )

        case .percentile(let p):
            return try await scanPercentile(
                scanner: scanner,
                indexSubspace: indexSubspace,
                p: p,
                transaction: transaction
            )
        }
    }

    /// Scan top K items using a bounded reverse storage range read.
    ///
    /// **Algorithm**: `collectRange(reverse: true, limit: k)` reads the K highest
    /// entries directly in O(K). No heap, no truncation, no later sort.
    private func scanTop(
        scanner: RankScanner,
        k: Int,
        transaction: any TransactionAccess
    ) async throws -> [(item: T, rank: Int)] {
        let entries = try await scanner.top(k: k)
        return try await fetchItemsWithRank(
            entries: entries,
            startRank: 0,
            transaction: transaction
        )
    }

    /// Scan bottom K items using a bounded forward storage range read.
    private func scanBottom(
        scanner: RankScanner,
        indexSubspace: Subspace,
        k: Int,
        transaction: any TransactionAccess
    ) async throws -> [(item: T, rank: Int)] {
        let entries = try await scanner.bottom(k: k)
        let countKey = indexSubspace.pack(Tuple("_count"))
        let countBytes = try await transaction.getValue(for: countKey, snapshot: true)
        let totalCount = try countBytes.map(RankCounterCodec.decodeInt) ?? 0
        let startRank = try RankScanner.bottomStartPosition(
            totalCount: totalCount,
            returnedCount: entries.count
        )
        return try await fetchItemsWithRank(
            entries: entries,
            startRank: startRank,
            rankStep: -1,
            transaction: transaction
        )
    }

    /// Scan items in a rank range [from, to) using reverse scan of `to` entries.
    private func scanRange(
        scanner: RankScanner,
        from: Int,
        to: Int,
        transaction: any TransactionAccess
    ) async throws -> [(item: T, rank: Int)] {
        let entries = try await scanner.rangeDescending(from: from, to: to)
        return try await fetchItemsWithRank(
            entries: entries,
            startRank: from,
            transaction: transaction
        )
    }

    /// Scan item at a specific percentile.
    ///
    /// Uses the atomic count key (`_count`) for O(1) total count, then reads the
    /// Nth-from-top entry directly.
    private func scanPercentile(
        scanner: RankScanner,
        indexSubspace: Subspace,
        p: Double,
        transaction: any TransactionAccess
    ) async throws -> [(item: T, rank: Int)] {
        let countKey = indexSubspace.pack(Tuple("_count"))
        let countBytes = try await transaction.getValue(for: countKey, snapshot: true)
        let totalCount: Int
        if let countBytes {
            totalCount = try RankCounterCodec.decodeInt(countBytes)
        } else {
            totalCount = 0
        }
        guard totalCount > 0 else { return [] }

        // percentile 0.5 (median) = middle rank; 1.0 = highest; 0.0 = lowest.
        let targetRank = Int(Double(totalCount) * (1.0 - p))
        let safeTargetRank = max(0, min(targetRank, totalCount - 1))

        guard let entry = try await scanner.nthFromTop(safeTargetRank) else {
            throw RankQueryError.missingIndexedEntity(rank: safeTargetRank)
        }
        return try await fetchItemsWithRank(
            entries: [entry],
            startRank: safeTargetRank,
            transaction: transaction
        )
    }

    /// Fetch items by primary key and pair each with its scan-position rank.
    ///
    /// Uses `fetchItemsPreservingOrder` so every fetched item retains its native
    /// index rank. A missing entity is an index consistency failure and throws;
    /// it is never removed from a successful result page.
    private func fetchItemsWithRank(
        entries: [RankScanEntry],
        startRank: Int,
        rankStep: Int = 1,
        transaction: any TransactionAccess
    ) async throws -> [(item: T, rank: Int)] {
        let ids = entries.map { $0.primaryKey }
        let items = try await queryContext.fetchItemsPreservingOrder(
            ids: ids,
            type: T.self,
            transaction: transaction
        )
        var results: [(item: T, rank: Int)] = []
        results.reserveCapacity(items.count)
        for (offset, maybeItem) in items.enumerated() {
            let rank = startRank + (offset * rankStep)
            guard let item = maybeItem else {
                throw RankQueryError.missingIndexedEntity(rank: rank)
            }
            results.append((item: item, rank: rank))
        }
        return results
    }

    internal func toSelectQuery() throws -> SelectQuery {
        var parameters: [String: FieldValue] = [
            RankReadParameter.fieldName: .string(fieldName)
        ]

        let limit: UInt64?
        switch queryMode {
        case .top(let count):
            parameters[RankReadParameter.mode] = .string(RankReadParameter.topMode)
            parameters[RankReadParameter.count] = .int64(Int64(count))
            limit = try queryLimit(count)
        case .bottom(let count):
            parameters[RankReadParameter.mode] = .string(RankReadParameter.bottomMode)
            parameters[RankReadParameter.count] = .int64(Int64(count))
            limit = try queryLimit(count)
        case .range(let from, let to):
            parameters[RankReadParameter.mode] = .string(RankReadParameter.rangeMode)
            parameters[RankReadParameter.from] = .int64(Int64(from))
            parameters[RankReadParameter.to] = .int64(Int64(to))
            limit = try queryLimit(to - from)
        case .percentile(let percentile):
            parameters[RankReadParameter.mode] = .string(RankReadParameter.percentileMode)
            parameters[RankReadParameter.percentile] = .float64(percentile)
            limit = 1
        }

        return SelectQuery(
            projection: .all,
            source: .table(TableRef(table: T.persistableType)),
            accessPath: .index(
                IndexScanSource(
                    indexName: try resolvedIndexName(),
                    indexType: .rank,
                    parameters: parameters
                )
            ),
            limit: limit
        )
    }

    private func queryLimit(_ value: Int) throws -> UInt64 {
        guard let limit = UInt64(exactly: value) else {
            throw RankQueryError.invalidCount(value)
        }
        return limit
    }

    private func resolvedIndexName() throws -> String {
        if let selectedIndexName {
            guard let descriptor = queryContext.schema.indexDescriptor(
                named: selectedIndexName
            ),
            descriptor.entityName == T.persistableType,
                descriptor.type == .rank,
                descriptor.fieldNames == [fieldName] else {
                throw RankQueryError.indexNotFound(selectedIndexName)
            }
            return selectedIndexName
        }

        let matches = queryContext.schema.indexDescriptors.filter {
            $0.entityName == T.persistableType
                && $0.type == .rank
                && $0.fieldNames == [fieldName]
        }
        guard let match = matches.first else {
            throw RankQueryError.indexNotFound(
                "\(T.persistableType).\(fieldName)"
            )
        }
        guard matches.count == 1 else {
            throw RankQueryError.ambiguousIndexes(
                entity: T.persistableType,
                field: fieldName
            )
        }
        return match.name
    }

    /// Execute and return a single item (useful for percentile queries)
    ///
    /// - Returns: The item at the requested position, or nil if not found
    /// - Throws: Error if execution fails
    public func executeOne() async throws -> T? {
        let results = try await execute()
        return results.first?.item
    }

}

// MARK: - Rank Entry Point

/// Entry point for ranking queries
public struct RankEntryPoint<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// Specify the field to rank by
    ///
    /// - Parameter field: Compiled field descriptor for an exact numeric field
    /// - Returns: Rank query builder
    public func by<Value: RankNumericValue>(
        _ field: Field<T, Value>
    ) -> RankQueryBuilder<T> {
        RankQueryBuilder(
            queryContext: queryContext,
            fieldName: field.name
        )
    }
}

// MARK: - DatabaseContext Extension

extension DatabaseContext {

    /// Start a ranking query
    ///
    /// This method is available when you import `RankIndex`.
    ///
    /// **Usage**:
    /// ```swift
    /// import RankIndex
    ///
    /// let leaderboard = try await context.rank(Player.self)
    ///     .by(Player.fields.score)
    ///     .top(100)
    ///     .execute()
    /// // Returns: [(item: Player, rank: Int)]
    ///
    /// let median = try await context.rank(Player.self)
    ///     .by(Player.fields.score)
    ///     .percentile(0.5)
    ///     .executeOne()
    /// // Returns: Player?
    /// ```
    ///
    /// - Parameter type: The Persistable type to rank
    /// - Returns: Entry point for configuring the ranking
    public func rank<T: Persistable>(_ type: T.Type) -> RankEntryPoint<T> {
        RankEntryPoint(queryContext: indexQueryContext)
    }
}

// MARK: - Rank Query Error

/// Errors for ranking query operations
public enum RankQueryError: Error, Sendable, Equatable, CustomStringConvertible {
    /// No ranking field specified
    case noRankingField

    /// Invalid count for top/bottom (must be > 0)
    case invalidCount(Int)

    /// Invalid rank range (must satisfy `from >= 0 && to > from`)
    case invalidRange(from: Int, to: Int)

    /// Invalid percentile value
    case invalidPercentile(Double)

    /// Index not found
    case indexNotFound(String)

    /// More than one rank index targets the selected field.
    case ambiguousIndexes(entity: String, field: String)

    /// Canonical query response is missing required metadata
    case invalidResponse(String)

    /// An index entry points to an entity that could not be fetched
    case missingIndexedEntity(rank: Int)

    public var description: String {
        switch self {
        case .noRankingField:
            return "No ranking field specified for rank query"
        case .invalidCount(let n):
            return "Invalid count: \(n). top/bottom require a positive count"
        case .invalidRange(let from, let to):
            return "Invalid rank range: from=\(from), to=\(to). Require from >= 0 and to > from"
        case .invalidPercentile(let p):
            return "Invalid percentile value: \(p). Must be between 0.0 and 1.0"
        case .indexNotFound(let name):
            return "Rank index not found: \(name)"
        case .ambiguousIndexes(let entity, let field):
            return "Multiple rank indexes target \(entity).\(field)"
        case .invalidResponse(let reason):
            return "Invalid rank query response: \(reason)"
        case .missingIndexedEntity(let rank):
            return "Rank index entry at rank \(rank) has no corresponding entity"
        }
    }
}
