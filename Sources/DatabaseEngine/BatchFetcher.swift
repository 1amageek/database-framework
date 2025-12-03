// BatchFetcher.swift
// DatabaseEngine - Optimized batch fetching for records from indexes
//
// Reference: FDB Record Layer Remote Fetch optimization
// Efficiently fetches multiple records by batching primary key lookups.

import Foundation
import FoundationDB
import Core
import Synchronization

// MARK: - BatchFetchConfiguration

/// Configuration for batch fetching
public struct BatchFetchConfiguration: Sendable, Equatable {
    /// Maximum number of records to fetch in a single batch
    ///
    /// Larger batches are more efficient but use more memory.
    /// Reference: FDB transaction size limits suggest ~1MB per transaction.
    public let batchSize: Int

    /// Whether to prefetch the next batch while processing current
    ///
    /// When enabled, uses a separate transaction for prefetching.
    public let prefetchEnabled: Bool

    /// Number of batches to prefetch ahead
    ///
    /// Only used when prefetchEnabled is true.
    public let prefetchCount: Int

    /// Maximum time to wait for a batch in seconds
    public let batchTimeoutSeconds: Double

    /// Whether to continue on individual fetch errors
    public let continueOnError: Bool

    /// Default configuration
    public static let `default` = BatchFetchConfiguration(
        batchSize: 100,
        prefetchEnabled: true,
        prefetchCount: 1,
        batchTimeoutSeconds: 5.0,
        continueOnError: false
    )

    /// Small batches for interactive use
    public static let interactive = BatchFetchConfiguration(
        batchSize: 20,
        prefetchEnabled: false,
        prefetchCount: 0,
        batchTimeoutSeconds: 1.0,
        continueOnError: false
    )

    /// Large batches for bulk operations
    public static let bulk = BatchFetchConfiguration(
        batchSize: 500,
        prefetchEnabled: true,
        prefetchCount: 2,
        batchTimeoutSeconds: 30.0,
        continueOnError: true
    )

    public init(
        batchSize: Int = 100,
        prefetchEnabled: Bool = true,
        prefetchCount: Int = 1,
        batchTimeoutSeconds: Double = 5.0,
        continueOnError: Bool = false
    ) {
        precondition(batchSize > 0, "batchSize must be positive")
        precondition(prefetchCount >= 0, "prefetchCount must be non-negative")

        self.batchSize = batchSize
        self.prefetchEnabled = prefetchEnabled
        self.prefetchCount = prefetchCount
        self.batchTimeoutSeconds = batchTimeoutSeconds
        self.continueOnError = continueOnError
    }
}

// MARK: - BatchFetcher

/// Optimized batch fetcher for records
///
/// Efficiently fetches multiple records by:
/// 1. Batching primary key lookups
/// 2. Parallelizing reads across keys
/// 3. Prefetching next batch while processing current
/// 4. Providing streaming access to results
///
/// **Usage**:
/// ```swift
/// let fetcher = BatchFetcher<User>(
///     database: database,
///     itemSubspace: itemSubspace,
///     itemType: "User",
///     configuration: .default
/// )
///
/// // Fetch from primary keys
/// let users = try await fetcher.fetch(primaryKeys: keys, transaction: tx)
///
/// // Stream from index entries
/// for try await user in fetcher.streamFromIndex(indexEntries: entries, transaction: tx) {
///     process(user)
/// }
/// ```
public struct BatchFetcher<Item: Persistable>: Sendable {
    // MARK: - Properties

    /// Configuration
    public let configuration: BatchFetchConfiguration

    /// Item subspace
    private let itemSubspace: Subspace

    /// Item type name
    private let itemType: String

    // MARK: - Initialization

    public init(
        itemSubspace: Subspace,
        itemType: String,
        configuration: BatchFetchConfiguration = .default
    ) {
        self.itemSubspace = itemSubspace
        self.itemType = itemType
        self.configuration = configuration
    }

    // MARK: - Batch Fetch

    /// Fetch items by primary keys
    ///
    /// **Thread Safety**: All reads are sequential within the transaction.
    /// FDB transactions are not thread-safe for concurrent access.
    ///
    /// - Parameters:
    ///   - primaryKeys: The primary keys to fetch
    ///   - transaction: The transaction to use
    /// - Returns: The fetched items (preserves order where found)
    public func fetch(
        primaryKeys: [Tuple],
        transaction: any TransactionProtocol
    ) async throws -> [Item] {
        guard !primaryKeys.isEmpty else { return [] }

        let itemTypeSubspace = itemSubspace.subspace(itemType)

        // All reads are sequential within a single transaction
        // FDB transactions are NOT thread-safe for concurrent access
        var results: [Item] = []
        results.reserveCapacity(primaryKeys.count)

        for pk in primaryKeys {
            let key = itemTypeSubspace.pack(pk)
            if let data = try await transaction.getValue(for: key) {
                let item: Item = try DataAccess.deserialize(Array(data))
                results.append(item)
            }
        }

        return results
    }

    /// Fetch a single batch of items
    ///
    /// **Thread Safety**: All reads are sequential.
    private func fetchBatch(
        primaryKeys: [Tuple],
        subspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [Item] {
        var results: [Item] = []
        results.reserveCapacity(primaryKeys.count)

        // Sequential reads only - FDB transactions are NOT thread-safe
        for pk in primaryKeys {
            let key = subspace.pack(pk)
            if let data = try await transaction.getValue(for: key) {
                let item: Item = try DataAccess.deserialize(Array(data))
                results.append(item)
            }
        }

        return results
    }

    // MARK: - Streaming Fetch

    /// Stream items from an async sequence of primary keys
    ///
    /// - Parameters:
    ///   - primaryKeys: Async sequence of primary keys
    ///   - transaction: The transaction to use
    /// - Returns: AsyncStream of fetched items
    public func stream<S: AsyncSequence>(
        primaryKeys: S,
        transaction: any TransactionProtocol
    ) -> AsyncStream<Item> where S.Element == Tuple, S: Sendable {
        AsyncStream { continuation in
            Task {
                var batch: [Tuple] = []
                batch.reserveCapacity(configuration.batchSize)

                let itemTypeSubspace = itemSubspace.subspace(itemType)

                do {
                    for try await pk in primaryKeys {
                        batch.append(pk)

                        if batch.count >= configuration.batchSize {
                            let items = try await fetchBatch(
                                primaryKeys: batch,
                                subspace: itemTypeSubspace,
                                transaction: transaction
                            )
                            for item in items {
                                continuation.yield(item)
                            }
                            batch.removeAll(keepingCapacity: true)
                        }
                    }

                    // Process remaining batch
                    if !batch.isEmpty {
                        let items = try await fetchBatch(
                            primaryKeys: batch,
                            subspace: itemTypeSubspace,
                            transaction: transaction
                        )
                        for item in items {
                            continuation.yield(item)
                        }
                    }

                    continuation.finish()
                } catch {
                    continuation.finish()
                }
            }
        }
    }

    /// Stream items from index entries
    ///
    /// Index entries are expected to contain primary keys as the last element(s).
    ///
    /// - Parameters:
    ///   - indexEntries: Async sequence of (key, value) pairs from index
    ///   - indexSubspace: The index subspace for unpacking keys
    ///   - transaction: The transaction to use
    /// - Returns: AsyncStream of fetched items
    public func streamFromIndex<S: AsyncSequence>(
        indexEntries: S,
        indexSubspace: Subspace,
        transaction: any TransactionProtocol
    ) -> AsyncStream<Item> where S.Element == (key: FDB.Bytes, value: FDB.Bytes), S: Sendable {
        AsyncStream { continuation in
            Task {
                var batch: [Tuple] = []
                batch.reserveCapacity(configuration.batchSize)

                let itemTypeSubspace = itemSubspace.subspace(itemType)

                do {
                    for try await (key, _) in indexEntries {
                        // Extract primary key from index entry
                        if let pk = try? extractPrimaryKey(from: key, indexSubspace: indexSubspace) {
                            batch.append(pk)

                            if batch.count >= configuration.batchSize {
                                let items = try await fetchBatch(
                                    primaryKeys: batch,
                                    subspace: itemTypeSubspace,
                                    transaction: transaction
                                )
                                for item in items {
                                    continuation.yield(item)
                                }
                                batch.removeAll(keepingCapacity: true)
                            }
                        }
                    }

                    // Process remaining batch
                    if !batch.isEmpty {
                        let items = try await fetchBatch(
                            primaryKeys: batch,
                            subspace: itemTypeSubspace,
                            transaction: transaction
                        )
                        for item in items {
                            continuation.yield(item)
                        }
                    }

                    continuation.finish()
                } catch {
                    continuation.finish()
                }
            }
        }
    }

    // MARK: - Primary Key Extraction

    /// Extract primary key from an index key
    ///
    /// Assumes the primary key is the last element of the index key tuple.
    private func extractPrimaryKey(from key: FDB.Bytes, indexSubspace: Subspace) throws -> Tuple? {
        let tuple = try indexSubspace.unpack(key)
        guard tuple.count > 0 else { return nil }

        // Primary key is typically the last element
        if let lastElement = tuple[tuple.count - 1] {
            return Tuple(lastElement)
        }

        return nil
    }
}

// MARK: - BatchFetchResult

/// Result of a batch fetch operation
public struct BatchFetchResult<Item: Persistable>: Sendable {
    /// Successfully fetched items
    public let items: [Item]

    /// Keys that were not found
    public let notFound: [Tuple]

    /// Keys that failed to fetch
    public let failed: [(key: Tuple, error: Error)]

    /// Total keys requested
    public var totalRequested: Int {
        items.count + notFound.count + failed.count
    }

    /// Success rate (0.0 - 1.0)
    public var successRate: Double {
        guard totalRequested > 0 else { return 0 }
        return Double(items.count) / Double(totalRequested)
    }
}

// MARK: - BatchFetcher with Results

extension BatchFetcher {
    /// Fetch items with detailed results
    ///
    /// - Parameters:
    ///   - primaryKeys: The primary keys to fetch
    ///   - transaction: The transaction to use
    /// - Returns: Detailed fetch results
    public func fetchWithResults(
        primaryKeys: [Tuple],
        transaction: any TransactionProtocol
    ) async -> BatchFetchResult<Item> {
        guard !primaryKeys.isEmpty else {
            return BatchFetchResult(items: [], notFound: [], failed: [])
        }

        let itemTypeSubspace = itemSubspace.subspace(itemType)

        var items: [Item] = []
        var notFound: [Tuple] = []
        var failed: [(key: Tuple, error: Error)] = []

        for pk in primaryKeys {
            do {
                let key = itemTypeSubspace.pack(pk)
                if let data = try await transaction.getValue(for: key) {
                    let item: Item = try DataAccess.deserialize(Array(data))
                    items.append(item)
                } else {
                    notFound.append(pk)
                }
            } catch {
                if configuration.continueOnError {
                    failed.append((key: pk, error: error))
                } else {
                    // On first error, return what we have
                    failed.append((key: pk, error: error))
                    return BatchFetchResult(items: items, notFound: notFound, failed: failed)
                }
            }
        }

        return BatchFetchResult(items: items, notFound: notFound, failed: failed)
    }
}

// MARK: - Prefetching BatchFetcher

/// Batch fetcher with prefetching support
///
/// Prefetches the next batch while the current batch is being processed.
public final class PrefetchingBatchFetcher<Item: Persistable>: @unchecked Sendable {
    private let baseFetcher: BatchFetcher<Item>
    nonisolated(unsafe) private let database: any DatabaseProtocol

    private struct State: Sendable {
        var prefetchTask: Task<[Item], Error>?
        var prefetchKeys: [Tuple]?
    }

    private let state: Mutex<State>

    public init(
        database: any DatabaseProtocol,
        itemSubspace: Subspace,
        itemType: String,
        configuration: BatchFetchConfiguration = .default
    ) {
        self.database = database
        self.baseFetcher = BatchFetcher<Item>(
            itemSubspace: itemSubspace,
            itemType: itemType,
            configuration: configuration
        )
        self.state = Mutex(State())
    }

    /// Start prefetching a batch of keys
    ///
    /// - Parameter primaryKeys: Keys to prefetch
    public func prefetch(primaryKeys: [Tuple]) {
        guard baseFetcher.configuration.prefetchEnabled else { return }

        state.withLock { state in
            // Cancel any existing prefetch
            state.prefetchTask?.cancel()

            // Start new prefetch
            state.prefetchKeys = primaryKeys
            state.prefetchTask = Task {
                try await self.database.withTransaction { transaction in
                    try await self.baseFetcher.fetch(
                        primaryKeys: primaryKeys,
                        transaction: transaction
                    )
                }
            }
        }
    }

    /// Get prefetched results if available, otherwise fetch fresh
    ///
    /// - Parameters:
    ///   - primaryKeys: The keys to fetch
    ///   - transaction: The transaction to use
    /// - Returns: The fetched items
    public func fetchOrUsePrefetched(
        primaryKeys: [Tuple],
        transaction: any TransactionProtocol
    ) async throws -> [Item] {
        // Check if we have a matching prefetch
        let (task, keys) = state.withLock { state in
            (state.prefetchTask, state.prefetchKeys)
        }

        if let task = task, let keys = keys, keys == primaryKeys {
            // Clear the prefetch
            state.withLock { state in
                state.prefetchTask = nil
                state.prefetchKeys = nil
            }

            // Use prefetched results
            return try await task.value
        }

        // No matching prefetch, fetch fresh
        return try await baseFetcher.fetch(
            primaryKeys: primaryKeys,
            transaction: transaction
        )
    }

    /// Cancel any pending prefetch
    public func cancelPrefetch() {
        state.withLock { state in
            state.prefetchTask?.cancel()
            state.prefetchTask = nil
            state.prefetchKeys = nil
        }
    }
}

// MARK: - Batch Fetch Statistics

/// Statistics about batch fetch operations
public struct BatchFetchStatistics: Sendable {
    /// Total items fetched
    public var totalFetched: Int = 0

    /// Total batches processed
    public var batchCount: Int = 0

    /// Total keys not found
    public var notFoundCount: Int = 0

    /// Total fetch errors
    public var errorCount: Int = 0

    /// Total fetch duration in seconds
    public var totalDurationSeconds: Double = 0

    /// Average items per batch
    public var averageItemsPerBatch: Double {
        guard batchCount > 0 else { return 0 }
        return Double(totalFetched) / Double(batchCount)
    }

    /// Throughput in items per second
    public var throughputPerSecond: Double {
        guard totalDurationSeconds > 0 else { return 0 }
        return Double(totalFetched) / totalDurationSeconds
    }

    /// Record a batch result
    public mutating func recordBatch(items: Int, notFound: Int, errors: Int, durationSeconds: Double) {
        totalFetched += items
        batchCount += 1
        notFoundCount += notFound
        errorCount += errors
        totalDurationSeconds += durationSeconds
    }
}
