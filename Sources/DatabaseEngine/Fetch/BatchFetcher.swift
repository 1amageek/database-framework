// BatchFetcher.swift
// DatabaseEngine - Optimized batch fetching for entities from indexes
//
// Reference: FDB Record Layer Remote Fetch optimization
// Efficiently fetches multiple entities by batching primary key lookups.

import DatabaseTypes
import StorageKit
import DatabaseKit

// MARK: - BatchFetchConfiguration

/// Configuration for batch fetching
public struct BatchFetchConfiguration: Sendable, Equatable {
    /// Maximum number of entities to fetch in a single batch
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

/// Optimized batch fetcher for entities
///
/// Efficiently fetches multiple entities by:
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

    /// Blobs subspace for large value storage
    private let blobsSubspace: Subspace

    /// Item type name
    private let itemType: String

    /// Container-scoped canonical entity storage policy
    private let itemStorageFactory: ItemStorageFactory

    // MARK: - Initialization

    public init(
        itemSubspace: Subspace,
        blobsSubspace: Subspace,
        itemType: String,
        itemStorageFactory: ItemStorageFactory,
        configuration: BatchFetchConfiguration = .default
    ) {
        self.itemSubspace = itemSubspace
        self.blobsSubspace = blobsSubspace
        self.itemType = itemType
        self.itemStorageFactory = itemStorageFactory
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
        transaction: any TransactionReadAccess
    ) async throws -> [Item] {
        guard !primaryKeys.isEmpty else { return [] }

        let itemTypeSubspace = itemSubspace.subspace(itemType)
        let storage = itemStorageFactory.makeReader(
            transaction: transaction,
            blobsSubspace: blobsSubspace
        )

        // All reads are sequential within a single transaction
        // FDB transactions are NOT thread-safe for concurrent access
        var results: [Item] = []
        results.reserveCapacity(primaryKeys.count)

        for pk in primaryKeys {
            let key = itemTypeSubspace.pack(pk)
            if let data = try await storage.read(for: key) {
                let item: Item = try DataAccess.deserialize(data)
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
        storage: ItemStorageReader
    ) async throws -> [Item] {
        var results: [Item] = []
        results.reserveCapacity(primaryKeys.count)

        // Sequential reads only - FDB transactions are NOT thread-safe
        for pk in primaryKeys {
            let key = subspace.pack(pk)
            if let data = try await storage.read(for: key) {
                let item: Item = try DataAccess.deserialize(data)
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
    /// - Returns: A throwing stream of fetched items
    public func stream<S: AsyncSequence>(
        primaryKeys: S,
        transaction: any TransactionReadAccess
    ) -> AsyncThrowingStream<Item, Error> where S.Element == Tuple, S: Sendable {
        AsyncThrowingStream { continuation in
            let task = Task {
                var batch: [Tuple] = []
                batch.reserveCapacity(configuration.batchSize)

                let itemTypeSubspace = itemSubspace.subspace(itemType)
                let storage = itemStorageFactory.makeReader(
                    transaction: transaction,
                    blobsSubspace: blobsSubspace
                )

                do {
                    var primaryKeyIterator = primaryKeys.makeAsyncIterator()
                    while let pk = try await primaryKeyIterator.next() {
                        try Task.checkCancellation()
                        batch.append(pk)

                        if batch.count >= configuration.batchSize {
                            let items = try await fetchBatch(
                                primaryKeys: batch,
                                subspace: itemTypeSubspace,
                                storage: storage
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
                            storage: storage
                        )
                        for item in items {
                            continuation.yield(item)
                        }
                    }

                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
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
    /// - Returns: A throwing stream of fetched items
    public func streamFromIndex<S: AsyncSequence>(
        indexEntries: S,
        indexSubspace: Subspace,
        transaction: any TransactionReadAccess
    ) -> AsyncThrowingStream<Item, Error>
    where S.Element == (key: ByteString, value: ByteString), S: Sendable {
        AsyncThrowingStream { continuation in
            let task = Task {
                var batch: [Tuple] = []
                batch.reserveCapacity(configuration.batchSize)

                let itemTypeSubspace = itemSubspace.subspace(itemType)
                let storage = itemStorageFactory.makeReader(
                    transaction: transaction,
                    blobsSubspace: blobsSubspace
                )

                do {
                    var indexEntryIterator = indexEntries.makeAsyncIterator()
                    while let (key, _) = try await indexEntryIterator.next() {
                        try Task.checkCancellation()
                        // Extract primary key from index entry
                        if let pk = try extractPrimaryKey(
                            from: key,
                            indexSubspace: indexSubspace
                        ) {
                            batch.append(pk)

                            if batch.count >= configuration.batchSize {
                                let items = try await fetchBatch(
                                    primaryKeys: batch,
                                    subspace: itemTypeSubspace,
                                    storage: storage
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
                            storage: storage
                        )
                        for item in items {
                            continuation.yield(item)
                        }
                    }

                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    // MARK: - Primary Key Extraction

    /// Extract primary key from an index key
    ///
    /// Assumes the primary key is the last element of the index key tuple.
    private func extractPrimaryKey(from key: ByteString, indexSubspace: Subspace) throws -> Tuple? {
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
        transaction: any TransactionReadAccess
    ) async -> BatchFetchResult<Item> {
        guard !primaryKeys.isEmpty else {
            return BatchFetchResult(items: [], notFound: [], failed: [])
        }

        let itemTypeSubspace = itemSubspace.subspace(itemType)
        let storage = itemStorageFactory.makeReader(
            transaction: transaction,
            blobsSubspace: blobsSubspace
        )

        var items: [Item] = []
        var notFound: [Tuple] = []
        var failed: [(key: Tuple, error: Error)] = []

        for pk in primaryKeys {
            do {
                let key = itemTypeSubspace.pack(pk)
                if let data = try await storage.read(for: key) {
                    let item: Item = try DataAccess.deserialize(data)
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
public actor PrefetchingBatchFetcher<Item: Persistable> {
    private struct TrackedPrefetch: Sendable {
        let identifier: UInt64
        let keys: [Tuple]
        let task: Task<[Item], Error>
    }

    private let baseFetcher: BatchFetcher<Item>
    private let context: DatabaseContext
    private var nextIdentifier: UInt64 = 1
    private var tracked: [UInt64: TrackedPrefetch] = [:]
    private var currentIdentifier: UInt64?
    private var isShutdown = false

    public init(
        context: DatabaseContext,
        itemSubspace: Subspace,
        blobsSubspace: Subspace,
        itemType: String,
        configuration: BatchFetchConfiguration = .default
    ) {
        self.context = context
        self.baseFetcher = BatchFetcher<Item>(
            itemSubspace: itemSubspace,
            blobsSubspace: blobsSubspace,
            itemType: itemType,
            itemStorageFactory: context.container.itemStorageFactory,
            configuration: configuration
        )
    }

    /// Start prefetching a batch of keys
    ///
    /// - Parameter primaryKeys: Keys to prefetch
    public func prefetch(primaryKeys: [Tuple]) async throws {
        guard !isShutdown else {
            throw PrefetchingBatchFetcherError.closed
        }
        guard baseFetcher.configuration.prefetchEnabled else { return }

        let previousIdentifier = currentIdentifier
        if let previousIdentifier,
           let previous = tracked[previousIdentifier] {
            previous.task.cancel()
        }

        let identifier = try allocateIdentifier()
        let context = self.context
        let baseFetcher = self.baseFetcher
        // A detached task intentionally does not inherit an active
        // transaction TaskLocal. The context must independently re-admit
        // this read to its configured data root before any key is read.
        let task = Task.detached { () throws -> [Item] in
            try await context.executeCanonicalRead(
                configuration: .batch
            ) { transaction in
                try await baseFetcher.fetch(
                    primaryKeys: primaryKeys,
                    transaction: transaction
                )
            }
        }
        tracked[identifier] = TrackedPrefetch(
            identifier: identifier,
            keys: primaryKeys,
            task: task
        )
        currentIdentifier = identifier

        if let previousIdentifier,
           let previous = tracked[previousIdentifier] {
            await drainRetired(previous.task)
            tracked.removeValue(forKey: previousIdentifier)
            guard !isShutdown else {
                throw PrefetchingBatchFetcherError.closed
            }
        }
    }

    /// Fetches through the caller-owned transaction.
    ///
    /// Independently prefetched values are never substituted here: doing so
    /// would lose read-your-writes and conflict tracking from this transaction.
    public func fetch(
        primaryKeys: [Tuple],
        transaction: any TransactionReadAccess
    ) async throws -> [Item] {
        guard !isShutdown else {
            throw PrefetchingBatchFetcherError.closed
        }
        return try await baseFetcher.fetch(
            primaryKeys: primaryKeys,
            transaction: transaction
        )
    }

    /// Consumes an explicitly nontransactional speculative read.
    ///
    /// If no matching prefetch exists, this method performs another independent
    /// canonical read. Callers requiring transaction consistency must use
    /// `fetch(primaryKeys:transaction:)` instead.
    public func consumePrefetched(
        primaryKeys: [Tuple]
    ) async throws -> [Item] {
        guard !isShutdown else {
            throw PrefetchingBatchFetcherError.closed
        }
        if let identifier = currentIdentifier,
           let current = tracked[identifier],
           current.keys == primaryKeys {
            currentIdentifier = nil
            do {
                let value = try await current.task.value
                tracked.removeValue(forKey: identifier)
                return value
            } catch {
                tracked.removeValue(forKey: identifier)
                throw error
            }
        }

        return try await context.executeCanonicalRead(
            configuration: .batch
        ) { transaction in
            try await self.baseFetcher.fetch(
                primaryKeys: primaryKeys,
                transaction: transaction
            )
        }
    }

    /// Cancels and drains every pending or replaced prefetch task.
    ///
    /// Owners must call this method before releasing the fetcher so no
    /// operation-owned read continues beyond the fetcher's lifecycle.
    public func shutdown() async throws {
        guard !isShutdown else { return }
        isShutdown = true
        currentIdentifier = nil
        let pending = tracked
        pending.values.forEach { $0.task.cancel() }
        var firstFailure: (any Error)?
        for prefetch in pending.values {
            do {
                _ = try await prefetch.task.value
            } catch is CancellationError {
                // Cancellation is the expected shutdown terminal state.
            } catch {
                if firstFailure == nil { firstFailure = error }
            }
            tracked.removeValue(forKey: prefetch.identifier)
        }
        if let firstFailure { throw firstFailure }
    }

    private func drainRetired(_ task: Task<[Item], Error>) async {
        do {
            _ = try await task.value
        } catch {
            // Replacement intentionally discards a speculative result. The
            // task is still drained before its tracking entry is removed.
        }
    }

    private func allocateIdentifier() throws -> UInt64 {
        let identifier = nextIdentifier
        let next = nextIdentifier.addingReportingOverflow(1)
        guard !next.overflow else {
            throw PrefetchingBatchFetcherError.identifierExhausted
        }
        nextIdentifier = next.partialValue
        return identifier
    }
}

public enum PrefetchingBatchFetcherError: Error, Sendable, Equatable {
    case closed
    case identifierExhausted
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
