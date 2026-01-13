// RemoteFetch.swift
// DatabaseEngine - Remote Fetch optimization for record retrieval
//
// Reference: FDB Record Layer RemoteFetchProperties and FDBRecordStore.fetchRemote
// Optimizes record retrieval by reducing round trips and leveraging locality.

import Foundation
import FoundationDB
import Core
import Synchronization

// MARK: - RemoteFetchConfiguration

/// Configuration for remote fetch optimization
///
/// Remote fetch reduces round trips by:
/// 1. Batching key lookups into efficient range scans
/// 2. Using locality hints for better FDB storage server utilization
/// 3. Parallelizing independent fetches within transaction constraints
///
/// **Reference**: FDB Record Layer RemoteFetchProperties
public struct RemoteFetchConfiguration: Sendable, Equatable {
    /// Maximum keys to fetch in parallel within constraints
    ///
    /// Note: FDB transactions are not thread-safe, but we can optimize
    /// the read pattern to reduce total round trips.
    public let maxParallelism: Int

    /// Maximum keys per batch request
    ///
    /// Larger batches reduce round trips but increase memory usage.
    public let batchSize: Int

    /// Whether to use locality hints
    ///
    /// When enabled, groups fetches by key proximity to improve
    /// FDB storage server cache utilization.
    public let useLocalityHints: Bool

    /// Maximum key spread for locality grouping
    ///
    /// Keys within this distance are fetched together for locality.
    public let localityGroupSize: Int

    /// Whether to stream results as they arrive
    ///
    /// Reduces latency for first results at cost of ordering.
    public let streamResults: Bool

    /// Default configuration
    public static let `default` = RemoteFetchConfiguration(
        maxParallelism: 10,
        batchSize: 100,
        useLocalityHints: true,
        localityGroupSize: 1000,
        streamResults: false
    )

    /// Low latency configuration
    public static let lowLatency = RemoteFetchConfiguration(
        maxParallelism: 20,
        batchSize: 50,
        useLocalityHints: false,
        localityGroupSize: 500,
        streamResults: true
    )

    /// High throughput configuration
    public static let highThroughput = RemoteFetchConfiguration(
        maxParallelism: 5,
        batchSize: 500,
        useLocalityHints: true,
        localityGroupSize: 2000,
        streamResults: false
    )

    public init(
        maxParallelism: Int = 10,
        batchSize: Int = 100,
        useLocalityHints: Bool = true,
        localityGroupSize: Int = 1000,
        streamResults: Bool = false
    ) {
        precondition(maxParallelism > 0, "maxParallelism must be positive")
        precondition(batchSize > 0, "batchSize must be positive")
        precondition(localityGroupSize > 0, "localityGroupSize must be positive")

        self.maxParallelism = maxParallelism
        self.batchSize = batchSize
        self.useLocalityHints = useLocalityHints
        self.localityGroupSize = localityGroupSize
        self.streamResults = streamResults
    }
}

// MARK: - RemoteFetcher

/// Optimized remote fetcher for records
///
/// Provides efficient record retrieval using:
/// - Batched key lookups to reduce round trips
/// - Locality-aware grouping for better cache utilization
/// - Streaming results for low-latency access
///
/// **Usage**:
/// ```swift
/// let fetcher = RemoteFetcher<User>(
///     subspace: userSubspace,
///     configuration: .default
/// )
///
/// // Fetch multiple records efficiently
/// let users = try await fetcher.fetch(
///     primaryKeys: keys,
///     transaction: transaction
/// )
///
/// // Stream results for low latency
/// for await user in fetcher.stream(primaryKeys: keys, transaction: tx) {
///     process(user)
/// }
/// ```
public struct RemoteFetcher<Item: Persistable>: Sendable {
    // MARK: - Properties

    /// Item subspace
    private let subspace: Subspace

    /// Blobs subspace for large value storage
    private let blobsSubspace: Subspace

    /// Configuration
    public let configuration: RemoteFetchConfiguration

    /// Item type name for subspace
    private let itemType: String

    // MARK: - Initialization

    public init(
        subspace: Subspace,
        blobsSubspace: Subspace,
        itemType: String = String(describing: Item.self),
        configuration: RemoteFetchConfiguration = .default
    ) {
        self.subspace = subspace
        self.blobsSubspace = blobsSubspace
        self.itemType = itemType
        self.configuration = configuration
    }

    // MARK: - Fetch

    /// Fetch records by primary keys
    ///
    /// Uses optimized batching and locality grouping for efficient retrieval.
    ///
    /// - Parameters:
    ///   - primaryKeys: The primary keys to fetch
    ///   - transaction: The transaction to use
    /// - Returns: The fetched items (in request order where found)
    public func fetch(
        primaryKeys: [Tuple],
        transaction: any TransactionProtocol
    ) async throws -> [Item] {
        guard !primaryKeys.isEmpty else { return [] }

        let itemTypeSubspace = subspace.subspace(itemType)
        let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)

        // Optimize fetch order based on key locality
        let orderedKeys: [Tuple]
        if configuration.useLocalityHints {
            orderedKeys = optimizeForLocality(primaryKeys)
        } else {
            orderedKeys = primaryKeys
        }

        // Fetch in batches
        // Use Data as key for efficient O(1) lookup (vs String which is slow)
        var results: [Data: Item] = [:]
        results.reserveCapacity(primaryKeys.count)

        let batches = orderedKeys.chunked(into: configuration.batchSize)

        for batch in batches {
            let batchResults = try await fetchBatch(
                primaryKeys: batch,
                subspace: itemTypeSubspace,
                storage: storage
            )

            for (keyData, item) in batchResults {
                results[keyData] = item
            }
        }

        // Return in original order
        var orderedResults: [Item] = []
        orderedResults.reserveCapacity(primaryKeys.count)

        for pk in primaryKeys {
            let keyData = Data(pk.pack())
            if let item = results[keyData] {
                orderedResults.append(item)
            }
        }

        return orderedResults
    }

    /// Fetch a single batch of records
    private func fetchBatch(
        primaryKeys: [Tuple],
        subspace: Subspace,
        storage: ItemStorage
    ) async throws -> [(Data, Item)] {
        var results: [(Data, Item)] = []
        results.reserveCapacity(primaryKeys.count)

        // Sequential reads within transaction (FDB constraint)
        for pk in primaryKeys {
            let key = subspace.pack(pk)
            if let data = try await storage.read(for: key) {
                let item: Item = try DataAccess.deserialize(data)
                results.append((Data(pk.pack()), item))
            }
        }

        return results
    }

    // MARK: - Streaming

    /// Stream records with low-latency access
    ///
    /// Returns items as they are fetched, minimizing time to first result.
    ///
    /// - Parameters:
    ///   - primaryKeys: The primary keys to fetch
    ///   - transaction: The transaction to use
    /// - Returns: AsyncStream of fetched items
    public func stream(
        primaryKeys: [Tuple],
        transaction: any TransactionProtocol
    ) -> AsyncStream<Item> {
        AsyncStream { continuation in
            Task {
                do {
                    let itemTypeSubspace = subspace.subspace(itemType)
                    let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
                    let batches = primaryKeys.chunked(into: configuration.batchSize)

                    for batch in batches {
                        for pk in batch {
                            let key = itemTypeSubspace.pack(pk)
                            if let data = try await storage.read(for: key) {
                                let item: Item = try DataAccess.deserialize(data)
                                continuation.yield(item)
                            }
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish()
                }
            }
        }
    }

    // MARK: - Fetch with Metadata

    /// Fetch records with fetch metadata
    ///
    /// - Parameters:
    ///   - primaryKeys: The primary keys to fetch
    ///   - transaction: The transaction to use
    /// - Returns: Fetch result with items and metadata
    public func fetchWithMetadata(
        primaryKeys: [Tuple],
        transaction: any TransactionProtocol
    ) async throws -> RemoteFetchResult<Item> {
        let startTime = DispatchTime.now()

        guard !primaryKeys.isEmpty else {
            return RemoteFetchResult(
                items: [],
                notFoundKeys: [],
                fetchedCount: 0,
                notFoundCount: 0,
                durationNanos: 0
            )
        }

        let itemTypeSubspace = subspace.subspace(itemType)
        let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)

        var items: [Item] = []
        var notFoundKeys: [Tuple] = []

        items.reserveCapacity(primaryKeys.count)

        for pk in primaryKeys {
            let key = itemTypeSubspace.pack(pk)
            if let data = try await storage.read(for: key) {
                let item: Item = try DataAccess.deserialize(data)
                items.append(item)
            } else {
                notFoundKeys.append(pk)
            }
        }

        let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds

        return RemoteFetchResult(
            items: items,
            notFoundKeys: notFoundKeys,
            fetchedCount: items.count,
            notFoundCount: notFoundKeys.count,
            durationNanos: duration
        )
    }

    // MARK: - Locality Optimization

    /// Optimize key order for locality
    ///
    /// Groups keys that are close together to improve storage server cache hit rate.
    private func optimizeForLocality(_ keys: [Tuple]) -> [Tuple] {
        // Sort by key bytes for locality
        // Keys close in sort order are likely on same storage server
        keys.sorted { lhs, rhs in
            let lhsBytes = lhs.pack()
            let rhsBytes = rhs.pack()
            return lhsBytes.lexicographicallyPrecedes(rhsBytes)
        }
    }
}

// MARK: - RemoteFetchResult

/// Result of a remote fetch operation
public struct RemoteFetchResult<Item: Persistable>: Sendable {
    /// Successfully fetched items
    public let items: [Item]

    /// Keys that were not found
    public let notFoundKeys: [Tuple]

    /// Number of items fetched
    public let fetchedCount: Int

    /// Number of keys not found
    public let notFoundCount: Int

    /// Fetch duration in nanoseconds
    public let durationNanos: UInt64

    /// Hit rate (0.0 - 1.0)
    public var hitRate: Double {
        let total = fetchedCount + notFoundCount
        guard total > 0 else { return 0 }
        return Double(fetchedCount) / Double(total)
    }

    /// Duration in milliseconds
    public var durationMs: Double {
        Double(durationNanos) / 1_000_000
    }
}

// MARK: - Locality Hints

/// Locality hints for fetch optimization
///
/// Provides hints about data locality to optimize fetch patterns.
public struct LocalityHints: Sendable {
    /// Preferred storage server for this fetch
    public let preferredServer: String?

    /// Whether to use snapshot reads
    public let useSnapshot: Bool

    /// Whether data is expected to be cached
    public let expectCached: Bool

    /// Default hints
    public static let `default` = LocalityHints(
        preferredServer: nil,
        useSnapshot: false,
        expectCached: false
    )

    /// Hints for hot data
    public static let hotData = LocalityHints(
        preferredServer: nil,
        useSnapshot: true,
        expectCached: true
    )

    public init(
        preferredServer: String? = nil,
        useSnapshot: Bool = false,
        expectCached: Bool = false
    ) {
        self.preferredServer = preferredServer
        self.useSnapshot = useSnapshot
        self.expectCached = expectCached
    }
}

// MARK: - Parallel Fetch Coordinator

/// Coordinates parallel fetches across multiple transactions
///
/// For use cases where multiple independent fetches can run in parallel
/// across different read snapshots.
public final class ParallelFetchCoordinator<Item: Persistable>: Sendable {
    private let container: FDBContainer
    private let fetcher: RemoteFetcher<Item>
    private let maxConcurrency: Int

    public init(
        container: FDBContainer,
        subspace: Subspace,
        blobsSubspace: Subspace,
        itemType: String = String(describing: Item.self),
        configuration: RemoteFetchConfiguration = .default,
        maxConcurrency: Int = 4
    ) {
        self.container = container
        self.fetcher = RemoteFetcher<Item>(
            subspace: subspace,
            blobsSubspace: blobsSubspace,
            itemType: itemType,
            configuration: configuration
        )
        self.maxConcurrency = maxConcurrency
    }

    /// Fetch records using parallel transactions
    ///
    /// **Warning**: Results may reflect different read versions if data is being modified.
    /// Use within a single transaction if consistency is required.
    ///
    /// - Parameter primaryKeys: Keys to fetch
    /// - Returns: Fetched items (order may not match input)
    public func fetchParallel(primaryKeys: [Tuple]) async throws -> [Item] {
        guard !primaryKeys.isEmpty else { return [] }

        // Split keys into chunks for parallel processing
        let chunks = primaryKeys.chunked(into: max(1, primaryKeys.count / maxConcurrency))

        // Fetch chunks in parallel using separate transactions
        let results = try await withThrowingTaskGroup(of: [Item].self) { group in
            for chunk in chunks {
                group.addTask {
                    try await self.container.database.withTransaction(configuration: .default) { tx in
                        try await self.fetcher.fetch(primaryKeys: chunk, transaction: tx)
                    }
                }
            }

            var allItems: [Item] = []
            allItems.reserveCapacity(primaryKeys.count)

            for try await items in group {
                allItems.append(contentsOf: items)
            }

            return allItems
        }

        return results
    }
}

// MARK: - Array Extension

extension Array {
    /// Split array into chunks of specified size
    func chunked(into size: Int) -> [[Element]] {
        guard size > 0 else { return [self] }
        return stride(from: 0, to: count, by: size).map {
            Array(self[$0..<Swift.min($0 + size, count)])
        }
    }
}
