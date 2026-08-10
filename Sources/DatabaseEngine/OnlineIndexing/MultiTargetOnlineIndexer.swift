// MultiTargetOnlineIndexer.swift
// DatabaseEngine - Build multiple indexes simultaneously with single data scan
//
// Reference: FDB Record Layer multi-target indexing strategy

import DatabaseTypes
import StorageKit
import DatabaseKit
import Synchronization

/// Multi-target online index builder
///
/// Builds multiple indexes simultaneously with a single pass over the data,
/// significantly reducing I/O compared to building indexes sequentially.
///
/// **Features**:
/// - Single data scan for multiple indexes
/// - Shared progress tracking
/// - Atomic state transitions for all indexes
/// - Configurable parallelism within batches
///
/// **When to Use**:
/// - Schema migration adding multiple indexes
/// - Initial data load with many indexes
/// - Reducing total index build time
///
/// **Usage Example**:
/// ```swift
/// let indexer = try MultiTargetOnlineIndexer<User>(
///     container: container,
///     storeSubspace: storeSubspace,
///     itemType: "User",
///     targets: [
///         IndexBuildTarget(index: emailIndex, maintainer: emailMaintainer),
///         IndexBuildTarget(index: nameIndex, maintainer: nameMaintainer),
///     ],
///     lifecycleStore: lifecycleStore
/// )
///
/// try await indexer.buildIndexes(clearFirst: true)
/// ```
package final class MultiTargetOnlineIndexer<Item: Persistable>: Sendable {
    // MARK: - Properties

    /// FDB Container for database access
    private let container: DBContainer

    /// Subspace where items are stored ([R]/)
    private let itemSubspace: Subspace

    /// Subspace where index data is stored ([I]/)
    private let indexSubspace: Subspace

    /// Subspace where blob chunks are stored ([B]/)
    private let blobsSubspace: Subspace

    /// Item type name
    private let itemType: String

    /// Index build targets
    private let targets: [IndexBuildTarget<Item>]

    /// Index state manager
    private let lifecycleStore: IndexLifecycleStore

    /// Shared uniqueness violation persistence for all targets.
    private let violationTracker: UniquenessViolationTracker

    // Configuration
    private let batchSize: Int
    private let throttleDelayMs: Int

    // Progress tracking
    private let progressKey: ByteString

    // MARK: - Metrics

    private let itemsIndexedCounter: DatabaseMetricCounter
    private let batchesProcessedCounter: DatabaseMetricCounter
    private let batchDurationTimer: DatabaseMetricTimer
    private let errorsCounter: DatabaseMetricCounter

    // MARK: - Initialization

    /// Initialize multi-target indexer
    ///
    /// - Parameters:
    ///   - container: FDB Container instance
    ///   - storeSubspace: Root subspace containing items, indexes, blobs, and metadata
    ///   - itemType: Type name of items to index
    ///   - targets: Index build targets (index + maintainer pairs)
    ///   - lifecycleStore: Index state manager
    ///   - batchSize: Number of items per batch (default: 100)
    ///   - throttleDelayMs: Delay between batches in ms (default: 0)
    public init(
        container: DBContainer,
        storeSubspace: Subspace,
        itemType: String,
        targets: [IndexBuildTarget<Item>],
        lifecycleStore: IndexLifecycleStore,
        batchSize: Int = 100,
        throttleDelayMs: Int = 0
    ) throws(OnlineIndexBuildError) {
        guard !targets.isEmpty else {
            throw .emptyTargetSet
        }
        guard batchSize > 0 else {
            throw .invalidBatchSize(batchSize)
        }
        guard throttleDelayMs >= 0 else {
            throw .invalidThrottleDelayMilliseconds(throttleDelayMs)
        }
        var targetNames = Set<String>()
        for target in targets where !targetNames.insert(target.index.name).inserted {
            throw .duplicateTargetIndexName(target.index.name)
        }
        for target in targets where target.maintainer.customBuildStrategy != nil {
            throw .unsupportedCustomBuildStrategy(indexName: target.index.name)
        }
        for target in targets where target.index.isUnique {
            guard target.uniquenessMaintainer != nil else {
                throw .unsupportedUniquenessConstraint(
                    indexName: target.index.name
                )
            }
        }
        self.container = container
        self.itemSubspace = storeSubspace.subspace(SubspaceKey.items)
        self.indexSubspace = storeSubspace.subspace(SubspaceKey.indexes)
        self.blobsSubspace = storeSubspace.subspace(SubspaceKey.blobs)
        self.itemType = itemType
        self.targets = targets
        self.lifecycleStore = lifecycleStore
        self.violationTracker = UniquenessViolationTracker(
            container: container,
            metadataSubspace: storeSubspace.subspace(SubspaceKey.metadata)
        )
        self.batchSize = batchSize
        self.throttleDelayMs = throttleDelayMs

        // Create unique progress key for this multi-target build
        let indexNames = targets.map { $0.index.name }.sorted().joined(separator: "+")
        self.progressKey = self.indexSubspace
            .subspace("_progress_multi")
            .pack(Tuple(indexNames))

        // Initialize metrics
        let baseDimensions: [(String, String)] = [
            ("item_type", itemType),
            ("target_count", String(targets.count))
        ]

        let metrics = container.configuration.metrics
        self.itemsIndexedCounter = metrics.counter(
            label: "database_multi_indexer_items_indexed_total",
            dimensions: baseDimensions
        )
        self.batchesProcessedCounter = metrics.counter(
            label: "database_multi_indexer_batches_processed_total",
            dimensions: baseDimensions
        )
        self.batchDurationTimer = metrics.timer(
            label: "database_multi_indexer_batch_duration_seconds",
            dimensions: baseDimensions
        )
        self.errorsCounter = metrics.counter(
            label: "database_multi_indexer_errors_total",
            dimensions: baseDimensions
        )
    }

    // MARK: - Public API

    /// Build all target indexes with single data scan
    ///
    /// **Process**:
    /// 1. Set all indexes to write-only state
    /// 2. Clear index data if requested
    /// 3. Scan items in batches
    /// 4. For each item, call all maintainers
    /// 5. Transition all to readable state
    ///
    /// - Parameter clearFirst: If true, clears existing index data
    public func buildIndexes(clearFirst: Bool = false) async throws {
        // Set all indexes to write-only state
        for target in targets {
            try await lifecycleStore.enable(target.index.name)
        }

        // Clear if requested
        if clearFirst {
            for target in targets {
                try await clearIndexData(for: target.index)
                if target.index.isUnique {
                    try await violationTracker.clearAllViolations(
                        indexName: target.index.name
                    )
                }
            }
        }

        // Build indexes with single scan
        try await buildIndexesInBatches()

        for target in targets {
            try await container.transactionExecutor.withTransaction(
                configuration: .batch,
                clock: container.monotonicClock
            ) { transaction in
                try await target.maintainer.finalizeBuild(
                    transaction: transaction
                )
            }
        }

        try await requireNoUniquenessViolations()

        // Transition all to readable
        for target in targets {
            try await lifecycleStore.makeReadable(target.index.name)
        }

        // Clear progress
        try await clearProgress()
    }

    /// Get current progress status
    ///
    /// Returns true if indexing is complete (no remaining ranges)
    public func isComplete() async throws -> Bool {
        guard let rangeSet = try await loadProgress() else {
            return false
        }
        return rangeSet.isEmpty
    }

    // MARK: - Private Implementation

    /// Build all indexes in batches with single data scan
    ///
    /// **Batching Strategy**:
    /// - Each batch is processed in a separate transaction
    /// - Batch size is controlled by FDB's getRange limit parameter
    /// - Progress is saved after each successful batch commit
    /// - If a batch fails, we resume from the last saved progress
    private func buildIndexesInBatches() async throws {
        let itemTypeSubspace = itemSubspace.subspace(itemType)
        let totalRange = itemTypeSubspace.range()

        // Initialize or load progress
        var rangeSet: RangeSet
        if let savedProgress = try await loadProgress() {
            rangeSet = savedProgress
        } else {
            rangeSet = RangeSet(initialRange: totalRange)
        }

        // Process batches - each batch in a separate transaction
        while let bounds = rangeSet.nextBatchBounds() {
            let batchStartTime = container.monotonicClock.now

            do {
                // Capture current rangeSet state before transaction
                let currentRangeSet = rangeSet

                // Process batch and save progress atomically in same transaction
                let (itemsInBatch, lastProcessedKey) = try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
                    var itemsInBatch = 0
                    var lastProcessedKey: ByteString? = nil

                    // Use ItemStorage.scan() to handle ItemEnvelope format (inline/external)
                    let storage = self.container.itemStorageFactory.make(
                        transaction: transaction,
                        blobsSubspace: self.blobsSubspace
                    )

                    let scanSequence = storage.scan(
                        begin: bounds.begin,
                        end: bounds.end,
                        snapshot: false,
                        limit: self.batchSize
                    )

                    var batchEntries: [(item: Item, id: Tuple)] = []
                    batchEntries.reserveCapacity(self.batchSize)

                    var iterator = scanSequence.makeAsyncIterator()
                    while let (key, data) = try await iterator.next() {
                        // Deserialize item once from decompressed data
                        let item: Item = try DataAccess.deserialize(data)
                        let id = try itemTypeSubspace.unpack(key)

                        batchEntries.append((item: item, id: id))

                        lastProcessedKey = key
                        itemsInBatch += 1
                    }

                    // Call all maintainers once per batch. Maintainers that do
                    // not override scanItems preserve scanItem behavior.
                    for target in self.targets {
                        try await OnlineIndexBatchWriter.write(
                            batchEntries,
                            index: target.index,
                            maintainer: target.maintainer,
                            uniquenessMaintainer: target.uniquenessMaintainer,
                            violationTracker: self.violationTracker,
                            transaction: transaction
                        )
                    }

                    // Save progress atomically with work
                    // Create updated rangeSet copy inside transaction for saving
                    var updatedRangeSet = currentRangeSet
                    if let lastKey = lastProcessedKey {
                        let isComplete = itemsInBatch < self.batchSize
                        try updatedRangeSet.recordProgress(
                            rangeIndex: bounds.rangeIndex,
                            lastProcessedKey: lastKey,
                            isComplete: isComplete
                        )
                    } else {
                        try updatedRangeSet.markRangeComplete(rangeIndex: bounds.rangeIndex)
                    }
                    try self.saveProgress(updatedRangeSet, transaction)

                    return (itemsInBatch, lastProcessedKey)
                }

                // Update in-memory rangeSet after successful commit
                if let lastKey = lastProcessedKey {
                    let isComplete = itemsInBatch < self.batchSize
                    try rangeSet.recordProgress(
                        rangeIndex: bounds.rangeIndex,
                        lastProcessedKey: lastKey,
                        isComplete: isComplete
                    )
                } else {
                    try rangeSet.markRangeComplete(rangeIndex: bounds.rangeIndex)
                }

                // Record metrics
                let batchDuration = DatabaseMonotonicMeasurement.nanoseconds(
                    from: batchStartTime,
                    to: container.monotonicClock.now
                )
                batchDurationTimer.recordNanoseconds(batchDuration)
                batchesProcessedCounter.increment()
                itemsIndexedCounter.increment(by: itemsInBatch * targets.count)

            } catch {
                errorsCounter.increment()
                throw error
            }

            // Throttle if configured
            if throttleDelayMs > 0 {
                try await container.monotonicClock.sleep(
                    for: .milliseconds(Int64(throttleDelayMs))
                )
            }
        }
    }

    // MARK: - Progress Management

    private func loadProgress() async throws -> RangeSet? {
        let progressKey = self.progressKey
        return try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            guard let bytes = try await transaction.getValue(for: progressKey, snapshot: false) else {
                return nil
            }
            return try RangeSetCodec.decode(bytes)
        }
    }

    private func saveProgress(_ rangeSet: RangeSet, _ transaction: any TransactionAccess) throws {
        try transaction.setValue(try RangeSetCodec.encode(rangeSet), for: progressKey)
    }

    private func clearProgress() async throws {
        let progressKey = self.progressKey
        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try transaction.clear(key: progressKey)
        }
    }

    // MARK: - Index Data Management

    private func clearIndexData(for index: Index) async throws {
        let indexRange = self.indexSubspace.subspace(index.name).range()
        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try transaction.clearRange(beginKey: indexRange.begin, endKey: indexRange.end)
        }
    }

    private func requireNoUniquenessViolations() async throws {
        for target in targets where target.index.isUnique {
            let summary = try await violationTracker.violationSummary(
                indexName: target.index.name
            )
            guard !summary.hasViolations else {
                throw OnlineIndexBuildError.uniquenessViolationsDetected(
                    indexName: target.index.name,
                    violationCount: summary.violationCount,
                    totalConflictingEntities: summary.totalConflictingEntities
                )
            }
        }
    }
}

// MARK: - Index Build Target

/// A target for index building
public struct IndexBuildTarget<Item: Persistable>: Sendable {
    /// The index to build
    public let index: Index

    /// The maintainer for this index
    public let maintainer: any IndexMaintainer<Item>

    /// The conflict lookup used when `index` is unique.
    public let uniquenessMaintainer: (any IndexUniquenessMaintainer<Item>)?

    public init(
        index: Index,
        maintainer: any IndexMaintainer<Item>,
        uniquenessMaintainer: (any IndexUniquenessMaintainer<Item>)? = nil
    ) {
        self.index = index
        self.maintainer = maintainer
        self.uniquenessMaintainer = uniquenessMaintainer
    }
}

// MARK: - CustomStringConvertible

extension MultiTargetOnlineIndexer: CustomStringConvertible {
    public var description: String {
        let indexNames = targets.map { $0.index.name }.joined(separator: ", ")
        return "MultiTargetOnlineIndexer(indexes: [\(indexNames)], itemType: \(itemType))"
    }
}
