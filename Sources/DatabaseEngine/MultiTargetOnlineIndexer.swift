// MultiTargetOnlineIndexer.swift
// DatabaseEngine - Build multiple indexes simultaneously with single data scan
//
// Reference: FDB Record Layer multi-target indexing strategy

import Foundation
import FoundationDB
import Core
import Metrics
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
/// let indexer = MultiTargetOnlineIndexer<User>(
///     database: database,
///     itemSubspace: itemSubspace,
///     indexSubspace: indexSubspace,
///     itemType: "User",
///     targets: [
///         IndexBuildTarget(index: emailIndex, maintainer: emailMaintainer),
///         IndexBuildTarget(index: nameIndex, maintainer: nameMaintainer),
///     ],
///     stateManager: stateManager
/// )
///
/// try await indexer.buildIndexes(clearFirst: true)
/// ```
public final class MultiTargetOnlineIndexer<Item: Persistable>: Sendable {
    // MARK: - Properties

    /// Database instance
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Subspace where items are stored ([R]/)
    private let itemSubspace: Subspace

    /// Subspace where index data is stored ([I]/)
    private let indexSubspace: Subspace

    /// Item type name
    private let itemType: String

    /// Index build targets
    private let targets: [IndexBuildTarget<Item>]

    /// Index state manager
    private let stateManager: IndexStateManager

    // Configuration
    private let batchSize: Int
    private let throttleDelayMs: Int

    // Progress tracking
    private let progressKey: FDB.Bytes

    // MARK: - Metrics

    private let itemsIndexedCounter: Counter
    private let batchesProcessedCounter: Counter
    private let batchDurationTimer: Metrics.Timer
    private let errorsCounter: Counter

    // MARK: - Initialization

    /// Initialize multi-target indexer
    ///
    /// - Parameters:
    ///   - database: Database instance
    ///   - itemSubspace: Subspace where items are stored
    ///   - indexSubspace: Subspace where index data is stored
    ///   - itemType: Type name of items to index
    ///   - targets: Index build targets (index + maintainer pairs)
    ///   - stateManager: Index state manager
    ///   - batchSize: Number of items per batch (default: 100)
    ///   - throttleDelayMs: Delay between batches in ms (default: 0)
    public init(
        database: any DatabaseProtocol,
        itemSubspace: Subspace,
        indexSubspace: Subspace,
        itemType: String,
        targets: [IndexBuildTarget<Item>],
        stateManager: IndexStateManager,
        batchSize: Int = 100,
        throttleDelayMs: Int = 0
    ) {
        self.database = database
        self.itemSubspace = itemSubspace
        self.indexSubspace = indexSubspace
        self.itemType = itemType
        self.targets = targets
        self.stateManager = stateManager
        self.batchSize = batchSize
        self.throttleDelayMs = throttleDelayMs

        // Create unique progress key for this multi-target build
        let indexNames = targets.map { $0.index.name }.sorted().joined(separator: "+")
        self.progressKey = indexSubspace
            .subspace("_progress_multi")
            .pack(Tuple(indexNames))

        // Initialize metrics
        let baseDimensions: [(String, String)] = [
            ("item_type", itemType),
            ("target_count", String(targets.count))
        ]

        self.itemsIndexedCounter = Counter(
            label: "fdb_multi_indexer_items_indexed_total",
            dimensions: baseDimensions
        )
        self.batchesProcessedCounter = Counter(
            label: "fdb_multi_indexer_batches_processed_total",
            dimensions: baseDimensions
        )
        self.batchDurationTimer = Metrics.Timer(
            label: "fdb_multi_indexer_batch_duration_seconds",
            dimensions: baseDimensions
        )
        self.errorsCounter = Counter(
            label: "fdb_multi_indexer_errors_total",
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
            try await stateManager.enable(target.index.name)
        }

        // Clear if requested
        if clearFirst {
            for target in targets {
                try await clearIndexData(for: target.index)
            }
        }

        // Build indexes with single scan
        try await buildIndexesInBatches()

        // Transition all to readable
        for target in targets {
            try await stateManager.makeReadable(target.index.name)
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
            let batchStartTime = DispatchTime.now()

            do {
                let (itemsInBatch, lastProcessedKey) = try await database.withTransaction(configuration: .batch) { transaction in
                    var itemsInBatch = 0
                    var lastProcessedKey: FDB.Bytes? = nil

                    let sequence = transaction.getRange(
                        beginSelector: .firstGreaterOrEqual(bounds.begin),
                        endSelector: .firstGreaterOrEqual(bounds.end),
                        snapshot: false
                    )

                    // Process up to batchSize items, then break
                    // This limits transaction size without requiring FDB limit parameter
                    for try await (key, value) in sequence {
                        // Deserialize item once
                        let item: Item = try DataAccess.deserialize(value)
                        let id = try itemTypeSubspace.unpack(key)

                        // Call all maintainers for this item
                        for target in self.targets {
                            try await target.maintainer.scanItem(
                                item,
                                id: id,
                                transaction: transaction
                            )
                        }

                        lastProcessedKey = Array(key)
                        itemsInBatch += 1

                        // Break after batchSize items to limit transaction size
                        if itemsInBatch >= self.batchSize {
                            break
                        }
                    }

                    return (itemsInBatch, lastProcessedKey)
                }

                // Record progress outside transaction
                if let lastKey = lastProcessedKey {
                    // If we got fewer items than batchSize, the range is complete
                    let isComplete = itemsInBatch < self.batchSize
                    rangeSet.recordProgress(
                        rangeIndex: bounds.rangeIndex,
                        lastProcessedKey: lastKey,
                        isComplete: isComplete
                    )
                } else {
                    // No items in range - mark as complete
                    rangeSet.markRangeComplete(rangeIndex: bounds.rangeIndex)
                }

                // Save progress in separate transaction
                let rangeSetCopy = rangeSet
                try await database.withTransaction(configuration: .batch) { transaction in
                    try self.saveProgress(rangeSetCopy, transaction)
                }

                // Record metrics
                let batchDuration = DispatchTime.now().uptimeNanoseconds - batchStartTime.uptimeNanoseconds
                batchDurationTimer.recordNanoseconds(Int64(batchDuration))
                batchesProcessedCounter.increment()
                itemsIndexedCounter.increment(by: itemsInBatch * targets.count)

            } catch {
                errorsCounter.increment()
                throw error
            }

            // Throttle if configured
            if throttleDelayMs > 0 {
                try await Task.sleep(nanoseconds: UInt64(throttleDelayMs) * 1_000_000)
            }
        }
    }

    // MARK: - Progress Management

    private func loadProgress() async throws -> RangeSet? {
        try await database.withTransaction(configuration: .batch) { transaction in
            guard let bytes = try await transaction.getValue(for: progressKey, snapshot: false) else {
                return nil
            }
            return try JSONDecoder().decode(RangeSet.self, from: Data(bytes))
        }
    }

    private func saveProgress(_ rangeSet: RangeSet, _ transaction: any TransactionProtocol) throws {
        let data = try JSONEncoder().encode(rangeSet)
        transaction.setValue(Array(data), for: progressKey)
    }

    private func clearProgress() async throws {
        try await database.withTransaction(configuration: .batch) { transaction in
            transaction.clear(key: progressKey)
        }
    }

    // MARK: - Index Data Management

    private func clearIndexData(for index: Index) async throws {
        try await database.withTransaction(configuration: .batch) { transaction in
            let indexRange = indexSubspace.subspace(index.name).range()
            transaction.clearRange(beginKey: indexRange.begin, endKey: indexRange.end)
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

    public init(index: Index, maintainer: any IndexMaintainer<Item>) {
        self.index = index
        self.maintainer = maintainer
    }
}

// MARK: - CustomStringConvertible

extension MultiTargetOnlineIndexer: CustomStringConvertible {
    public var description: String {
        let indexNames = targets.map { $0.index.name }.joined(separator: ", ")
        return "MultiTargetOnlineIndexer(indexes: [\(indexNames)], itemType: \(itemType))"
    }
}
