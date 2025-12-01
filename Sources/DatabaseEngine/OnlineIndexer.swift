import Foundation
import FoundationDB
import Core
import Metrics

/// Online index builder for batch index construction
///
/// OnlineIndexer provides infrastructure for building indexes in batches with
/// progress tracking and resumability. It supports both standard scan-based builds
/// (for most index types) and custom build strategies (e.g., HNSW bulk construction).
///
/// **Features**:
/// - Batch processing with configurable batch size
/// - Progress tracking via RangeSet (resumable after interruption)
/// - Custom build strategies for specialized indexes
/// - Automatic state transition (writeOnly → readable)
/// - Throttling support for production workloads
///
/// **Usage Example**:
/// ```swift
/// // Create indexer
/// let indexer = OnlineIndexer(
///     database: database,
///     itemSubspace: itemSubspace,
///     indexSubspace: indexSubspace,
///     itemType: "User",
///     index: emailIndex,
///     indexMaintainer: emailIndexMaintainer,
///     indexStateManager: stateManager,
///     batchSize: 100
/// )
///
/// // Build index
/// try await indexer.buildIndex(clearFirst: false)
/// ```
///
/// **Build Strategies**:
///
/// 1. **Standard Build** (default):
///    - Scans items in batches
///    - Calls `indexMaintainer.scanItem()` for each item
///    - Tracks progress with RangeSet
///    - Resumes from last batch on interruption
///
/// 2. **Custom Build** (via IndexBuildStrategy):
///    - Used when `indexMaintainer.customBuildStrategy` is provided
///    - Delegates entire build to custom strategy
///    - Example: HNSW bulk graph construction
public final class OnlineIndexer<Item: Persistable>: Sendable {
    // MARK: - Properties

    /// Database instance
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Subspace where items are stored ([R]/)
    private let itemSubspace: Subspace

    /// Subspace where index data is stored ([I]/)
    private let indexSubspace: Subspace

    /// Item type name (e.g., "User", "Product")
    private let itemType: String

    /// Index definition
    private let index: Index

    /// IndexMaintainer for this index
    private let indexMaintainer: any IndexMaintainer<Item>

    /// Index state manager
    private let indexStateManager: IndexStateManager

    // Configuration
    private let batchSize: Int
    private let throttleDelayMs: Int

    // Progress tracking
    private let progressKey: FDB.Bytes

    // MARK: - Metrics

    /// Counter for items indexed
    private let itemsIndexedCounter: Counter

    /// Counter for batches processed
    private let batchesProcessedCounter: Counter

    /// Timer for batch duration
    private let batchDurationTimer: Metrics.Timer

    /// Counter for errors
    private let errorsCounter: Counter

    // MARK: - Initialization

    /// Initialize online indexer
    ///
    /// - Parameters:
    ///   - database: Database instance
    ///   - itemSubspace: Subspace where items are stored
    ///   - indexSubspace: Subspace where index data is stored
    ///   - itemType: Type name of items to index
    ///   - index: Index definition
    ///   - indexMaintainer: IndexMaintainer for this index
    ///   - indexStateManager: Index state manager
    ///   - batchSize: Number of items per batch (default: 100)
    ///   - throttleDelayMs: Delay between batches in milliseconds (default: 0)
    public init(
        database: any DatabaseProtocol,
        itemSubspace: Subspace,
        indexSubspace: Subspace,
        itemType: String,
        index: Index,
        indexMaintainer: any IndexMaintainer<Item>,
        indexStateManager: IndexStateManager,
        batchSize: Int = 100,
        throttleDelayMs: Int = 0
    ) {
        self.database = database
        self.itemSubspace = itemSubspace
        self.indexSubspace = indexSubspace
        self.itemType = itemType
        self.index = index
        self.indexMaintainer = indexMaintainer
        self.indexStateManager = indexStateManager
        self.batchSize = batchSize
        self.throttleDelayMs = throttleDelayMs

        // Progress key: [indexSubspace]["_progress"][indexName]
        self.progressKey = indexSubspace
            .subspace("_progress")
            .pack(Tuple(index.name))

        // Initialize metrics with index-specific dimensions
        let baseDimensions: [(String, String)] = [
            ("index", index.name),
            ("item_type", itemType)
        ]

        self.itemsIndexedCounter = Counter(
            label: "fdb_indexer_items_indexed_total",
            dimensions: baseDimensions
        )
        self.batchesProcessedCounter = Counter(
            label: "fdb_indexer_batches_processed_total",
            dimensions: baseDimensions
        )
        self.batchDurationTimer = Metrics.Timer(
            label: "fdb_indexer_batch_duration_seconds",
            dimensions: baseDimensions
        )
        self.errorsCounter = Counter(
            label: "fdb_indexer_errors_total",
            dimensions: baseDimensions
        )
    }

    // MARK: - Public API

    /// Build index
    ///
    /// Uses custom build strategy if provided by IndexMaintainer,
    /// otherwise falls back to standard scan-based build.
    ///
    /// **Process**:
    /// 1. Clear index data if requested
    /// 2. Check for custom build strategy
    ///    - If present: delegate to strategy
    ///    - If absent: use standard scan-based build
    /// 3. Transition to readable state
    ///
    /// **Resumability**:
    /// - Standard build: Resumes from last completed batch (via RangeSet)
    /// - Custom build: Resumability depends on strategy implementation
    ///
    /// - Parameter clearFirst: If true, clears existing index data before building
    /// - Throws: Error if build fails
    public func buildIndex(clearFirst: Bool = false) async throws {
        // Clear existing data if requested
        if clearFirst {
            try await clearIndexData()
        }

        // Check if IndexMaintainer provides custom build strategy
        if let customStrategy = indexMaintainer.customBuildStrategy {
            // Use custom strategy (e.g., HNSW bulk build)
            try await customStrategy.buildIndex(
                database: database,
                itemSubspace: itemSubspace,
                indexSubspace: indexSubspace,
                itemType: itemType,
                index: index
            )
        } else {
            // Standard scan-based build
            try await buildIndexInBatches()
        }

        // Transition to readable state
        try await indexStateManager.makeReadable(index.name)
    }

    // MARK: - Standard Build

    /// Build index using standard scan-based approach
    ///
    /// **Process**:
    /// 1. Initialize or load RangeSet progress
    /// 2. Loop until all ranges processed:
    ///    a. Get next batch range
    ///    b. Scan items in range
    ///    c. Call indexMaintainer.scanItem() for each item
    ///    d. Mark range as completed
    ///    e. Save progress
    ///    f. Throttle if configured
    /// 3. Clear progress after completion
    ///
    /// **Resumability**:
    /// - Progress saved after each batch
    /// - On interruption, resumes from last completed batch
    private func buildIndexInBatches() async throws {
        // Get total range to process
        let itemTypeSubspace = itemSubspace.subspace(itemType)
        let totalRange = itemTypeSubspace.range()

        // Initialize or load RangeSet
        var rangeSet: RangeSet
        if let savedProgress = try await loadProgress() {
            rangeSet = savedProgress
        } else {
            rangeSet = RangeSet(initialRange: totalRange)
        }

        // Process batches until complete
        while !rangeSet.isEmpty {
            guard let batchRange = rangeSet.nextBatch(size: batchSize) else {
                break
            }

            let batchStartTime = DispatchTime.now()
            var itemsInBatch = 0

            do {
                // Process batch in transaction
                try await database.withTransaction { transaction in
                    let sequence = transaction.getRange(
                        beginSelector: .firstGreaterOrEqual(batchRange.begin),
                        endSelector: .firstGreaterOrEqual(batchRange.end),
                        snapshot: false
                    )

                    for try await (key, value) in sequence {
                        // Deserialize item using DataAccess static method
                        let item: Item = try DataAccess.deserialize(value)

                        // Extract id
                        let id = try itemTypeSubspace.unpack(key)

                        // Call IndexMaintainer to build index entry
                        try await indexMaintainer.scanItem(
                            item,
                            id: id,
                            transaction: transaction
                        )

                        itemsInBatch += 1
                    }

                    // Mark batch as completed
                    rangeSet.markCompleted(batchRange)

                    // Save progress
                    try saveProgress(rangeSet, transaction)
                }

                // Record metrics
                let batchDuration = DispatchTime.now().uptimeNanoseconds - batchStartTime.uptimeNanoseconds
                batchDurationTimer.recordNanoseconds(Int64(batchDuration))
                batchesProcessedCounter.increment()
                itemsIndexedCounter.increment(by: itemsInBatch)

            } catch {
                errorsCounter.increment()
                throw error
            }

            // Throttle if configured
            if throttleDelayMs > 0 {
                try await Task.sleep(nanoseconds: UInt64(throttleDelayMs) * 1_000_000)
            }
        }

        // Clear progress after successful completion
        try await clearProgress()
    }

    // MARK: - Progress Management

    /// Load saved progress
    ///
    /// - Returns: RangeSet if progress exists, nil otherwise
    private func loadProgress() async throws -> RangeSet? {
        return try await database.withTransaction { transaction in
            guard let bytes = try await transaction.getValue(for: progressKey, snapshot: false) else {
                return nil
            }

            let decoder = JSONDecoder()
            return try decoder.decode(RangeSet.self, from: Data(bytes))
        }
    }

    /// Save progress
    ///
    /// - Parameters:
    ///   - rangeSet: Current progress
    ///   - transaction: Transaction to use
    private func saveProgress(
        _ rangeSet: RangeSet,
        _ transaction: any TransactionProtocol
    ) throws {
        let encoder = JSONEncoder()
        let data = try encoder.encode(rangeSet)
        transaction.setValue(Array(data), for: progressKey)
    }

    /// Clear progress
    ///
    /// Called after successful completion
    private func clearProgress() async throws {
        try await database.withTransaction { transaction in
            transaction.clear(key: progressKey)
        }
    }

    // MARK: - Index Data Management

    /// Clear all index data
    ///
    /// Removes all entries in the index subspace for this index.
    /// Used when `clearFirst: true` is specified.
    private func clearIndexData() async throws {
        try await database.withTransaction { transaction in
            let indexRange = indexSubspace.subspace(index.name).range()
            transaction.clearRange(
                beginKey: indexRange.begin,
                endKey: indexRange.end
            )
        }
    }
}

// MARK: - CustomStringConvertible

extension OnlineIndexer: CustomStringConvertible {
    public var description: String {
        return "OnlineIndexer(index: \(index.name), itemType: \(itemType), batchSize: \(batchSize))"
    }
}
