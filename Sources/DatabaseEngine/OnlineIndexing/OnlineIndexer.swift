import DatabaseTypes
import StorageKit
import DatabaseKit

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
/// let indexer = try OnlineIndexer(
///     container: container,
///     storeSubspace: storeSubspace,
///     itemType: "User",
///     index: emailIndex,
///     indexMaintainer: emailIndexMaintainer,
///     indexLifecycleStore: lifecycleStore,
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
///    - Calls `indexMaintainer.scanItems()` once per batch
///    - Tracks progress with RangeSet
///    - Resumes from last batch on interruption
///
/// 2. **Custom Build** (via IndexBuildStrategy):
///    - Used when `indexMaintainer.customBuildStrategy` is provided
///    - Delegates entire build to custom strategy
///    - Example: HNSW bulk graph construction
public final class OnlineIndexer<Item: Persistable>: Sendable {
    // MARK: - Properties

    /// Database container for transaction execution
    let container: DBContainer

    /// Store root subspace (parent of R/I/B/M)
    private let storeSubspace: Subspace

    /// Subspace where items are stored ([R]/)
    private let itemSubspace: Subspace

    /// Subspace where index data is stored ([I]/)
    private let indexSubspace: Subspace

    /// Subspace where blob chunks are stored ([B]/)
    private let blobsSubspace: Subspace

    /// Item type name (e.g., "User", "Product")
    private let itemType: String

    /// Index definition
    private let index: Index

    /// IndexMaintainer for this index
    private let indexMaintainer: any IndexMaintainer<Item>

    /// Uniqueness conflict lookup for unique indexes.
    private let uniquenessMaintainer: (any IndexUniquenessMaintainer<Item>)?

    /// Index state manager
    private let indexLifecycleStore: IndexLifecycleStore

    // Configuration
    private let batchSize: Int
    private let throttleDelayMs: Int

    // Progress tracking
    private let progressKey: ByteString

    // Uniqueness enforcement
    private let metadataSubspace: Subspace
    private let violationTracker: UniquenessViolationTracker

    // MARK: - Metrics

    /// Counter for items indexed
    private let itemsIndexedCounter: DatabaseMetricCounter

    /// Counter for batches processed
    private let batchesProcessedCounter: DatabaseMetricCounter

    /// Timer for batch duration
    private let batchDurationTimer: DatabaseMetricTimer

    /// Counter for errors
    private let errorsCounter: DatabaseMetricCounter

    // MARK: - Initialization

    /// Initialize online indexer
    ///
    /// - Parameters:
    ///   - container: DBContainer for transaction execution
    ///   - storeSubspace: Store root subspace (parent of items/indexes/blobs/metadata)
    ///   - itemType: Type name of items to index
    ///   - index: Index definition
    ///   - indexMaintainer: IndexMaintainer for this index
    ///   - indexLifecycleStore: Index state manager
    ///   - batchSize: Number of items per batch (default: 100)
    ///   - throttleDelayMs: Delay between batches in milliseconds (default: 0)
    public init(
        container: DBContainer,
        storeSubspace: Subspace,
        itemType: String,
        index: Index,
        indexMaintainer: any IndexMaintainer<Item>,
        uniquenessMaintainer: (any IndexUniquenessMaintainer<Item>)? = nil,
        indexLifecycleStore: IndexLifecycleStore,
        batchSize: Int = 100,
        throttleDelayMs: Int = 0
    ) throws(OnlineIndexBuildError) {
        guard batchSize > 0 else {
            throw .invalidBatchSize(batchSize)
        }
        guard throttleDelayMs >= 0 else {
            throw .invalidThrottleDelayMilliseconds(throttleDelayMs)
        }
        if index.isUnique, indexMaintainer.customBuildStrategy != nil {
            throw .unsupportedUniqueCustomBuildStrategy(indexName: index.name)
        }
        if index.isUnique, uniquenessMaintainer == nil {
            throw .unsupportedUniquenessConstraint(indexName: index.name)
        }
        self.container = container
        self.storeSubspace = storeSubspace
        self.itemSubspace = storeSubspace.subspace(SubspaceKey.items)
        self.indexSubspace = storeSubspace.subspace(SubspaceKey.indexes)
        self.blobsSubspace = storeSubspace.subspace(SubspaceKey.blobs)
        self.itemType = itemType
        self.index = index
        self.indexMaintainer = indexMaintainer
        self.uniquenessMaintainer = uniquenessMaintainer
        self.indexLifecycleStore = indexLifecycleStore
        self.batchSize = batchSize
        self.throttleDelayMs = throttleDelayMs

        // Progress key: [indexSubspace]["_progress"][indexName]
        self.progressKey = self.indexSubspace
            .subspace("_progress")
            .pack(Tuple(index.name))

        // Metadata and violation tracking for unique indexes
        // Violations stored in [store]/M/_violations/[indexName]/
        self.metadataSubspace = storeSubspace.subspace(SubspaceKey.metadata)
        self.violationTracker = UniquenessViolationTracker(
            container: container,
            metadataSubspace: metadataSubspace
        )

        // Initialize metrics with index-specific dimensions
        let baseDimensions: [(String, String)] = [
            ("index", index.name),
            ("item_type", itemType)
        ]

        let metrics = container.configuration.metrics
        self.itemsIndexedCounter = metrics.counter(
            label: "database_indexer_items_indexed_total",
            dimensions: baseDimensions
        )
        self.batchesProcessedCounter = metrics.counter(
            label: "database_indexer_batches_processed_total",
            dimensions: baseDimensions
        )
        self.batchDurationTimer = metrics.timer(
            label: "database_indexer_batch_duration_seconds",
            dimensions: baseDimensions
        )
        self.errorsCounter = metrics.counter(
            label: "database_indexer_errors_total",
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
    /// 3. For unique indexes: check for violations
    /// 4. Transition to readable state (if no violations)
    ///
    /// **Uniqueness Enforcement**:
    /// For unique indexes (`index.isUnique == true`), violations detected during
    /// the build are tracked instead of immediately rejected. After the build
    /// completes, this method checks for violations. If any exist, an error
    /// is thrown and the index remains in write-only state.
    ///
    /// **Resumability**:
    /// - Standard build: Resumes from last completed batch (via RangeSet)
    /// - Custom build: Resumability depends on strategy implementation
    ///
    /// - Parameter clearFirst: If true, clears existing index data before building
    /// - Throws: `OnlineIndexBuildError.uniquenessViolationsDetected` if unique index has violations
    /// - Throws: Error if build fails
    public func buildIndex(clearFirst: Bool = false) async throws {
        // Clear existing data if requested
        if clearFirst {
            try await clearIndexData()
            // Also clear any existing violation entries for this index
            if index.isUnique {
                try await violationTracker.clearAllViolations(indexName: index.name)
            }
        }

        // Check if IndexMaintainer provides custom build strategy
        if let customStrategy = indexMaintainer.customBuildStrategy {
            // Use custom strategy (e.g., HNSW bulk build)
            try await customStrategy.buildIndex(
                container: container,
                itemSubspace: itemSubspace,
                indexSubspace: indexSubspace,
                itemType: itemType,
                index: index
            )
        } else {
            // Standard scan-based build
            try await buildIndexInBatches()
        }

        // For unique indexes, check for violations before making readable
        if index.isUnique {
            let hasViolations = try await violationTracker.hasViolations(indexName: index.name)
            if hasViolations {
                let summary = try await violationTracker.violationSummary(indexName: index.name)
                throw OnlineIndexBuildError.uniquenessViolationsDetected(
                    indexName: index.name,
                    violationCount: summary.violationCount,
                    totalConflictingEntities: summary.totalConflictingEntities
                )
            }
        }

        // Transition to readable state
        try await indexLifecycleStore.makeReadable(index.name)
    }

    // MARK: - Standard Build

    /// Build index using standard scan-based approach
    ///
    /// **Process**:
    /// 1. Initialize or load RangeSet progress
    /// 2. Loop until all ranges processed:
    ///    a. Get next batch range
    ///    b. Scan items in range
    ///    c. Call indexMaintainer.scanItems() for each batch
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

        // Process batches - each batch in a separate transaction
        while let bounds = rangeSet.nextBatchBounds() {
            let batchStartTime = container.monotonicClock.now

            do {
                // Capture current rangeSet state before transaction
                let currentRangeSet = rangeSet

                // Process batch in transaction with batch priority
                // Progress is saved atomically in the same transaction
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
                        // Deserialize item from decompressed data
                        let item: Item = try DataAccess.deserialize(data)

                        // Extract id
                        let id = try itemTypeSubspace.unpack(key)

                        batchEntries.append((item: item, id: id))

                        lastProcessedKey = key
                        itemsInBatch += 1
                    }

                    // Call IndexMaintainer once per batch. Maintainers that do
                    // not override scanItems preserve scanItem behavior.
                    try await OnlineIndexBatchWriter.write(
                        batchEntries,
                        index: self.index,
                        maintainer: self.indexMaintainer,
                        uniquenessMaintainer: self.uniquenessMaintainer,
                        violationTracker: self.violationTracker,
                        transaction: transaction
                    )

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

                    // Save progress in same transaction for atomicity
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
                itemsIndexedCounter.increment(by: itemsInBatch)

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

        // Clear progress after successful completion
        try await clearProgress()
    }

    // MARK: - Progress Management

    /// Load saved progress
    ///
    /// - Returns: RangeSet if progress exists, nil otherwise
    private func loadProgress() async throws -> RangeSet? {
        let progressKey = self.progressKey
        return try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            guard let bytes = try await transaction.getValue(for: progressKey, snapshot: false) else {
                return nil
            }

            return try RangeSetCodec.decode(bytes)
        }
    }

    /// Save progress
    ///
    /// - Parameters:
    ///   - rangeSet: Current progress
    ///   - transaction: Transaction to use
    private func saveProgress(
        _ rangeSet: RangeSet,
        _ transaction: any TransactionAccess
    ) throws {
        try transaction.setValue(try RangeSetCodec.encode(rangeSet), for: progressKey)
    }

    /// Clear progress
    ///
    /// Called after successful completion
    private func clearProgress() async throws {
        let progressKey = self.progressKey
        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try transaction.clear(key: progressKey)
        }
    }

    // MARK: - Index Data Management

    /// Clear all index data
    ///
    /// Removes all entries in the index subspace for this index.
    /// Used when `clearFirst: true` is specified.
    private func clearIndexData() async throws {
        let indexRange = self.indexSubspace.subspace(self.index.name).range()
        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try transaction.clearRange(
                beginKey: indexRange.begin,
                endKey: indexRange.end
            )
        }
    }

    // MARK: - Parallel Build

    /// Build index in parallel using range split points
    ///
    /// This method divides the item range into chunks using the storage engine's
    /// range split points
    /// and processes them in parallel with controlled concurrency. This can provide
    /// 10-100x speedup for large datasets.
    ///
    /// **Process**:
    /// 1. Get storage range split points (divides data by estimated size)
    /// 2. Load any existing progress (for resumability)
    /// 3. Create task group with maxConcurrency workers
    /// 4. Each worker processes assigned chunks, updating progress atomically
    /// 5. On completion, clear progress data and transition to readable
    ///
    /// **Resumability**:
    /// - Progress is stored per-chunk under `[indexSubspace]/_build/[indexName]/`
    /// - On restart, completed chunks are skipped
    /// - In-progress chunks resume from the last processed key
    /// - Progress data is cleared on successful completion
    ///
    /// - Parameters:
    ///   - clearFirst: If true, clears existing index data and progress before building
    ///   - maxConcurrency: Maximum parallel workers (default: 4)
    ///   - chunkSizeBytes: Target size per chunk in bytes (default: 10MB)
    /// - Throws: Error if build fails
    public func buildIndexInParallel(
        clearFirst: Bool = false,
        maxConcurrency: Int = 4,
        chunkSizeBytes: Int = 10_000_000
    ) async throws {
        guard maxConcurrency > 0 else {
            throw OnlineIndexBuildError.invalidMaximumConcurrency(maxConcurrency)
        }
        guard chunkSizeBytes > 0 else {
            throw OnlineIndexBuildError.invalidChunkSizeBytes(chunkSizeBytes)
        }

        // Initialize progress tracker
        let progress = ParallelBuildProgress(
            indexSubspace: indexSubspace,
            indexName: index.name,
            container: container
        )

        // Clear existing data and progress if requested
        if clearFirst {
            try await clearIndexData()
            try await progress.clearProgress()
            // Also clear any existing violation entries for this index
            if index.isUnique {
                try await violationTracker.clearAllViolations(indexName: index.name)
            }
        }

        // Get item type subspace
        let itemTypeSubspace = itemSubspace.subspace(itemType)
        let (begin, end) = itemTypeSubspace.range()

        // Get split points from FDB
        let splitPoints = try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try await transaction.getRangeSplitPoints(
                beginKey: begin,
                endKey: end,
                chunkSize: chunkSizeBytes
            )
        }

        // If no split points or only one range, fall back to standard build
        guard splitPoints.count > 1 else {
            try await buildIndexInBatches()
            // Check for violations before making readable (for unique indexes)
            if index.isUnique {
                let hasViolations = try await violationTracker.hasViolations(indexName: index.name)
                if hasViolations {
                    let summary = try await violationTracker.violationSummary(indexName: index.name)
                    throw OnlineIndexBuildError.uniquenessViolationsDetected(
                        indexName: index.name,
                        violationCount: summary.violationCount,
                        totalConflictingEntities: summary.totalConflictingEntities
                    )
                }
            }
            try await indexLifecycleStore.makeReadable(index.name)
            return
        }

        // Build chunk ranges from split points
        var chunks: [(begin: ByteString, end: ByteString)] = []
        var prevPoint = begin
        for point in splitPoints {
            chunks.append((begin: prevPoint, end: point))
            prevPoint = point
        }
        chunks.append((begin: prevPoint, end: end))

        // Load existing progress for resumability
        let existingProgress = try await progress.loadProgress(chunkCount: chunks.count)

        // Build list of chunks to process (skip completed ones)
        var chunksToProcess: [(index: Int, begin: ByteString, end: ByteString, startKey: ByteString?)] = []
        for (idx, chunk) in chunks.enumerated() {
            if let chunkProgress = existingProgress[idx] {
                switch chunkProgress.status {
                case .complete:
                    // Skip completed chunks
                    continue
                case .inProgress:
                    // Resume from last processed key
                    let startKey = chunkProgress.lastProcessedKey.map {
                        $0.appending(0x00)
                    }
                    chunksToProcess.append((index: idx, begin: chunk.begin, end: chunk.end, startKey: startKey))
                case .notStarted:
                    chunksToProcess.append((index: idx, begin: chunk.begin, end: chunk.end, startKey: nil))
                }
            } else {
                // No progress recorded, start from beginning
                chunksToProcess.append((index: idx, begin: chunk.begin, end: chunk.end, startKey: nil))
            }
        }

        // If all chunks are complete, just transition state
        guard !chunksToProcess.isEmpty else {
            try await progress.clearProgress()
            // Check for violations before making readable (for unique indexes)
            if index.isUnique {
                let hasViolations = try await violationTracker.hasViolations(indexName: index.name)
                if hasViolations {
                    let summary = try await violationTracker.violationSummary(indexName: index.name)
                    throw OnlineIndexBuildError.uniquenessViolationsDetected(
                        indexName: index.name,
                        violationCount: summary.violationCount,
                        totalConflictingEntities: summary.totalConflictingEntities
                    )
                }
            }
            try await indexLifecycleStore.makeReadable(index.name)
            return
        }

        // Process chunks in parallel with controlled concurrency
        try await withThrowingTaskGroup(of: Int.self) { group in
            var processIndex = 0

            // Start initial batch of workers
            while processIndex < min(maxConcurrency, chunksToProcess.count) {
                let chunkInfo = chunksToProcess[processIndex]
                group.addTask {
                    try await self.processChunkWithProgress(
                        chunkIndex: chunkInfo.index,
                        begin: chunkInfo.startKey ?? chunkInfo.begin,
                        end: chunkInfo.end,
                        itemTypeSubspace: itemTypeSubspace,
                        progress: progress
                    )
                }
                processIndex += 1
            }

            // As workers complete, start new ones
            while let itemsProcessed = try await group.next() {
                itemsIndexedCounter.increment(by: itemsProcessed)

                // Start next chunk if available
                if processIndex < chunksToProcess.count {
                    let chunkInfo = chunksToProcess[processIndex]
                    group.addTask {
                        try await self.processChunkWithProgress(
                            chunkIndex: chunkInfo.index,
                            begin: chunkInfo.startKey ?? chunkInfo.begin,
                            end: chunkInfo.end,
                            itemTypeSubspace: itemTypeSubspace,
                            progress: progress
                        )
                    }
                    processIndex += 1
                }
            }
        }

        // Clear progress data on successful completion
        try await progress.clearProgress()

        // For unique indexes, check for violations before making readable
        if index.isUnique {
            let hasViolations = try await violationTracker.hasViolations(indexName: index.name)
            if hasViolations {
                let summary = try await violationTracker.violationSummary(indexName: index.name)
                throw OnlineIndexBuildError.uniquenessViolationsDetected(
                    indexName: index.name,
                    violationCount: summary.violationCount,
                    totalConflictingEntities: summary.totalConflictingEntities
                )
            }
        }

        // Transition to readable state
        try await indexLifecycleStore.makeReadable(index.name)
    }

    /// Process a single chunk with progress tracking
    ///
    /// - Parameters:
    ///   - chunkIndex: Index of this chunk (for progress tracking)
    ///   - begin: Begin key of chunk (may be after original begin if resuming)
    ///   - end: End key of chunk
    ///   - itemTypeSubspace: Subspace for item type
    ///   - progress: Progress tracker
    /// - Returns: Number of items processed
    private func processChunkWithProgress(
        chunkIndex: Int,
        begin: ByteString,
        end: ByteString,
        itemTypeSubspace: Subspace,
        progress: ParallelBuildProgress
    ) async throws -> Int {
        var itemsProcessed = 0
        var lastKey: ByteString? = nil
        var currentBegin = begin

        // Mark chunk as in-progress
        try await progress.updateProgress(chunkIndex: chunkIndex, status: .inProgress, lastKey: nil)

        // Process in batches within this chunk
        while true {
            // Capture current begin for Sendable closure
            let rangeBegin = currentBegin

            let (batchCount, newLastKey): (Int, ByteString?) = try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
                var count = 0
                var processedKey: ByteString? = nil

                // Use ItemStorage.scan() to handle ItemEnvelope format (inline/external)
                let storage = self.container.itemStorageFactory.make(
                    transaction: transaction,
                    blobsSubspace: self.blobsSubspace
                )

                let scanSequence = storage.scan(
                    begin: rangeBegin,
                    end: end,
                    snapshot: false,
                    limit: self.batchSize
                )

                var batchEntries: [(item: Item, id: Tuple)] = []
                batchEntries.reserveCapacity(self.batchSize)

                var iterator = scanSequence.makeAsyncIterator()
                while let (key, data) = try await iterator.next() {
                    // Deserialize item from decompressed data
                    let item: Item = try DataAccess.deserialize(data)

                    // Extract id
                    let id = try itemTypeSubspace.unpack(key)

                    batchEntries.append((item: item, id: id))

                    processedKey = key
                    count += 1
                }

                // Call IndexMaintainer once per batch. Maintainers that do
                // not override scanItems preserve scanItem behavior.
                try await OnlineIndexBatchWriter.write(
                    batchEntries,
                    index: self.index,
                    maintainer: self.indexMaintainer,
                    uniquenessMaintainer: self.uniquenessMaintainer,
                    violationTracker: self.violationTracker,
                    transaction: transaction
                )

                // Update progress atomically with the batch
                if let processedKey = processedKey {
                    try progress.updateProgress(
                        chunkIndex: chunkIndex,
                        status: .inProgress,
                        lastKey: processedKey,
                        transaction: transaction
                    )
                }

                return (count, processedKey)
            }

            itemsProcessed += batchCount
            lastKey = newLastKey

            // Update current begin for next batch
            if let newLastKey = newLastKey {
                currentBegin = newLastKey.appending(0x00)
            }

            // If we processed fewer than batchSize, we've reached the end of this chunk
            if batchCount < batchSize || newLastKey == nil {
                break
            }

            // Throttle if configured
            if throttleDelayMs > 0 {
                try await container.monotonicClock.sleep(
                    for: .milliseconds(Int64(throttleDelayMs))
                )
            }
        }

        // Mark chunk as complete
        try await progress.updateProgress(chunkIndex: chunkIndex, status: .complete, lastKey: lastKey)

        return itemsProcessed
    }

}

// MARK: - CustomStringConvertible

extension OnlineIndexer: CustomStringConvertible {
    public var description: String {
        return "OnlineIndexer(index: \(index.name), itemType: \(itemType), batchSize: \(batchSize))"
    }
}

// MARK: - Parallel Build Progress

/// Progress tracker for parallel index builds
///
/// Stores per-chunk progress in the database to enable resumability after failures.
/// Each chunk tracks its status (not_started, in_progress, complete) and
/// the last processed key for in-progress chunks.
///
/// **Data Layout**:
/// ```
/// [indexSubspace]/_build/[indexName]/[chunkIndex]
///   → Tuple(status: Int, lastProcessedKey: ByteString?)
/// ```
///
/// **Thread Safety**:
/// Each chunk writes to its own key, so parallel workers don't conflict.
internal final class ParallelBuildProgress: Sendable {

    /// Chunk processing status
    enum ChunkStatus: Int, Sendable {
        case notStarted = 0
        case inProgress = 1
        case complete = 2
    }

    /// Progress data for a single chunk
    struct ChunkProgress: Sendable {
        let status: ChunkStatus
        let lastProcessedKey: ByteString?

        static let notStarted = ChunkProgress(status: .notStarted, lastProcessedKey: nil)
    }

    /// Subspace for progress data
    private let progressSubspace: Subspace

    /// Database container for transaction execution
    let container: DBContainer

    /// Initialize progress tracker
    ///
    /// - Parameters:
    ///   - indexSubspace: Index subspace (progress stored under _build/)
    ///   - indexName: Name of the index being built
    ///   - container: DBContainer for transaction execution
    init(indexSubspace: Subspace, indexName: String, container: DBContainer) {
        self.progressSubspace = indexSubspace.subspace("_build").subspace(indexName)
        self.container = container
    }

    /// Load progress for all chunks
    ///
    /// - Parameter chunkCount: Total number of chunks
    /// - Returns: Dictionary of chunk index to progress
    func loadProgress(chunkCount: Int) async throws -> [Int: ChunkProgress] {
        let (begin, end) = progressSubspace.range()

        return try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            var progress: [Int: ChunkProgress] = [:]

            let sequence = try await TransactionRangeCollection.collect(using: transaction,
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )

            for (key, value) in sequence {
                let chunkIndex = try self.extractChunkIndex(from: key)
                guard chunkIndex >= 0, chunkIndex < chunkCount else {
                    throw OnlineIndexBuildError.corruptedProgress
                }
                let chunkProgress = try self.decodeProgress(from: value)
                progress[chunkIndex] = chunkProgress
            }

            return progress
        }
    }

    /// Update progress for a chunk
    ///
    /// - Parameters:
    ///   - chunkIndex: Index of the chunk
    ///   - status: New status
    ///   - lastKey: Last processed key (for in_progress status)
    func updateProgress(
        chunkIndex: Int,
        status: ChunkStatus,
        lastKey: ByteString?
    ) async throws {
        let key = progressSubspace.pack(Tuple(chunkIndex))
        let value = encodeProgress(status: status, lastKey: lastKey)

        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try transaction.setValue(value, for: key)
        }
    }

    /// Update progress atomically within an existing transaction
    ///
    /// - Parameters:
    ///   - chunkIndex: Index of the chunk
    ///   - status: New status
    ///   - lastKey: Last processed key
    ///   - transaction: Active transaction
    func updateProgress(
        chunkIndex: Int,
        status: ChunkStatus,
        lastKey: ByteString?,
        transaction: any TransactionAccess
    ) throws {
        let key = progressSubspace.pack(Tuple(chunkIndex))
        let value = encodeProgress(status: status, lastKey: lastKey)
        try transaction.setValue(value, for: key)
    }

    /// Clear all progress data
    ///
    /// Called on successful completion or when clearFirst is requested.
    func clearProgress() async throws {
        let (begin, end) = progressSubspace.range()

        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Private Helpers

    private func extractChunkIndex(from key: ByteString) throws -> Int {
        let tuple = try progressSubspace.unpack(key)
        guard tuple.count == 1,
              case .signedInteger(let index) = try tuple.value(at: 0),
              let converted = Int(exactly: index) else {
            throw OnlineIndexBuildError.corruptedProgress
        }
        return converted
    }

    private func encodeProgress(status: ChunkStatus, lastKey: ByteString?) -> ByteString {
        if let lastKey = lastKey {
            return Tuple(status.rawValue, lastKey).pack()
        } else {
            return Tuple(status.rawValue).pack()
        }
    }

    private func decodeProgress(from data: ByteString) throws -> ChunkProgress {
        let tuple = try Tuple(packed: data)
        guard tuple.count == 1 || tuple.count == 2,
              case .signedInteger(let statusRaw) = try tuple.value(at: 0),
              let statusValue = Int(exactly: statusRaw),
              let status = ChunkStatus(rawValue: statusValue) else {
            throw OnlineIndexBuildError.corruptedProgress
        }
        let lastKey: ByteString?
        if tuple.count == 2 {
            guard case .bytes(let bytes) = try tuple.value(at: 1) else {
                throw OnlineIndexBuildError.corruptedProgress
            }
            lastKey = bytes
        } else {
            lastKey = nil
        }
        return ChunkProgress(status: status, lastProcessedKey: lastKey)
    }
}
