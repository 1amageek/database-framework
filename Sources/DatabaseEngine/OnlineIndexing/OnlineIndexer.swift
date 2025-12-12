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

    /// Subspace where blob chunks are stored ([B]/)
    private let blobsSubspace: Subspace

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

    // Uniqueness enforcement
    private let metadataSubspace: Subspace
    private let violationTracker: UniquenessViolationTracker

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
    ///   - blobsSubspace: Subspace where blob chunks are stored
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
        blobsSubspace: Subspace,
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
        self.blobsSubspace = blobsSubspace
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

        // Metadata and violation tracking for unique indexes
        // Derive metadata subspace from itemSubspace parent (assumes [store]/R/ structure)
        // Violations stored in [store]/M/_violations/[indexName]/
        self.metadataSubspace = itemSubspace.subspace(SubspaceKey.metadata)
        self.violationTracker = UniquenessViolationTracker(
            database: database,
            metadataSubspace: metadataSubspace
        )

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
    /// - Throws: `OnlineIndexerError.uniquenessViolationsDetected` if unique index has violations
    /// - Throws: Error if build fails
    public func buildIndex(clearFirst: Bool = false) async throws {
        // Clear existing data if requested
        if clearFirst {
            try await clearIndexData()
            // Also clear any existing violation records for this index
            if index.isUnique {
                try await violationTracker.clearAllViolations(indexName: index.name)
            }
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

        // For unique indexes, check for violations before making readable
        if index.isUnique {
            let hasViolations = try await violationTracker.hasViolations(indexName: index.name)
            if hasViolations {
                let summary = try await violationTracker.violationSummary(indexName: index.name)
                throw OnlineIndexerError.uniquenessViolationsDetected(
                    indexName: index.name,
                    violationCount: summary.violationCount,
                    totalConflictingRecords: summary.totalConflictingRecords
                )
            }
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

        // Process batches - each batch in a separate transaction
        while let bounds = rangeSet.nextBatchBounds() {
            let batchStartTime = DispatchTime.now()

            do {
                // Capture current rangeSet state before transaction
                let currentRangeSet = rangeSet

                // Process batch in transaction with batch priority
                // Progress is saved atomically in the same transaction
                let (itemsInBatch, lastProcessedKey) = try await database.withTransaction(configuration: .batch) { transaction in

                    var itemsInBatch = 0
                    var lastProcessedKey: FDB.Bytes? = nil

                    // Use ItemStorage.scan() to handle ItemEnvelope format (inline/external)
                    let storage = ItemStorage(
                        transaction: transaction,
                        blobsSubspace: self.blobsSubspace
                    )

                    let scanSequence = storage.scan(
                        begin: bounds.begin,
                        end: bounds.end,
                        snapshot: false,
                        limit: self.batchSize
                    )

                    for try await (key, data) in scanSequence {
                        // Deserialize item from decompressed data
                        let item: Item = try DataAccess.deserialize(data)

                        // Extract id
                        let id = try itemTypeSubspace.unpack(key)

                        // Call IndexMaintainer to build index entry
                        try await self.indexMaintainer.scanItem(
                            item,
                            id: id,
                            transaction: transaction
                        )

                        lastProcessedKey = Array(key)
                        itemsInBatch += 1
                    }

                    // Save progress atomically with work
                    // Create updated rangeSet copy inside transaction for saving
                    var updatedRangeSet = currentRangeSet
                    if let lastKey = lastProcessedKey {
                        let isComplete = itemsInBatch < self.batchSize
                        updatedRangeSet.recordProgress(
                            rangeIndex: bounds.rangeIndex,
                            lastProcessedKey: lastKey,
                            isComplete: isComplete
                        )
                    } else {
                        updatedRangeSet.markRangeComplete(rangeIndex: bounds.rangeIndex)
                    }

                    // Save progress in same transaction for atomicity
                    try self.saveProgress(updatedRangeSet, transaction)

                    return (itemsInBatch, lastProcessedKey)
                }

                // Update in-memory rangeSet after successful commit
                if let lastKey = lastProcessedKey {
                    let isComplete = itemsInBatch < self.batchSize
                    rangeSet.recordProgress(
                        rangeIndex: bounds.rangeIndex,
                        lastProcessedKey: lastKey,
                        isComplete: isComplete
                    )
                } else {
                    rangeSet.markRangeComplete(rangeIndex: bounds.rangeIndex)
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
        return try await database.withTransaction(configuration: .batch) { transaction in
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
        try await database.withTransaction(configuration: .batch) { transaction in
            transaction.clear(key: progressKey)
        }
    }

    // MARK: - Index Data Management

    /// Clear all index data
    ///
    /// Removes all entries in the index subspace for this index.
    /// Used when `clearFirst: true` is specified.
    private func clearIndexData() async throws {
        try await database.withTransaction(configuration: .batch) { transaction in
            let indexRange = indexSubspace.subspace(index.name).range()
            transaction.clearRange(
                beginKey: indexRange.begin,
                endKey: indexRange.end
            )
        }
    }

    // MARK: - Parallel Build

    /// Build index in parallel using range split points
    ///
    /// This method divides the item range into chunks using FDB's `getRangeSplitPoints`
    /// and processes them in parallel with controlled concurrency. This can provide
    /// 10-100x speedup for large datasets.
    ///
    /// **Process**:
    /// 1. Get range split points from FDB (divides data by estimated size)
    /// 2. Load any existing progress (for resumability)
    /// 3. Create task group with maxConcurrency workers
    /// 4. Each worker processes assigned chunks, updating progress atomically
    /// 5. On completion, clear progress data and transition to readable
    ///
    /// **Resumability**:
    /// - Progress is stored per-chunk in FDB under `[indexSubspace]/_build/[indexName]/`
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
        // Initialize progress tracker
        let progress = ParallelBuildProgress(
            indexSubspace: indexSubspace,
            indexName: index.name,
            database: database
        )

        // Clear existing data and progress if requested
        if clearFirst {
            try await clearIndexData()
            try await progress.clearProgress()
            // Also clear any existing violation records for this index
            if index.isUnique {
                try await violationTracker.clearAllViolations(indexName: index.name)
            }
        }

        // Get item type subspace
        let itemTypeSubspace = itemSubspace.subspace(itemType)
        let (begin, end) = itemTypeSubspace.range()

        // Get split points from FDB
        let splitPoints = try await database.withTransaction { transaction in
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
                    throw OnlineIndexerError.uniquenessViolationsDetected(
                        indexName: index.name,
                        violationCount: summary.violationCount,
                        totalConflictingRecords: summary.totalConflictingRecords
                    )
                }
            }
            try await indexStateManager.makeReadable(index.name)
            return
        }

        // Build chunk ranges from split points
        var chunks: [(begin: [UInt8], end: [UInt8])] = []
        var prevPoint = begin
        for point in splitPoints {
            chunks.append((begin: prevPoint, end: point))
            prevPoint = point
        }
        chunks.append((begin: prevPoint, end: end))

        // Load existing progress for resumability
        let existingProgress = try await progress.loadProgress(chunkCount: chunks.count)

        // Build list of chunks to process (skip completed ones)
        var chunksToProcess: [(index: Int, begin: [UInt8], end: [UInt8], startKey: [UInt8]?)] = []
        for (idx, chunk) in chunks.enumerated() {
            if let chunkProgress = existingProgress[idx] {
                switch chunkProgress.status {
                case .complete:
                    // Skip completed chunks
                    continue
                case .inProgress:
                    // Resume from last processed key
                    let startKey = chunkProgress.lastProcessedKey.map { Array($0) + [0x00] }
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
                    throw OnlineIndexerError.uniquenessViolationsDetected(
                        indexName: index.name,
                        violationCount: summary.violationCount,
                        totalConflictingRecords: summary.totalConflictingRecords
                    )
                }
            }
            try await indexStateManager.makeReadable(index.name)
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
            for try await itemsProcessed in group {
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
                throw OnlineIndexerError.uniquenessViolationsDetected(
                    indexName: index.name,
                    violationCount: summary.violationCount,
                    totalConflictingRecords: summary.totalConflictingRecords
                )
            }
        }

        // Transition to readable state
        try await indexStateManager.makeReadable(index.name)
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
        begin: [UInt8],
        end: [UInt8],
        itemTypeSubspace: Subspace,
        progress: ParallelBuildProgress
    ) async throws -> Int {
        var itemsProcessed = 0
        var lastKey: [UInt8]? = nil
        var currentBegin = begin

        // Mark chunk as in-progress
        try await progress.updateProgress(chunkIndex: chunkIndex, status: .inProgress, lastKey: nil)

        // Process in batches within this chunk
        while true {
            // Capture current begin for Sendable closure
            let rangeBegin = currentBegin

            let (batchCount, newLastKey): (Int, [UInt8]?) = try await database.withTransaction(configuration: .batch) { transaction in
                var count = 0
                var processedKey: [UInt8]? = nil

                // Use ItemStorage.scan() to handle ItemEnvelope format (inline/external)
                let storage = ItemStorage(
                    transaction: transaction,
                    blobsSubspace: self.blobsSubspace
                )

                let scanSequence = storage.scan(
                    begin: rangeBegin,
                    end: end,
                    snapshot: false,
                    limit: self.batchSize
                )

                for try await (key, data) in scanSequence {
                    // Deserialize item from decompressed data
                    let item: Item = try DataAccess.deserialize(data)

                    // Extract id
                    let id = try itemTypeSubspace.unpack(key)

                    // Call IndexMaintainer to build index entry
                    try await self.indexMaintainer.scanItem(
                        item,
                        id: id,
                        transaction: transaction
                    )

                    processedKey = Array(key)
                    count += 1
                }

                // Update progress atomically with the batch
                if let processedKey = processedKey {
                    progress.updateProgress(
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
                currentBegin = Array(newLastKey) + [0x00]
            }

            // If we processed fewer than batchSize, we've reached the end of this chunk
            if batchCount < batchSize || newLastKey == nil {
                break
            }

            // Throttle if configured
            if throttleDelayMs > 0 {
                try await Task.sleep(nanoseconds: UInt64(throttleDelayMs) * 1_000_000)
            }
        }

        // Mark chunk as complete
        try await progress.updateProgress(chunkIndex: chunkIndex, status: .complete, lastKey: lastKey)

        return itemsProcessed
    }

    /// Process a single chunk of items
    ///
    /// - Parameters:
    ///   - chunkIndex: Index of this chunk (for logging)
    ///   - begin: Begin key of chunk
    ///   - end: End key of chunk
    ///   - itemTypeSubspace: Subspace for item type
    /// - Returns: Number of items processed
    private func processChunk(
        chunkIndex: Int,
        begin: [UInt8],
        end: [UInt8],
        itemTypeSubspace: Subspace
    ) async throws -> Int {
        var itemsProcessed = 0
        var lastKey: [UInt8]? = nil

        // Process in batches within this chunk
        while true {
            let currentLastKey = lastKey

            let (batchCount, newLastKey): (Int, [UInt8]?) = try await database.withTransaction(configuration: .batch) { transaction in
                var count = 0
                var processedKey: [UInt8]? = nil

                let rangeBegin = currentLastKey.map { Array($0) + [0x00] } ?? begin

                // Use ItemStorage.scan() to handle ItemEnvelope format (inline/external)
                let storage = ItemStorage(
                    transaction: transaction,
                    blobsSubspace: self.blobsSubspace
                )

                let scanSequence = storage.scan(
                    begin: rangeBegin,
                    end: end,
                    snapshot: false,
                    limit: self.batchSize
                )

                for try await (key, data) in scanSequence {
                    // Deserialize item from decompressed data
                    let item: Item = try DataAccess.deserialize(data)

                    // Extract id
                    let id = try itemTypeSubspace.unpack(key)

                    // Call IndexMaintainer to build index entry
                    try await self.indexMaintainer.scanItem(
                        item,
                        id: id,
                        transaction: transaction
                    )

                    processedKey = Array(key)
                    count += 1
                }

                return (count, processedKey)
            }

            itemsProcessed += batchCount
            lastKey = newLastKey

            // If we processed fewer than batchSize, we've reached the end of this chunk
            if batchCount < batchSize || newLastKey == nil {
                break
            }

            // Throttle if configured
            if throttleDelayMs > 0 {
                try await Task.sleep(nanoseconds: UInt64(throttleDelayMs) * 1_000_000)
            }
        }

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
/// Stores per-chunk progress in FDB to enable resumability after failures.
/// Each chunk tracks its status (not_started, in_progress, complete) and
/// the last processed key for in-progress chunks.
///
/// **Data Layout**:
/// ```
/// [indexSubspace]/_build/[indexName]/[chunkIndex]
///   → Tuple(status: Int, lastProcessedKey: Bytes?)
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
        let lastProcessedKey: [UInt8]?

        static let notStarted = ChunkProgress(status: .notStarted, lastProcessedKey: nil)
    }

    /// Subspace for progress data
    private let progressSubspace: Subspace

    /// Database for persistence
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Initialize progress tracker
    ///
    /// - Parameters:
    ///   - indexSubspace: Index subspace (progress stored under _build/)
    ///   - indexName: Name of the index being built
    ///   - database: Database for persistence
    init(indexSubspace: Subspace, indexName: String, database: any DatabaseProtocol) {
        self.progressSubspace = indexSubspace.subspace("_build").subspace(indexName)
        self.database = database
    }

    /// Load progress for all chunks
    ///
    /// - Parameter chunkCount: Total number of chunks
    /// - Returns: Dictionary of chunk index to progress
    func loadProgress(chunkCount: Int) async throws -> [Int: ChunkProgress] {
        let (begin, end) = progressSubspace.range()

        return try await database.withTransaction { transaction in
            var progress: [Int: ChunkProgress] = [:]

            let sequence = transaction.getRange(
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                snapshot: true,
                streamingMode: .wantAll
            )

            for try await (key, value) in sequence {
                guard let chunkIndex = self.extractChunkIndex(from: key) else { continue }
                guard let chunkProgress = self.decodeProgress(from: value) else { continue }
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
        lastKey: [UInt8]?
    ) async throws {
        let key = progressSubspace.pack(Tuple(chunkIndex))
        let value = encodeProgress(status: status, lastKey: lastKey)

        try await database.withTransaction(configuration: .batch) { transaction in
            transaction.setValue(value, for: key)
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
        lastKey: [UInt8]?,
        transaction: any TransactionProtocol
    ) {
        let key = progressSubspace.pack(Tuple(chunkIndex))
        let value = encodeProgress(status: status, lastKey: lastKey)
        transaction.setValue(value, for: key)
    }

    /// Clear all progress data
    ///
    /// Called on successful completion or when clearFirst is requested.
    func clearProgress() async throws {
        let (begin, end) = progressSubspace.range()

        try await database.withTransaction(configuration: .batch) { transaction in
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Private Helpers

    private func extractChunkIndex(from key: [UInt8]) -> Int? {
        do {
            let tuple = try progressSubspace.unpack(key)
            if let index = tuple[0] as? Int64 {
                return Int(index)
            } else if let index = tuple[0] as? Int {
                return index
            }
        } catch {}
        return nil
    }

    private func encodeProgress(status: ChunkStatus, lastKey: [UInt8]?) -> [UInt8] {
        if let lastKey = lastKey {
            // Use [UInt8] directly as it conforms to TupleElement (FDB.Bytes)
            return Tuple(status.rawValue, lastKey).pack()
        } else {
            return Tuple(status.rawValue).pack()
        }
    }

    private func decodeProgress(from data: [UInt8]) -> ChunkProgress? {
        do {
            let tuple = try Tuple.unpack(from: data)
            guard let statusRaw = tuple[0] as? Int64 ?? (tuple[0] as? Int).map({ Int64($0) }),
                  let status = ChunkStatus(rawValue: Int(statusRaw)) else {
                return nil
            }

            let lastKey: [UInt8]?
            if tuple.count > 1, let keyBytes = tuple[1] as? [UInt8] {
                lastKey = keyBytes
            } else {
                lastKey = nil
            }

            return ChunkProgress(status: status, lastProcessedKey: lastKey)
        } catch {
            return nil
        }
    }
}

// MARK: - OnlineIndexerError

/// Errors that can occur during online index building
public enum OnlineIndexerError: Error, CustomStringConvertible {
    /// Uniqueness violations were detected during index build
    ///
    /// The index was built but cannot be made readable because
    /// duplicate values exist. Review violations using
    /// `context.scanUniquenessViolations(for:indexName:)`.
    ///
    /// **Recovery**:
    /// 1. Scan violations to identify duplicates
    /// 2. Resolve duplicates (delete or update records)
    /// 3. Re-run the index build
    ///
    /// - Parameters:
    ///   - indexName: Name of the affected index
    ///   - violationCount: Number of distinct duplicate values
    ///   - totalConflictingRecords: Total records with duplicates
    case uniquenessViolationsDetected(
        indexName: String,
        violationCount: Int,
        totalConflictingRecords: Int
    )

    public var description: String {
        switch self {
        case .uniquenessViolationsDetected(let indexName, let violationCount, let totalRecords):
            return """
            Unique index '\(indexName)' has violations: \
            \(violationCount) duplicate value(s) affecting \(totalRecords) record(s). \
            Index remains in write-only state. \
            Use scanUniquenessViolations() to review and resolve duplicates.
            """
        }
    }
}
