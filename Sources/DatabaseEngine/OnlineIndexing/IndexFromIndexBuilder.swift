// IndexFromIndexBuilder.swift
// DatabaseEngine - Build index from existing index
//
// Reference: FDB Record Layer IndexingByIndex.java
// Builds a new index by scanning an existing index instead of the base data.
// This can significantly reduce I/O when the source index contains
// all the information needed for the target index.

import Foundation
import FoundationDB
import Core
import Metrics
import Synchronization

// MARK: - IndexSourceCompatibility

/// Compatibility of source index for building target index
public enum IndexSourceCompatibility: Sendable {
    /// Source index contains all required fields - can build directly
    case compatible

    /// Source index provides primary keys but requires data fetch for values
    case requiresDataFetch

    /// Source index cannot be used
    case incompatible(reason: String)
}

// MARK: - IndexFromIndexBuilder

/// Builds a new index by scanning an existing index
///
/// This is more efficient than scanning the base data when:
/// - The source index is smaller than the full data set
/// - The source index contains all fields needed by the target index
/// - The source index is already sorted in a useful order
///
/// **When to Use**:
/// - Adding an index on a subset of fields already covered by another index
/// - Creating a reverse index from a forward index
/// - Building aggregate indexes from value indexes
///
/// **Usage**:
/// ```swift
/// let builder = IndexFromIndexBuilder<User>(
///     database: database,
///     sourceIndex: emailIndex,
///     targetIndex: emailDomainIndex,
///     sourceMaintainer: emailMaintainer,
///     targetMaintainer: emailDomainMaintainer,
///     stateManager: stateManager
/// )
///
/// // Check compatibility first
/// let compatibility = builder.checkCompatibility()
/// guard case .compatible = compatibility else {
///     // Fall back to standard indexing
/// }
///
/// try await builder.build(clearFirst: true)
/// ```
public final class IndexFromIndexBuilder<Item: Persistable>: Sendable {
    // MARK: - Properties

    /// Database instance
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Subspace where items are stored
    private let itemSubspace: Subspace

    /// Subspace where indexes are stored
    private let indexSubspace: Subspace

    /// Subspace for blob storage (large values)
    private let blobsSubspace: Subspace

    /// Item type name
    private let itemType: String

    /// Source index to scan
    private let sourceIndex: Index

    /// Target index to build
    private let targetIndex: Index

    /// Source index maintainer
    private let sourceMaintainer: any IndexMaintainer<Item>

    /// Target index maintainer
    private let targetMaintainer: any IndexMaintainer<Item>

    /// Index state manager
    private let stateManager: IndexStateManager

    /// Throttler for batch operations
    private let throttler: AdaptiveThrottler

    /// Progress tracking key
    private let progressKey: FDB.Bytes

    // MARK: - Metrics

    private let itemsIndexedCounter: Counter
    private let batchesProcessedCounter: Counter
    private let batchDurationTimer: Metrics.Timer
    private let errorsCounter: Counter
    private let dataFetchesCounter: Counter

    // MARK: - State

    private struct State: Sendable {
        var isRunning: Bool = false
        var lastError: String?
    }

    private let state: Mutex<State>

    // MARK: - Initialization

    public init(
        database: any DatabaseProtocol,
        itemSubspace: Subspace,
        blobsSubspace: Subspace,
        indexSubspace: Subspace,
        itemType: String,
        sourceIndex: Index,
        targetIndex: Index,
        sourceMaintainer: any IndexMaintainer<Item>,
        targetMaintainer: any IndexMaintainer<Item>,
        stateManager: IndexStateManager,
        throttleConfiguration: ThrottleConfiguration = .default
    ) {
        self.database = database
        self.itemSubspace = itemSubspace
        self.blobsSubspace = blobsSubspace
        self.indexSubspace = indexSubspace
        self.itemType = itemType
        self.sourceIndex = sourceIndex
        self.targetIndex = targetIndex
        self.sourceMaintainer = sourceMaintainer
        self.targetMaintainer = targetMaintainer
        self.stateManager = stateManager
        self.throttler = AdaptiveThrottler(configuration: throttleConfiguration)
        self.state = Mutex(State())

        // Progress key
        self.progressKey = indexSubspace
            .subspace("_progress_from_index")
            .pack(Tuple(targetIndex.name, sourceIndex.name))

        // Initialize metrics
        let baseDimensions: [(String, String)] = [
            ("item_type", itemType),
            ("source_index", sourceIndex.name),
            ("target_index", targetIndex.name)
        ]

        self.itemsIndexedCounter = Counter(
            label: "fdb_index_from_index_items_total",
            dimensions: baseDimensions
        )
        self.batchesProcessedCounter = Counter(
            label: "fdb_index_from_index_batches_total",
            dimensions: baseDimensions
        )
        self.batchDurationTimer = Metrics.Timer(
            label: "fdb_index_from_index_batch_duration_seconds",
            dimensions: baseDimensions
        )
        self.errorsCounter = Counter(
            label: "fdb_index_from_index_errors_total",
            dimensions: baseDimensions
        )
        self.dataFetchesCounter = Counter(
            label: "fdb_index_from_index_data_fetches_total",
            dimensions: baseDimensions
        )
    }

    // MARK: - Compatibility Check

    /// Check if the source index is compatible with building the target index
    ///
    /// - Returns: Compatibility status
    public func checkCompatibility() -> IndexSourceCompatibility {
        // Check if source index is readable
        // In a real implementation, we would check the actual index state

        // Check if source index provides enough information
        guard let sourceKeyPaths = sourceIndex.keyPaths,
              let targetKeyPaths = targetIndex.keyPaths else {
            return .incompatible(reason: "Cannot determine index field paths")
        }

        // Check if target fields are subset of source fields
        let sourceFields = Set(sourceKeyPaths)
        let targetFields = Set(targetKeyPaths)

        if targetFields.isSubset(of: sourceFields) {
            return .compatible
        }

        // If not a subset, we can still use source for PKs but need data fetch
        return .requiresDataFetch
    }

    // MARK: - Build

    /// Build the target index from the source index
    ///
    /// - Parameters:
    ///   - clearFirst: If true, clears existing target index data
    ///   - verifyConsistency: If true, verifies entries after building
    public func build(clearFirst: Bool = false, verifyConsistency: Bool = false) async throws {
        // Check if already running
        let alreadyRunning = state.withLock { state in
            if state.isRunning { return true }
            state.isRunning = true
            return false
        }

        guard !alreadyRunning else {
            throw IndexFromIndexError.alreadyRunning
        }

        defer {
            state.withLock { state in
                state.isRunning = false
            }
        }

        // Set target index to write-only state
        try await stateManager.enable(targetIndex.name)

        // Clear if requested
        if clearFirst {
            try await clearTargetIndex()
        }

        // Check compatibility and build accordingly
        let compatibility = checkCompatibility()

        switch compatibility {
        case .compatible:
            try await buildDirectly()
        case .requiresDataFetch:
            try await buildWithDataFetch()
        case .incompatible(let reason):
            throw IndexFromIndexError.incompatibleSource(reason)
        }

        // Verify if requested
        if verifyConsistency {
            try await verifyIndex()
        }

        // Transition to readable
        try await stateManager.makeReadable(targetIndex.name)

        // Clear progress
        try await clearProgress()
    }

    // MARK: - Direct Build

    /// Build target index directly from source index entries
    private func buildDirectly() async throws {
        let sourceSubspace = indexSubspace.subspace(sourceIndex.name)
        let sourceRange = sourceSubspace.range()

        // Load or initialize progress
        var rangeSet: RangeSet
        if let saved = try await loadProgress() {
            rangeSet = saved
        } else {
            rangeSet = RangeSet(initialRange: sourceRange)
        }

        // Process batches - each batch in a separate transaction
        while let bounds = rangeSet.nextBatchBounds() {
            let batchSize = throttler.currentBatchSize
            let batchStart = DispatchTime.now()

            do {
                // Capture current rangeSet state before transaction
                let currentRangeSet = rangeSet

                // Process batch and save progress atomically in same transaction
                let (itemsInBatch, lastProcessedKey) = try await database.withTransaction(configuration: .batch) { transaction in
                    var itemsInBatch = 0
                    var lastProcessedKey: FDB.Bytes? = nil

                    // Use .iterator for adaptive batching that respects transaction limits
                    let sequence = transaction.getRange(
                        from: .firstGreaterOrEqual(bounds.begin),
                        to: .firstGreaterOrEqual(bounds.end),
                        snapshot: false,
                        streamingMode: .iterator
                    )

                    for try await (key, value) in sequence {
                        // Extract primary key from source index entry
                        guard let pk = try self.extractPrimaryKey(from: key, sourceSubspace: sourceSubspace) else {
                            continue
                        }

                        // Extract field values from source index entry
                        let fieldValues = try self.extractFieldValues(from: key, value: value, sourceSubspace: sourceSubspace)

                        // Build target index entry
                        try await self.buildTargetEntry(pk: pk, fieldValues: fieldValues, transaction: transaction)

                        lastProcessedKey = Array(key)
                        itemsInBatch += 1

                        // Break after batchSize items to limit transaction size
                        if itemsInBatch >= batchSize {
                            break
                        }
                    }

                    // Save progress atomically with work
                    var updatedRangeSet = currentRangeSet
                    if let lastKey = lastProcessedKey {
                        let isComplete = itemsInBatch < batchSize
                        updatedRangeSet.recordProgress(
                            rangeIndex: bounds.rangeIndex,
                            lastProcessedKey: lastKey,
                            isComplete: isComplete
                        )
                    } else {
                        updatedRangeSet.markRangeComplete(rangeIndex: bounds.rangeIndex)
                    }
                    try self.saveProgress(updatedRangeSet, transaction: transaction)

                    return (itemsInBatch, lastProcessedKey)
                }

                // Update in-memory rangeSet after successful commit
                if let lastKey = lastProcessedKey {
                    let isComplete = itemsInBatch < batchSize
                    rangeSet.recordProgress(
                        rangeIndex: bounds.rangeIndex,
                        lastProcessedKey: lastKey,
                        isComplete: isComplete
                    )
                } else {
                    rangeSet.markRangeComplete(rangeIndex: bounds.rangeIndex)
                }

                let batchDuration = DispatchTime.now().uptimeNanoseconds - batchStart.uptimeNanoseconds
                throttler.recordSuccess(itemCount: itemsInBatch, durationNs: batchDuration)
                batchDurationTimer.recordNanoseconds(Int64(batchDuration))
                batchesProcessedCounter.increment()
                itemsIndexedCounter.increment(by: itemsInBatch)

            } catch {
                errorsCounter.increment()
                throttler.recordFailure(error: error)

                if !throttler.isRetryable(error) {
                    throw error
                }
            }

            try await throttler.waitBeforeNextBatch()
        }
    }

    // MARK: - Build with Data Fetch

    /// Build target index using source index for PKs but fetching data
    private func buildWithDataFetch() async throws {
        let sourceSubspace = indexSubspace.subspace(sourceIndex.name)
        let sourceRange = sourceSubspace.range()
        let itemTypeSubspace = itemSubspace.subspace(itemType)

        // Load or initialize progress
        var rangeSet: RangeSet
        if let saved = try await loadProgress() {
            rangeSet = saved
        } else {
            rangeSet = RangeSet(initialRange: sourceRange)
        }

        // Process batches - each batch in a separate transaction
        while let bounds = rangeSet.nextBatchBounds() {
            let batchSize = throttler.currentBatchSize
            let batchStart = DispatchTime.now()

            do {
                // Capture current rangeSet state before transaction
                let currentRangeSet = rangeSet

                // Process batch and save progress atomically in same transaction
                let (itemsInBatch, dataFetches, lastProcessedKey) = try await database.withTransaction(configuration: .batch) { transaction in
                    var itemsInBatch = 0
                    var lastProcessedKey: FDB.Bytes? = nil
                    var dataFetches = 0

                    // Use .iterator for adaptive batching that respects transaction limits
                    let sequence = transaction.getRange(
                        from: .firstGreaterOrEqual(bounds.begin),
                        to: .firstGreaterOrEqual(bounds.end),
                        snapshot: false,
                        streamingMode: .iterator
                    )

                    // Use ItemStorage to handle ItemEnvelope format (inline/external)
                    let storage = ItemStorage(
                        transaction: transaction,
                        blobsSubspace: self.blobsSubspace
                    )

                    for try await (key, _) in sequence {
                        // Extract primary key from source index
                        guard let pk = try self.extractPrimaryKey(from: key, sourceSubspace: sourceSubspace) else {
                            continue
                        }

                        // Fetch the actual item data using ItemStorage
                        let itemKey = itemTypeSubspace.pack(pk)
                        guard let itemData = try await storage.read(for: itemKey) else {
                            continue  // Item was deleted
                        }

                        dataFetches += 1

                        // Deserialize item from ItemEnvelope-unwrapped data
                        let item: Item = try DataAccess.deserialize(itemData)

                        // Build target index entry using maintainer
                        try await self.targetMaintainer.scanItem(item, id: pk, transaction: transaction)

                        lastProcessedKey = Array(key)
                        itemsInBatch += 1

                        // Break after batchSize items to limit transaction size
                        if itemsInBatch >= batchSize {
                            break
                        }
                    }

                    // Save progress atomically with work
                    var updatedRangeSet = currentRangeSet
                    if let lastKey = lastProcessedKey {
                        let isComplete = itemsInBatch < batchSize
                        updatedRangeSet.recordProgress(
                            rangeIndex: bounds.rangeIndex,
                            lastProcessedKey: lastKey,
                            isComplete: isComplete
                        )
                    } else {
                        updatedRangeSet.markRangeComplete(rangeIndex: bounds.rangeIndex)
                    }
                    try self.saveProgress(updatedRangeSet, transaction: transaction)

                    return (itemsInBatch, dataFetches, lastProcessedKey)
                }

                // Update in-memory rangeSet after successful commit
                if let lastKey = lastProcessedKey {
                    let isComplete = itemsInBatch < batchSize
                    rangeSet.recordProgress(
                        rangeIndex: bounds.rangeIndex,
                        lastProcessedKey: lastKey,
                        isComplete: isComplete
                    )
                } else {
                    rangeSet.markRangeComplete(rangeIndex: bounds.rangeIndex)
                }

                // Update metrics outside transaction
                self.dataFetchesCounter.increment(by: dataFetches)

                let batchDuration = DispatchTime.now().uptimeNanoseconds - batchStart.uptimeNanoseconds
                throttler.recordSuccess(itemCount: itemsInBatch, durationNs: batchDuration)
                batchDurationTimer.recordNanoseconds(Int64(batchDuration))
                batchesProcessedCounter.increment()
                itemsIndexedCounter.increment(by: itemsInBatch)

            } catch {
                errorsCounter.increment()
                throttler.recordFailure(error: error)

                if !throttler.isRetryable(error) {
                    throw error
                }
            }

            try await throttler.waitBeforeNextBatch()
        }
    }

    // MARK: - Helper Methods

    /// Extract primary key from source index entry
    private func extractPrimaryKey(from key: FDB.Bytes, sourceSubspace: Subspace) throws -> Tuple? {
        let indexTuple = try sourceSubspace.unpack(key)

        // Primary key is typically the last element(s) of the index key
        // This depends on the index structure
        guard indexTuple.count > 0 else { return nil }

        // For simple indexes, the last element is the PK
        // For compound indexes, we need to know how many fields are in the index
        if let pkElement = indexTuple[indexTuple.count - 1] {
            return Tuple(pkElement)
        }

        return nil
    }

    /// Extract field values from source index entry
    private func extractFieldValues(from key: FDB.Bytes, value: FDB.Bytes, sourceSubspace: Subspace) throws -> [any TupleElement] {
        let indexTuple = try sourceSubspace.unpack(key)

        // Field values are all elements except the last (which is the PK)
        var values: [any TupleElement] = []
        for i in 0..<(indexTuple.count - 1) {
            if let elem = indexTuple[i] {
                values.append(elem)
            }
        }

        return values
    }

    /// Build a target index entry
    private func buildTargetEntry(pk: Tuple, fieldValues: [any TupleElement], transaction: any TransactionProtocol) async throws {
        // This is a simplified implementation
        // In reality, we would need to map source fields to target fields
        let targetSubspace = indexSubspace.subspace(targetIndex.name)

        // Build target key: [field values][pk]
        var targetElements: [any TupleElement] = fieldValues
        for i in 0..<pk.count {
            if let elem = pk[i] {
                targetElements.append(elem)
            }
        }

        let targetKey = targetSubspace.pack(Tuple(targetElements))
        transaction.setValue([], for: targetKey)
    }

    // MARK: - Progress Management

    private func loadProgress() async throws -> RangeSet? {
        try await database.withTransaction(configuration: .batch) { transaction in
            guard let bytes = try await transaction.getValue(for: self.progressKey) else {
                return nil
            }
            return try JSONDecoder().decode(RangeSet.self, from: Data(bytes))
        }
    }

    private func saveProgress(_ rangeSet: RangeSet, transaction: any TransactionProtocol) throws {
        let data = try JSONEncoder().encode(rangeSet)
        transaction.setValue(Array(data), for: progressKey)
    }

    private func clearProgress() async throws {
        try await database.withTransaction(configuration: .batch) { transaction in
            transaction.clear(key: self.progressKey)
        }
    }

    // MARK: - Index Management

    private func clearTargetIndex() async throws {
        try await database.withTransaction(configuration: .batch) { transaction in
            let targetRange = self.indexSubspace.subspace(self.targetIndex.name).range()
            transaction.clearRange(beginKey: targetRange.begin, endKey: targetRange.end)
        }
    }

    /// Verify target index correctness using sample-based verification
    ///
    /// **Verification Strategy**:
    /// Uses reservoir sampling to select a statistically representative sample
    /// of entries from the source index, then verifies corresponding target
    /// index entries exist.
    ///
    /// Reference: Vitter, J.S. "Random Sampling with a Reservoir", ACM TOMS 1985
    ///
    /// - Parameters:
    ///   - sampleSize: Number of entries to verify (default: 1000)
    ///   - errorThreshold: Maximum allowed missing entry ratio (default: 0.001 = 0.1%)
    /// - Throws: `IndexFromIndexError.buildFailed` if verification fails
    private func verifyIndex(sampleSize: Int = 1000, errorThreshold: Double = 0.001) async throws {
        let sourceSubspace = indexSubspace.subspace(sourceIndex.name)
        let targetSubspace = indexSubspace.subspace(targetIndex.name)
        let sourceRange = sourceSubspace.range()

        // Collect sample using reservoir sampling
        // Move reservoir and itemsSeen inside transaction to avoid Sendable capture issues
        let reservoir: [(key: FDB.Bytes, value: FDB.Bytes)] = try await database.withTransaction(configuration: .batch) { transaction in
            var reservoir: [(key: FDB.Bytes, value: FDB.Bytes)] = []
            var itemsSeen = 0

            let sequence = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(sourceRange.begin),
                endSelector: .firstGreaterOrEqual(sourceRange.end),
                snapshot: true
            )

            for try await (key, value) in sequence {
                itemsSeen += 1

                if reservoir.count < sampleSize {
                    // Fill reservoir
                    reservoir.append((Array(key), Array(value)))
                } else {
                    // Reservoir sampling: replace with probability sampleSize/itemsSeen
                    let randomIndex = Int.random(in: 0..<itemsSeen)
                    if randomIndex < sampleSize {
                        reservoir[randomIndex] = (Array(key), Array(value))
                    }
                }
            }

            return reservoir
        }

        guard !reservoir.isEmpty else {
            // No items to verify, index is empty - this is valid
            return
        }

        // Verify sampled entries exist in target index
        var missingCount = 0
        var verifiedCount = 0

        // Process in batches to avoid long transactions
        let verifyBatchSize = min(100, reservoir.count)
        for batchStart in stride(from: 0, to: reservoir.count, by: verifyBatchSize) {
            let batchEnd = min(batchStart + verifyBatchSize, reservoir.count)
            let batch = Array(reservoir[batchStart..<batchEnd])

            let (batchMissing, batchVerified) = try await database.withTransaction(configuration: .batch) { transaction in
                var batchMissingCount = 0
                var batchVerifiedCount = 0

                for (sourceKey, sourceValue) in batch {
                    // Extract PK and field values from source
                    guard let pk = try self.extractPrimaryKey(from: sourceKey, sourceSubspace: sourceSubspace) else {
                        continue
                    }
                    let fieldValues = try self.extractFieldValues(from: sourceKey, value: sourceValue, sourceSubspace: sourceSubspace)

                    // Build expected target key
                    var targetElements: [any TupleElement] = fieldValues
                    for i in 0..<pk.count {
                        if let elem = pk[i] {
                            targetElements.append(elem)
                        }
                    }
                    let expectedTargetKey = targetSubspace.pack(Tuple(targetElements))

                    // Check if target entry exists
                    let targetValue = try await transaction.getValue(for: expectedTargetKey)
                    if targetValue == nil {
                        batchMissingCount += 1
                    }
                    batchVerifiedCount += 1
                }

                return (batchMissingCount, batchVerifiedCount)
            }

            // Accumulate results outside transaction
            missingCount += batchMissing
            verifiedCount += batchVerified
        }

        // Check error rate
        let errorRate = Double(missingCount) / Double(verifiedCount)
        if errorRate > errorThreshold {
            throw IndexFromIndexError.buildFailed(
                "Verification failed: \(missingCount)/\(verifiedCount) entries missing " +
                "(\(String(format: "%.2f", errorRate * 100))% error rate exceeds " +
                "\(String(format: "%.2f", errorThreshold * 100))% threshold)"
            )
        }
    }

    // MARK: - Status

    /// Get current build progress
    public func getProgress() async throws -> IndexBuildProgress {
        guard let rangeSet = try await loadProgress() else {
            return IndexBuildProgress(
                isComplete: false,
                percentComplete: 0,
                itemsProcessed: 0,
                errorCount: 0
            )
        }

        let stats = throttler.statistics

        // Calculate percentage based on RangeSet progress estimate
        let percentComplete: Int
        if rangeSet.isEmpty {
            percentComplete = 100
        } else {
            // Get progress estimate from RangeSet (0.0 to 1.0)
            percentComplete = Int(rangeSet.progressEstimate * 100)
        }

        return IndexBuildProgress(
            isComplete: rangeSet.isEmpty,
            percentComplete: percentComplete,
            itemsProcessed: stats.totalItemsProcessed,
            errorCount: stats.totalFailures
        )
    }
}

// MARK: - IndexBuildProgress

/// Progress information for index building
public struct IndexBuildProgress: Sendable {
    /// Whether building is complete
    public let isComplete: Bool

    /// Estimated percentage complete
    public let percentComplete: Int

    /// Number of items processed
    public let itemsProcessed: Int

    /// Number of errors encountered
    public let errorCount: Int
}

// MARK: - IndexFromIndexError

/// Errors from index-from-index building
public enum IndexFromIndexError: Error, CustomStringConvertible, Sendable {
    case alreadyRunning
    case incompatibleSource(String)
    case sourceIndexNotReadable
    case buildFailed(String)

    public var description: String {
        switch self {
        case .alreadyRunning:
            return "Index build is already running"
        case .incompatibleSource(let reason):
            return "Source index is incompatible: \(reason)"
        case .sourceIndexNotReadable:
            return "Source index is not in readable state"
        case .buildFailed(let reason):
            return "Index build failed: \(reason)"
        }
    }
}
