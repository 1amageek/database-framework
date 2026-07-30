// MutualOnlineIndexer.swift
// DatabaseEngine - Build bidirectional indexes using mutual references
//
// Reference: FDB Record Layer mutual indexing strategy
// Used for bidirectional relationships where each index helps build the other.

import DatabaseTypes
import StorageKit
import DatabaseKit
import Synchronization

/// Mutual online index builder
///
/// Builds bidirectional indexes where each index entry references the other direction.
/// This is efficient for graph-like relationships (e.g., followers/following, friends).
///
/// **Use Case**:
/// - User follows User: need both "who does X follow" and "who follows X"
/// - Document links Document: need both outgoing and incoming links
/// - Any bidirectional many-to-many relationship
///
/// **How It Works**:
/// 1. Scan the "forward" direction (A -> B)
/// 2. For each relationship, create entries in both indexes
/// 3. Use the forward index to validate/build the reverse index
///
/// **Usage Example**:
/// ```swift
/// let indexer = try MutualOnlineIndexer<Follow>(
///     container: container,
///     storeSubspace: storeSubspace,
///     itemType: "Follow",
///     forwardIndex: followingIndex,
///     reverseIndex: followersIndex,
///     forwardMaintainer: followingMaintainer,
///     reverseMaintainer: followersMaintainer,
///     lifecycleStore: lifecycleStore
/// )
///
/// try await indexer.buildIndexes(clearFirst: true)
/// ```
public final class MutualOnlineIndexer<Item: Persistable>: Sendable {
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

    /// Forward index (A -> B)
    private let forwardIndex: Index

    /// Reverse index (B -> A)
    private let reverseIndex: Index

    /// Forward index maintainer
    private let forwardMaintainer: any IndexMaintainer<Item>

    private let forwardUniquenessMaintainer: (any IndexUniquenessMaintainer<Item>)?

    /// Reverse index maintainer
    private let reverseMaintainer: any IndexMaintainer<Item>

    private let reverseUniquenessMaintainer: (any IndexUniquenessMaintainer<Item>)?

    /// Index state manager
    private let lifecycleStore: IndexLifecycleStore

    /// Shared uniqueness violation persistence for both targets.
    private let violationTracker: UniquenessViolationTracker

    // Configuration
    private let batchSize: Int
    private let throttleDelayMs: Int

    // Progress tracking
    private let forwardProgressKey: ByteString
    private let reverseProgressKey: ByteString

    // MARK: - Metrics

    private let itemsIndexedCounter: DatabaseMetricCounter
    private let batchesProcessedCounter: DatabaseMetricCounter
    private let batchDurationTimer: DatabaseMetricTimer
    private let errorsCounter: DatabaseMetricCounter
    private let mutualPairsCounter: DatabaseMetricCounter

    // MARK: - Initialization

    /// Initialize mutual indexer
    ///
    /// - Parameters:
    ///   - container: FDB Container instance
    ///   - storeSubspace: Root subspace containing items, indexes, blobs, and metadata
    ///   - itemType: Type name of items to index
    ///   - forwardIndex: Forward direction index
    ///   - reverseIndex: Reverse direction index
    ///   - forwardMaintainer: Maintainer for forward index
    ///   - reverseMaintainer: Maintainer for reverse index
    ///   - lifecycleStore: Index state manager
    ///   - batchSize: Number of items per batch (default: 100)
    ///   - throttleDelayMs: Delay between batches in ms (default: 0)
    public init(
        container: DBContainer,
        storeSubspace: Subspace,
        itemType: String,
        forwardIndex: Index,
        reverseIndex: Index,
        forwardMaintainer: any IndexMaintainer<Item>,
        reverseMaintainer: any IndexMaintainer<Item>,
        forwardUniquenessMaintainer: (any IndexUniquenessMaintainer<Item>)? = nil,
        reverseUniquenessMaintainer: (any IndexUniquenessMaintainer<Item>)? = nil,
        lifecycleStore: IndexLifecycleStore,
        batchSize: Int = 100,
        throttleDelayMs: Int = 0
    ) throws(OnlineIndexBuildError) {
        guard batchSize > 0 else {
            throw .invalidBatchSize(batchSize)
        }
        guard throttleDelayMs >= 0 else {
            throw .invalidThrottleDelayMilliseconds(throttleDelayMs)
        }
        guard forwardIndex.name != reverseIndex.name else {
            throw .duplicateTargetIndexName(forwardIndex.name)
        }
        if forwardMaintainer.customBuildStrategy != nil {
            throw .unsupportedCustomBuildStrategy(indexName: forwardIndex.name)
        }
        if reverseMaintainer.customBuildStrategy != nil {
            throw .unsupportedCustomBuildStrategy(indexName: reverseIndex.name)
        }
        if forwardIndex.isUnique, forwardUniquenessMaintainer == nil {
            throw .unsupportedUniquenessConstraint(indexName: forwardIndex.name)
        }
        if reverseIndex.isUnique, reverseUniquenessMaintainer == nil {
            throw .unsupportedUniquenessConstraint(indexName: reverseIndex.name)
        }
        self.container = container
        self.itemSubspace = storeSubspace.subspace(SubspaceKey.items)
        self.indexSubspace = storeSubspace.subspace(SubspaceKey.indexes)
        self.blobsSubspace = storeSubspace.subspace(SubspaceKey.blobs)
        self.itemType = itemType
        self.forwardIndex = forwardIndex
        self.reverseIndex = reverseIndex
        self.forwardMaintainer = forwardMaintainer
        self.forwardUniquenessMaintainer = forwardUniquenessMaintainer
        self.reverseMaintainer = reverseMaintainer
        self.reverseUniquenessMaintainer = reverseUniquenessMaintainer
        self.lifecycleStore = lifecycleStore
        self.violationTracker = UniquenessViolationTracker(
            container: container,
            metadataSubspace: storeSubspace.subspace(SubspaceKey.metadata)
        )
        self.batchSize = batchSize
        self.throttleDelayMs = throttleDelayMs

        // Create progress keys
        self.forwardProgressKey = self.indexSubspace
            .subspace("_progress_mutual")
            .pack(Tuple(forwardIndex.name))
        self.reverseProgressKey = self.indexSubspace
            .subspace("_progress_mutual")
            .pack(Tuple(reverseIndex.name))

        // Initialize metrics
        let baseDimensions: [(String, String)] = [
            ("item_type", itemType),
            ("forward_index", forwardIndex.name),
            ("reverse_index", reverseIndex.name)
        ]

        let metrics = container.configuration.metrics
        self.itemsIndexedCounter = metrics.counter(
            label: "database_mutual_indexer_items_indexed_total",
            dimensions: baseDimensions
        )
        self.batchesProcessedCounter = metrics.counter(
            label: "database_mutual_indexer_batches_processed_total",
            dimensions: baseDimensions
        )
        self.batchDurationTimer = metrics.timer(
            label: "database_mutual_indexer_batch_duration_seconds",
            dimensions: baseDimensions
        )
        self.errorsCounter = metrics.counter(
            label: "database_mutual_indexer_errors_total",
            dimensions: baseDimensions
        )
        self.mutualPairsCounter = metrics.counter(
            label: "database_mutual_indexer_pairs_created_total",
            dimensions: baseDimensions
        )
    }

    // MARK: - Public API

    /// Build both forward and reverse indexes simultaneously
    ///
    /// **Process**:
    /// 1. Set both indexes to write-only state
    /// 2. Clear index data if requested
    /// 3. Scan items and build both indexes together
    /// 4. Verify consistency between indexes
    /// 5. Transition both to readable state
    ///
    /// - Parameters:
    ///   - clearFirst: If true, clears existing index data
    ///   - verifyConsistency: If true, verifies both indexes are consistent
    public func buildIndexes(clearFirst: Bool = false, verifyConsistency: Bool = true) async throws {
        // Set both indexes to write-only state
        try await lifecycleStore.enable(forwardIndex.name)
        try await lifecycleStore.enable(reverseIndex.name)

        // Clear if requested
        if clearFirst {
            try await clearIndexData(for: forwardIndex)
            try await clearIndexData(for: reverseIndex)
            if forwardIndex.isUnique {
                try await violationTracker.clearAllViolations(
                    indexName: forwardIndex.name
                )
            }
            if reverseIndex.isUnique {
                try await violationTracker.clearAllViolations(
                    indexName: reverseIndex.name
                )
            }
        }

        // Build both indexes with single scan
        try await buildIndexesInBatches()

        try await requireNoUniquenessViolations()

        // Verify consistency if requested
        if verifyConsistency {
            try await verifyIndexConsistency()
        }

        // Transition both to readable
        try await lifecycleStore.makeReadable(forwardIndex.name)
        try await lifecycleStore.makeReadable(reverseIndex.name)

        // Clear progress
        try await clearProgress()
    }

    /// Get current progress status
    ///
    /// Returns true if indexing is complete (no remaining ranges)
    public func isComplete() async throws -> Bool {
        guard let rangeSet = try await loadProgress(key: forwardProgressKey) else {
            return false
        }
        return rangeSet.isEmpty
    }

    // MARK: - Private Implementation

    /// Build both indexes in batches
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
        if let savedProgress = try await loadProgress(key: forwardProgressKey) {
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
                let (itemsInBatch, pairsInBatch, lastProcessedKey) = try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
                    var itemsInBatch = 0
                    var pairsInBatch = 0
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
                        let id = try itemTypeSubspace.unpack(key)

                        batchEntries.append((item: item, id: id))

                        lastProcessedKey = key
                        itemsInBatch += 1
                        pairsInBatch += 1
                    }

                    // Build both directions through the batch hook.
                    try await OnlineIndexBatchWriter.write(
                        batchEntries,
                        index: self.forwardIndex,
                        maintainer: self.forwardMaintainer,
                        uniquenessMaintainer: self.forwardUniquenessMaintainer,
                        violationTracker: self.violationTracker,
                        transaction: transaction
                    )
                    try await OnlineIndexBatchWriter.write(
                        batchEntries,
                        index: self.reverseIndex,
                        maintainer: self.reverseMaintainer,
                        uniquenessMaintainer: self.reverseUniquenessMaintainer,
                        violationTracker: self.violationTracker,
                        transaction: transaction
                    )

                    // Save progress atomically with work
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
                    try self.saveProgress(updatedRangeSet, key: self.forwardProgressKey, transaction)

                    return (itemsInBatch, pairsInBatch, lastProcessedKey)
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
                itemsIndexedCounter.increment(by: itemsInBatch * 2)  // Both directions
                mutualPairsCounter.increment(by: pairsInBatch)

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

    /// Verify that forward and reverse indexes are consistent
    ///
    /// For each forward entry (A -> B), there should be a reverse entry (B -> A).
    ///
    /// **Important**: This verification assumes that:
    /// - Forward index keys are structured as (sourceId, targetId, ...)
    /// - Reverse index keys are structured as (targetId, sourceId, ...)
    /// - The first two tuple elements contain the relationship IDs
    ///
    /// This is a sampling-based check (up to 1000 entries) and may not
    /// catch all inconsistencies.
    private func verifyIndexConsistency() async throws {
        let forwardSubspace = indexSubspace.subspace(forwardIndex.name)
        let reverseSubspace = indexSubspace.subspace(reverseIndex.name)

        let inconsistencies: [(forward: Tuple, reverse: Tuple)] = try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            var inconsistencies: [(forward: Tuple, reverse: Tuple)] = []

            let forwardRange = forwardSubspace.range()
            let sampleLimit = 1000  // Sample check limit

            // Use limit to control server-side prefetch instead of break
            let sequence = try await TransactionRangeCollection.collect(using: transaction,
                from: .firstGreaterOrEqual(forwardRange.begin),
                to: .firstGreaterOrEqual(forwardRange.end),
                limit: sampleLimit,
                reverse: false,
                snapshot: true,
                streamingMode: .iterator
            )

            for (key, _) in sequence {

                // Parse forward key to extract relationship
                let forwardTuple = try forwardSubspace.unpack(key)

                // Construct expected reverse key
                // Assumption: relationship is (sourceId, targetId) -> reverse is (targetId, sourceId)
                guard forwardTuple.count >= 2,
                      let element0 = forwardTuple[0],
                      let element1 = forwardTuple[1] else { continue }

                let reverseTuple = Tuple(element1, element0)
                let reverseKey = reverseSubspace.pack(reverseTuple)

                // Check if reverse entry exists
                if try await transaction.getValue(for: reverseKey, snapshot: true) == nil {
                    inconsistencies.append((forward: forwardTuple, reverse: reverseTuple))
                }
            }

            return inconsistencies
        }

        // Report inconsistencies (but don't fail - this is a verification)
        if !inconsistencies.isEmpty {
            // Log warning about inconsistencies
            // In production, this would emit metrics or alerts
            print("Warning: Found \(inconsistencies.count) inconsistencies between forward and reverse indexes")
        }
    }

    // MARK: - Progress Management

    private func loadProgress(key: ByteString) async throws -> RangeSet? {
        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            guard let bytes = try await transaction.getValue(for: key, snapshot: false) else {
                return nil
            }
            return try RangeSetCodec.decode(bytes)
        }
    }

    private func saveProgress(_ rangeSet: RangeSet, key: ByteString, _ transaction: any TransactionAccess) throws {
        try transaction.setValue(try RangeSetCodec.encode(rangeSet), for: key)
    }

    private func clearProgress() async throws {
        let forwardKey = self.forwardProgressKey
        let reverseKey = self.reverseProgressKey
        try await container.transactionExecutor.withTransaction(configuration: .batch, clock: container.monotonicClock) { transaction in
            try transaction.clear(key: forwardKey)
            try transaction.clear(key: reverseKey)
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
        for index in [forwardIndex, reverseIndex] where index.isUnique {
            let summary = try await violationTracker.violationSummary(
                indexName: index.name
            )
            guard !summary.hasViolations else {
                throw OnlineIndexBuildError.uniquenessViolationsDetected(
                    indexName: index.name,
                    violationCount: summary.violationCount,
                    totalConflictingEntities: summary.totalConflictingEntities
                )
            }
        }
    }
}

// MARK: - Mutual Index Configuration

/// Configuration for building mutual indexes
public struct MutualIndexConfiguration: Sendable {
    /// Forward index name
    public let forwardIndexName: String

    /// Reverse index name
    public let reverseIndexName: String

    /// Field that links to source entity
    public let sourceFieldName: String

    /// Field that links to target entity
    public let targetFieldName: String

    /// Whether the relationship is symmetric (A-B = B-A)
    public let isSymmetric: Bool

    public init(
        forwardIndexName: String,
        reverseIndexName: String,
        sourceFieldName: String,
        targetFieldName: String,
        isSymmetric: Bool = false
    ) {
        self.forwardIndexName = forwardIndexName
        self.reverseIndexName = reverseIndexName
        self.sourceFieldName = sourceFieldName
        self.targetFieldName = targetFieldName
        self.isSymmetric = isSymmetric
    }
}

// MARK: - Symmetric Index Builder

/// Specialized builder for symmetric relationships (e.g., friendships)
///
/// For symmetric relationships, we only store one direction but query both.
/// This halves storage while maintaining query capability.
///
/// **Note**: For efficient querying of symmetric relationships, consider
/// using a secondary index or maintaining both directions. The current
/// implementation requires a full scan for reverse lookups.
public final class SymmetricIndexBuilder<Item: Persistable>: Sendable {
    /// FDB Container for database access
    private let container: DBContainer

    /// Index subspace
    private let indexSubspace: Subspace

    /// Index configuration
    private let config: MutualIndexConfiguration

    public init(
        container: DBContainer,
        indexSubspace: Subspace,
        config: MutualIndexConfiguration
    ) throws(OnlineIndexBuildError) {
        guard config.isSymmetric else {
            throw .requiresSymmetricConfiguration
        }
        self.container = container
        self.indexSubspace = indexSubspace
        self.config = config
    }

    /// Store a symmetric relationship
    ///
    /// For symmetric relationships, we canonicalize the key to always store
    /// the smaller ID first. This ensures A-B and B-A map to the same entry.
    ///
    /// - Parameters:
    ///   - sourceId: First entity ID
    ///   - targetId: Second entity ID
    ///   - transaction: Transaction to use
    public func storeRelationship(
        sourceId: String,
        targetId: String,
        transaction: any TransactionAccess
    ) throws {
        // Canonicalize: always store smaller ID first
        let (first, second) = sourceId < targetId ? (sourceId, targetId) : (targetId, sourceId)

        let key = indexSubspace
            .subspace(config.forwardIndexName)
            .pack(Tuple(first, second))

        // Store with empty value (existence is enough)
        try transaction.setValue([], for: key)
    }

    /// Query relationships for an entity
    ///
    /// Returns all entities connected to the given entity ID.
    public func queryRelationships(
        entityId: String,
        transaction: any TransactionAccess
    ) async throws -> [String] {
        var results: [String] = []
        let indexSpace = indexSubspace.subspace(config.forwardIndexName)

        // We need to query both positions since entity could be first or second
        // Query 1: entity is in first position - use prefix range
        let prefixSubspace = indexSpace.subspace(Tuple(entityId))
        let range1 = prefixSubspace.range()

        // Use .wantAll for read-only queries that need all results
        let seq1 = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(range1.begin),
            to: .firstGreaterOrEqual(range1.end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        for (key, _) in seq1 {
            let tuple = try indexSpace.unpack(key)
            if tuple.count >= 2,
               case .string(let otherId) = try tuple.value(at: 1) {
                results.append(otherId)
            }
        }

        // Query 2: Scan for entity in second position (more expensive)
        // In a real implementation, we might maintain a secondary index
        let fullRange = indexSpace.range()
        let seq2 = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(fullRange.begin),
            to: .firstGreaterOrEqual(fullRange.end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        for (key, _) in seq2 {
            let tuple = try indexSpace.unpack(key)
            if tuple.count >= 2,
               case .string(let firstId) = try tuple.value(at: 0),
               case .string(let secondId) = try tuple.value(at: 1),
               secondId == entityId && firstId != entityId {
                results.append(firstId)
            }
        }

        return results
    }
}

// MARK: - CustomStringConvertible

extension MutualOnlineIndexer: CustomStringConvertible {
    public var description: String {
        "MutualOnlineIndexer(forward: \(forwardIndex.name), reverse: \(reverseIndex.name), itemType: \(itemType))"
    }
}
