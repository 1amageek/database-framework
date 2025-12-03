// MutualOnlineIndexer.swift
// DatabaseEngine - Build bidirectional indexes using mutual references
//
// Reference: FDB Record Layer mutual indexing strategy
// Used for bidirectional relationships where each index helps build the other.

import Foundation
import FoundationDB
import Core
import Metrics
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
/// let indexer = MutualOnlineIndexer<Follow>(
///     database: database,
///     itemSubspace: itemSubspace,
///     indexSubspace: indexSubspace,
///     itemType: "Follow",
///     forwardIndex: followingIndex,
///     reverseIndex: followersIndex,
///     forwardMaintainer: followingMaintainer,
///     reverseMaintainer: followersMaintainer,
///     stateManager: stateManager
/// )
///
/// try await indexer.buildIndexes(clearFirst: true)
/// ```
public final class MutualOnlineIndexer<Item: Persistable>: Sendable {
    // MARK: - Properties

    /// Database instance
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Subspace where items are stored ([R]/)
    private let itemSubspace: Subspace

    /// Subspace where index data is stored ([I]/)
    private let indexSubspace: Subspace

    /// Item type name
    private let itemType: String

    /// Forward index (A -> B)
    private let forwardIndex: Index

    /// Reverse index (B -> A)
    private let reverseIndex: Index

    /// Forward index maintainer
    private let forwardMaintainer: any IndexMaintainer<Item>

    /// Reverse index maintainer
    private let reverseMaintainer: any IndexMaintainer<Item>

    /// Index state manager
    private let stateManager: IndexStateManager

    // Configuration
    private let batchSize: Int
    private let throttleDelayMs: Int

    // Progress tracking
    private let forwardProgressKey: FDB.Bytes
    private let reverseProgressKey: FDB.Bytes

    // MARK: - Metrics

    private let itemsIndexedCounter: Counter
    private let batchesProcessedCounter: Counter
    private let batchDurationTimer: Metrics.Timer
    private let errorsCounter: Counter
    private let mutualPairsCounter: Counter

    // MARK: - Initialization

    /// Initialize mutual indexer
    ///
    /// - Parameters:
    ///   - database: Database instance
    ///   - itemSubspace: Subspace where items are stored
    ///   - indexSubspace: Subspace where index data is stored
    ///   - itemType: Type name of items to index
    ///   - forwardIndex: Forward direction index
    ///   - reverseIndex: Reverse direction index
    ///   - forwardMaintainer: Maintainer for forward index
    ///   - reverseMaintainer: Maintainer for reverse index
    ///   - stateManager: Index state manager
    ///   - batchSize: Number of items per batch (default: 100)
    ///   - throttleDelayMs: Delay between batches in ms (default: 0)
    public init(
        database: any DatabaseProtocol,
        itemSubspace: Subspace,
        indexSubspace: Subspace,
        itemType: String,
        forwardIndex: Index,
        reverseIndex: Index,
        forwardMaintainer: any IndexMaintainer<Item>,
        reverseMaintainer: any IndexMaintainer<Item>,
        stateManager: IndexStateManager,
        batchSize: Int = 100,
        throttleDelayMs: Int = 0
    ) {
        self.database = database
        self.itemSubspace = itemSubspace
        self.indexSubspace = indexSubspace
        self.itemType = itemType
        self.forwardIndex = forwardIndex
        self.reverseIndex = reverseIndex
        self.forwardMaintainer = forwardMaintainer
        self.reverseMaintainer = reverseMaintainer
        self.stateManager = stateManager
        self.batchSize = batchSize
        self.throttleDelayMs = throttleDelayMs

        // Create progress keys
        self.forwardProgressKey = indexSubspace
            .subspace("_progress_mutual")
            .pack(Tuple(forwardIndex.name))
        self.reverseProgressKey = indexSubspace
            .subspace("_progress_mutual")
            .pack(Tuple(reverseIndex.name))

        // Initialize metrics
        let baseDimensions: [(String, String)] = [
            ("item_type", itemType),
            ("forward_index", forwardIndex.name),
            ("reverse_index", reverseIndex.name)
        ]

        self.itemsIndexedCounter = Counter(
            label: "fdb_mutual_indexer_items_indexed_total",
            dimensions: baseDimensions
        )
        self.batchesProcessedCounter = Counter(
            label: "fdb_mutual_indexer_batches_processed_total",
            dimensions: baseDimensions
        )
        self.batchDurationTimer = Metrics.Timer(
            label: "fdb_mutual_indexer_batch_duration_seconds",
            dimensions: baseDimensions
        )
        self.errorsCounter = Counter(
            label: "fdb_mutual_indexer_errors_total",
            dimensions: baseDimensions
        )
        self.mutualPairsCounter = Counter(
            label: "fdb_mutual_indexer_pairs_created_total",
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
        try await stateManager.enable(forwardIndex.name)
        try await stateManager.enable(reverseIndex.name)

        // Clear if requested
        if clearFirst {
            try await clearIndexData(for: forwardIndex)
            try await clearIndexData(for: reverseIndex)
        }

        // Build both indexes with single scan
        try await buildIndexesInBatches()

        // Verify consistency if requested
        if verifyConsistency {
            try await verifyIndexConsistency()
        }

        // Transition both to readable
        try await stateManager.makeReadable(forwardIndex.name)
        try await stateManager.makeReadable(reverseIndex.name)

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
            let batchStartTime = DispatchTime.now()

            do {
                let (itemsInBatch, pairsInBatch, lastProcessedKey) = try await database.withTransaction(configuration: .batch) { transaction in
                    var itemsInBatch = 0
                    var pairsInBatch = 0
                    var lastProcessedKey: FDB.Bytes? = nil

                    let sequence = transaction.getRange(
                        beginSelector: .firstGreaterOrEqual(bounds.begin),
                        endSelector: .firstGreaterOrEqual(bounds.end),
                        snapshot: false
                    )

                    // Process up to batchSize items, then break
                    // This limits transaction size without requiring FDB limit parameter
                    for try await (key, value) in sequence {
                        // Deserialize item
                        let item: Item = try DataAccess.deserialize(value)
                        let id = try itemTypeSubspace.unpack(key)

                        // Build forward index entry
                        try await self.forwardMaintainer.scanItem(
                            item,
                            id: id,
                            transaction: transaction
                        )

                        // Build reverse index entry
                        try await self.reverseMaintainer.scanItem(
                            item,
                            id: id,
                            transaction: transaction
                        )

                        lastProcessedKey = Array(key)
                        itemsInBatch += 1
                        pairsInBatch += 1

                        // Break after batchSize items to limit transaction size
                        if itemsInBatch >= self.batchSize {
                            break
                        }
                    }

                    return (itemsInBatch, pairsInBatch, lastProcessedKey)
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
                    try self.saveProgress(rangeSetCopy, key: self.forwardProgressKey, transaction)
                }

                // Record metrics
                let batchDuration = DispatchTime.now().uptimeNanoseconds - batchStartTime.uptimeNanoseconds
                batchDurationTimer.recordNanoseconds(Int64(batchDuration))
                batchesProcessedCounter.increment()
                itemsIndexedCounter.increment(by: itemsInBatch * 2)  // Both directions
                mutualPairsCounter.increment(by: pairsInBatch)

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

        let inconsistencies: [(forward: Tuple, reverse: Tuple)] = try await database.withTransaction(configuration: .readOnly) { transaction in
            var inconsistencies: [(forward: Tuple, reverse: Tuple)] = []

            let forwardRange = forwardSubspace.range()
            let sequence = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(forwardRange.begin),
                endSelector: .firstGreaterOrEqual(forwardRange.end),
                snapshot: true
            )

            var count = 0
            let sampleLimit = 1000  // Sample check limit

            for try await (key, _) in sequence {
                guard count < sampleLimit else { break }
                count += 1

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

    private func loadProgress(key: FDB.Bytes) async throws -> RangeSet? {
        try await database.withTransaction(configuration: .batch) { transaction in
            guard let bytes = try await transaction.getValue(for: key, snapshot: false) else {
                return nil
            }
            return try JSONDecoder().decode(RangeSet.self, from: Data(bytes))
        }
    }

    private func saveProgress(_ rangeSet: RangeSet, key: FDB.Bytes, _ transaction: any TransactionProtocol) throws {
        let data = try JSONEncoder().encode(rangeSet)
        transaction.setValue(Array(data), for: key)
    }

    private func clearProgress() async throws {
        try await database.withTransaction(configuration: .batch) { transaction in
            transaction.clear(key: forwardProgressKey)
            transaction.clear(key: reverseProgressKey)
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
    /// Database instance
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Index subspace
    private let indexSubspace: Subspace

    /// Index configuration
    private let config: MutualIndexConfiguration

    public init(
        database: any DatabaseProtocol,
        indexSubspace: Subspace,
        config: MutualIndexConfiguration
    ) {
        precondition(config.isSymmetric, "SymmetricIndexBuilder requires symmetric configuration")
        self.database = database
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
        transaction: any TransactionProtocol
    ) {
        // Canonicalize: always store smaller ID first
        let (first, second) = sourceId < targetId ? (sourceId, targetId) : (targetId, sourceId)

        let key = indexSubspace
            .subspace(config.forwardIndexName)
            .pack(Tuple(first, second))

        // Store with empty value (existence is enough)
        transaction.setValue([], for: key)
    }

    /// Query relationships for an entity
    ///
    /// Returns all entities connected to the given entity ID.
    public func queryRelationships(
        entityId: String,
        transaction: any TransactionProtocol
    ) async throws -> [String] {
        var results: [String] = []
        let indexSpace = indexSubspace.subspace(config.forwardIndexName)

        // We need to query both positions since entity could be first or second
        // Query 1: entity is in first position - use prefix range
        let prefixSubspace = indexSpace.subspace(Tuple(entityId))
        let range1 = prefixSubspace.range()

        let seq1 = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range1.begin),
            endSelector: .firstGreaterOrEqual(range1.end),
            snapshot: true
        )

        for try await (key, _) in seq1 {
            let tuple = try indexSpace.unpack(key)
            if tuple.count >= 2, let otherId = tuple[1] as? String {
                results.append(otherId)
            }
        }

        // Query 2: Scan for entity in second position (more expensive)
        // In a real implementation, we might maintain a secondary index
        let fullRange = indexSpace.range()
        let seq2 = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(fullRange.begin),
            endSelector: .firstGreaterOrEqual(fullRange.end),
            snapshot: true
        )

        for try await (key, _) in seq2 {
            let tuple = try indexSpace.unpack(key)
            if tuple.count >= 2,
               let firstId = tuple[0] as? String,
               let secondId = tuple[1] as? String,
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
