import Foundation
import FoundationDB
import Core
import Metrics

/// Online index scrubber for detecting and repairing index inconsistencies
///
/// OnlineIndexScrubber verifies index consistency by performing two-phase scanning:
///
/// **Phase 1: Index → Item (Dangling Entry Detection)**
/// - Scans all index entries
/// - For each entry, verifies the referenced item exists
/// - Detects "dangling" entries where index points to non-existent items
///
/// **Phase 2: Item → Index (Missing Entry Detection)**
/// - Scans all items of the indexed type
/// - For each item, verifies expected index entries exist
/// - Detects "missing" entries where items aren't properly indexed
///
/// **Usage Example**:
/// ```swift
/// let scrubber = OnlineIndexScrubber<User>(
///     database: database,
///     itemSubspace: itemSubspace,
///     indexSubspace: indexSubspace,
///     itemType: "User",
///     index: emailIndex,
///     indexMaintainer: emailIndexMaintainer,
///     configuration: .default
/// )
///
/// // Run scrubbing (detection only)
/// let result = try await scrubber.scrubIndex()
///
/// // Run with automatic repair
/// let scrubberWithRepair = OnlineIndexScrubber<User>(
///     ...,
///     configuration: ScrubberConfiguration(allowRepair: true)
/// )
/// let repairedResult = try await scrubberWithRepair.scrubIndex()
/// ```
///
/// **Resumability**:
/// - Progress is tracked via RangeSet
/// - If interrupted, scrubbing resumes from last completed batch
/// - Progress is stored in `[indexSubspace]/_scrub_progress/[indexName]`
public final class OnlineIndexScrubber<Item: Persistable>: Sendable {
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

    /// Scrubber configuration
    private let configuration: ScrubberConfiguration

    // Progress tracking keys
    private let phase1ProgressKey: FDB.Bytes
    private let phase2ProgressKey: FDB.Bytes

    // MARK: - Metrics

    /// Counter for index entries scanned
    private let entriesScannedCounter: Counter

    /// Counter for items scanned
    private let itemsScannedCounter: Counter

    /// Counter for dangling entries detected
    private let danglingEntriesCounter: Counter

    /// Counter for missing entries detected
    private let missingEntriesCounter: Counter

    /// Counter for entries repaired
    private let entriesRepairedCounter: Counter

    /// Timer for scrub duration
    private let scrubDurationTimer: Metrics.Timer

    /// Counter for errors
    private let errorsCounter: Counter

    // MARK: - Initialization

    /// Initialize online index scrubber
    ///
    /// - Parameters:
    ///   - database: Database instance
    ///   - itemSubspace: Subspace where items are stored
    ///   - indexSubspace: Subspace where index data is stored
    ///   - itemType: Type name of items to scrub
    ///   - index: Index definition
    ///   - indexMaintainer: IndexMaintainer for this index
    ///   - configuration: Scrubber configuration (default: .default)
    public init(
        database: any DatabaseProtocol,
        itemSubspace: Subspace,
        indexSubspace: Subspace,
        itemType: String,
        index: Index,
        indexMaintainer: any IndexMaintainer<Item>,
        configuration: ScrubberConfiguration = .default
    ) {
        self.database = database
        self.itemSubspace = itemSubspace
        self.indexSubspace = indexSubspace
        self.itemType = itemType
        self.index = index
        self.indexMaintainer = indexMaintainer
        self.configuration = configuration

        // Progress keys
        let progressSubspace = indexSubspace.subspace("_scrub_progress").subspace(index.name)
        self.phase1ProgressKey = progressSubspace.pack(Tuple("phase1"))
        self.phase2ProgressKey = progressSubspace.pack(Tuple("phase2"))

        // Initialize metrics with index-specific dimensions
        let baseDimensions: [(String, String)] = [
            ("index", index.name),
            ("item_type", itemType)
        ]

        self.entriesScannedCounter = Counter(
            label: "fdb_scrubber_entries_scanned_total",
            dimensions: baseDimensions
        )
        self.itemsScannedCounter = Counter(
            label: "fdb_scrubber_items_scanned_total",
            dimensions: baseDimensions
        )
        self.danglingEntriesCounter = Counter(
            label: "fdb_scrubber_dangling_entries_total",
            dimensions: baseDimensions
        )
        self.missingEntriesCounter = Counter(
            label: "fdb_scrubber_missing_entries_total",
            dimensions: baseDimensions
        )
        self.entriesRepairedCounter = Counter(
            label: "fdb_scrubber_entries_repaired_total",
            dimensions: baseDimensions
        )
        self.scrubDurationTimer = Metrics.Timer(
            label: "fdb_scrubber_duration_seconds",
            dimensions: baseDimensions
        )
        self.errorsCounter = Counter(
            label: "fdb_scrubber_errors_total",
            dimensions: baseDimensions
        )
    }

    // MARK: - Public API

    /// Scrub the index for inconsistencies
    ///
    /// Performs two-phase scanning to detect and optionally repair index inconsistencies.
    ///
    /// - Returns: ScrubberResult with health status and statistics
    /// - Throws: ScrubberError if scrubbing fails
    public func scrubIndex() async throws -> ScrubberResult {
        let startTime = Date()
        var entriesScanned = 0
        var itemsScanned = 0
        var danglingEntriesDetected = 0
        var danglingEntriesRepaired = 0
        var missingEntriesDetected = 0
        var missingEntriesRepaired = 0

        do {
            // Phase 1: Index → Item (detect dangling entries)
            let phase1Result = try await runPhase1()
            entriesScanned = phase1Result.entriesScanned
            danglingEntriesDetected = phase1Result.danglingDetected
            danglingEntriesRepaired = phase1Result.danglingRepaired

            // Phase 2: Item → Index (detect missing entries)
            let phase2Result = try await runPhase2()
            itemsScanned = phase2Result.itemsScanned
            missingEntriesDetected = phase2Result.missingDetected
            missingEntriesRepaired = phase2Result.missingRepaired

            // Clear progress after successful completion
            try await clearProgress()

            // Record duration
            let duration = Date().timeIntervalSince(startTime)
            scrubDurationTimer.recordSeconds(duration)

            let summary = ScrubberSummary(
                timeElapsed: duration,
                entriesScanned: entriesScanned,
                itemsScanned: itemsScanned,
                danglingEntriesDetected: danglingEntriesDetected,
                danglingEntriesRepaired: danglingEntriesRepaired,
                missingEntriesDetected: missingEntriesDetected,
                missingEntriesRepaired: missingEntriesRepaired,
                indexName: index.name
            )

            let isHealthy = danglingEntriesDetected == 0 && missingEntriesDetected == 0

            return ScrubberResult(
                isHealthy: isHealthy,
                completedSuccessfully: true,
                summary: summary
            )

        } catch {
            errorsCounter.increment()

            let duration = Date().timeIntervalSince(startTime)
            let summary = ScrubberSummary(
                timeElapsed: duration,
                entriesScanned: entriesScanned,
                itemsScanned: itemsScanned,
                danglingEntriesDetected: danglingEntriesDetected,
                danglingEntriesRepaired: danglingEntriesRepaired,
                missingEntriesDetected: missingEntriesDetected,
                missingEntriesRepaired: missingEntriesRepaired,
                indexName: index.name
            )

            return ScrubberResult(
                isHealthy: false,
                completedSuccessfully: false,
                summary: summary,
                terminationReason: error.localizedDescription,
                error: error
            )
        }
    }

    // MARK: - Phase 1: Index → Item

    /// Phase 1 result
    private struct Phase1Result {
        let entriesScanned: Int
        let danglingDetected: Int
        let danglingRepaired: Int
    }

    /// Run Phase 1: Scan index entries and verify items exist
    private func runPhase1() async throws -> Phase1Result {
        let indexNameSubspace = indexSubspace.subspace(index.name)
        let totalRange = indexNameSubspace.range()

        // Load or create progress
        var rangeSet: RangeSet
        if let savedProgress = try await loadProgress(key: phase1ProgressKey) {
            rangeSet = savedProgress
        } else {
            rangeSet = RangeSet(initialRange: totalRange)
        }

        var totalEntriesScanned = 0
        var totalDanglingDetected = 0
        var totalDanglingRepaired = 0

        // Process batches until complete
        while !rangeSet.isEmpty {
            guard let batchRange = rangeSet.nextBatch(size: configuration.entriesScanLimit) else {
                break
            }

            var retryCount = 0
            var batchSuccess = false

            while !batchSuccess && retryCount < configuration.maxRetries {
                do {
                    let batchResult = try await processPhase1Batch(
                        range: batchRange,
                        indexNameSubspace: indexNameSubspace
                    )

                    totalEntriesScanned += batchResult.entriesScanned
                    totalDanglingDetected += batchResult.danglingDetected
                    totalDanglingRepaired += batchResult.danglingRepaired

                    // Update metrics
                    entriesScannedCounter.increment(by: batchResult.entriesScanned)
                    danglingEntriesCounter.increment(by: batchResult.danglingDetected)
                    entriesRepairedCounter.increment(by: batchResult.danglingRepaired)

                    // Mark batch completed and save progress
                    rangeSet.markCompleted(batchRange)
                    try await saveProgress(rangeSet, key: phase1ProgressKey)

                    batchSuccess = true

                } catch {
                    retryCount += 1
                    if retryCount >= configuration.maxRetries {
                        throw ScrubberError.retryLimitExceeded(
                            phase: "Phase 1 (Index → Item)",
                            attempts: retryCount,
                            lastError: error
                        )
                    }
                    try await Task.sleep(nanoseconds: UInt64(configuration.retryDelayMillis) * 1_000_000)
                }
            }

            // Throttle between batches
            if configuration.throttleDelayMs > 0 {
                try await Task.sleep(nanoseconds: UInt64(configuration.throttleDelayMs) * 1_000_000)
            }
        }

        return Phase1Result(
            entriesScanned: totalEntriesScanned,
            danglingDetected: totalDanglingDetected,
            danglingRepaired: totalDanglingRepaired
        )
    }

    /// Process a single batch in Phase 1
    private func processPhase1Batch(
        range: RangeSet.Range,
        indexNameSubspace: Subspace
    ) async throws -> Phase1Result {
        var entriesScanned = 0
        var danglingDetected = 0
        var danglingRepaired = 0

        try await database.withTransaction { transaction in
            let sequence = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(range.begin),
                endSelector: .firstGreaterOrEqual(range.end),
                snapshot: false
            )

            for try await (indexKey, _) in sequence {
                entriesScanned += 1

                // Extract primary key from index key
                guard let primaryKey = try? self.extractPrimaryKeyFromIndexKey(
                    indexKey,
                    indexSubspace: indexNameSubspace
                ) else {
                    continue
                }

                // Check if item exists
                let itemKey = self.itemSubspace.subspace(self.itemType).pack(primaryKey)
                let itemExists = try await transaction.getValue(for: itemKey, snapshot: false) != nil

                if !itemExists {
                    // Dangling entry detected
                    danglingDetected += 1

                    if self.configuration.allowRepair {
                        // Repair: Remove dangling index entry
                        transaction.clear(key: indexKey)
                        danglingRepaired += 1
                    }
                }
            }
        }

        return Phase1Result(
            entriesScanned: entriesScanned,
            danglingDetected: danglingDetected,
            danglingRepaired: danglingRepaired
        )
    }

    // MARK: - Phase 2: Item → Index

    /// Phase 2 result
    private struct Phase2Result {
        let itemsScanned: Int
        let missingDetected: Int
        let missingRepaired: Int
    }

    /// Run Phase 2: Scan items and verify index entries exist
    private func runPhase2() async throws -> Phase2Result {
        let itemTypeSubspace = itemSubspace.subspace(itemType)
        let totalRange = itemTypeSubspace.range()

        // Load or create progress
        var rangeSet: RangeSet
        if let savedProgress = try await loadProgress(key: phase2ProgressKey) {
            rangeSet = savedProgress
        } else {
            rangeSet = RangeSet(initialRange: totalRange)
        }

        var totalItemsScanned = 0
        var totalMissingDetected = 0
        var totalMissingRepaired = 0

        // Process batches until complete
        while !rangeSet.isEmpty {
            guard let batchRange = rangeSet.nextBatch(size: configuration.entriesScanLimit) else {
                break
            }

            var retryCount = 0
            var batchSuccess = false

            while !batchSuccess && retryCount < configuration.maxRetries {
                do {
                    let batchResult = try await processPhase2Batch(
                        range: batchRange,
                        itemTypeSubspace: itemTypeSubspace
                    )

                    totalItemsScanned += batchResult.itemsScanned
                    totalMissingDetected += batchResult.missingDetected
                    totalMissingRepaired += batchResult.missingRepaired

                    // Update metrics
                    itemsScannedCounter.increment(by: batchResult.itemsScanned)
                    missingEntriesCounter.increment(by: batchResult.missingDetected)
                    entriesRepairedCounter.increment(by: batchResult.missingRepaired)

                    // Mark batch completed and save progress
                    rangeSet.markCompleted(batchRange)
                    try await saveProgress(rangeSet, key: phase2ProgressKey)

                    batchSuccess = true

                } catch {
                    retryCount += 1
                    if retryCount >= configuration.maxRetries {
                        throw ScrubberError.retryLimitExceeded(
                            phase: "Phase 2 (Item → Index)",
                            attempts: retryCount,
                            lastError: error
                        )
                    }
                    try await Task.sleep(nanoseconds: UInt64(configuration.retryDelayMillis) * 1_000_000)
                }
            }

            // Throttle between batches
            if configuration.throttleDelayMs > 0 {
                try await Task.sleep(nanoseconds: UInt64(configuration.throttleDelayMs) * 1_000_000)
            }
        }

        return Phase2Result(
            itemsScanned: totalItemsScanned,
            missingDetected: totalMissingDetected,
            missingRepaired: totalMissingRepaired
        )
    }

    /// Process a single batch in Phase 2
    private func processPhase2Batch(
        range: RangeSet.Range,
        itemTypeSubspace: Subspace
    ) async throws -> Phase2Result {
        var itemsScanned = 0
        var missingDetected = 0
        var missingRepaired = 0

        try await database.withTransaction { transaction in
            let sequence = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(range.begin),
                endSelector: .firstGreaterOrEqual(range.end),
                snapshot: false
            )

            for try await (key, value) in sequence {
                itemsScanned += 1

                // Deserialize item
                let item: Item = try DataAccess.deserialize(value)

                // Extract id from key
                let id = try itemTypeSubspace.unpack(key)

                // Compute expected index keys using IndexMaintainer
                let expectedIndexKeys = try await self.indexMaintainer.computeIndexKeys(
                    for: item,
                    id: id
                )

                // Check if all expected index entries exist
                for expectedKey in expectedIndexKeys {
                    let indexEntryExists = try await transaction.getValue(
                        for: expectedKey,
                        snapshot: false
                    ) != nil

                    if !indexEntryExists {
                        // Missing entry detected
                        missingDetected += 1

                        if self.configuration.allowRepair {
                            // Repair: Add missing index entry using scanItem
                            try await self.indexMaintainer.scanItem(
                                item,
                                id: id,
                                transaction: transaction
                            )
                            missingRepaired += 1
                        }
                    }
                }
            }
        }

        return Phase2Result(
            itemsScanned: itemsScanned,
            missingDetected: missingDetected,
            missingRepaired: missingRepaired
        )
    }

    // MARK: - Helper Methods

    /// Extract primary key from index key
    ///
    /// Index key structure: [indexSubspace]/[indexName]/[indexValues...]/[primaryKey]
    /// The primary key is typically the last element(s) of the tuple.
    private func extractPrimaryKeyFromIndexKey(
        _ indexKey: FDB.Bytes,
        indexSubspace: Subspace
    ) throws -> Tuple {
        let tuple = try indexSubspace.unpack(indexKey)

        // For scalar indexes, primary key is the last element
        // This may need customization for different index types
        guard !tuple.isEmpty else {
            throw ScrubberError.invalidItemType("Empty tuple in index key")
        }

        // Return the last element as primary key tuple
        // Adjust this logic based on your index key structure
        guard let lastElement = tuple[tuple.count - 1] else {
            throw ScrubberError.invalidItemType("Unable to extract primary key from index key")
        }
        return Tuple([lastElement])
    }

    // MARK: - Progress Management

    /// Load saved progress
    private func loadProgress(key: FDB.Bytes) async throws -> RangeSet? {
        return try await database.withTransaction { transaction in
            guard let bytes = try await transaction.getValue(for: key, snapshot: false) else {
                return nil
            }

            let decoder = JSONDecoder()
            return try decoder.decode(RangeSet.self, from: Data(bytes))
        }
    }

    /// Save progress
    private func saveProgress(_ rangeSet: RangeSet, key: FDB.Bytes) async throws {
        try await database.withTransaction { transaction in
            let encoder = JSONEncoder()
            let data = try encoder.encode(rangeSet)
            transaction.setValue(Array(data), for: key)
        }
    }

    /// Clear all progress
    private func clearProgress() async throws {
        try await database.withTransaction { transaction in
            transaction.clear(key: phase1ProgressKey)
            transaction.clear(key: phase2ProgressKey)
        }
    }
}

// MARK: - CustomStringConvertible

extension OnlineIndexScrubber: CustomStringConvertible {
    public var description: String {
        return "OnlineIndexScrubber(index: \(index.name), itemType: \(itemType), allowRepair: \(configuration.allowRepair))"
    }
}
