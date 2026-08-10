import DatabaseTypes
import StorageKit
import DatabaseKit

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
///     container: container,
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
package final class OnlineIndexScrubber<Item: Persistable>: Sendable {
    // MARK: - Properties

    /// FDB Container for database access
    private let container: DBContainer

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

    /// Scrubber configuration
    private let configuration: ScrubberConfiguration

    // Progress tracking keys
    private let phase1ProgressKey: ByteString
    private let phase2ProgressKey: ByteString

    // MARK: - Metrics

    /// Counter for index entries scanned
    private let entriesScannedCounter: DatabaseMetricCounter

    /// Counter for items scanned
    private let itemsScannedCounter: DatabaseMetricCounter

    /// Counter for dangling entries detected
    private let danglingEntriesCounter: DatabaseMetricCounter

    /// Counter for missing entries detected
    private let missingEntriesCounter: DatabaseMetricCounter

    /// Counter for entries repaired
    private let entriesRepairedCounter: DatabaseMetricCounter

    /// Timer for scrub duration
    private let scrubDurationTimer: DatabaseMetricTimer

    /// Counter for errors
    private let errorsCounter: DatabaseMetricCounter

    // MARK: - Initialization

    /// Initialize online index scrubber
    ///
    /// - Parameters:
    ///   - container: FDB Container instance
    ///   - itemSubspace: Subspace where items are stored
    ///   - indexSubspace: Subspace where index data is stored
    ///   - blobsSubspace: Subspace where blob chunks are stored
    ///   - itemType: Type name of items to scrub
    ///   - index: Index definition
    ///   - indexMaintainer: IndexMaintainer for this index
    ///   - configuration: Scrubber configuration (default: .default)
    public init(
        container: DBContainer,
        itemSubspace: Subspace,
        indexSubspace: Subspace,
        blobsSubspace: Subspace,
        itemType: String,
        index: Index,
        indexMaintainer: any IndexMaintainer<Item>,
        configuration: ScrubberConfiguration = .default
    ) {
        self.container = container
        self.itemSubspace = itemSubspace
        self.indexSubspace = indexSubspace
        self.blobsSubspace = blobsSubspace
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

        let metrics = container.configuration.metrics
        self.entriesScannedCounter = metrics.counter(
            label: "database_index_scrubber_entries_scanned_total",
            dimensions: baseDimensions
        )
        self.itemsScannedCounter = metrics.counter(
            label: "database_index_scrubber_items_scanned_total",
            dimensions: baseDimensions
        )
        self.danglingEntriesCounter = metrics.counter(
            label: "database_index_scrubber_dangling_entries_total",
            dimensions: baseDimensions
        )
        self.missingEntriesCounter = metrics.counter(
            label: "database_index_scrubber_missing_entries_total",
            dimensions: baseDimensions
        )
        self.entriesRepairedCounter = metrics.counter(
            label: "database_index_scrubber_entries_repaired_total",
            dimensions: baseDimensions
        )
        self.scrubDurationTimer = metrics.timer(
            label: "database_index_scrubber_duration_seconds",
            dimensions: baseDimensions
        )
        self.errorsCounter = metrics.counter(
            label: "database_index_scrubber_errors_total",
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
        try validateConfiguration()
        let capabilities = try requirePhysicalEntryCapabilities()
        let startTime = container.monotonicClock.now
        do {
            // Phase 1: Index → Item (detect dangling entries)
            let phase1Result = try await runPhase1(capabilities: capabilities)

            // Phase 2: Item → Index (detect missing entries)
            let phase2Result = try await runPhase2()

            // Clear progress after successful completion
            try await clearProgress()

            // Record duration
            let duration = startTime.duration(to: container.monotonicClock.now)
            scrubDurationTimer.recordNanoseconds(
                DatabaseMonotonicMeasurement.nanoseconds(duration)
            )

            let summary = ScrubberSummary(
                timeElapsed: duration,
                entriesScanned: phase1Result.entriesScanned,
                itemsScanned: phase2Result.itemsScanned,
                danglingEntriesDetected: phase1Result.danglingDetected,
                danglingEntriesRepaired: phase1Result.danglingRepaired,
                missingEntriesDetected: phase2Result.missingDetected,
                missingEntriesRepaired: phase2Result.missingRepaired,
                indexName: index.name
            )

            let isHealthy = phase1Result.danglingDetected == 0
                && phase2Result.missingDetected == 0

            return ScrubberResult(
                isHealthy: isHealthy,
                completedSuccessfully: true,
                summary: summary
            )

        } catch {
            errorsCounter.increment()
            scrubDurationTimer.recordNanoseconds(
                DatabaseMonotonicMeasurement.nanoseconds(
                    startTime.duration(to: container.monotonicClock.now)
                )
            )
            throw error
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
    private func runPhase1(
        capabilities: IndexPhysicalEntryCapabilities
    ) async throws -> Phase1Result {
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

        // Process batches - each batch in a separate transaction
        while let bounds = rangeSet.nextBatchBounds() {
            let batchResult = try await processPhase1Batch(
                bounds: bounds,
                batchSize: configuration.entriesScanLimit,
                indexNameSubspace: indexNameSubspace,
                decoder: capabilities.decoder,
                rangeSet: &rangeSet
            )

            totalEntriesScanned += batchResult.entriesScanned
            totalDanglingDetected += batchResult.danglingDetected
            totalDanglingRepaired += batchResult.danglingRepaired

            entriesScannedCounter.increment(by: batchResult.entriesScanned)
            danglingEntriesCounter.increment(by: batchResult.danglingDetected)
            entriesRepairedCounter.increment(by: batchResult.danglingRepaired)

            try await saveProgress(rangeSet, key: phase1ProgressKey)

            // Throttle between batches
            if configuration.throttleDelayMs > 0 {
                try await sleep(milliseconds: configuration.throttleDelayMs)
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
        bounds: RangeSet.BatchBounds,
        batchSize: Int,
        indexNameSubspace: Subspace,
        decoder: any IndexPhysicalEntryDecoder,
        rangeSet: inout RangeSet
    ) async throws -> Phase1Result {

        let result = try await container.transactionExecutor.withTransaction(
            configuration: transactionConfiguration,
            clock: container.monotonicClock
        ) { transaction in
            var entriesScanned = 0
            var danglingDetected = 0
            var danglingRepaired = 0
            var lastProcessedKey: ByteString? = nil
            var bytesScanned = 0

            // Use limit + .iterator for efficient batch scrubbing
            // .iterator is appropriate since we do reads within the transaction
            let sequence = try await TransactionRangeCollection.collect(using: transaction,
                from: .firstGreaterOrEqual(bounds.begin),
                to: .firstGreaterOrEqual(bounds.end),
                limit: batchSize,
                reverse: false,
                snapshot: false,
                streamingMode: .iterator
            )

            for (indexKey, value) in sequence {
                let (entryBytes, entryOverflow) = indexKey.count
                    .addingReportingOverflow(value.count)
                let (newBytesScanned, totalOverflow) = bytesScanned
                    .addingReportingOverflow(entryBytes)
                guard !entryOverflow,
                      !totalOverflow,
                      newBytesScanned <= self.configuration.maxTransactionBytes else {
                    throw ScrubberError.transactionByteLimitExceeded(
                        maximum: self.configuration.maxTransactionBytes
                    )
                }
                bytesScanned = newBytesScanned
                entriesScanned += 1

                // Extract primary key from index key. A malformed physical key
                // invalidates the scrub rather than being counted as healthy.
                let entry: IndexPhysicalEntry
                do {
                    entry = try decoder.decode(
                        key: indexKey,
                        in: indexNameSubspace,
                        index: self.index
                    )
                } catch {
                    throw ScrubberError.invalidPhysicalEntry(
                        indexName: self.index.name,
                        reason: "physical index entry decoding failed"
                    )
                }

                // Check if item exists
                let itemKey = self.itemSubspace
                    .subspace(self.itemType)
                    .pack(entry.primaryKey)
                let itemExists = try await transaction.getValue(for: itemKey, snapshot: false) != nil

                if !itemExists {
                    // Dangling entry detected
                    danglingDetected += 1

                    if self.configuration.allowRepair {
                        // Repair: Remove dangling index entry
                        try transaction.clear(key: indexKey)
                        danglingRepaired += 1
                    }
                }

                lastProcessedKey = indexKey
            }

            return (entriesScanned, danglingDetected, danglingRepaired, lastProcessedKey)
        }

        // Record progress outside transaction
        if let lastKey = result.3 {
            let isComplete = result.0 < batchSize
            try rangeSet.recordProgress(
                rangeIndex: bounds.rangeIndex,
                lastProcessedKey: lastKey,
                isComplete: isComplete
            )
        } else {
            try rangeSet.markRangeComplete(rangeIndex: bounds.rangeIndex)
        }

        return Phase1Result(
            entriesScanned: result.0,
            danglingDetected: result.1,
            danglingRepaired: result.2
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

        // Process batches - each batch in a separate transaction
        while let bounds = rangeSet.nextBatchBounds() {
            let batchResult = try await processPhase2Batch(
                bounds: bounds,
                batchSize: configuration.entriesScanLimit,
                itemTypeSubspace: itemTypeSubspace,
                rangeSet: &rangeSet
            )

            totalItemsScanned += batchResult.itemsScanned
            totalMissingDetected += batchResult.missingDetected
            totalMissingRepaired += batchResult.missingRepaired

            itemsScannedCounter.increment(by: batchResult.itemsScanned)
            missingEntriesCounter.increment(by: batchResult.missingDetected)
            entriesRepairedCounter.increment(by: batchResult.missingRepaired)

            try await saveProgress(rangeSet, key: phase2ProgressKey)

            // Throttle between batches
            if configuration.throttleDelayMs > 0 {
                try await sleep(milliseconds: configuration.throttleDelayMs)
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
        bounds: RangeSet.BatchBounds,
        batchSize: Int,
        itemTypeSubspace: Subspace,
        rangeSet: inout RangeSet
    ) async throws -> Phase2Result {

        let result = try await container.transactionExecutor.withTransaction(
            configuration: transactionConfiguration,
            clock: container.monotonicClock
        ) { transaction in
            var itemsScanned = 0
            var missingDetected = 0
            var missingRepaired = 0
            var lastProcessedKey: ByteString? = nil
            var bytesScanned = 0

            // Use ItemStorage.scan() to handle ItemEnvelope format (inline/external)
            let storage = self.container.itemStorageFactory.make(
                transaction: transaction,
                blobsSubspace: self.blobsSubspace
            )

            let scanSequence = storage.scan(
                begin: bounds.begin,
                end: bounds.end,
                snapshot: false,
                limit: batchSize
            )

            var iterator = scanSequence.makeAsyncIterator()
            while let (key, data) = try await iterator.next() {
                let (entryBytes, entryOverflow) = key.count
                    .addingReportingOverflow(data.count)
                let (newBytesScanned, totalOverflow) = bytesScanned
                    .addingReportingOverflow(entryBytes)
                guard !entryOverflow,
                      !totalOverflow,
                      newBytesScanned <= self.configuration.maxTransactionBytes else {
                    throw ScrubberError.transactionByteLimitExceeded(
                        maximum: self.configuration.maxTransactionBytes
                    )
                }
                bytesScanned = newBytesScanned
                itemsScanned += 1

                // Deserialize item from decompressed data
                let item: Item = try DataAccess.deserialize(data)

                // Extract id from key
                let id = try itemTypeSubspace.unpack(key)

                // Compute expected index keys using IndexMaintainer
                // Use transaction-aware version for indexes that need to load related data (e.g., RelationshipIndex)
                let expectedIndexKeys = try await self.indexMaintainer.computeIndexKeys(
                    for: item,
                    id: id,
                    transaction: transaction
                )

                // Check if all expected index entries exist
                var missingForItem = 0
                for expectedKey in expectedIndexKeys {
                    let indexEntryExists = try await transaction.getValue(
                        for: expectedKey,
                        snapshot: false
                    ) != nil

                    if !indexEntryExists {
                        missingDetected += 1
                        missingForItem += 1
                    }
                }

                if missingForItem > 0 && self.configuration.allowRepair {
                    try await self.indexMaintainer.scanItem(
                        item,
                        id: id,
                        transaction: transaction
                    )
                    missingRepaired += missingForItem
                }

                lastProcessedKey = key
            }

            return (itemsScanned, missingDetected, missingRepaired, lastProcessedKey)
        }

        // Record progress outside transaction
        if let lastKey = result.3 {
            let isComplete = result.0 < batchSize
            try rangeSet.recordProgress(
                rangeIndex: bounds.rangeIndex,
                lastProcessedKey: lastKey,
                isComplete: isComplete
            )
        } else {
            try rangeSet.markRangeComplete(rangeIndex: bounds.rangeIndex)
        }

        return Phase2Result(
            itemsScanned: result.0,
            missingDetected: result.1,
            missingRepaired: result.2
        )
    }

    // MARK: - Helper Methods

    // MARK: - Progress Management

    /// Load saved progress
    private func loadProgress(key: ByteString) async throws -> RangeSet? {
        return try await container.transactionExecutor.withTransaction(
            configuration: transactionConfiguration,
            clock: container.monotonicClock
        ) { transaction in
            guard let bytes = try await transaction.getValue(for: key, snapshot: false) else {
                return nil
            }

            return try RangeSetCodec.decode(bytes)
        }
    }

    /// Save progress
    private func saveProgress(_ rangeSet: RangeSet, key: ByteString) async throws {
        try await container.transactionExecutor.withTransaction(
            configuration: transactionConfiguration,
            clock: container.monotonicClock
        ) { transaction in
            try transaction.setValue(try RangeSetCodec.encode(rangeSet), for: key)
        }
    }

    /// Clear all progress
    private func clearProgress() async throws {
        let phase1Key = self.phase1ProgressKey
        let phase2Key = self.phase2ProgressKey
        try await container.transactionExecutor.withTransaction(
            configuration: transactionConfiguration,
            clock: container.monotonicClock
        ) { transaction in
            try transaction.clear(key: phase1Key)
            try transaction.clear(key: phase2Key)
        }
    }

    private var transactionConfiguration: TransactionConfiguration {
        TransactionConfiguration(
            timeout: configuration.transactionTimeoutMillis,
            maximumAttempts: configuration.maxRetries,
            maxRetryDelay: configuration.retryDelayMillis,
            initialRetryDelay: configuration.retryDelayMillis,
            priority: .batch,
            readPriority: .low,
            disableReadCache: true
        )
    }

    private func requirePhysicalEntryCapabilities()
        throws -> IndexPhysicalEntryCapabilities {
        guard let capabilities = container.runtimeConfiguration
            .indexMaintainerProviders
            .physicalEntryCapabilities(for: index.kind.identifier),
              capabilities.supportsItemReferenceValidation else {
            throw ScrubberError.unsupportedIndexType(
                indexName: index.name,
                indexType: index.kind.identifier
            )
        }
        if configuration.allowRepair,
           !capabilities.supportsIndependentEntryRepair {
            throw ScrubberError.repairUnsupported(
                indexName: index.name,
                indexType: index.kind.identifier
            )
        }
        return capabilities
    }

    private func validateConfiguration() throws {
        let positiveValues = [
            "entriesScanLimit": configuration.entriesScanLimit,
            "maxTransactionBytes": configuration.maxTransactionBytes,
            "transactionTimeoutMillis": configuration.transactionTimeoutMillis,
            "maxRetries": configuration.maxRetries
        ]
        for (field, value) in positiveValues where value <= 0 {
            throw ScrubberError.invalidConfiguration(field: field, value: value)
        }
        let nonnegativeValues = [
            "retryDelayMillis": configuration.retryDelayMillis,
            "throttleDelayMs": configuration.throttleDelayMs
        ]
        for (field, value) in nonnegativeValues where value < 0 {
            throw ScrubberError.invalidConfiguration(field: field, value: value)
        }
        guard configuration.throttleDelayMs <= Int(UInt64.max / 1_000_000) else {
            throw ScrubberError.invalidConfiguration(
                field: "throttleDelayMs",
                value: configuration.throttleDelayMs
            )
        }
    }

    private func sleep(milliseconds: Int) async throws {
        try await container.monotonicClock.sleep(
            for: .milliseconds(Int64(milliseconds))
        )
    }
}

// MARK: - CustomStringConvertible

extension OnlineIndexScrubber: CustomStringConvertible {
    public var description: String {
        return "OnlineIndexScrubber(index: \(index.name), itemType: \(itemType), allowRepair: \(configuration.allowRepair))"
    }
}
