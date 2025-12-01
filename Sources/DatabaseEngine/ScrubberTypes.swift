import Foundation
import FoundationDB

// MARK: - ScrubberConfiguration

/// Configuration for index scrubbing operations
///
/// Controls batch sizes, timeouts, and repair behavior.
///
/// ## Index Type Behavior
///
/// **Per-Item Indexes (Scalar, Min, Max, Version)**:
/// - Phase 1: Detects dangling entries (index → missing item)
/// - Phase 2: Detects missing entries (item → missing index)
/// - Repair: Can fully repair by adding/removing individual entries
///
/// **Aggregation Indexes (Count, Sum)**:
/// - Phase 1: Detects dangling group keys (keys with no contributing items)
/// - Phase 2: Detects missing group keys (items with no group key entry)
/// - Repair: **Limited** - `scanItem` adds incremental values, not absolute values
///
/// **Important**: For aggregation indexes, use `allowRepair=false` for detection only.
/// To fix aggregation index values, use `OnlineIndexer.rebuildIndex()` for a full rebuild.
public struct ScrubberConfiguration: Sendable {
    // MARK: - Scan Limits

    /// Maximum number of entries to scan per batch
    ///
    /// - Default: 1,000
    /// - Note: Consider FoundationDB's 5-second transaction limit
    public let entriesScanLimit: Int

    /// Maximum transaction size in bytes (for read data)
    ///
    /// - Default: 9 MB (leaving room for 10MB limit)
    public let maxTransactionBytes: Int

    /// Maximum transaction execution time in milliseconds
    ///
    /// - Default: 4,000 ms (leaving room for 5 second limit)
    public let transactionTimeoutMillis: Int

    // MARK: - Repair Settings

    /// Whether to automatically repair detected inconsistencies
    ///
    /// - Default: false (detection only, no repair)
    /// - **Caution**: Enable carefully in production environments
    ///
    /// **Per-Item Indexes (Scalar, Min, Max, Version)**:
    /// When `true`, scrubber will:
    /// - Remove dangling index entries (Phase 1)
    /// - Add missing index entries via `scanItem` (Phase 2)
    ///
    /// **Aggregation Indexes (Count, Sum)**:
    /// When `true`, scrubber will:
    /// - Remove dangling group keys (Phase 1) - **Use with caution**
    /// - Increment counts/sums via `scanItem` (Phase 2) - **May cause incorrect values**
    ///
    /// For aggregation indexes, prefer `allowRepair=false` and use
    /// `OnlineIndexer.rebuildIndex()` for full rebuild instead.
    public let allowRepair: Bool

    // MARK: - Retry Settings

    /// Maximum number of retries on transient errors
    ///
    /// - Default: 10
    public let maxRetries: Int

    /// Retry delay in milliseconds
    ///
    /// - Default: 100ms
    public let retryDelayMillis: Int

    /// Delay between batches in milliseconds (throttling)
    ///
    /// - Default: 0 (no throttling)
    public let throttleDelayMs: Int

    // MARK: - Presets

    /// Default configuration (balanced settings)
    public static let `default` = ScrubberConfiguration(
        entriesScanLimit: 1_000,
        maxTransactionBytes: 9_000_000,
        transactionTimeoutMillis: 4_000,
        allowRepair: false,
        maxRetries: 10,
        retryDelayMillis: 100,
        throttleDelayMs: 0
    )

    /// Conservative configuration (production environments)
    public static let conservative = ScrubberConfiguration(
        entriesScanLimit: 100,
        maxTransactionBytes: 1_000_000,
        transactionTimeoutMillis: 2_000,
        allowRepair: false,
        maxRetries: 5,
        retryDelayMillis: 200,
        throttleDelayMs: 50
    )

    /// Aggressive configuration (maintenance windows)
    public static let aggressive = ScrubberConfiguration(
        entriesScanLimit: 10_000,
        maxTransactionBytes: 9_000_000,
        transactionTimeoutMillis: 4_000,
        allowRepair: true,
        maxRetries: 20,
        retryDelayMillis: 50,
        throttleDelayMs: 0
    )

    // MARK: - Initialization

    /// Initialize scrubber configuration
    public init(
        entriesScanLimit: Int = 1_000,
        maxTransactionBytes: Int = 9_000_000,
        transactionTimeoutMillis: Int = 4_000,
        allowRepair: Bool = false,
        maxRetries: Int = 10,
        retryDelayMillis: Int = 100,
        throttleDelayMs: Int = 0
    ) {
        self.entriesScanLimit = entriesScanLimit
        self.maxTransactionBytes = maxTransactionBytes
        self.transactionTimeoutMillis = transactionTimeoutMillis
        self.allowRepair = allowRepair
        self.maxRetries = maxRetries
        self.retryDelayMillis = retryDelayMillis
        self.throttleDelayMs = throttleDelayMs
    }
}

// MARK: - ScrubberResult

/// Result of a scrubbing operation
public struct ScrubberResult: Sendable {
    /// Whether the index is healthy (no issues detected)
    public let isHealthy: Bool

    /// Whether scrubbing completed successfully (vs interrupted by error)
    public let completedSuccessfully: Bool

    /// Summary statistics
    public let summary: ScrubberSummary

    /// Reason for termination if not completed successfully
    public let terminationReason: String?

    /// The error if scrubbing failed
    public let error: Error?

    /// Initialize a scrubber result
    public init(
        isHealthy: Bool,
        completedSuccessfully: Bool,
        summary: ScrubberSummary,
        terminationReason: String? = nil,
        error: Error? = nil
    ) {
        self.isHealthy = isHealthy
        self.completedSuccessfully = completedSuccessfully
        self.summary = summary
        self.terminationReason = terminationReason
        self.error = error
    }
}

// MARK: - ScrubberSummary

/// Summary statistics from a scrubbing operation
public struct ScrubberSummary: Sendable {
    /// Time elapsed during scrubbing
    public let timeElapsed: TimeInterval

    /// Number of index entries scanned (Phase 1)
    public let entriesScanned: Int

    /// Number of items scanned (Phase 2)
    public let itemsScanned: Int

    /// Number of dangling entries detected (index entry without item)
    public let danglingEntriesDetected: Int

    /// Number of dangling entries repaired
    public let danglingEntriesRepaired: Int

    /// Number of missing entries detected (item without index entry)
    public let missingEntriesDetected: Int

    /// Number of missing entries repaired
    public let missingEntriesRepaired: Int

    /// Name of the index that was scrubbed
    public let indexName: String

    /// Total issues detected
    public var issuesDetected: Int {
        danglingEntriesDetected + missingEntriesDetected
    }

    /// Total issues repaired
    public var issuesRepaired: Int {
        danglingEntriesRepaired + missingEntriesRepaired
    }

    /// Initialize a scrubber summary
    public init(
        timeElapsed: TimeInterval,
        entriesScanned: Int,
        itemsScanned: Int,
        danglingEntriesDetected: Int,
        danglingEntriesRepaired: Int,
        missingEntriesDetected: Int,
        missingEntriesRepaired: Int,
        indexName: String
    ) {
        self.timeElapsed = timeElapsed
        self.entriesScanned = entriesScanned
        self.itemsScanned = itemsScanned
        self.danglingEntriesDetected = danglingEntriesDetected
        self.danglingEntriesRepaired = danglingEntriesRepaired
        self.missingEntriesDetected = missingEntriesDetected
        self.missingEntriesRepaired = missingEntriesRepaired
        self.indexName = indexName
    }
}

// MARK: - ScrubberIssue

/// Represents a detected index inconsistency
public struct ScrubberIssue: Sendable {
    /// Type of issue
    public let type: IssueType

    /// Index key where issue was found
    public let indexKey: FDB.Bytes

    /// Primary key extracted from index key
    public let primaryKey: [any TupleElement]

    /// Whether the issue was repaired
    public let repaired: Bool

    /// Additional context information
    public let context: String?

    /// Issue type enumeration
    public enum IssueType: String, Sendable {
        /// Index entry exists but corresponding item does not
        case danglingEntry = "dangling_entry"

        /// Item exists but corresponding index entry does not
        case missingEntry = "missing_entry"
    }

    /// Initialize a scrubber issue
    public init(
        type: IssueType,
        indexKey: FDB.Bytes,
        primaryKey: [any TupleElement],
        repaired: Bool,
        context: String?
    ) {
        self.type = type
        self.indexKey = indexKey
        self.primaryKey = primaryKey
        self.repaired = repaired
        self.context = context
    }
}

// MARK: - ScrubberError

/// Errors that can occur during scrubbing
public enum ScrubberError: Error, CustomStringConvertible {
    /// Index not found in schema
    case indexNotFound(String)

    /// Index is not in readable state
    case indexNotReadable(indexName: String, currentState: String)

    /// Unsupported index type for scrubbing
    case unsupportedIndexType(indexName: String, indexType: String)

    /// Retry limit exceeded
    case retryLimitExceeded(phase: String, attempts: Int, lastError: Error)

    /// Invalid item type
    case invalidItemType(String)

    public var description: String {
        switch self {
        case .indexNotFound(let name):
            return "Index '\(name)' not found"
        case .indexNotReadable(let name, let state):
            return "Index '\(name)' is not readable (current state: \(state))"
        case .unsupportedIndexType(let name, let type):
            return "Index '\(name)' has unsupported type '\(type)' for scrubbing"
        case .retryLimitExceeded(let phase, let attempts, let error):
            return "\(phase): Retry limit exceeded after \(attempts) attempts. Last error: \(error)"
        case .invalidItemType(let type):
            return "Invalid item type: \(type)"
        }
    }
}

// MARK: - CustomStringConvertible

extension ScrubberResult: CustomStringConvertible {
    public var description: String {
        if completedSuccessfully {
            return "ScrubberResult(healthy: \(isHealthy), issues: \(summary.issuesDetected), repaired: \(summary.issuesRepaired))"
        } else {
            return "ScrubberResult(incomplete, reason: \(terminationReason ?? "unknown"))"
        }
    }
}

extension ScrubberSummary: CustomStringConvertible {
    public var description: String {
        return """
        ScrubberSummary(
            index: \(indexName),
            timeElapsed: \(String(format: "%.2f", timeElapsed))s,
            entriesScanned: \(entriesScanned),
            itemsScanned: \(itemsScanned),
            danglingEntries: \(danglingEntriesDetected) detected / \(danglingEntriesRepaired) repaired,
            missingEntries: \(missingEntriesDetected) detected / \(missingEntriesRepaired) repaired
        )
        """
    }
}
