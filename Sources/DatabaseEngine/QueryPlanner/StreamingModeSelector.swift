// StreamingModeSelector.swift
// DatabaseEngine - Automatic StreamingMode selection for range queries
//
// Reference: FDB documentation on streaming modes
// https://apple.github.io/foundationdb/api-general.html#streaming-mode

import Foundation
import FoundationDB

// MARK: - StreamingMode Selection

extension FDB.StreamingMode {
    /// Select optimal StreamingMode based on query characteristics
    ///
    /// This method analyzes the query pattern and selects the most efficient
    /// streaming mode for the operation.
    ///
    /// **Selection Logic**:
    /// - Small result sets with limit → `.exact` (single round-trip)
    /// - Full table scans → `.wantAll` (aggressive prefetch)
    /// - Large result sets → `.serial` (maximum throughput)
    /// - Unknown/default → `.iterator` (adaptive batching)
    ///
    /// - Parameters:
    ///   - estimatedRows: Estimated number of rows (nil if unknown)
    ///   - limit: Query limit (nil if unlimited)
    ///   - isFullScan: Whether this is a full table scan
    ///   - isSingleClient: Whether only one client is scanning this range
    /// - Returns: Optimal StreamingMode for the query
    public static func forQuery(
        estimatedRows: Int? = nil,
        limit: Int? = nil,
        isFullScan: Bool = false,
        isSingleClient: Bool = false
    ) -> FDB.StreamingMode {
        // Case 1: Small result set with limit - use exact for single round-trip
        if let limit = limit, limit <= 100 {
            return .exact
        }

        // Case 2: Full scan with single client - use serial for max throughput
        if isFullScan && isSingleClient {
            return .serial
        }

        // Case 3: Full scan - use wantAll for aggressive prefetch
        if isFullScan {
            return .wantAll
        }

        // Case 4: Large known result set - use serial or large
        if let rows = estimatedRows {
            if rows > 10_000 {
                return .serial
            }
            if rows > 1_000 {
                return .large
            }
            if rows > 100 {
                return .medium
            }
            return .small
        }

        // Default: adaptive batching
        return .iterator
    }

    /// Select StreamingMode for index scan
    ///
    /// Optimized for index-based queries which typically return
    /// smaller, more targeted result sets.
    ///
    /// - Parameters:
    ///   - selectivity: Index selectivity (0.0 = highly selective, 1.0 = full scan)
    ///   - limit: Query limit
    /// - Returns: Optimal StreamingMode
    public static func forIndexScan(
        selectivity: Double,
        limit: Int? = nil
    ) -> FDB.StreamingMode {
        // Highly selective (< 1%) - small batches
        if selectivity < 0.01 {
            if let limit = limit, limit <= 10 {
                return .exact
            }
            return .small
        }

        // Moderately selective (1-10%)
        if selectivity < 0.1 {
            return .medium
        }

        // Low selectivity (10-50%) - larger batches
        if selectivity < 0.5 {
            return .large
        }

        // Very low selectivity (> 50%) - treat as full scan
        return .wantAll
    }

    /// Select StreamingMode for batch fetching
    ///
    /// Optimized for fetching multiple items by ID.
    ///
    /// - Parameter batchSize: Number of items to fetch
    /// - Returns: Optimal StreamingMode
    public static func forBatchFetch(batchSize: Int) -> FDB.StreamingMode {
        if batchSize <= 10 {
            return .exact
        }
        if batchSize <= 100 {
            return .small
        }
        if batchSize <= 1000 {
            return .medium
        }
        return .large
    }
}

// MARK: - Query Context Extension

/// Configuration for streaming mode selection
public struct StreamingModeConfiguration: Sendable, Equatable {
    /// Override streaming mode (nil = automatic selection)
    public let override: FDB.StreamingMode?

    /// Hint: estimated result set size
    public let estimatedRows: Int?

    /// Hint: is this a full table scan?
    public let isFullScan: Bool

    /// Hint: is this the only client scanning this range?
    public let isSingleClient: Bool

    /// Default configuration (automatic selection)
    public static let `default` = StreamingModeConfiguration()

    /// Force a specific streaming mode
    public static func force(_ mode: FDB.StreamingMode) -> StreamingModeConfiguration {
        StreamingModeConfiguration(override: mode)
    }

    /// Hint for full table scan
    public static let fullScan = StreamingModeConfiguration(isFullScan: true)

    /// Hint for small result set
    public static func small(estimatedRows: Int) -> StreamingModeConfiguration {
        StreamingModeConfiguration(estimatedRows: estimatedRows)
    }

    public init(
        override: FDB.StreamingMode? = nil,
        estimatedRows: Int? = nil,
        isFullScan: Bool = false,
        isSingleClient: Bool = false
    ) {
        self.override = override
        self.estimatedRows = estimatedRows
        self.isFullScan = isFullScan
        self.isSingleClient = isSingleClient
    }

    /// Resolve the streaming mode to use
    public func resolve(limit: Int?) -> FDB.StreamingMode {
        if let override = override {
            return override
        }

        return .forQuery(
            estimatedRows: estimatedRows,
            limit: limit,
            isFullScan: isFullScan,
            isSingleClient: isSingleClient
        )
    }
}
