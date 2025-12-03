// InJoinExecutor.swift
// DatabaseEngine - Optimized IN-Join execution strategies
//
// Reference: FDB Record Layer InExtractor.java, PostgreSQL ScalarArrayOpExpr
//
// This module implements strategy selection for IN-Join operations.

import Foundation
import FoundationDB
import Core

// MARK: - InJoinExecutionStrategy

/// Strategy for executing IN-Join operations
public enum InJoinExecutionStrategy: Sendable, Equatable {
    /// Full index scan with hash set lookup
    case fullScan

    /// Bounded range scan between min and max values
    case boundedRangeScan

    /// Fall back to IN-Union (multiple point seeks)
    case convertToUnion
}

// MARK: - InJoinStrategySelector

/// Selects the optimal execution strategy for IN-Join operations
///
/// **Strategy Selection Criteria**:
/// - convertToUnion: Small value count (< threshold) where seeks are faster
/// - boundedRangeScan: Values are clustered within a small range
/// - fullScan: Values are sparse, need to scan entire index
public struct InJoinStrategySelector: Sendable {

    public struct Configuration: Sendable {
        /// Threshold for switching to IN-Union
        public let unionThreshold: Int

        /// Estimated cost of a single index seek
        public let seekCost: Double

        /// Estimated cost of scanning one index entry
        public let scanCost: Double

        /// Maximum range span ratio for bounded scan
        public let maxRangeSpanRatio: Double

        public static var `default`: Configuration {
            Configuration(
                unionThreshold: 15,
                seekCost: 10.0,
                scanCost: 0.1,
                maxRangeSpanRatio: 0.3
            )
        }

        public init(
            unionThreshold: Int = 15,
            seekCost: Double = 10.0,
            scanCost: Double = 0.1,
            maxRangeSpanRatio: Double = 0.3
        ) {
            self.unionThreshold = unionThreshold
            self.seekCost = seekCost
            self.scanCost = scanCost
            self.maxRangeSpanRatio = maxRangeSpanRatio
        }
    }

    private let configuration: Configuration

    public init(configuration: Configuration = .default) {
        self.configuration = configuration
    }

    /// Select execution strategy based on operator properties
    public func selectStrategy<T: Persistable>(
        for op: any InOperatorExecutable<T>,
        estimatedIndexSize: Int
    ) -> InJoinExecutionStrategy {
        let valueCount = op.valueCount

        // Check if we should use IN-Union instead
        if valueCount <= configuration.unionThreshold {
            let unionCost = Double(valueCount) * configuration.seekCost
            let fullScanCost = Double(estimatedIndexSize) * configuration.scanCost

            if unionCost < fullScanCost {
                return .convertToUnion
            }
        }

        // Check if bounded range scan is beneficial
        if op.valueRange() != nil {
            // Use bounded range scan if values are clustered
            let spanRatio = Double(valueCount) / Double(max(1, estimatedIndexSize))
            if spanRatio < configuration.maxRangeSpanRatio {
                return .boundedRangeScan
            }
        }

        return .fullScan
    }
}
