// SkipListSubspaces.swift
// Subspace layout for Skip List with Span Counters
//
// References:
// - FoundationDB Record Layer RankedSet
// - Skip Lists: A Probabilistic Alternative to Balanced Trees (Pugh 1990)

import Foundation
import FoundationDB

/// Subspace layout for Skip List index storage
///
/// Layout:
/// ```
/// [base]/0/[score][primaryKey] = SpanValue(count=1)           # Level 0 (leaf)
/// [base]/1/[level]/[score][primaryKey] = SpanValue(count=n)   # Level 1-N
/// [base]/2/_numLevels = Int64                                 # Metadata
/// [base]/2/_fanout = Int64
/// [base]/2/_count = Int64
/// ```
public struct SkipListSubspaces: Sendable {

    // MARK: - Subspace Keys

    /// Subspace keys using integer encoding for efficiency
    private enum SubspaceKey: Int64 {
        case leaf = 0       // Level 0: all elements
        case levels = 1     // Level 1-N: skip list layers
        case metadata = 2   // Configuration and counters
    }

    /// Metadata keys
    private enum MetadataKey: String {
        case numLevels = "_numLevels"  // Current number of levels (Int64)
        case fanout = "_fanout"        // Fanout factor (Int64, default 4)
        case count = "_count"          // Total element count (Int64, atomic)
    }

    // MARK: - Subspaces

    /// Base subspace for the entire skip list
    public let base: Subspace

    /// Level 0 subspace: stores all elements with span=1
    public let leaf: Subspace

    /// Level 1-N subspace: stores skip list layers
    public let levels: Subspace

    /// Metadata subspace: configuration and counters
    public let metadata: Subspace

    // MARK: - Initialization

    /// Initialize subspaces from base subspace
    ///
    /// - Parameter base: Base subspace for the skip list
    public init(base: Subspace) {
        self.base = base
        self.leaf = base.subspace(SubspaceKey.leaf.rawValue)
        self.levels = base.subspace(SubspaceKey.levels.rawValue)
        self.metadata = base.subspace(SubspaceKey.metadata.rawValue)
    }

    // MARK: - Level Subspace Access

    /// Get subspace for a specific level
    ///
    /// - Parameter level: Level number (0 = leaf, 1+ = skip list layers)
    /// - Returns: Subspace for the level
    public func subspace(for level: Int) -> Subspace {
        if level == 0 {
            return leaf
        } else {
            return levels.subspace(Int64(level))
        }
    }

    // MARK: - Metadata Keys

    /// Key for storing current number of levels
    public var numLevelsKey: [UInt8] {
        metadata.pack(Tuple(MetadataKey.numLevels.rawValue))
    }

    /// Key for storing fanout factor
    public var fanoutKey: [UInt8] {
        metadata.pack(Tuple(MetadataKey.fanout.rawValue))
    }

    /// Key for storing total element count (atomic counter)
    public var countKey: [UInt8] {
        metadata.pack(Tuple(MetadataKey.count.rawValue))
    }
}
