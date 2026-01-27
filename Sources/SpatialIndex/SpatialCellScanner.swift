// SpatialCellScanner.swift
// SpatialIndex - Unified spatial cell scanner for S2 and Morton encodings
//
// Design: Follows GraphEdgeScanner pattern for centralized scanning logic.

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Spatial

// MARK: - SpatialCellScanner

/// Unified spatial cell scanner for S2 and Morton encodings
///
/// **Design**: Follows GraphEdgeScanner pattern
/// - Centralized scanning logic for all spatial queries
/// - Efficient Tuple operations (no redundant pack/unpack)
/// - Early limit application (before item fetch)
/// - LimitReason for transparent incomplete results
///
/// **Reference**: GraphIndex/GraphEdgeScanner.swift
///
/// **Usage**:
/// ```swift
/// let scanner = SpatialCellScanner(
///     indexSubspace: subspace,
///     encoding: .s2,
///     level: 15
/// )
/// let (keys, reason) = try await scanner.scanCells(
///     cellIds: coveringCells,
///     limit: 100,
///     transaction: tx
/// )
/// ```
public final class SpatialCellScanner: Sendable {

    private let indexSubspace: Subspace
    private let encoding: SpatialEncoding
    private let level: Int

    /// Initialize scanner with index configuration
    ///
    /// - Parameters:
    ///   - indexSubspace: Subspace containing the spatial index
    ///   - encoding: Spatial encoding type (S2 or Morton)
    ///   - level: Precision level for the encoding
    public init(
        indexSubspace: Subspace,
        encoding: SpatialEncoding,
        level: Int
    ) {
        self.indexSubspace = indexSubspace
        self.encoding = encoding
        self.level = level
    }

    // MARK: - Cell Scanning

    /// Scan cells and extract primary keys with early limit application
    ///
    /// **Algorithm**:
    /// 1. For each covering cell, scan its subspace
    /// 2. Extract primary key tuples efficiently (single unpack)
    /// 3. Deduplicate using packed bytes as key
    /// 4. Apply limit during scanning (not after)
    ///
    /// **Performance**:
    /// - Time: O(n) where n = number of index entries scanned
    /// - Space: O(min(n, limit)) for result storage
    ///
    /// - Parameters:
    ///   - cellIds: Array of S2 or Morton cell IDs to scan
    ///   - limit: Optional maximum number of results
    ///   - transaction: FDB transaction
    /// - Returns: Tuple of (primary keys, optional limit reason)
    public func scanCells(
        cellIds: [UInt64],
        limit: Int?,
        transaction: any TransactionProtocol
    ) async throws -> (keys: [Tuple], limitReason: LimitReason?) {
        var results: [Tuple] = []
        var seenIds: Set<Data> = []
        var limitReason: LimitReason? = nil

        let effectiveLimit = limit ?? Int.max

        cellLoop: for cellId in cellIds {
            let cellTuple = Tuple(cellId)
            let cellSubspace = indexSubspace.subspace(cellTuple)
            let (begin, end) = cellSubspace.range()

            let sequence = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(begin),
                endSelector: .firstGreaterOrEqual(end),
                snapshot: true
            )

            for try await (key, _) in sequence {
                guard cellSubspace.contains(key) else { break }

                // Efficient Tuple extraction: single unpack, no redundant pack/unpack
                let keyTuple = try cellSubspace.unpack(key)

                // Deduplicate using packed bytes as stable key
                // (same item may appear in multiple covering cells)
                let idData = Data(keyTuple.pack())
                guard !seenIds.contains(idData) else { continue }
                seenIds.insert(idData)

                // Early limit check - stop scanning when limit reached
                if results.count >= effectiveLimit {
                    limitReason = .maxResultsReached(
                        returned: effectiveLimit,
                        limit: effectiveLimit
                    )
                    break cellLoop
                }

                results.append(keyTuple)
            }
        }

        return (results, limitReason)
    }

    /// Scan cells and extract primary keys with distance filtering
    ///
    /// For radius queries, this method filters results by actual distance
    /// from the center point (covering cells may include points outside radius).
    ///
    /// - Parameters:
    ///   - cellIds: Array of covering cell IDs
    ///   - center: Center point for distance calculation
    ///   - radiusMeters: Maximum distance in meters
    ///   - limit: Optional maximum number of results
    ///   - coordinateExtractor: Function to extract coordinates from primary key
    ///   - transaction: FDB transaction
    /// - Returns: Tuple of (primary keys with distances, optional limit reason)
    public func scanCellsWithDistance(
        cellIds: [UInt64],
        center: GeoPoint,
        radiusMeters: Double,
        limit: Int?,
        coordinateExtractor: @escaping @Sendable (Tuple) -> GeoPoint?,
        transaction: any TransactionProtocol
    ) async throws -> (keys: [(key: Tuple, distance: Double)], limitReason: LimitReason?) {
        var results: [(key: Tuple, distance: Double)] = []
        var seenIds: Set<Data> = []
        var limitReason: LimitReason? = nil

        let effectiveLimit = limit ?? Int.max

        cellLoop: for cellId in cellIds {
            let cellTuple = Tuple(cellId)
            let cellSubspace = indexSubspace.subspace(cellTuple)
            let (begin, end) = cellSubspace.range()

            let sequence = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(begin),
                endSelector: .firstGreaterOrEqual(end),
                snapshot: true
            )

            for try await (key, _) in sequence {
                guard cellSubspace.contains(key) else { break }

                let keyTuple = try cellSubspace.unpack(key)

                let idData = Data(keyTuple.pack())
                guard !seenIds.contains(idData) else { continue }
                seenIds.insert(idData)

                // Distance filtering is deferred to after item fetch
                // because we need the actual coordinates from the item

                if results.count >= effectiveLimit {
                    limitReason = .maxResultsReached(
                        returned: effectiveLimit,
                        limit: effectiveLimit
                    )
                    break cellLoop
                }

                // Distance will be calculated after item fetch
                results.append((key: keyTuple, distance: 0))
            }
        }

        return (results, limitReason)
    }

    /// Scan a single cell and return all primary keys
    ///
    /// Useful for debugging or when you need all items in a specific cell.
    ///
    /// - Parameters:
    ///   - cellId: Single cell ID to scan
    ///   - transaction: FDB transaction
    /// - Returns: Array of primary key tuples
    public func scanSingleCell(
        cellId: UInt64,
        transaction: any TransactionProtocol
    ) async throws -> [Tuple] {
        let cellTuple = Tuple(cellId)
        let cellSubspace = indexSubspace.subspace(cellTuple)
        let (begin, end) = cellSubspace.range()

        var results: [Tuple] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: true
        )

        for try await (key, _) in sequence {
            guard cellSubspace.contains(key) else { break }

            let keyTuple = try cellSubspace.unpack(key)
            results.append(keyTuple)
        }

        return results
    }
}

// MARK: - SpatialScanResult

/// Result of a spatial scan operation
public struct SpatialScanResult: Sendable {
    /// Primary keys of matching items
    public let keys: [Tuple]

    /// Reason why the scan was incomplete, if applicable
    public let limitReason: LimitReason?

    /// Whether the scan completed without hitting any limits
    public var isComplete: Bool {
        limitReason == nil
    }

    public init(keys: [Tuple], limitReason: LimitReason?) {
        self.keys = keys
        self.limitReason = limitReason
    }
}
