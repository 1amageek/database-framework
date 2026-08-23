// SpatialCellScanner.swift
// SpatialIndex - Unified spatial cell scanner for S2 and Morton encodings
//
// Design: Follows GraphEdgeScanner pattern for centralized scanning logic.

import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import StorageKit

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
        transaction: any TransactionAccess
    ) async throws -> (keys: [Tuple], limitReason: LimitReason?) {
        var results: [Tuple] = []
        var seenIds: Set<ByteString> = []
        var limitReason: LimitReason? = nil

        let effectiveLimit = limit ?? Int.max

        cellLoop: for cellId in cellIds {
            let cellTuple = Tuple(cellId)
            let cellSubspace = indexSubspace.subspace(cellTuple)
            let (begin, end) = cellSubspace.range()

            let readLimit = SpatialScanBudget.rangeReadLimit(totalLimit: limit, emittedCount: results.count)
            let sequence = try await TransactionRangeCollection.collect(using: transaction,
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                limit: readLimit,
                reverse: false,
                snapshot: true,
                streamingMode: .iterator
            )

            for (key, _) in sequence {
                guard cellSubspace.contains(key) else { break }

                // Efficient Tuple extraction: single unpack, no redundant pack/unpack
                let keyTuple = try cellSubspace.unpack(key)
                guard !keyTuple.isEmpty else {
                    throw SpatialCellScannerError.missingPrimaryKey
                }

                // Deduplicate using packed bytes as stable key
                // (same item may appear in multiple covering cells)
                let identifier = keyTuple.pack()
                guard !seenIds.contains(identifier) else { continue }
                seenIds.insert(identifier)

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

            if limit != nil && limitReason == nil && sequence.count >= readLimit {
                limitReason = .maxResultsReached(returned: results.count, limit: effectiveLimit)
                break cellLoop
            }
        }

        return (results, limitReason)
    }

    internal func scan(
        plan: SpatialScanPlan,
        limit: Int?,
        transaction: any TransactionAccess
    ) async throws -> (keys: [Tuple], limitReason: LimitReason?) {
        switch plan {
        case .cells(let cellIds):
            return try await scanCells(cellIds: cellIds, limit: limit, transaction: transaction)
        case .codeRange(let minCode, let maxCode):
            return try await scanCodeRange(
                minCode: minCode,
                maxCode: maxCode,
                limit: limit,
                transaction: transaction
            )
        }
    }

    internal func scanCodeRange(
        minCode: UInt64,
        maxCode: UInt64,
        limit: Int?,
        transaction: any TransactionAccess
    ) async throws -> (keys: [Tuple], limitReason: LimitReason?) {
        var results: [Tuple] = []
        var seenIds: Set<ByteString> = []
        var limitReason: LimitReason? = nil

        let effectiveLimit = limit ?? Int.max
        let rangeStart = indexSubspace.pack(Tuple(minCode))
        let maxKey = indexSubspace.pack(Tuple(maxCode))
        let rangeEnd = try strinc(maxKey)

        let readLimit = SpatialScanBudget.rangeReadLimit(totalLimit: limit, emittedCount: results.count)
        let sequence = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(rangeStart),
            to: .firstGreaterOrEqual(rangeEnd),
            limit: readLimit,
            reverse: false,
            snapshot: true,
            streamingMode: .iterator
        )

        for (key, _) in sequence {
            guard indexSubspace.contains(key) else { break }

            let keyTuple = try indexSubspace.unpack(key)
            guard keyTuple.count >= 2 else {
                throw SpatialCellScannerError.missingPrimaryKey
            }

            var idElements: [any TupleElement] = []
            for index in 1..<keyTuple.count {
                if let element = keyTuple[index] {
                    idElements.append(element)
                }
            }
            let idTuple = Tuple(idElements)
            let identifier = idTuple.pack()
            guard !seenIds.contains(identifier) else { continue }
            seenIds.insert(identifier)

            if results.count >= effectiveLimit {
                limitReason = .maxResultsReached(returned: effectiveLimit, limit: effectiveLimit)
                break
            }

            results.append(idTuple)
        }

        if limit != nil && limitReason == nil && sequence.count >= readLimit {
            limitReason = .maxResultsReached(returned: results.count, limit: effectiveLimit)
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
        transaction: any TransactionAccess
    ) async throws -> [Tuple] {
        let cellTuple = Tuple(cellId)
        let cellSubspace = indexSubspace.subspace(cellTuple)
        let (begin, end) = cellSubspace.range()

        var results: [Tuple] = []

        let sequence = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        for (key, _) in sequence {
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
