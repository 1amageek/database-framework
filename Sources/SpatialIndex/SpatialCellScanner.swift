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
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> (keys: [Tuple], limitReason: LimitReason?) {
        let retention = try workMeter?.reserveIntermediate(
            bytes: UInt64(MemoryLayout<[Tuple]>.stride)
                + UInt64(MemoryLayout<Set<ByteString>>.stride),
            at: .indexScan
        )
        defer { retention?.release() }
        var results: [Tuple] = []
        var seenIds: Set<ByteString> = []
        var limitReason: LimitReason? = nil

        let effectiveLimit = limit ?? Int.max

        cellLoop: for cellId in cellIds {
            let cellSubspace = indexSubspace.subspace(cellId)
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
                try workMeter?.consume(at: .indexScan)
                try retention?.reserveAdditional(
                    bytes: UInt64(key.count) + 128,
                    at: .indexScan
                )

                // Efficient Tuple extraction: single unpack, no redundant pack/unpack
                let keyTuple = try cellSubspace.unpack(key)
                guard !keyTuple.isEmpty else {
                    throw SpatialCellScannerError.missingPrimaryKey
                }

                // Deduplicate using packed bytes as stable key
                // (same item may appear in multiple covering cells)
                let identifier = keyTuple.pack()
                guard !seenIds.contains(identifier) else { continue }
                try retention?.reserveAdditional(
                    rows: 1,
                    bytes: UInt64(identifier.count)
                        + UInt64(MemoryLayout<Tuple>.stride)
                        + 64,
                    at: .indexScan
                )
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
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> (keys: [Tuple], limitReason: LimitReason?) {
        switch plan {
        case .cells(let cellPlan):
            return try await scanCells(
                cellIds: cellPlan.cells,
                limit: limit,
                transaction: transaction,
                workMeter: workMeter
            )
        case .codeRanges(let ranges):
            return try await scanCodeRanges(
                ranges,
                limit: limit,
                transaction: transaction,
                workMeter: workMeter
            )
        }
    }

    /// Scans spatial entries into request-accounted retained ownership.
    ///
    /// Unlike the compatibility array-returning entry points, this path keeps
    /// the key reservation alive through downstream model fetch and Fusion
    /// materialization. Backend rows are consumed directly from their cursor,
    /// so no unaccounted range-result array exists before admission.
    internal func scanRetained(
        plan: SpatialScanPlan,
        limit: Int?,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RetainedSpatialScanResult {
        switch plan {
        case .cells(let cellPlan):
            return try await scanCellsRetained(
                cellIds: cellPlan.cells,
                limit: limit,
                transaction: transaction,
                workMeter: workMeter
            )
        case .codeRanges(let ranges):
            return try await scanCodeRangesRetained(
                ranges,
                limit: limit,
                transaction: transaction,
                workMeter: workMeter
            )
        }
    }

    /// Scans the complete spatial index in canonical key order, subject to a
    /// hard candidate cap. This is the correctness reference path for exact
    /// nearest-neighbor execution: it is independent of the configured space-
    /// filling curve and reports truncation only when a sentinel row proves
    /// that the index contains additional entries.
    internal func scanAllRetained(
        maximumEntries: Int,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RetainedSpatialScanResult {
        guard maximumEntries > 0 else {
            throw SpatialCellScannerError.invalidMaximumEntries(
                maximumEntries
            )
        }
        let readLimit = maximumEntries == Int.max
            ? Int.max
            : maximumEntries + 1

        var results = try DatabaseRetainedArrayBuilder<Tuple>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: Tuple.self)
        )
        let range = indexSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: readLimit,
            reverse: false,
            snapshot: true,
            streamingMode: .iterator
        )
        var limitReason: LimitReason?
        do {
            while let (key, _) = try await cursor.next() {
                guard indexSubspace.contains(key) else { break }
                try workMeter.consume(at: .indexScan)
                guard results.count < maximumEntries else {
                    limitReason = .maxCandidatesReached(
                        scanned: results.count,
                        limit: maximumEntries
                    )
                    break
                }

                let indexEntry = try indexSubspace.unpack(key)
                guard indexEntry.count >= 2 else {
                    throw SpatialCellScannerError.missingPrimaryKey
                }
                let primaryKey = try indexEntry.droppingFirstElements(1)
                let packedPrimaryKey = primaryKey.pack()
                try results.append(
                    footprint: DatabaseIntermediateFootprint(
                        rows: 1,
                        bytes: UInt64(packedPrimaryKey.count)
                    ),
                    at: .indexScan,
                    make: {
                        try Tuple(packed: packedPrimaryKey.detached())
                    }
                )
            }
        } catch {
            let iterationError = error
            do {
                try await cursor.finish()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            throw iterationError
        }
        try await cursor.finish()
        return RetainedSpatialScanResult(
            keys: try results.finish().moveToSharedOwnership(at: .indexScan),
            limitReason: limitReason
        )
    }

    private func scanCellsRetained(
        cellIds: [UInt64],
        limit: Int?,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RetainedSpatialScanResult {
        var results = try DatabaseRetainedArrayBuilder<Tuple>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: Tuple.self),
            expectedCount: 0
        )
        let seenReservation = try workMeter.reserveIntermediate(
            bytes: UInt64(MemoryLayout<Set<ByteString>>.stride),
            at: .indexScan
        )
        defer { seenReservation.release() }
        var seenIds: Set<ByteString> = []
        var limitReason: LimitReason?
        let effectiveLimit = limit ?? Int.max

        cellLoop: for cellID in cellIds {
            let cellSubspace = indexSubspace.subspace(cellID)
            let range = cellSubspace.range()
            let readLimit = SpatialScanBudget.rangeReadLimit(
                totalLimit: limit,
                emittedCount: results.count
            )
            var cursor = transaction.rangeCursor(
                from: .firstGreaterOrEqual(range.begin),
                to: .firstGreaterOrEqual(range.end),
                limit: readLimit,
                reverse: false,
                snapshot: true,
                streamingMode: .iterator
            )
            var scannedCount = 0
            do {
                while let (key, _) = try await cursor.next() {
                    scannedCount += 1
                    guard cellSubspace.contains(key) else { break }
                    try workMeter.consume(at: .indexScan)
                    let primaryKey = try cellSubspace.unpack(key)
                    guard !primaryKey.isEmpty else {
                        throw SpatialCellScannerError.missingPrimaryKey
                    }
                    let identifierView = primaryKey.pack()
                    guard !seenIds.contains(identifierView) else { continue }
                    guard results.count < effectiveLimit else {
                        limitReason = .maxResultsReached(
                            returned: effectiveLimit,
                            limit: effectiveLimit
                        )
                        break
                    }
                    try seenReservation.reserveAdditional(
                        bytes: UInt64(identifierView.count)
                            + UInt64(MemoryLayout<ByteString>.stride)
                            + 64,
                        at: .indexScan
                    )
                    let identifier = identifierView.detached()
                    let ownedPrimaryKey = try Tuple(packed: identifier)
                    seenIds.insert(identifier)
                    try results.append(
                        footprint: DatabaseIntermediateFootprint(
                            rows: 1,
                            bytes: UInt64(identifier.count) + 64
                        ),
                        at: .indexScan,
                        make: { ownedPrimaryKey }
                    )
                }
            } catch {
                let iterationError = error
                do {
                    try await cursor.finish()
                } catch {
                    throw StorageRangeCleanupError(
                        iterationError: iterationError,
                        cleanupError: error
                    )
                }
                throw iterationError
            }
            try await cursor.finish()
            if limitReason != nil { break cellLoop }
            if limit != nil && scannedCount >= readLimit {
                limitReason = .maxResultsReached(
                    returned: results.count,
                    limit: effectiveLimit
                )
                break cellLoop
            }
        }
        return RetainedSpatialScanResult(
            keys: try results.finish().moveToSharedOwnership(at: .indexScan),
            limitReason: limitReason
        )
    }

    private func scanCodeRangesRetained(
        _ ranges: [SpatialCodeRange],
        limit: Int?,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RetainedSpatialScanResult {
        var results = try DatabaseRetainedArrayBuilder<Tuple>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: Tuple.self),
            expectedCount: 0
        )
        let seenReservation = try workMeter.reserveIntermediate(
            bytes: UInt64(MemoryLayout<Set<ByteString>>.stride),
            at: .indexScan
        )
        defer { seenReservation.release() }
        var seenIds: Set<ByteString> = []
        var limitReason: LimitReason?
        let effectiveLimit = limit ?? Int.max
        rangeLoop: for range in ranges {
            let rangeStart = indexSubspace.pack(Tuple(range.min))
            let rangeEnd = try strinc(indexSubspace.pack(Tuple(range.max)))
            let readLimit = SpatialScanBudget.rangeReadLimit(
                totalLimit: limit,
                emittedCount: results.count
            )
            var cursor = transaction.rangeCursor(
                from: .firstGreaterOrEqual(rangeStart),
                to: .firstGreaterOrEqual(rangeEnd),
                limit: readLimit,
                reverse: false,
                snapshot: true,
                streamingMode: .iterator
            )
            var scannedCount = 0
            do {
                while let (key, _) = try await cursor.next() {
                    scannedCount += 1
                    guard indexSubspace.contains(key) else { break }
                    try workMeter.consume(at: .indexScan)
                    let keyTuple = try indexSubspace.unpack(key)
                    guard keyTuple.count >= 2 else {
                        throw SpatialCellScannerError.missingPrimaryKey
                    }
                    var identifierElements: [any TupleElement] = []
                    identifierElements.reserveCapacity(keyTuple.count - 1)
                    for index in 1..<keyTuple.count {
                        if let element = keyTuple[index] {
                            identifierElements.append(element)
                        }
                    }
                    let primaryKey = Tuple(identifierElements)
                    let identifierView = primaryKey.pack()
                    guard !seenIds.contains(identifierView) else { continue }
                    guard results.count < effectiveLimit else {
                        limitReason = .maxResultsReached(
                            returned: effectiveLimit,
                            limit: effectiveLimit
                        )
                        break
                    }
                    try seenReservation.reserveAdditional(
                        bytes: UInt64(identifierView.count)
                            + UInt64(MemoryLayout<ByteString>.stride)
                            + 64,
                        at: .indexScan
                    )
                    let identifier = identifierView.detached()
                    let ownedPrimaryKey = try Tuple(packed: identifier)
                    seenIds.insert(identifier)
                    try results.append(
                        footprint: DatabaseIntermediateFootprint(
                            rows: 1,
                            bytes: UInt64(identifier.count) + 64
                        ),
                        at: .indexScan,
                        make: { ownedPrimaryKey }
                    )
                }
            } catch {
                let iterationError = error
                do {
                    try await cursor.finish()
                } catch {
                    throw StorageRangeCleanupError(
                        iterationError: iterationError,
                        cleanupError: error
                    )
                }
                throw iterationError
            }
            try await cursor.finish()
            if limitReason != nil { break rangeLoop }
            if limit != nil && scannedCount >= readLimit {
                limitReason = .maxResultsReached(
                    returned: results.count,
                    limit: effectiveLimit
                )
                break rangeLoop
            }
        }
        return RetainedSpatialScanResult(
            keys: try results.finish().moveToSharedOwnership(at: .indexScan),
            limitReason: limitReason
        )
    }

    internal func scanCodeRange(
        minCode: UInt64,
        maxCode: UInt64,
        limit: Int?,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> (keys: [Tuple], limitReason: LimitReason?) {
        try await scanCodeRanges(
            [SpatialCodeRange(min: minCode, max: maxCode)],
            limit: limit,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    private func scanCodeRanges(
        _ ranges: [SpatialCodeRange],
        limit: Int?,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> (keys: [Tuple], limitReason: LimitReason?) {
        let retention = try workMeter?.reserveIntermediate(
            bytes: UInt64(MemoryLayout<[Tuple]>.stride)
                + UInt64(MemoryLayout<Set<ByteString>>.stride),
            at: .indexScan
        )
        defer { retention?.release() }
        var results: [Tuple] = []
        var seenIds: Set<ByteString> = []
        var limitReason: LimitReason? = nil

        let effectiveLimit = limit ?? Int.max
        rangeLoop: for range in ranges {
            let rangeStart = indexSubspace.pack(Tuple(range.min))
            let rangeEnd = try strinc(indexSubspace.pack(Tuple(range.max)))
            let readLimit = SpatialScanBudget.rangeReadLimit(
                totalLimit: limit,
                emittedCount: results.count
            )
            let sequence = try await TransactionRangeCollection.collect(
                using: transaction,
                from: .firstGreaterOrEqual(rangeStart),
                to: .firstGreaterOrEqual(rangeEnd),
                limit: readLimit,
                reverse: false,
                snapshot: true,
                streamingMode: .iterator
            )

            for (key, _) in sequence {
                guard indexSubspace.contains(key) else { break }
                try workMeter?.consume(at: .indexScan)
                try retention?.reserveAdditional(
                    bytes: UInt64(key.count) + 128,
                    at: .indexScan
                )

                let keyTuple = try indexSubspace.unpack(key)
                guard keyTuple.count >= 2 else {
                    throw SpatialCellScannerError.missingPrimaryKey
                }

                var idElements: [any TupleElement] = []
                idElements.reserveCapacity(keyTuple.count - 1)
                for index in 1..<keyTuple.count {
                    if let element = keyTuple[index] {
                        idElements.append(element)
                    }
                }
                let idTuple = Tuple(idElements)
                let identifier = idTuple.pack()
                guard !seenIds.contains(identifier) else { continue }
                try retention?.reserveAdditional(
                    rows: 1,
                    bytes: UInt64(identifier.count)
                        + UInt64(MemoryLayout<Tuple>.stride)
                        + 64,
                    at: .indexScan
                )
                seenIds.insert(identifier)

                if results.count >= effectiveLimit {
                    limitReason = .maxResultsReached(
                        returned: effectiveLimit,
                        limit: effectiveLimit
                    )
                    break
                }

                results.append(idTuple)
            }

            if limitReason != nil { break rangeLoop }
            if limit != nil && sequence.count >= readLimit {
                limitReason = .maxResultsReached(
                    returned: results.count,
                    limit: effectiveLimit
                )
                break rangeLoop
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
        transaction: any TransactionReadAccess
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

internal struct RetainedSpatialScanResult: Sendable {
    internal let keys: DatabaseSharedRetainedArray<Tuple>
    internal let limitReason: LimitReason?
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
