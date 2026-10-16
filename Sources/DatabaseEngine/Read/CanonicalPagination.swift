import DatabaseWire
import DatabaseTypes
import DatabaseKit

public struct CanonicalPageWindow<Item: Sendable>: Sendable {
    public let items: [Item]
    public let columns: [QueryColumn]
    public let continuation: QueryContinuation?

    public init(items: [Item], columns: [QueryColumn], continuation: QueryContinuation?) throws {
        try QueryColumn.validate(columns)
        self.items = items
        self.columns = columns
        self.continuation = continuation
    }
}

package struct CanonicalRetainedPageWindow: Sendable {
    package let columns: [QueryColumn]
    package let range: Range<Int>
    package let continuation: QueryContinuation?
}

public enum CanonicalQueryPagination {
    private static let fingerprintedCursorMarker: UInt32 = 0x4351_5031
    private static let stableSnapshotCursorMarker: UInt32 = 0x4351_5032
    private static let fingerprintByteCount = SHA256Accumulator.digestByteCount

    private struct Cursor: Sendable {
        let queryFingerprint: ByteString
        let resultFingerprint: ByteString?
        let storagePosition: ByteString?
        let offset: UInt64

        func encode() throws -> QueryContinuation {
            let bytes = try StorageFrameEncoder.encode(
                limits: try CanonicalQueryPagination.cursorLimits()
            ) {
                (writer: inout StorageFrameEncoder) throws(
                    StorageFrameError
                ) in
                writer.writeUInt32(
                    resultFingerprint == nil
                        ? CanonicalQueryPagination.stableSnapshotCursorMarker
                        : CanonicalQueryPagination.fingerprintedCursorMarker
                )
                try writer.writeBytes(queryFingerprint)
                if let resultFingerprint {
                    try writer.writeBytes(resultFingerprint)
                } else {
                    try writer.writeBytes(storagePosition ?? [])
                }
                writer.writeUInt64(offset)
            }
            return QueryContinuation(bytes)
        }

        static func decode(
            _ continuation: QueryContinuation
        ) throws -> Cursor {
            do {
                var reader = try StorageFrameDecoder(
                    continuation.bytes,
                    limits: try CanonicalQueryPagination.cursorLimits()
                )
                let marker = try reader.readUInt32()
                guard marker == CanonicalQueryPagination.fingerprintedCursorMarker
                        || marker == CanonicalQueryPagination.stableSnapshotCursorMarker else {
                    throw CanonicalReadError.invalidContinuation
                }
                let queryFingerprint = try reader.readBytes()
                let resultFingerprint = marker
                    == CanonicalQueryPagination.fingerprintedCursorMarker
                    ? try reader.readBytes()
                    : nil
                let storagePosition = marker
                    == CanonicalQueryPagination.stableSnapshotCursorMarker
                    ? try reader.readBytes()
                    : nil
                let offset = try reader.readUInt64()
                try reader.ensureFullyRead()
                guard queryFingerprint.count
                        == CanonicalQueryPagination.fingerprintByteCount,
                      resultFingerprint.map({
                          $0.count == CanonicalQueryPagination.fingerprintByteCount
                      }) ?? true else {
                    throw CanonicalReadError.invalidContinuation
                }
                return Cursor(
                    queryFingerprint: queryFingerprint,
                    resultFingerprint: resultFingerprint,
                    storagePosition: storagePosition?.isEmpty == false
                        ? storagePosition
                        : nil,
                    offset: offset
                )
            } catch {
                throw CanonicalReadError.invalidContinuation
            }
        }
    }

    public static func window(
        rows: consuming [QueryRow],
        columns: [QueryColumn],
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        rowsAreContinuationRelative: Bool = false,
        continuationPosition: ByteString? = nil,
        prevalidatedQueryFingerprint: ByteString? = nil
    ) throws -> CanonicalPageWindow<QueryRow> {
        let selection = try retainedWindowSelection(
            rowCount: rows.count,
            columns: columns,
            withRow: { index, body in try body(rows[index]) },
            selectQuery: selectQuery,
            options: options,
            rowsAreContinuationRelative: rowsAreContinuationRelative,
            continuationPosition: continuationPosition,
            prevalidatedQueryFingerprint: prevalidatedQueryFingerprint
        )
        return try CanonicalPageWindow(
            items: trimOwnedRows(consume rows, to: selection.range),
            columns: columns,
            continuation: selection.continuation
        )
    }

    /// Resolves a page without releasing or copying the retained row owner.
    /// Internal query composition uses the returned range as a zero-copy view;
    /// only a public result boundary promotes the backing Array.
    package static func retainedWindow(
        rows: DatabaseSharedRetainedQueryRows,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        rowsAreLogicalQueryRelative: Bool = false,
        rowsAreContinuationRelative: Bool = false,
        continuationPosition: ByteString? = nil,
        prevalidatedQueryFingerprint: ByteString? = nil
    ) throws -> CanonicalRetainedPageWindow {
        try retainedWindowSelection(
            rowCount: rows.count,
            columns: rows.columns,
            withRow: { index, body in try rows.withElement(at: index, body) },
            selectQuery: selectQuery,
            options: options,
            rowsAreLogicalQueryRelative: rowsAreLogicalQueryRelative,
            rowsAreContinuationRelative: rowsAreContinuationRelative,
            continuationPosition: continuationPosition,
            prevalidatedQueryFingerprint: prevalidatedQueryFingerprint
        )
    }

    private static func retainedWindowSelection(
        rowCount: Int,
        columns: [QueryColumn],
        withRow: (Int, (borrowing QueryRow) throws -> Void) throws -> Void,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        rowsAreLogicalQueryRelative: Bool = false,
        rowsAreContinuationRelative: Bool,
        continuationPosition: ByteString?,
        prevalidatedQueryFingerprint: ByteString?
    ) throws -> CanonicalRetainedPageWindow {
        try QueryColumn.validate(columns)
        for index in 0..<rowCount { try withRow(index) { try $0.validate(columns: columns) } }
        // Pagination can be reached from native and graph execution paths, so
        // it performs its own bounded admission before internal wire limits are
        // relaxed for canonical streaming.
        try QueryStructuralValidator.validate(
            selectQuery,
            limits: options.queryStructuralLimits
        )
        func fixed(_ count: QueryCount?, name: String) throws -> UInt64? {
            guard let count else { return nil }
            guard case .fixed(let value) = count else {
                throw CanonicalReadError.unsupportedSelectQuery("Expression pagination requires selected invocation evaluation")
            }
            return value
        }
        let rawOffset = try rowsAreLogicalQueryRelative ? 0 : fixed(selectQuery.pagination.offset, name: "OFFSET") ?? 0
        guard let queryOffset = Int(exactly: rawOffset) else {
            throw CanonicalReadError.paginationValueExceedsRuntimeRange(name: "OFFSET", value: rawOffset)
        }
        let rawLimit: UInt64?
        if rowsAreLogicalQueryRelative { rawLimit = nil }
        else if case .count(let count, _) = selectQuery.pagination.limit { rawLimit = try fixed(count, name: "LIMIT") }
        else { rawLimit = nil }
        let logicalLimit: Int?
        if let rawLimit {
            guard let value = Int(exactly: rawLimit) else {
                throw CanonicalReadError.paginationValueExceedsRuntimeRange(name: "LIMIT", value: rawLimit)
            }
            logicalLimit = value
        } else { logicalLimit = nil }
        let requestedPageSize = try options.resolvePageSize()
        guard requestedPageSize.map({ $0 > 0 }) ?? true else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Pagination page size must be positive"
            )
        }

        if !options.options.appliesExternalPageWindow {
            guard options.continuation == nil else {
                throw CanonicalReadError.invalidContinuation
            }
            let visible = trimOwnedRows(
                totalCount: rowCount,
                offset: rowsAreLogicalQueryRelative ? 0 : queryOffset,
                count: rowsAreLogicalQueryRelative ? nil : logicalLimit
            )
            try options.workMeter.consume(
                UInt64(visible.count),
                at: .resultMaterialization
            )
            return CanonicalRetainedPageWindow(
                columns: columns,
                range: visible,
                continuation: nil
            )
        }

        let cursor = try options.continuation.map(Cursor.decode(_:))
        var validatedQueryFingerprint: ByteString?
        var validatedResultFingerprint: ByteString?
        if let cursor {
            let queryFingerprint: ByteString
            if let prevalidatedQueryFingerprint {
                queryFingerprint = prevalidatedQueryFingerprint
            } else {
                queryFingerprint = try Self.queryFingerprint(
                    selectQuery,
                    scope: options.options.continuationScope,
                    workMeter: options.workMeter
                )
            }
            let resultFingerprint = options.options
                .continuationSnapshotIsStable
                ? nil
                : try Self.resultFingerprint(
                    rowCount: rowCount, columns: columns, withRow: withRow,
                    workMeter: options.workMeter
                )
            guard cursor.queryFingerprint == queryFingerprint,
                  cursor.resultFingerprint == resultFingerprint else {
                throw CanonicalReadError.invalidContinuation
            }
            validatedQueryFingerprint = queryFingerprint
            validatedResultFingerprint = resultFingerprint
        }
        guard let continuationOffset = Int(
            exactly: cursor?.offset ?? 0
        ) else {
            throw CanonicalReadError.invalidContinuation
        }
        let (absoluteOffset, offsetOverflow) = queryOffset
            .addingReportingOverflow(continuationOffset)
        guard !offsetOverflow else {
            throw CanonicalReadError.invalidContinuation
        }
        let baseOffset: Int
        if rowsAreContinuationRelative {
            baseOffset = 0
        } else if rowsAreLogicalQueryRelative {
            baseOffset = continuationOffset
        } else {
            baseOffset = absoluteOffset
        }

        let remainingLimit = logicalLimit.map {
            continuationOffset >= $0 ? 0 : $0 - continuationOffset
        }
        guard remainingLimit != 0 else {
            return CanonicalRetainedPageWindow(
                columns: columns,
                range: 0..<0,
                continuation: nil
            )
        }
        let effectivePageSize = pageSize(
            requested: requestedPageSize,
            remainingLimit: remainingLimit
        )
        guard let effectivePageSize else {
            let visible = trimOwnedRows(
                totalCount: rowCount,
                offset: baseOffset,
                count: nil
            )
            try options.workMeter.consume(
                UInt64(visible.count),
                at: .resultMaterialization
            )
            return CanonicalRetainedPageWindow(
                columns: columns,
                range: visible,
                continuation: nil
            )
        }

        let (lookaheadCount, lookaheadOverflow) = effectivePageSize
            .addingReportingOverflow(1)
        let availableCount = baseOffset >= rowCount
            ? 0
            : rowCount - baseOffset
        let inspectedCount = min(
            availableCount,
            lookaheadOverflow ? Int.max : lookaheadCount
        )
        let visibleCount = min(inspectedCount, effectivePageSize)
        let (nextOffset, nextOffsetOverflow) = continuationOffset
            .addingReportingOverflow(visibleCount)
        guard !nextOffsetOverflow else {
            throw CanonicalReadError.invalidContinuation
        }
        let withinLogicalLimit = logicalLimit.map {
            nextOffset < $0
        } ?? true
        let hasMore = inspectedCount > effectivePageSize
            && withinLogicalLimit
        let continuation: QueryContinuation?
        if hasMore {
            let queryFingerprint: ByteString
            if let validatedQueryFingerprint {
                queryFingerprint = validatedQueryFingerprint
            } else if let prevalidatedQueryFingerprint {
                queryFingerprint = prevalidatedQueryFingerprint
            } else {
                queryFingerprint = try Self.queryFingerprint(
                    selectQuery,
                    scope: options.options.continuationScope,
                    workMeter: options.workMeter
                )
            }
            let resultFingerprint: ByteString?
            if options.options.continuationSnapshotIsStable {
                resultFingerprint = nil
            } else if let validatedResultFingerprint {
                resultFingerprint = validatedResultFingerprint
            } else {
                resultFingerprint = try Self.resultFingerprint(
                    rowCount: rowCount, columns: columns, withRow: withRow,
                    workMeter: options.workMeter
                )
            }
            continuation = try Cursor(
                queryFingerprint: queryFingerprint,
                resultFingerprint: resultFingerprint,
                storagePosition: continuationPosition,
                offset: UInt64(nextOffset)
            ).encode()
        } else {
            continuation = nil
        }
        try options.workMeter.consume(
            UInt64(visibleCount),
            at: .resultMaterialization
        )
        let visible = trimOwnedRows(
            totalCount: rowCount,
            offset: baseOffset,
            count: visibleCount
        )
        return CanonicalRetainedPageWindow(
            columns: columns,
            range: visible,
            continuation: continuation
        )
    }

    /// Validates a continuation before a stable-snapshot storage window is
    /// planned and returns its logical offset. A non-nil query fingerprint is
    /// proof that an incoming continuation was validated, independently of
    /// whether its storage position permits physical window pushdown. This
    /// does not inspect result rows because the caller-owned read point already
    /// fixes their identity.
    package struct StableSnapshotCursor: Sendable {
        package let queryFingerprint: ByteString?
        package let offset: Int
        package let storagePosition: ByteString?
    }

    package static func validatedStableSnapshotCursor(
        selectQuery: SelectQuery,
        options: ReadExecutionContext
    ) throws -> StableSnapshotCursor {
        guard options.options.continuationSnapshotIsStable else {
            return StableSnapshotCursor(
                queryFingerprint: nil,
                offset: 0,
                storagePosition: nil
            )
        }
        guard let continuation = options.continuation else {
            return StableSnapshotCursor(
                queryFingerprint: nil,
                offset: 0,
                storagePosition: nil
            )
        }
        let queryFingerprint = try queryFingerprint(
            selectQuery,
            scope: options.options.continuationScope,
            workMeter: options.workMeter
        )
        let cursor = try Cursor.decode(continuation)
        guard cursor.queryFingerprint == queryFingerprint,
              cursor.resultFingerprint == nil,
              let offset = Int(exactly: cursor.offset) else {
            throw CanonicalReadError.invalidContinuation
        }
        return StableSnapshotCursor(
            queryFingerprint: queryFingerprint,
            offset: offset,
            storagePosition: cursor.storagePosition
        )
    }

    private static func trimOwnedRows(
        totalCount: Int,
        offset: Int,
        count requestedCount: Int?
    ) -> Range<Int> {
        guard offset < totalCount else { return totalCount..<totalCount }
        let availableCount = totalCount - offset
        let visibleCount = min(requestedCount ?? availableCount, availableCount)
        return offset..<(offset + visibleCount)
    }

    /// Narrows a uniquely-owned result buffer in place. The owned array is
    /// consumed so pagination never allocates ArraySlice and Array copies for
    /// the visible page.
    private static func trimOwnedRows(
        _ rows: consuming [QueryRow],
        to range: Range<Int>
    ) -> [QueryRow] {
        guard !range.isEmpty else { return [] }
        var result = consume rows
        if range.upperBound < result.count {
            result.removeLast(result.count - range.upperBound)
        }
        if range.lowerBound > 0 {
            result.removeFirst(range.lowerBound)
        }
        return result
    }

    private static func pageSize(
        requested: Int?,
        remainingLimit: Int?
    ) -> Int? {
        switch (requested, remainingLimit) {
        case let (.some(pageSize), .some(limit)):
            return min(pageSize, limit)
        case let (.some(pageSize), .none):
            return pageSize
        case let (.none, .some(limit)):
            return limit
        case (.none, .none):
            return nil
        }
    }

    private static func queryFingerprint(
        _ selectQuery: SelectQuery,
        scope: ByteString,
        workMeter: DatabaseWorkMeter
    ) throws -> ByteString {
        var hasher = SHA256Accumulator()
        let maximumIntermediateBytes = workMeter.budget.maximumIntermediateBytes
        let maximumFrameBytes = Int(
            min(maximumIntermediateBytes, UInt64(Int.max))
        )
        let limits: DatabaseWireLimits
        do {
            limits = try DatabaseWireLimits(
                maximumFrameBytes: maximumFrameBytes,
                maximumStringBytes: maximumFrameBytes,
                maximumByteStringBytes: maximumFrameBytes,
                maximumCollectionCount:
                    DatabaseWireLimits.default.maximumCollectionCount,
                maximumNestingDepth:
                    DatabaseWireLimits.maximumSupportedNestingDepth,
                maximumObjectCount:
                    DatabaseWireLimits.default.maximumObjectCount
            )
        } catch {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Query fingerprint limits are invalid"
            )
        }
        do {
            try QueryIRWireFormat.emitCanonicalEncoding(
                .select(selectQuery),
                limits: limits,
                prepare: { queryByteCount in
                    try claimFingerprintBytes(
                        queryByteCount: queryByteCount,
                        scopeByteCount: scope.count,
                        workMeter: workMeter
                    )
                    updateDomain(0x0151_4244, hasher: &hasher)
                    appendLength(queryByteCount, to: &hasher)
                },
                consume: { bytes in
                    hasher.update(bytes)
                }
            )
        } catch let emissionError {
            switch emissionError {
            case .encoding(let wireError):
                switch wireError {
                case .frameTooLarge(let actual, _),
                     .stringTooLarge(let actual, _),
                     .byteStringTooLarge(let actual, _):
                    throw DatabaseWorkLimitError.maximumIntermediateBytes(
                        stage: .resultMaterialization,
                        consumed: 0,
                        requested: UInt64(actual),
                        maximum: maximumIntermediateBytes
                    )
                case .byteCountOverflow:
                    throw DatabaseWorkLimitError.maximumIntermediateBytes(
                        stage: .resultMaterialization,
                        consumed: 0,
                        requested: UInt64.max,
                        maximum: maximumIntermediateBytes
                    )
                default:
                    throw CanonicalReadError.unsupportedSelectQuery(
                        "Query cannot be represented for continuation fingerprinting"
                    )
                }
            case .destination(let destinationError):
                throw destinationError
            }
        }
        append(scope, to: &hasher)
        return hasher.finalize()
    }

    private static func cursorLimits() throws -> StorageFrameLimits {
        try StorageFrameLimits(
            maximumFrameBytes: 4_096,
            maximumStringBytes: 0,
            maximumByteStringBytes: 4_000,
            maximumCollectionCount: 0,
            maximumNestingDepth: 0
        )
    }

    private static func claimFingerprintBytes(
        queryByteCount: Int,
        scopeByteCount: Int,
        workMeter: DatabaseWorkMeter
    ) throws {
        let queryBytes = UInt64(queryByteCount)
        let scopeBytes = UInt64(scopeByteCount)
        let (totalBytes, overflow) = queryBytes.addingReportingOverflow(
            scopeBytes
        )
        let maximum = workMeter.budget.maximumIntermediateBytes
        guard !overflow, totalBytes <= maximum else {
            throw DatabaseWorkLimitError.maximumIntermediateBytes(
                stage: .resultMaterialization,
                consumed: 0,
                requested: overflow ? UInt64.max : totalBytes,
                maximum: maximum
            )
        }
        try workMeter.consume(totalBytes, at: .resultMaterialization)
    }

    private static func resultFingerprint(
        rowCount: Int,
        columns: [QueryColumn],
        withRow: (Int, (borrowing QueryRow) throws -> Void) throws -> Void,
        workMeter: DatabaseWorkMeter
    ) throws -> ByteString {
        var hasher = SHA256Accumulator()
        updateDomain(0x0150_4244, hasher: &hasher)
        for index in 0..<rowCount {
            try withRow(index) { row in
                let fingerprint = try CanonicalRowFingerprint.compute(row, columns: columns, workMeter: workMeter)
                hasher.update(fingerprint)
            }
        }
        return hasher.finalize()
    }

    private static func append(
        _ bytes: ByteString,
        to hasher: inout SHA256Accumulator
    ) {
        appendLength(bytes.count, to: &hasher)
        hasher.update(bytes)
    }

    private static func appendLength(
        _ byteCount: Int,
        to hasher: inout SHA256Accumulator
    ) {
        var length = UInt64(byteCount).littleEndian
        withUnsafeBytes(of: &length) { buffer in
            hasher.update(buffer)
        }
    }

    private static func updateDomain(
        _ value: UInt32,
        hasher: inout SHA256Accumulator
    ) {
        var littleEndian = value.littleEndian
        withUnsafeBytes(of: &littleEndian) { buffer in
            hasher.update(buffer)
        }
    }
}
