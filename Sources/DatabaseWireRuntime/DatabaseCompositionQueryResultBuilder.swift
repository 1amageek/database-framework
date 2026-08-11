#if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire

/// Owns the bounded in-memory result window and publishes continuation pages
/// only after every durable page is complete.
actor DatabaseCompositionQueryResultBuilder {
    private struct PageBuffer: Sendable {
        var rows: [DatabaseWire.QueryRow] = []
        var origins: [CompositionOrigin] = []
        var reservation: DatabaseIntermediateReservation?

        var count: Int { rows.count }

        mutating func append(
            row: DatabaseWire.QueryRow,
            origin: CompositionOrigin,
            footprint: UInt64,
            workMeter: DatabaseWorkMeter
        ) throws {
            if let reservation {
                try reservation.reserveAdditional(
                    rows: 1,
                    bytes: footprint,
                    at: .resultMaterialization
                )
            } else {
                reservation = try workMeter.reserveIntermediate(
                    rows: 1,
                    bytes: footprint,
                    at: .resultMaterialization
                )
            }
            rows.append(row)
            origins.append(origin)
        }

        mutating func release() {
            reservation?.release()
            reservation = nil
            rows.removeAll(keepingCapacity: false)
            origins.removeAll(keepingCapacity: false)
        }

        /// The first page is already the operation's final output. Its row
        /// count is governed by the output/page limits, so keeping it while
        /// the remaining pages are durably spooled must not also consume the
        /// intermediate-row budget.
        mutating func promoteToOutput() {
            reservation?.release()
            reservation = nil
        }
    }

    private enum State: Sendable, Equatable {
        case accumulating
        case completed
        case aborted
    }

    private let compositionID: Base.Composition.ID
    private let compositionGeneration: UInt64
    private let schemaGeneration: UInt64
    private let baseIDs: [Base.ID]
    private let consistency: DatabaseKit.DatabaseReadConsistency
    private let pageLimit: Int
    private let queryFingerprint: ByteString
    private let authorization: AuthorizationContext
    private let maximumIntermediateBytes: UInt64
    private let snapshotStore: DatabaseQuerySnapshotStore?
    private let workMeter: DatabaseWorkMeter

    private var state: State = .accumulating
    private var columnNames: [String]?
    private var columns: [QueryColumn] = []
    private var firstPageBuffer: PageBuffer?
    private var currentPageBuffer = PageBuffer()
    private var writeReservation:
        DatabaseQuerySnapshotStore.WriteReservation?
    private var firstContinuationPageID: ByteString?
    private var currentPageID: ByteString?
    private var continuationPageCount: UInt32 = 0
    private var totalPayloadBytes: UInt64 = 0

    init(
        compositionID: Base.Composition.ID,
        compositionGeneration: UInt64,
        schemaGeneration: UInt64,
        baseIDs: [Base.ID],
        consistency: DatabaseKit.DatabaseReadConsistency,
        pageLimit: Int,
        queryFingerprint: ByteString,
        authorization: AuthorizationContext,
        maximumIntermediateBytes: UInt64,
        snapshotStore: DatabaseQuerySnapshotStore?,
        initialWriteReservation:
            DatabaseQuerySnapshotStore.WriteReservation? = nil,
        initialPayloadBytes: UInt64 = 0,
        workMeter: DatabaseWorkMeter
    ) {
        self.compositionID = compositionID
        self.compositionGeneration = compositionGeneration
        self.schemaGeneration = schemaGeneration
        self.baseIDs = baseIDs
        self.consistency = consistency
        self.pageLimit = pageLimit
        self.queryFingerprint = queryFingerprint
        self.authorization = authorization
        self.maximumIntermediateBytes = maximumIntermediateBytes
        self.snapshotStore = snapshotStore
        self.writeReservation = initialWriteReservation
        self.totalPayloadBytes = initialPayloadBytes
        self.workMeter = workMeter
    }

    func append(
        _ row: DatabaseEngine.QueryRow,
        origin: CompositionOrigin,
        footprint: UInt64
    ) async throws {
        guard state == .accumulating else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }
        let names = row.fields.keys.sorted()
        if let columnNames {
            guard names == columnNames else {
                throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                    "Composition members produced incompatible projected columns"
                )
            }
        } else {
            self.columnNames = names
            columns = try names.enumerated().map { offset, name in
                guard let number = UInt32(exactly: offset + 1) else {
                    throw DatabaseWireError.byteCountOverflow
                }
                return QueryColumn(number: number, name: name)
            }
        }

        if currentPageBuffer.count == pageLimit {
            try await makeRoomForAdditionalRow()
        }
        let wireRow = try DatabaseQueryRowEncoder.encode(
            row,
            columnNames: names
        )
        try currentPageBuffer.append(
            row: wireRow,
            origin: origin,
            footprint: footprint,
            workMeter: workMeter
        )
    }

    func adoptWorkspace(
        _ reservation: DatabaseQuerySnapshotStore.WriteReservation
    ) throws {
        guard state == .accumulating,
              writeReservation == nil,
              firstPageBuffer == nil,
              currentPageBuffer.count == 0,
              totalPayloadBytes == 0 else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }
        writeReservation = reservation
    }

    func accountWorkspacePayloadBytes(_ byteCount: UInt64) throws {
        guard state == .accumulating,
              writeReservation != nil,
              firstPageBuffer == nil,
              currentPageBuffer.count == 0,
              totalPayloadBytes == 0,
              byteCount <= maximumIntermediateBytes else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }
        totalPayloadBytes = byteCount
    }

    func finish() async throws -> QueryRowPage {
        guard state == .accumulating else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }
        guard firstPageBuffer != nil else {
            let page = try makePage(
                currentPageBuffer,
                continuation: nil
            )
            if let writeReservation, let snapshotStore {
                try await snapshotStore.abortWrite(writeReservation)
            }
            currentPageBuffer.release()
            self.writeReservation = nil
            state = .completed
            return page
        }
        guard let writeReservation,
              let snapshotStore,
              let currentPageID,
              let firstContinuationPageID,
              let firstPageBuffer else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }

        let lastPage = try makePage(currentPageBuffer, continuation: nil)
        totalPayloadBytes = try await snapshotStore.appendPage(
            lastPage,
            pageID: currentPageID,
            to: writeReservation,
            consumedPayloadBytes: totalPayloadBytes,
            maximumIntermediateBytes: maximumIntermediateBytes
        )
        continuationPageCount = try increment(continuationPageCount)

        let firstPage = try makePage(
            firstPageBuffer,
            continuation: writeReservation.continuation(
                pageID: firstContinuationPageID
            )
        )
        try await snapshotStore.commitWrite(
            writeReservation,
            pageCount: continuationPageCount,
            totalPayloadBytes: totalPayloadBytes
        )
        currentPageBuffer.release()
        var retainedFirstPage = firstPageBuffer
        retainedFirstPage.release()
        self.firstPageBuffer = nil
        self.writeReservation = nil
        state = .completed
        return firstPage
    }

    func abort() async throws {
        guard state == .accumulating else { return }
        state = .aborted
        defer {
            currentPageBuffer.release()
            if var firstPageBuffer {
                firstPageBuffer.release()
            }
            self.firstPageBuffer = nil
            writeReservation = nil
        }
        if let writeReservation, let snapshotStore {
            try await snapshotStore.abortWrite(writeReservation)
        }
    }

    private func makeRoomForAdditionalRow() async throws {
        guard let snapshotStore else {
            throw DatabaseQueryExecutionError.querySnapshotUnavailable(
                "the host did not provide durable Composition snapshot storage"
            )
        }
        if firstPageBuffer == nil {
            let reservation = if let writeReservation {
                writeReservation
            } else {
                try await snapshotStore.beginWrite(
                compositionID: compositionID,
                compositionGeneration: compositionGeneration,
                schemaGeneration: schemaGeneration,
                queryFingerprint: queryFingerprint,
                authorization: authorization
                )
            }
            let pageID = try await snapshotStore.reservePage(in: reservation)
            writeReservation = reservation
            firstContinuationPageID = pageID
            currentPageID = pageID
            var outputPageBuffer = currentPageBuffer
            outputPageBuffer.promoteToOutput()
            firstPageBuffer = outputPageBuffer
            currentPageBuffer = PageBuffer()
            return
        }
        guard let writeReservation, let currentPageID else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }
        let nextPageID = try await snapshotStore.reservePage(
            in: writeReservation
        )
        let page = try makePage(
            currentPageBuffer,
            continuation: writeReservation.continuation(pageID: nextPageID)
        )
        totalPayloadBytes = try await snapshotStore.appendPage(
            page,
            pageID: currentPageID,
            to: writeReservation,
            consumedPayloadBytes: totalPayloadBytes,
            maximumIntermediateBytes: maximumIntermediateBytes
        )
        continuationPageCount = try increment(continuationPageCount)
        currentPageBuffer.release()
        currentPageBuffer = PageBuffer()
        self.currentPageID = nextPageID
    }

    private func makePage(
        _ buffer: PageBuffer,
        continuation: ByteString?
    ) throws -> QueryRowPage {
        let provenance = try CompositionPageProvenance(
            compositionID: compositionID,
            generation: compositionGeneration,
            baseIDs: baseIDs,
            origins: buffer.origins
        )
        return try QueryRowPage(
            columns: columns,
            rows: buffer.rows,
            continuation: continuation,
            provenance: provenance,
            consistency: consistency
        )
    }

    private func increment(_ value: UInt32) throws -> UInt32 {
        let result = value.addingReportingOverflow(1)
        guard !result.overflow else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }
        return result.partialValue
    }
}

#endif
