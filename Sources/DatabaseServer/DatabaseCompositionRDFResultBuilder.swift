#if DATABASE_SERVER_GRAPH_INDEXES
import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

/// Publishes a globally distinct Composition RDF graph while retaining only
/// the visible page in memory. Exact identity and merged contributors live in
/// the control-domain spill owned by the snapshot reservation.
actor DatabaseCompositionRDFResultBuilder {
    private struct PageBuffer: Sendable {
        var quads: [RDFQuad] = []
        var origins: [CompositionOrigin] = []
        var reservation: DatabaseIntermediateReservation?

        var count: Int { quads.count }

        mutating func append(
            quad: RDFQuad,
            origin: CompositionOrigin,
            footprint: DatabaseIntermediateFootprint,
            workMeter: DatabaseWorkMeter
        ) throws {
            if let reservation {
                try reservation.reserveAdditional(
                    rows: footprint.rows,
                    bytes: footprint.bytes,
                    at: .resultMaterialization
                )
            } else {
                reservation = try workMeter.reserveIntermediate(
                    rows: footprint.rows,
                    bytes: footprint.bytes,
                    at: .resultMaterialization
                )
            }
            quads.append(quad)
            origins.append(origin)
        }

        mutating func release() {
            reservation?.release()
            reservation = nil
            quads.removeAll(keepingCapacity: false)
            origins.removeAll(keepingCapacity: false)
        }
    }

    private enum State: Sendable, Equatable {
        case accumulating
        case completed
        case aborted
    }

    private let compositionID: Base.Composition.ID
    private let compositionGeneration: UInt64
    private let baseIDs: [Base.ID]
    private let consistency: DatabaseKit.DatabaseReadConsistency
    private let pageLimit: Int
    private let maximumIntermediateBytes: UInt64
    private let snapshotStore: DatabaseCompositionSnapshotStore
    private let workMeter: DatabaseWorkMeter
    private let footprintMeter: DatabaseRDFQuadFootprintMeter

    private var state: State = .accumulating
    private var firstPageBuffer: PageBuffer?
    private var currentPageBuffer = PageBuffer()
    private var writeReservation: DatabaseCompositionSnapshotStore.WriteReservation?
    private var firstContinuationPageID: ByteString?
    private var currentPageID: ByteString?
    private var continuationPageCount: UInt32 = 0
    private var totalPayloadBytes: UInt64

    init(
        compositionID: Base.Composition.ID,
        compositionGeneration: UInt64,
        baseIDs: [Base.ID],
        consistency: DatabaseKit.DatabaseReadConsistency,
        pageLimit: Int,
        maximumIntermediateBytes: UInt64,
        snapshotStore: DatabaseCompositionSnapshotStore,
        reservation: DatabaseCompositionSnapshotStore.WriteReservation,
        initialPayloadBytes: UInt64,
        workMeter: DatabaseWorkMeter
    ) throws {
        self.compositionID = compositionID
        self.compositionGeneration = compositionGeneration
        self.baseIDs = baseIDs
        self.consistency = consistency
        self.pageLimit = pageLimit
        self.maximumIntermediateBytes = maximumIntermediateBytes
        self.snapshotStore = snapshotStore
        self.writeReservation = reservation
        self.totalPayloadBytes = initialPayloadBytes
        self.workMeter = workMeter
        self.footprintMeter = try DatabaseRDFQuadFootprintMeter.make(
            workMeter: workMeter,
            stage: .resultMaterialization
        )
    }

    func append(
        _ quad: RDFQuad,
        origin: CompositionOrigin
    ) async throws {
        guard state == .accumulating else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        if currentPageBuffer.count == pageLimit {
            try await makeRoomForAdditionalQuad()
        }
        let footprint = try footprintMeter.footprint(of: quad)
        try currentPageBuffer.append(
            quad: quad,
            origin: origin,
            footprint: footprint,
            workMeter: workMeter
        )
    }

    func finish() async throws -> RDFGraphPage {
        guard state == .accumulating else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        defer { footprintMeter.shutdown() }
        guard firstPageBuffer != nil else {
            let page = try makePage(currentPageBuffer, continuation: nil)
            if let writeReservation {
                try await snapshotStore.abortWrite(writeReservation)
            }
            currentPageBuffer.release()
            writeReservation = nil
            state = .completed
            return page
        }
        guard let writeReservation,
              let currentPageID,
              let firstContinuationPageID,
              let firstPageBuffer else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
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
        footprintMeter.shutdown()
        defer {
            currentPageBuffer.release()
            if var firstPageBuffer { firstPageBuffer.release() }
            self.firstPageBuffer = nil
            writeReservation = nil
        }
        if let writeReservation {
            try await snapshotStore.abortWrite(writeReservation)
        }
    }

    private func makeRoomForAdditionalQuad() async throws {
        guard let writeReservation else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        if firstPageBuffer == nil {
            let pageID = try await snapshotStore.reservePage(
                in: writeReservation
            )
            firstContinuationPageID = pageID
            currentPageID = pageID
            firstPageBuffer = currentPageBuffer
            currentPageBuffer = PageBuffer()
            return
        }
        guard let currentPageID else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
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
    ) throws -> RDFGraphPage {
        try RDFGraphPage(
            quads: buffer.quads,
            continuation: continuation,
            provenance: CompositionPageProvenance(
                compositionID: compositionID,
                generation: compositionGeneration,
                baseIDs: baseIDs,
                origins: buffer.origins
            ),
            consistency: consistency
        )
    }

    private func increment(_ value: UInt32) throws -> UInt32 {
        let result = value.addingReportingOverflow(1)
        guard !result.overflow else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        return result.partialValue
    }
}
#endif
