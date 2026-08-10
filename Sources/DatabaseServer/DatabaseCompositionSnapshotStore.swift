import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import StorageKit

/// Durable fixed-result paging for federated Composition reads.
package struct DatabaseCompositionSnapshotStore: Sendable {
    package struct WriteReservation: Sendable {
        package let snapshotID: ByteString
        fileprivate let principalDigest: ByteString
        fileprivate let slot: UInt8
        fileprivate let expiresAt: Timestamp
        fileprivate let compositionID: Base.Composition.ID
        fileprivate let compositionGeneration: UInt64
        fileprivate let schemaGeneration: UInt64
        fileprivate let queryFingerprint: ByteString

        package func continuation(pageID: ByteString) -> ByteString {
            snapshotID.appending(contentsOf: pageID)
        }
    }

    private struct Manifest: StorageFrameValue {
        let compositionID: Base.Composition.ID
        let compositionGeneration: UInt64
        let schemaGeneration: UInt64
        let principalDigest: ByteString
        let queryFingerprint: ByteString
        let expiresAt: Timestamp
        let slot: UInt8
        let pageCount: UInt32
        let totalPayloadBytes: UInt64

        func encode(
            to encoder: inout StorageFrameEncoder
        ) throws(StorageFrameError) {
            try encoder.writeString(compositionID.value)
            encoder.writeUInt64(compositionGeneration)
            encoder.writeUInt64(schemaGeneration)
            try encoder.writeBytes(principalDigest)
            try encoder.writeBytes(queryFingerprint)
            encoder.writeInt64(expiresAt.secondsSinceUnixEpoch)
            encoder.writeUInt32(expiresAt.nanoseconds)
            encoder.writeUInt8(slot)
            encoder.writeUInt32(pageCount)
            encoder.writeUInt64(totalPayloadBytes)
        }

        init(
            from decoder: inout StorageFrameDecoder
        ) throws(StorageFrameError) {
            do {
                compositionID = try Base.Composition.ID(
                    decoder.readString()
                )
            } catch {
                throw .invalidValue
            }
            compositionGeneration = try decoder.readUInt64()
            schemaGeneration = try decoder.readUInt64()
            principalDigest = try decoder.readBytes()
            queryFingerprint = try decoder.readBytes()
            do {
                expiresAt = try Timestamp(
                    secondsSinceUnixEpoch: decoder.readInt64(),
                    nanoseconds: decoder.readUInt32()
                )
            } catch {
                throw .invalidTimestamp
            }
            slot = try decoder.readUInt8()
            pageCount = try decoder.readUInt32()
            totalPayloadBytes = try decoder.readUInt64()
            guard compositionGeneration > 0,
                  principalDigest.count == 32,
                  queryFingerprint.count == 32,
                  slot < DatabaseCompositionSnapshotStore.maximumActiveCount,
                  pageCount > 0,
                  totalPayloadBytes > 0,
                  totalPayloadBytes
                    <= DatabaseCompositionSnapshotStore.maximumSpoolBytes
            else {
                throw .invalidValue
            }
        }

        init(
            compositionID: Base.Composition.ID,
            compositionGeneration: UInt64,
            schemaGeneration: UInt64,
            principalDigest: ByteString,
            queryFingerprint: ByteString,
            expiresAt: Timestamp,
            slot: UInt8,
            pageCount: UInt32,
            totalPayloadBytes: UInt64
        ) {
            self.compositionID = compositionID
            self.compositionGeneration = compositionGeneration
            self.schemaGeneration = schemaGeneration
            self.principalDigest = principalDigest
            self.queryFingerprint = queryFingerprint
            self.expiresAt = expiresAt
            self.slot = slot
            self.pageCount = pageCount
            self.totalPayloadBytes = totalPayloadBytes
        }
    }

    private struct PageDescriptor: StorageFrameValue {
        let chunkCount: UInt32
        let payloadByteCount: UInt64
        let digest: ByteString

        func encode(
            to encoder: inout StorageFrameEncoder
        ) throws(StorageFrameError) {
            encoder.writeUInt32(chunkCount)
            encoder.writeUInt64(payloadByteCount)
            try encoder.writeBytes(digest)
        }

        init(
            from decoder: inout StorageFrameDecoder
        ) throws(StorageFrameError) {
            chunkCount = try decoder.readUInt32()
            payloadByteCount = try decoder.readUInt64()
            digest = try decoder.readBytes()
            guard chunkCount > 0,
                  payloadByteCount > 0,
                  payloadByteCount
                    <= DatabaseCompositionSnapshotStore.maximumSpoolBytes,
                  digest.count == 32 else {
                throw .invalidValue
            }
        }

        init(
            chunkCount: UInt32,
            payloadByteCount: UInt64,
            digest: ByteString
        ) {
            self.chunkCount = chunkCount
            self.payloadByteCount = payloadByteCount
            self.digest = digest
        }
    }

    private struct PrincipalSlot: StorageFrameValue {
        let snapshotID: ByteString
        let expiresAt: Timestamp

        func encode(
            to encoder: inout StorageFrameEncoder
        ) throws(StorageFrameError) {
            try encoder.writeBytes(snapshotID)
            encoder.writeInt64(expiresAt.secondsSinceUnixEpoch)
            encoder.writeUInt32(expiresAt.nanoseconds)
        }

        init(
            from decoder: inout StorageFrameDecoder
        ) throws(StorageFrameError) {
            snapshotID = try decoder.readBytes()
            do {
                expiresAt = try Timestamp(
                    secondsSinceUnixEpoch: decoder.readInt64(),
                    nanoseconds: decoder.readUInt32()
                )
            } catch {
                throw .invalidTimestamp
            }
            guard snapshotID.count == 16 else {
                throw .invalidValue
            }
        }

        init(snapshotID: ByteString, expiresAt: Timestamp) {
            self.snapshotID = snapshotID
            self.expiresAt = expiresAt
        }
    }

    private struct PendingSnapshot: StorageFrameValue {
        let principalDigest: ByteString
        let slot: UInt8
        let expiresAt: Timestamp

        func encode(
            to encoder: inout StorageFrameEncoder
        ) throws(StorageFrameError) {
            try encoder.writeBytes(principalDigest)
            encoder.writeUInt8(slot)
            encoder.writeInt64(expiresAt.secondsSinceUnixEpoch)
            encoder.writeUInt32(expiresAt.nanoseconds)
        }

        init(
            from decoder: inout StorageFrameDecoder
        ) throws(StorageFrameError) {
            principalDigest = try decoder.readBytes()
            slot = try decoder.readUInt8()
            do {
                expiresAt = try Timestamp(
                    secondsSinceUnixEpoch: decoder.readInt64(),
                    nanoseconds: decoder.readUInt32()
                )
            } catch {
                throw .invalidTimestamp
            }
            guard principalDigest.count == 32,
                  slot < DatabaseCompositionSnapshotStore.maximumActiveCount
            else {
                throw .invalidValue
            }
        }

        init(
            principalDigest: ByteString,
            slot: UInt8,
            expiresAt: Timestamp
        ) {
            self.principalDigest = principalDigest
            self.slot = slot
            self.expiresAt = expiresAt
        }
    }

    private struct PageReservation: StorageFrameValue {
        let marker: UInt8

        func encode(
            to encoder: inout StorageFrameEncoder
        ) throws(StorageFrameError) {
            encoder.writeUInt8(marker)
        }

        init(
            from decoder: inout StorageFrameDecoder
        ) throws(StorageFrameError) {
            marker = try decoder.readUInt8()
            guard marker == 1 else { throw .invalidValue }
        }

        init() {
            marker = 1
        }
    }

    private struct ExpiryRecord: StorageFrameValue {
        let snapshotID: ByteString
        let principalDigest: ByteString
        let slot: UInt8
        let expiresAt: Timestamp

        func encode(
            to encoder: inout StorageFrameEncoder
        ) throws(StorageFrameError) {
            try encoder.writeBytes(snapshotID)
            try encoder.writeBytes(principalDigest)
            encoder.writeUInt8(slot)
            encoder.writeInt64(expiresAt.secondsSinceUnixEpoch)
            encoder.writeUInt32(expiresAt.nanoseconds)
        }

        init(
            from decoder: inout StorageFrameDecoder
        ) throws(StorageFrameError) {
            snapshotID = try decoder.readBytes()
            principalDigest = try decoder.readBytes()
            slot = try decoder.readUInt8()
            do {
                expiresAt = try Timestamp(
                    secondsSinceUnixEpoch: decoder.readInt64(),
                    nanoseconds: decoder.readUInt32()
                )
            } catch {
                throw .invalidTimestamp
            }
            guard snapshotID.count == 16,
                  principalDigest.count == 32,
                  slot < DatabaseCompositionSnapshotStore.maximumActiveCount
            else {
                throw .invalidValue
            }
        }

        init(
            snapshotID: ByteString,
            principalDigest: ByteString,
            slot: UInt8,
            expiresAt: Timestamp
        ) {
            self.snapshotID = snapshotID
            self.principalDigest = principalDigest
            self.slot = slot
            self.expiresAt = expiresAt
        }
    }

    private static let snapshotLifetimeSeconds: Int64 = 15 * 60
    private static let chunkByteCount = 60 * 1_024
    private static let maximumActiveCount: UInt8 = 8
    private static let maximumSpoolBytes: UInt64 = 16 * 1_024 * 1_024

    private let container: DBContainer
    private let clock: AnyDatabaseWallClock
    private let identifierGenerator: AnyDatabaseUUIDGenerator
    private let scheduler: AnyDatabaseJobScheduler
    private let wireLimits: DatabaseWireLimits
    private let snapshots: Subspace
    private let principalSlots: Subspace
    private let expirations: Subspace

    package var controlContainer: DBContainer { container }

    init(
        container: DBContainer,
        clock: AnyDatabaseWallClock,
        identifierGenerator: AnyDatabaseUUIDGenerator,
        scheduler: AnyDatabaseJobScheduler,
        wireLimits: DatabaseWireLimits
    ) {
        self.container = container
        self.clock = clock
        self.identifierGenerator = identifierGenerator
        self.scheduler = scheduler
        self.wireLimits = wireLimits
        let root = container.storageTopology.controlDomain.root
            .subspace("composition-snapshots")
        self.snapshots = root.subspace("snapshots")
        self.principalSlots = root.subspace("principals")
        self.expirations = root.subspace("expirations")
    }

    package func beginWrite(
        compositionID: Base.Composition.ID,
        compositionGeneration: UInt64,
        schemaGeneration: UInt64,
        queryFingerprint: ByteString,
        authorization: AuthorizationContext
    ) async throws -> WriteReservation {
        guard compositionGeneration > 0,
              queryFingerprint.count == 32,
              let principal = authorization.principal else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        let principalDigest = Self.digest(utf8: principal.identifier)
        let now = clock.now
        let expiresAt = try Self.addingLifetime(to: now)

        // Arm cleanup before publishing any durable state. A wake-up with no
        // matching snapshot is harmless, while reserving first would leave a
        // crash window in which no process is responsible for cleanup.
        try await scheduler.ensureWakeUp(noLaterThan: expiresAt)

        for _ in 0..<16 {
            let snapshotID = Self.identifierBytes(
                identifierGenerator.generate()
            )
            guard let pending = try await reserve(
                snapshotID: snapshotID,
                principalDigest: principalDigest,
                expiresAt: expiresAt,
                now: now
            ) else {
                continue
            }
            return WriteReservation(
                snapshotID: snapshotID,
                principalDigest: principalDigest,
                slot: pending.slot,
                expiresAt: expiresAt,
                compositionID: compositionID,
                compositionGeneration: compositionGeneration,
                schemaGeneration: schemaGeneration,
                queryFingerprint: queryFingerprint
            )
        }
        throw DatabaseQueryExecutionError.compositionSnapshotUnavailable(
            "unable to reserve a unique opaque snapshot identifier"
        )
    }

    package func reservePage(
        in reservation: WriteReservation
    ) async throws -> ByteString {
        for _ in 0..<16 {
            let pageID = Self.identifierBytes(identifierGenerator.generate())
            let didReserve = try await container
                .withControlMetadataTransaction { transaction in
                    guard try await self.validatePending(
                        reservation,
                        transaction: transaction.storageAccess
                    ) else {
                        throw DatabaseQueryExecutionError
                            .compositionSnapshotCorrupted
                    }
                    let reservationKey = self.pageReservationKey(
                        snapshotID: reservation.snapshotID,
                        pageID: pageID
                    )
                    guard try await transaction.storageAccess.getValue(
                        for: reservationKey,
                        snapshot: false
                    ) == nil,
                          try await transaction.storageAccess.getValue(
                            for: self.pageDescriptorKey(
                                snapshotID: reservation.snapshotID,
                                pageID: pageID
                            ),
                            snapshot: false
                          ) == nil else {
                        return false
                    }
                    try transaction.storageAccess.setValue(
                        StorageFrameCodec.encode(PageReservation()),
                        for: reservationKey
                    )
                    return true
                }
            if didReserve { return pageID }
        }
        throw DatabaseQueryExecutionError.compositionSnapshotUnavailable(
            "unable to reserve a unique opaque page identifier"
        )
    }

    package func appendPage(
        _ page: QueryRowPage,
        pageID: ByteString,
        to reservation: WriteReservation,
        consumedPayloadBytes: UInt64,
        maximumIntermediateBytes: UInt64
    ) async throws -> UInt64 {
        try await appendResponse(
            .rows(page),
            pageID: pageID,
            to: reservation,
            consumedPayloadBytes: consumedPayloadBytes,
            maximumIntermediateBytes: maximumIntermediateBytes
        )
    }

    package func appendPage(
        _ page: RDFGraphPage,
        pageID: ByteString,
        to reservation: WriteReservation,
        consumedPayloadBytes: UInt64,
        maximumIntermediateBytes: UInt64
    ) async throws -> UInt64 {
        try await appendResponse(
            .rdfGraph(page),
            pageID: pageID,
            to: reservation,
            consumedPayloadBytes: consumedPayloadBytes,
            maximumIntermediateBytes: maximumIntermediateBytes
        )
    }

    private func appendResponse(
        _ response: QueryExecuteOperation.Response,
        pageID: ByteString,
        to reservation: WriteReservation,
        consumedPayloadBytes: UInt64,
        maximumIntermediateBytes: UInt64
    ) async throws -> UInt64 {
        guard pageID.count == 16 else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        let payload = try DatabaseWireEncoder(limits: wireLimits)
            .encodeResponseAndPayload(
                DatabaseOperations.queryExecute,
                requestID: 0,
                response: response
            ).payload
        let maximum = min(
            maximumIntermediateBytes,
            Self.maximumSpoolBytes
        )
        let nextTotal = consumedPayloadBytes.addingReportingOverflow(
            UInt64(payload.count)
        )
        guard !nextTotal.overflow,
              nextTotal.partialValue <= maximum else {
            throw DatabaseWorkLimitError.maximumIntermediateBytes(
                stage: .resultMaterialization,
                consumed: consumedPayloadBytes,
                requested: UInt64(payload.count),
                maximum: maximum
            )
        }

        // This is the required durability copy boundary: continuation page
        // bytes must outlive the request and survive process restart. Chunk
        // slices retain the one encoded payload owner until each authoritative
        // control-domain transaction commits.
        try await writePage(
            payload,
            reservation: reservation,
            pageID: pageID
        )
        return nextTotal.partialValue
    }

    package func commitWrite(
        _ reservation: WriteReservation,
        pageCount: UInt32,
        totalPayloadBytes: UInt64
    ) async throws {
        guard pageCount > 0, totalPayloadBytes > 0 else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        try await publish(
            snapshotID: reservation.snapshotID,
            pending: PendingSnapshot(
                principalDigest: reservation.principalDigest,
                slot: reservation.slot,
                expiresAt: reservation.expiresAt
            ),
            compositionID: reservation.compositionID,
            compositionGeneration: reservation.compositionGeneration,
            schemaGeneration: reservation.schemaGeneration,
            principalDigest: reservation.principalDigest,
            queryFingerprint: reservation.queryFingerprint,
            expiresAt: reservation.expiresAt,
            pageCount: pageCount,
            totalPayloadBytes: totalPayloadBytes
        )
    }

    package func abortWrite(
        _ reservation: WriteReservation
    ) async throws {
        // Cancellation must not cancel cleanup itself. If the process crashes,
        // the reservation and expiry index let startup or the durable scheduler
        // finish the same idempotent cleanup.
        try await Task.detached {
            try await self.clearReserved(
                snapshotID: reservation.snapshotID,
                principalDigest: reservation.principalDigest,
                slot: reservation.slot,
                expiresAt: reservation.expiresAt
            )
        }.value
    }

    package func distinctWorkspace(
        in reservation: WriteReservation
    ) -> Subspace {
        snapshotSubspace(reservation.snapshotID).subspace("distinct")
    }

    /// Removes every expired snapshot in bounded transactions, then arms the
    /// host scheduler for the earliest remaining expiry.
    func cleanupExpired() async throws {
        while true {
            let outcome = try await container.withControlMetadataTransaction {
                transaction in
                let range = self.expirations.range()
                let entries = try await TransactionRangeCollection.collect(
                    using: transaction.storageAccess,
                    from: .firstGreaterOrEqual(range.begin),
                    to: .firstGreaterOrEqual(range.end),
                    limit: 129,
                    reverse: false,
                    snapshot: false,
                    streamingMode: .iterator
                )
                var removed = 0
                var nextExpiry: Timestamp?
                for (key, bytes) in entries {
                    let record: ExpiryRecord
                    do {
                        record = try StorageFrameCodec.decode(
                            ExpiryRecord.self,
                            from: bytes
                        )
                    } catch {
                        throw DatabaseQueryExecutionError
                            .compositionSnapshotCorrupted
                    }
                    guard record.expiresAt <= self.clock.now else {
                        nextExpiry = record.expiresAt
                        break
                    }
                    guard removed < 128 else { break }
                    try self.clearSnapshot(
                        record.snapshotID,
                        transaction: transaction.storageAccess
                    )
                    let slotKey = self.principalSlotKey(
                        principalDigest: record.principalDigest,
                        slot: record.slot
                    )
                    if let slotBytes = try await transaction.storageAccess
                        .getValue(for: slotKey, snapshot: false) {
                        let slot = try StorageFrameCodec.decode(
                            PrincipalSlot.self,
                            from: slotBytes
                        )
                        if slot.snapshotID == record.snapshotID {
                            try transaction.storageAccess.clear(key: slotKey)
                        }
                    }
                    try transaction.storageAccess.clear(key: key)
                    removed += 1
                }
                return (removed: removed, nextExpiry: nextExpiry)
            }
            if outcome.removed == 128 {
                continue
            }
            if let nextExpiry = outcome.nextExpiry {
                try await scheduler.ensureWakeUp(noLaterThan: nextExpiry)
            }
            return
        }
    }

    func load(
        continuation: ByteString,
        composition: DatabaseCompositionRecord,
        schemaGeneration: UInt64,
        queryFingerprint: ByteString,
        authorization: AuthorizationContext
    ) async throws -> QueryRowPage {
        let response = try await loadResponse(
            continuation: continuation,
            composition: composition,
            schemaGeneration: schemaGeneration,
            queryFingerprint: queryFingerprint,
            authorization: authorization
        )
        guard case .rows(let page) = response else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        return page
    }

    func loadRDFGraph(
        continuation: ByteString,
        composition: DatabaseCompositionRecord,
        schemaGeneration: UInt64,
        queryFingerprint: ByteString,
        authorization: AuthorizationContext
    ) async throws -> RDFGraphPage {
        let response = try await loadResponse(
            continuation: continuation,
            composition: composition,
            schemaGeneration: schemaGeneration,
            queryFingerprint: queryFingerprint,
            authorization: authorization
        )
        guard case .rdfGraph(let page) = response else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        return page
    }

    private func loadResponse(
        continuation: ByteString,
        composition: DatabaseCompositionRecord,
        schemaGeneration: UInt64,
        queryFingerprint: ByteString,
        authorization: AuthorizationContext
    ) async throws -> QueryExecuteOperation.Response {
        guard continuation.count == 32,
              queryFingerprint.count == 32,
              let principal = authorization.principal else {
            throw DatabaseQueryExecutionError.invalidContinuation
        }
        let snapshotID = continuation[0..<16]
        let pageID = continuation[16..<32]
        let principalDigest = Self.digest(utf8: principal.identifier)
        let now = clock.now

        let loaded: (Manifest, ByteString)
        do {
            loaded = try await container.withControlMetadataTransaction(
                configuration: .readOnly
            ) { transaction in
                guard let manifestBytes = try await transaction.storageAccess
                    .getValue(
                        for: self.manifestKey(snapshotID),
                        snapshot: false
                    ) else {
                    throw DatabaseQueryExecutionError.invalidContinuation
                }
                let manifest = try StorageFrameCodec.decode(
                    Manifest.self,
                    from: manifestBytes
                )
                guard let descriptorBytes = try await transaction.storageAccess
                    .getValue(
                        for: self.pageDescriptorKey(
                            snapshotID: snapshotID,
                            pageID: pageID
                        ),
                        snapshot: false
                    ) else {
                    throw DatabaseQueryExecutionError
                        .compositionSnapshotCorrupted
                }
                let descriptor = try StorageFrameCodec.decode(
                    PageDescriptor.self,
                    from: descriptorBytes
                )
                guard let payloadByteCount = Int(
                    exactly: descriptor.payloadByteCount
                ),
                      let chunkCount = Int(exactly: descriptor.chunkCount)
                else {
                    throw DatabaseQueryExecutionError
                        .compositionSnapshotCorrupted
                }
                var chunks: [ByteString] = []
                chunks.reserveCapacity(chunkCount)
                var actualByteCount = 0
                for chunkIndex in 0..<chunkCount {
                    guard let chunk = try await transaction.storageAccess
                        .getValue(
                            for: self.pageChunkKey(
                                snapshotID: snapshotID,
                                pageID: pageID,
                                chunkIndex: chunkIndex
                            ),
                            snapshot: false
                        ) else {
                        throw DatabaseQueryExecutionError
                            .compositionSnapshotCorrupted
                    }
                    let next = actualByteCount.addingReportingOverflow(
                        chunk.count
                    )
                    guard !next.overflow,
                          next.partialValue <= payloadByteCount else {
                        throw DatabaseQueryExecutionError
                            .compositionSnapshotCorrupted
                    }
                    actualByteCount = next.partialValue
                    chunks.append(chunk)
                }
                guard actualByteCount == payloadByteCount else {
                    throw DatabaseQueryExecutionError
                        .compositionSnapshotCorrupted
                }
                let payload = ByteString.copying(count: payloadByteCount) {
                    destination in
                    var offset = 0
                    for chunk in chunks {
                        chunk.withUnsafeBytes { source in
                            UnsafeMutableRawBufferPointer(
                                rebasing: destination[
                                    offset..<(offset + source.count)
                                ]
                            ).copyMemory(from: source)
                            offset += source.count
                        }
                    }
                }
                guard Self.constantTimeEqual(
                    Self.digest(payload),
                    descriptor.digest
                ) else {
                    throw DatabaseQueryExecutionError
                        .compositionSnapshotCorrupted
                }
                return (manifest, payload)
            }
        } catch let error as DatabaseQueryExecutionError {
            throw error
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }

        let manifest = loaded.0
        if manifest.expiresAt <= now {
            try await Task.detached {
                try await self.clearReserved(
                    snapshotID: snapshotID,
                    principalDigest: manifest.principalDigest,
                    slot: manifest.slot,
                    expiresAt: manifest.expiresAt
                )
            }.value
            throw DatabaseQueryExecutionError.compositionSnapshotExpired
        }
        guard Self.constantTimeEqual(
            manifest.principalDigest,
            principalDigest
        ) else {
            throw DatabaseCompositionAccessError.unavailable(
                composition.composition.id
            )
        }
        guard manifest.compositionID == composition.composition.id,
              manifest.compositionGeneration == composition.generation,
              manifest.schemaGeneration == schemaGeneration,
              Self.constantTimeEqual(
                manifest.queryFingerprint,
                queryFingerprint
              ) else {
            throw DatabaseQueryExecutionError.compositionSnapshotStale
        }
        let response: QueryExecuteOperation.Response
        do {
            response = try DatabaseWireDecoder(limits: wireLimits)
                .decodeResponsePayload(
                    DatabaseOperations.queryExecute,
                    from: loaded.1
                )
        } catch {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        let provenance: CompositionPageProvenance?
        let next: ByteString?
        switch response {
        case .rows(let page):
            provenance = page.provenance
            next = page.continuation
        case .rdfGraph(let page):
            provenance = page.provenance
            next = page.continuation
        case .boolean:
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        guard let provenance,
              provenance.compositionID == manifest.compositionID,
              provenance.generation == manifest.compositionGeneration,
              provenance.baseIDs == composition.composition.bases else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        if let next {
            guard next.count == 32,
                  next[0..<16] == snapshotID else {
                throw DatabaseQueryExecutionError
                    .compositionSnapshotCorrupted
            }
        }
        return response
    }

    static func queryFingerprint(
        query: SelectQuery,
        request: QueryExecuteOperation.Request,
        limits: DatabaseWireLimits
    ) throws -> ByteString {
        try queryFingerprint(
            statement: .select(query),
            request: request,
            limits: limits
        )
    }

    static func queryFingerprint(
        statement: QueryStatement,
        request: QueryExecuteOperation.Request,
        limits: DatabaseWireLimits
    ) throws -> ByteString {
        let normalized = QueryExecuteOperation.Request(
            input: .ir(statement),
            parameters: [],
            graphPartitions: request.graphPartitions,
            page: QueryExecuteOperation.Page(limit: request.page.limit),
            budget: request.budget
        )
        return digest(
            try DatabaseWireEncoder(limits: limits).encodeRequestPayload(
                DatabaseOperations.queryExecute,
                request: normalized
            )
        )
    }

    private func writePage(
        _ payload: ByteString,
        reservation: WriteReservation,
        pageID: ByteString
    ) async throws {
        let snapshotID = reservation.snapshotID
        let chunkCount = (payload.count - 1) / Self.chunkByteCount + 1
        guard let encodedChunkCount = UInt32(exactly: chunkCount) else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        for chunkIndex in 0..<chunkCount {
            let start = chunkIndex * Self.chunkByteCount
            let end = min(start + Self.chunkByteCount, payload.count)
            let chunk = payload[start..<end]
            try await container.withControlMetadataTransaction {
                transaction in
                try transaction.storageAccess.setValue(
                    chunk,
                    for: self.pageChunkKey(
                        snapshotID: snapshotID,
                        pageID: pageID,
                        chunkIndex: chunkIndex
                    )
                )
            }
        }
        let descriptor = PageDescriptor(
            chunkCount: encodedChunkCount,
            payloadByteCount: UInt64(payload.count),
            digest: Self.digest(payload)
        )
        try await container.withControlMetadataTransaction { transaction in
            guard try await self.validatePending(
                reservation,
                transaction: transaction.storageAccess
            ),
                  let pageReservationBytes = try await transaction.storageAccess
                    .getValue(
                        for: self.pageReservationKey(
                            snapshotID: snapshotID,
                            pageID: pageID
                        ),
                        snapshot: false
                    ) else {
                throw DatabaseQueryExecutionError
                    .compositionSnapshotCorrupted
            }
            do {
                _ = try StorageFrameCodec.decode(
                    PageReservation.self,
                    from: pageReservationBytes
                )
            } catch {
                throw DatabaseQueryExecutionError
                    .compositionSnapshotCorrupted
            }
            guard try await transaction.storageAccess.getValue(
                for: self.pageDescriptorKey(
                    snapshotID: snapshotID,
                    pageID: pageID
                ),
                snapshot: false
            ) == nil else {
                throw DatabaseQueryExecutionError
                    .compositionSnapshotCorrupted
            }
            try transaction.storageAccess.setValue(
                StorageFrameCodec.encode(descriptor),
                for: self.pageDescriptorKey(
                    snapshotID: snapshotID,
                    pageID: pageID
                )
            )
            try transaction.storageAccess.clear(
                key: self.pageReservationKey(
                    snapshotID: snapshotID,
                    pageID: pageID
                )
            )
        }
    }

    private func publish(
        snapshotID: ByteString,
        pending: PendingSnapshot,
        compositionID: Base.Composition.ID,
        compositionGeneration: UInt64,
        schemaGeneration: UInt64,
        principalDigest: ByteString,
        queryFingerprint: ByteString,
        expiresAt: Timestamp,
        pageCount: UInt32,
        totalPayloadBytes: UInt64
    ) async throws {
        try await container.withControlMetadataTransaction {
            transaction in
            guard let current = try await self.container.compositionCatalog
                .load(
                    compositionID,
                    transaction: transaction.storageAccess
                ),
                  current.generation == compositionGeneration else {
                throw DatabaseQueryExecutionError.compositionSnapshotStale
            }
            guard try await transaction.storageAccess.getValue(
                for: self.manifestKey(snapshotID),
                snapshot: false
            ) == nil else {
                throw DatabaseQueryExecutionError
                    .compositionSnapshotCorrupted
            }
            guard principalDigest == pending.principalDigest,
                  expiresAt == pending.expiresAt,
                  let pendingBytes = try await transaction.storageAccess
                .getValue(
                    for: self.pendingKey(snapshotID),
                    snapshot: false
                ) else {
                throw DatabaseQueryExecutionError
                    .compositionSnapshotCorrupted
            }
            let storedPending: PendingSnapshot
            do {
                storedPending = try StorageFrameCodec.decode(
                    PendingSnapshot.self,
                    from: pendingBytes
                )
            } catch {
                throw DatabaseQueryExecutionError
                    .compositionSnapshotCorrupted
            }
            guard storedPending.principalDigest == pending.principalDigest,
                  storedPending.slot == pending.slot,
                  storedPending.expiresAt == pending.expiresAt,
                  let slotBytes = try await transaction.storageAccess
                    .getValue(
                        for: self.principalSlotKey(
                            principalDigest: pending.principalDigest,
                            slot: pending.slot
                        ),
                        snapshot: false
                    ) else {
                throw DatabaseQueryExecutionError
                    .compositionSnapshotCorrupted
            }
            let storedSlot: PrincipalSlot
            do {
                storedSlot = try StorageFrameCodec.decode(
                    PrincipalSlot.self,
                    from: slotBytes
                )
            } catch {
                throw DatabaseQueryExecutionError
                    .compositionSnapshotCorrupted
            }
            guard storedSlot.snapshotID == snapshotID,
                  storedSlot.expiresAt == expiresAt else {
                throw DatabaseQueryExecutionError
                    .compositionSnapshotCorrupted
            }
            let manifest = Manifest(
                compositionID: compositionID,
                compositionGeneration: compositionGeneration,
                schemaGeneration: schemaGeneration,
                principalDigest: principalDigest,
                queryFingerprint: queryFingerprint,
                expiresAt: expiresAt,
                slot: pending.slot,
                pageCount: pageCount,
                totalPayloadBytes: totalPayloadBytes
            )
            try transaction.storageAccess.setValue(
                StorageFrameCodec.encode(manifest),
                for: self.manifestKey(snapshotID)
            )
            try transaction.storageAccess.clear(
                key: self.pendingKey(snapshotID)
            )
        }
    }

    private func reserve(
        snapshotID: ByteString,
        principalDigest: ByteString,
        expiresAt: Timestamp,
        now: Timestamp
    ) async throws -> PendingSnapshot? {
        try await container.withControlMetadataTransaction { transaction in
            guard try await transaction.storageAccess.getValue(
                for: self.pendingKey(snapshotID),
                snapshot: false
            ) == nil,
                  try await transaction.storageAccess.getValue(
                    for: self.manifestKey(snapshotID),
                    snapshot: false
                  ) == nil else { return nil }
            let selection = try await self.availableSlot(
                principalDigest: principalDigest,
                now: now,
                transaction: transaction.storageAccess
            )
            if let expiredSnapshotID = selection.expiredSnapshotID {
                guard let expiredAt = selection.expiredAt else {
                    throw DatabaseQueryExecutionError
                        .compositionSnapshotCorrupted
                }
                try self.clearSnapshot(
                    expiredSnapshotID,
                    transaction: transaction.storageAccess
                )
                try transaction.storageAccess.clear(
                    key: self.expiryKey(
                        snapshotID: expiredSnapshotID,
                        expiresAt: expiredAt
                    )
                )
            }
            let pending = PendingSnapshot(
                principalDigest: principalDigest,
                slot: selection.slot,
                expiresAt: expiresAt
            )
            try transaction.storageAccess.setValue(
                StorageFrameCodec.encode(pending),
                for: self.pendingKey(snapshotID)
            )
            try transaction.storageAccess.setValue(
                StorageFrameCodec.encode(
                    PrincipalSlot(
                        snapshotID: snapshotID,
                        expiresAt: expiresAt
                    )
                ),
                for: self.principalSlotKey(
                    principalDigest: principalDigest,
                    slot: selection.slot
                )
            )
            try transaction.storageAccess.setValue(
                StorageFrameCodec.encode(
                    ExpiryRecord(
                        snapshotID: snapshotID,
                        principalDigest: principalDigest,
                        slot: selection.slot,
                        expiresAt: expiresAt
                    )
                ),
                for: self.expiryKey(
                    snapshotID: snapshotID,
                    expiresAt: expiresAt
                )
            )
            return pending
        }
    }

    private func validatePending(
        _ reservation: WriteReservation,
        transaction: any TransactionAccess
    ) async throws -> Bool {
        guard let bytes = try await transaction.getValue(
            for: pendingKey(reservation.snapshotID),
            snapshot: false
        ) else {
            return false
        }
        let stored: PendingSnapshot
        do {
            stored = try StorageFrameCodec.decode(
                PendingSnapshot.self,
                from: bytes
            )
        } catch {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        return Self.constantTimeEqual(
            stored.principalDigest,
            reservation.principalDigest
        ) && stored.slot == reservation.slot
            && stored.expiresAt == reservation.expiresAt
    }

    private func availableSlot(
        principalDigest: ByteString,
        now: Timestamp,
        transaction: any TransactionAccess
    ) async throws -> (
        slot: UInt8,
        expiredSnapshotID: ByteString?,
        expiredAt: Timestamp?
    ) {
        for slot in 0..<Self.maximumActiveCount {
            guard let bytes = try await transaction.getValue(
                for: principalSlotKey(
                    principalDigest: principalDigest,
                    slot: slot
                ),
                snapshot: false
            ) else {
                return (slot, nil, nil)
            }
            let record: PrincipalSlot
            do {
                record = try StorageFrameCodec.decode(
                    PrincipalSlot.self,
                    from: bytes
                )
            } catch {
                throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
            }
            if record.expiresAt <= now {
                return (slot, record.snapshotID, record.expiresAt)
            }
        }
        throw DatabaseQueryExecutionError.compositionSnapshotLimitExceeded(
            maximum: Self.maximumActiveCount
        )
    }

    private func clearReserved(
        snapshotID: ByteString,
        principalDigest: ByteString,
        slot: UInt8,
        expiresAt: Timestamp
    ) async throws {
        try await container.withControlMetadataTransaction { transaction in
            try self.clearSnapshot(
                snapshotID,
                transaction: transaction.storageAccess
            )
            try transaction.storageAccess.clear(
                key: self.expiryKey(
                    snapshotID: snapshotID,
                    expiresAt: expiresAt
                )
            )
            let slotKey = self.principalSlotKey(
                principalDigest: principalDigest,
                slot: slot
            )
            if let slotBytes = try await transaction.storageAccess.getValue(
                for: slotKey,
                snapshot: false
            ) {
                let slot: PrincipalSlot
                do {
                    slot = try StorageFrameCodec.decode(
                        PrincipalSlot.self,
                        from: slotBytes
                    )
                } catch {
                    throw DatabaseQueryExecutionError
                        .compositionSnapshotCorrupted
                }
                if slot.snapshotID == snapshotID {
                    try transaction.storageAccess.clear(key: slotKey)
                }
            }
        }
    }

    private func clearSnapshot(
        _ snapshotID: ByteString,
        transaction: any TransactionAccess
    ) throws {
        let range = snapshotSubspace(snapshotID).range()
        try transaction.clearRange(
            beginKey: range.begin,
            endKey: range.end
        )
    }

    private func snapshotSubspace(_ snapshotID: ByteString) -> Subspace {
        snapshots.subspace(snapshotID)
    }

    private func manifestKey(_ snapshotID: ByteString) -> ByteString {
        snapshotSubspace(snapshotID).pack(Tuple("manifest"))
    }

    private func pendingKey(_ snapshotID: ByteString) -> ByteString {
        snapshotSubspace(snapshotID).pack(Tuple("pending"))
    }

    private func pageSubspace(
        snapshotID: ByteString,
        pageID: ByteString
    ) -> Subspace {
        snapshotSubspace(snapshotID).subspace("pages").subspace(pageID)
    }

    private func pageDescriptorKey(
        snapshotID: ByteString,
        pageID: ByteString
    ) -> ByteString {
        pageSubspace(snapshotID: snapshotID, pageID: pageID)
            .pack(Tuple("descriptor"))
    }

    private func pageReservationKey(
        snapshotID: ByteString,
        pageID: ByteString
    ) -> ByteString {
        pageSubspace(snapshotID: snapshotID, pageID: pageID)
            .pack(Tuple("reservation"))
    }

    private func pageChunkKey(
        snapshotID: ByteString,
        pageID: ByteString,
        chunkIndex: Int
    ) -> ByteString {
        pageSubspace(snapshotID: snapshotID, pageID: pageID)
            .subspace("chunks")
            .pack(Tuple(UInt64(chunkIndex)))
    }

    private func principalSlotKey(
        principalDigest: ByteString,
        slot: UInt8
    ) -> ByteString {
        principalSlots.subspace(principalDigest).pack(Tuple(UInt64(slot)))
    }

    private func expiryKey(
        snapshotID: ByteString,
        expiresAt: Timestamp
    ) -> ByteString {
        expirations.pack(
            Tuple(
                expiresAt.secondsSinceUnixEpoch,
                UInt64(expiresAt.nanoseconds),
                snapshotID
            )
        )
    }

    private static func addingLifetime(
        to timestamp: Timestamp
    ) throws -> Timestamp {
        let seconds = timestamp.secondsSinceUnixEpoch.addingReportingOverflow(
            snapshotLifetimeSeconds
        )
        guard !seconds.overflow else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        return try Timestamp(
            secondsSinceUnixEpoch: seconds.partialValue,
            nanoseconds: timestamp.nanoseconds
        )
    }

    private static func identifierBytes(
        _ identifier: DatabaseTypes.UUID
    ) -> ByteString {
        ByteString.copying(count: 16) { destination in
            for offset in 0..<16 {
                destination[offset] = identifier[offset]
            }
        }
    }

    private static func digest(_ bytes: ByteString) -> ByteString {
        var accumulator = SHA256Accumulator()
        bytes.withUnsafeBytes { accumulator.update($0) }
        return accumulator.finalize()
    }

    private static func digest(utf8 string: String) -> ByteString {
        var accumulator = SHA256Accumulator()
        let usedContiguousStorage = string.utf8
            .withContiguousStorageIfAvailable { bytes -> Bool in
                accumulator.update(UnsafeRawBufferPointer(bytes))
                return true
            } ?? false
        if !usedContiguousStorage {
            for byte in string.utf8 {
                withUnsafeBytes(of: byte) { accumulator.update($0) }
            }
        }
        return accumulator.finalize()
    }

    private static func constantTimeEqual(
        _ lhs: ByteString,
        _ rhs: ByteString
    ) -> Bool {
        guard lhs.count == rhs.count else { return false }
        return lhs.withUnsafeBytes { left in
            rhs.withUnsafeBytes { right in
                var difference: UInt8 = 0
                for index in 0..<left.count {
                    difference |= left[index] ^ right[index]
                }
                return difference == 0
            }
        }
    }
}
