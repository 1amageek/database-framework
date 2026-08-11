import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire
import StorageKit

/// Durable fixed-result paging for query results whose storage read point
/// cannot be restored across independent requests.
package struct DatabaseQuerySnapshotStore: Sendable {
    fileprivate enum Target: Sendable, Equatable {
        case resource(Security.Resource, generation: UInt64)
        case composition(Base.Composition.ID, generation: UInt64)

        func hasSameIdentity(as other: Self) -> Bool {
            switch (self, other) {
            case (.resource(let lhs, _), .resource(let rhs, _)):
                lhs == rhs
            case (.composition(let lhs, _), .composition(let rhs, _)):
                lhs == rhs
            default:
                false
            }
        }

        func encode(
            to encoder: inout StorageFrameEncoder
        ) throws(StorageFrameError) {
            switch self {
            case .resource(.database, let generation):
                encoder.writeUInt8(0)
                encoder.writeUInt64(generation)
            case .resource(.base(let id), let generation):
                encoder.writeUInt8(1)
                try encoder.writeString(id.value)
                encoder.writeUInt64(generation)
            case .composition(let id, let generation):
                encoder.writeUInt8(2)
                try encoder.writeString(id.value)
                encoder.writeUInt64(generation)
            }
        }

        init(from decoder: inout StorageFrameDecoder) throws(StorageFrameError) {
            let kind = try decoder.readUInt8()
            do {
                switch kind {
                case 0:
                    self = .resource(
                        .database,
                        generation: try decoder.readUInt64()
                    )
                case 1:
                    self = .resource(
                        .base(try Base.ID(decoder.readString())),
                        generation: try decoder.readUInt64()
                    )
                case 2:
                    self = .composition(
                        try Base.Composition.ID(decoder.readString()),
                        generation: try decoder.readUInt64()
                    )
                default:
                    throw StorageFrameError.invalidValue
                }
            } catch let error as StorageFrameError {
                throw error
            } catch {
                throw .invalidValue
            }
            switch self {
            case .resource(.database, _):
                break
            case .resource(.base, let generation),
                 .composition(_, let generation):
                guard generation > 0 else { throw .invalidValue }
            }
        }
    }

    package struct WriteReservation: Sendable {
        package let snapshotID: ByteString
        fileprivate let principalDigest: ByteString
        fileprivate let slot: UInt8
        fileprivate let expiresAt: Timestamp
        fileprivate let target: Target
        fileprivate let schemaGeneration: UInt64
        fileprivate let queryFingerprint: ByteString

        package func continuation(pageID: ByteString) -> ByteString {
            ByteString([DatabaseQuerySnapshotStore.continuationMarker])
                .appending(contentsOf: snapshotID)
                .appending(contentsOf: pageID)
        }
    }

    private struct Manifest: StorageFrameValue {
        let target: Target
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
            try target.encode(to: &encoder)
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
            target = try Target(from: &decoder)
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
            guard principalDigest.count == 32,
                  queryFingerprint.count == 32,
                  slot < DatabaseQuerySnapshotStore.maximumActiveCount,
                  pageCount > 0,
                  totalPayloadBytes > 0,
                  totalPayloadBytes
                    <= DatabaseQuerySnapshotStore.maximumSpoolBytes
            else {
                throw .invalidValue
            }
        }

        init(
            target: Target,
            schemaGeneration: UInt64,
            principalDigest: ByteString,
            queryFingerprint: ByteString,
            expiresAt: Timestamp,
            slot: UInt8,
            pageCount: UInt32,
            totalPayloadBytes: UInt64
        ) {
            self.target = target
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
                    <= DatabaseQuerySnapshotStore.maximumSpoolBytes,
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
                  slot < DatabaseQuerySnapshotStore.maximumActiveCount
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
                  slot < DatabaseQuerySnapshotStore.maximumActiveCount
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
    package static let continuationMarker: UInt8 = 0x51
    package static let continuationByteCount = 33

    private let container: DBContainer
    private let clock: AnyDatabaseWallClock
    private let identifierGenerator: AnyDatabaseUUIDGenerator
    private let scheduler: AnyDatabaseJobScheduler
    private let wireLimits: DatabaseWireLimits
    private let snapshots: Subspace
    private let principalSlots: Subspace
    private let expirations: Subspace

    package var controlContainer: DBContainer { container }

    /// Reuses the caller's data transaction only when its physical storage
    /// domain is also the control domain. This keeps result pages and their
    /// manifest in one atomic commit and avoids a nested transaction runner.
    package func controlWriteTransaction(
        for lease: DatabaseDataRootLease,
        active transaction: any TransactionAccess
    ) -> (any TransactionAccess)? {
        guard lease.domain.id == container.storageTopology.controlDomain.id
        else {
            return nil
        }
        return transaction
    }

    package init(
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
            .subspace("query-snapshots")
        self.snapshots = root.subspace("snapshots")
        self.principalSlots = root.subspace("principals")
        self.expirations = root.subspace("expirations")
    }

    package func beginWrite(
        resource: Security.Resource,
        dataGeneration: UInt64,
        schemaGeneration: UInt64,
        queryFingerprint: ByteString,
        authorization: AuthorizationContext,
        transaction: (any TransactionAccess)? = nil
    ) async throws -> WriteReservation {
        try await beginWrite(
            target: .resource(resource, generation: dataGeneration),
            schemaGeneration: schemaGeneration,
            queryFingerprint: queryFingerprint,
            authorization: authorization,
            transaction: transaction
        )
    }

    #if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
    package func beginWrite(
        compositionID: Base.Composition.ID,
        compositionGeneration: UInt64,
        schemaGeneration: UInt64,
        queryFingerprint: ByteString,
        authorization: AuthorizationContext,
        transaction: (any TransactionAccess)? = nil
    ) async throws -> WriteReservation {
        try await beginWrite(
            target: .composition(
                compositionID,
                generation: compositionGeneration
            ),
            schemaGeneration: schemaGeneration,
            queryFingerprint: queryFingerprint,
            authorization: authorization,
            transaction: transaction
        )
    }
    #endif

    private func beginWrite(
        target: Target,
        schemaGeneration: UInt64,
        queryFingerprint: ByteString,
        authorization: AuthorizationContext,
        transaction: (any TransactionAccess)?
    ) async throws -> WriteReservation {
        guard Self.isValid(target: target),
              queryFingerprint.count == 32,
              let principal = authorization.principal else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
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
                now: now,
                transaction: transaction
            ) else {
                continue
            }
            return WriteReservation(
                snapshotID: snapshotID,
                principalDigest: principalDigest,
                slot: pending.slot,
                expiresAt: expiresAt,
                target: target,
                schemaGeneration: schemaGeneration,
                queryFingerprint: queryFingerprint
            )
        }
        throw DatabaseQueryExecutionError.querySnapshotUnavailable(
            "unable to reserve a unique opaque snapshot identifier"
        )
    }

    private static func isValid(target: Target) -> Bool {
        switch target {
        case .resource(.database, _):
            return true
        case .resource(.base, let generation),
             .composition(_, let generation):
            return generation > 0
        }
    }

    package func reservePage(
        in reservation: WriteReservation,
        transaction: (any TransactionAccess)? = nil
    ) async throws -> ByteString {
        for _ in 0..<16 {
            let pageID = Self.identifierBytes(identifierGenerator.generate())
            let didReserve = try await withControlAccess(
                transaction: transaction
            ) { transaction in
                    guard try await self.validatePending(
                        reservation,
                        transaction: transaction
                    ) else {
                        throw DatabaseQueryExecutionError
                            .querySnapshotCorrupted
                    }
                    let reservationKey = self.pageReservationKey(
                        snapshotID: reservation.snapshotID,
                        pageID: pageID
                    )
                    guard try await transaction.getValue(
                        for: reservationKey,
                        snapshot: false
                    ) == nil,
                          try await transaction.getValue(
                            for: self.pageDescriptorKey(
                                snapshotID: reservation.snapshotID,
                                pageID: pageID
                            ),
                            snapshot: false
                          ) == nil else {
                        return false
                    }
                    try transaction.setValue(
                        StorageFrameCodec.encode(PageReservation()),
                        for: reservationKey
                    )
                    return true
            }
            if didReserve { return pageID }
        }
        throw DatabaseQueryExecutionError.querySnapshotUnavailable(
            "unable to reserve a unique opaque page identifier"
        )
    }

    package func appendPage(
        _ page: QueryRowPage,
        pageID: ByteString,
        to reservation: WriteReservation,
        consumedPayloadBytes: UInt64,
        maximumIntermediateBytes: UInt64,
        transaction: (any TransactionAccess)? = nil
    ) async throws -> UInt64 {
        try await appendResponse(
            .rows(page),
            pageID: pageID,
            to: reservation,
            consumedPayloadBytes: consumedPayloadBytes,
            maximumIntermediateBytes: maximumIntermediateBytes,
            transaction: transaction
        )
    }

    package func appendPage(
        _ page: RDFGraphPage,
        pageID: ByteString,
        to reservation: WriteReservation,
        consumedPayloadBytes: UInt64,
        maximumIntermediateBytes: UInt64,
        transaction: (any TransactionAccess)? = nil
    ) async throws -> UInt64 {
        try await appendResponse(
            .rdfGraph(page),
            pageID: pageID,
            to: reservation,
            consumedPayloadBytes: consumedPayloadBytes,
            maximumIntermediateBytes: maximumIntermediateBytes,
            transaction: transaction
        )
    }

    private func appendResponse(
        _ response: QueryExecuteOperation.Response,
        pageID: ByteString,
        to reservation: WriteReservation,
        consumedPayloadBytes: UInt64,
        maximumIntermediateBytes: UInt64,
        transaction: (any TransactionAccess)?
    ) async throws -> UInt64 {
        guard pageID.count == 16 else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
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
            pageID: pageID,
            transaction: transaction
        )
        return nextTotal.partialValue
    }

    package func commitWrite(
        _ reservation: WriteReservation,
        pageCount: UInt32,
        totalPayloadBytes: UInt64,
        transaction: (any TransactionAccess)? = nil
    ) async throws {
        guard pageCount > 0, totalPayloadBytes > 0 else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }
        try await publish(
            snapshotID: reservation.snapshotID,
            pending: PendingSnapshot(
                principalDigest: reservation.principalDigest,
                slot: reservation.slot,
                expiresAt: reservation.expiresAt
            ),
            target: reservation.target,
            schemaGeneration: reservation.schemaGeneration,
            principalDigest: reservation.principalDigest,
            queryFingerprint: reservation.queryFingerprint,
            expiresAt: reservation.expiresAt,
            pageCount: pageCount,
            totalPayloadBytes: totalPayloadBytes,
            transaction: transaction
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
                            .querySnapshotCorrupted
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
        resource: Security.Resource,
        dataGeneration: UInt64,
        schemaGeneration: UInt64,
        queryFingerprint: ByteString,
        authorization: AuthorizationContext
    ) async throws -> QueryRowPage {
        let response = try await loadResponse(
            continuation: continuation,
            target: .resource(resource, generation: dataGeneration),
            schemaGeneration: schemaGeneration,
            queryFingerprint: queryFingerprint,
            authorization: authorization,
            compositionBaseIDs: nil
        )
        guard case .rows(let page) = response,
              page.provenance == nil else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }
        return page
    }

    func loadRDFGraph(
        continuation: ByteString,
        resource: Security.Resource,
        dataGeneration: UInt64,
        schemaGeneration: UInt64,
        queryFingerprint: ByteString,
        authorization: AuthorizationContext
    ) async throws -> RDFGraphPage {
        let response = try await loadResponse(
            continuation: continuation,
            target: .resource(resource, generation: dataGeneration),
            schemaGeneration: schemaGeneration,
            queryFingerprint: queryFingerprint,
            authorization: authorization,
            compositionBaseIDs: nil
        )
        guard case .rdfGraph(let page) = response,
              page.provenance == nil else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }
        return page
    }

    #if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
    func load(
        continuation: ByteString,
        composition: DatabaseCompositionRecord,
        schemaGeneration: UInt64,
        queryFingerprint: ByteString,
        authorization: AuthorizationContext
    ) async throws -> QueryRowPage {
        let response = try await loadResponse(
            continuation: continuation,
            target: .composition(
                composition.composition.id,
                generation: composition.generation
            ),
            schemaGeneration: schemaGeneration,
            queryFingerprint: queryFingerprint,
            authorization: authorization,
            compositionBaseIDs: composition.composition.bases
        )
        guard case .rows(let page) = response else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
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
            target: .composition(
                composition.composition.id,
                generation: composition.generation
            ),
            schemaGeneration: schemaGeneration,
            queryFingerprint: queryFingerprint,
            authorization: authorization,
            compositionBaseIDs: composition.composition.bases
        )
        guard case .rdfGraph(let page) = response else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }
        return page
    }
    #endif

    private func loadResponse(
        continuation: ByteString,
        target: Target,
        schemaGeneration: UInt64,
        queryFingerprint: ByteString,
        authorization: AuthorizationContext,
        compositionBaseIDs: [Base.ID]?
    ) async throws -> QueryExecuteOperation.Response {
        guard continuation.count == Self.continuationByteCount,
              continuation.first == Self.continuationMarker,
              queryFingerprint.count == 32,
              let principal = authorization.principal else {
            throw DatabaseQueryExecutionError.invalidContinuation
        }
        let snapshotID = continuation[1..<17]
        let pageID = continuation[17..<33]
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
                        .querySnapshotCorrupted
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
                        .querySnapshotCorrupted
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
                            .querySnapshotCorrupted
                    }
                    let next = actualByteCount.addingReportingOverflow(
                        chunk.count
                    )
                    guard !next.overflow,
                          next.partialValue <= payloadByteCount else {
                        throw DatabaseQueryExecutionError
                            .querySnapshotCorrupted
                    }
                    actualByteCount = next.partialValue
                    chunks.append(chunk)
                }
                guard actualByteCount == payloadByteCount else {
                    throw DatabaseQueryExecutionError
                        .querySnapshotCorrupted
                }
                // Persisted pages are split into backend-sized chunks, while
                // DatabaseWireDecoder requires one contiguous retained owner.
                // This is the single required reassembly copy at the durable
                // snapshot boundary; both the chunks and destination are
                // bounded by `maximumSpoolBytes` (16 MiB).
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
                        .querySnapshotCorrupted
                }
                return (manifest, payload)
            }
        } catch let error as DatabaseQueryExecutionError {
            throw error
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }

        let manifest = loaded.0
        guard Self.constantTimeEqual(
            manifest.principalDigest,
            principalDigest
        ) else {
            throw DatabaseQueryExecutionError.invalidContinuation
        }
        guard manifest.target.hasSameIdentity(as: target),
              Self.constantTimeEqual(
                manifest.queryFingerprint,
                queryFingerprint
              ) else {
            throw DatabaseQueryExecutionError.invalidContinuation
        }
        guard manifest.target == target,
              manifest.schemaGeneration == schemaGeneration else {
            throw DatabaseQueryExecutionError.querySnapshotStale
        }
        if manifest.expiresAt <= now {
            try await Task.detached {
                try await self.clearReserved(
                    snapshotID: snapshotID,
                    principalDigest: manifest.principalDigest,
                    slot: manifest.slot,
                    expiresAt: manifest.expiresAt
                )
            }.value
            throw DatabaseQueryExecutionError.querySnapshotExpired
        }
        let response: QueryExecuteOperation.Response
        do {
            response = try DatabaseWireDecoder(limits: wireLimits)
                .decodeResponsePayload(
                    DatabaseOperations.queryExecute,
                    from: loaded.1
                )
        } catch {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
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
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }
        switch target {
        case .resource:
            guard provenance == nil else {
                throw DatabaseQueryExecutionError.querySnapshotCorrupted
            }
        case .composition(let compositionID, let generation):
            guard let provenance,
                  provenance.compositionID == compositionID,
                  provenance.generation == generation,
                  provenance.baseIDs == compositionBaseIDs else {
                throw DatabaseQueryExecutionError.querySnapshotCorrupted
            }
        }
        if let next {
            guard next.count == Self.continuationByteCount,
                  next.first == Self.continuationMarker,
                  next[1..<17] == snapshotID else {
                throw DatabaseQueryExecutionError
                    .querySnapshotCorrupted
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
        pageID: ByteString,
        transaction: (any TransactionAccess)?
    ) async throws {
        let snapshotID = reservation.snapshotID
        let chunkCount = (payload.count - 1) / Self.chunkByteCount + 1
        guard let encodedChunkCount = UInt32(exactly: chunkCount) else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }
        for chunkIndex in 0..<chunkCount {
            let start = chunkIndex * Self.chunkByteCount
            let end = min(start + Self.chunkByteCount, payload.count)
            let chunk = payload[start..<end]
            try await withControlAccess(transaction: transaction) {
                transaction in
                try transaction.setValue(
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
        try await withControlAccess(transaction: transaction) { transaction in
            guard try await self.validatePending(
                reservation,
                transaction: transaction
            ),
                  let pageReservationBytes = try await transaction
                    .getValue(
                        for: self.pageReservationKey(
                            snapshotID: snapshotID,
                            pageID: pageID
                        ),
                        snapshot: false
                    ) else {
                throw DatabaseQueryExecutionError
                    .querySnapshotCorrupted
            }
            do {
                _ = try StorageFrameCodec.decode(
                    PageReservation.self,
                    from: pageReservationBytes
                )
            } catch {
                throw DatabaseQueryExecutionError
                    .querySnapshotCorrupted
            }
            guard try await transaction.getValue(
                for: self.pageDescriptorKey(
                    snapshotID: snapshotID,
                    pageID: pageID
                ),
                snapshot: false
            ) == nil else {
                throw DatabaseQueryExecutionError
                    .querySnapshotCorrupted
            }
            try transaction.setValue(
                StorageFrameCodec.encode(descriptor),
                for: self.pageDescriptorKey(
                    snapshotID: snapshotID,
                    pageID: pageID
                )
            )
            try transaction.clear(
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
        target: Target,
        schemaGeneration: UInt64,
        principalDigest: ByteString,
        queryFingerprint: ByteString,
        expiresAt: Timestamp,
        pageCount: UInt32,
        totalPayloadBytes: UInt64,
        transaction: (any TransactionAccess)?
    ) async throws {
        try await withControlAccess(transaction: transaction) {
            transaction in
            try await self.validateCurrentTarget(
                target,
                transaction: transaction
            )
            guard try await transaction.getValue(
                for: self.manifestKey(snapshotID),
                snapshot: false
            ) == nil else {
                throw DatabaseQueryExecutionError
                    .querySnapshotCorrupted
            }
            guard principalDigest == pending.principalDigest,
                  expiresAt == pending.expiresAt,
                  let pendingBytes = try await transaction
                .getValue(
                    for: self.pendingKey(snapshotID),
                    snapshot: false
                ) else {
                throw DatabaseQueryExecutionError
                    .querySnapshotCorrupted
            }
            let storedPending: PendingSnapshot
            do {
                storedPending = try StorageFrameCodec.decode(
                    PendingSnapshot.self,
                    from: pendingBytes
                )
            } catch {
                throw DatabaseQueryExecutionError
                    .querySnapshotCorrupted
            }
            guard storedPending.principalDigest == pending.principalDigest,
                  storedPending.slot == pending.slot,
                  storedPending.expiresAt == pending.expiresAt,
                  let slotBytes = try await transaction
                    .getValue(
                        for: self.principalSlotKey(
                            principalDigest: pending.principalDigest,
                            slot: pending.slot
                        ),
                        snapshot: false
                    ) else {
                throw DatabaseQueryExecutionError
                    .querySnapshotCorrupted
            }
            let storedSlot: PrincipalSlot
            do {
                storedSlot = try StorageFrameCodec.decode(
                    PrincipalSlot.self,
                    from: slotBytes
                )
            } catch {
                throw DatabaseQueryExecutionError
                    .querySnapshotCorrupted
            }
            guard storedSlot.snapshotID == snapshotID,
                  storedSlot.expiresAt == expiresAt else {
                throw DatabaseQueryExecutionError
                    .querySnapshotCorrupted
            }
            let manifest = Manifest(
                target: target,
                schemaGeneration: schemaGeneration,
                principalDigest: principalDigest,
                queryFingerprint: queryFingerprint,
                expiresAt: expiresAt,
                slot: pending.slot,
                pageCount: pageCount,
                totalPayloadBytes: totalPayloadBytes
            )
            try transaction.setValue(
                StorageFrameCodec.encode(manifest),
                for: self.manifestKey(snapshotID)
            )
            try transaction.clear(
                key: self.pendingKey(snapshotID)
            )
        }
    }

    private func validateCurrentTarget(
        _ target: Target,
        transaction: any TransactionAccess
    ) async throws {
        switch target {
        case .resource(.database, _):
            return
        case .resource(.base(let id), let generation):
            #if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
            guard let current = try await container.baseCatalog.load(
                id,
                transaction: transaction
            ), current.placementGeneration == generation,
               current.lifecycle == .active else {
                throw DatabaseQueryExecutionError.querySnapshotStale
            }
            #else
            _ = id
            _ = generation
            throw DatabaseQueryExecutionError.querySnapshotStale
            #endif
        case .composition(let id, let generation):
            #if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
            guard let current = try await container.compositionCatalog.load(
                id,
                transaction: transaction
            ), current.generation == generation else {
                throw DatabaseQueryExecutionError.querySnapshotStale
            }
            #else
            _ = id
            _ = generation
            throw DatabaseQueryExecutionError.querySnapshotStale
            #endif
        }
    }

    private func reserve(
        snapshotID: ByteString,
        principalDigest: ByteString,
        expiresAt: Timestamp,
        now: Timestamp,
        transaction: (any TransactionAccess)?
    ) async throws -> PendingSnapshot? {
        try await withControlAccess(transaction: transaction) { transaction in
            guard try await transaction.getValue(
                for: self.pendingKey(snapshotID),
                snapshot: false
            ) == nil,
                  try await transaction.getValue(
                    for: self.manifestKey(snapshotID),
                    snapshot: false
                  ) == nil else { return nil }
            let selection = try await self.availableSlot(
                principalDigest: principalDigest,
                now: now,
                transaction: transaction
            )
            if let expiredSnapshotID = selection.expiredSnapshotID {
                guard let expiredAt = selection.expiredAt else {
                    throw DatabaseQueryExecutionError
                        .querySnapshotCorrupted
                }
                try self.clearSnapshot(
                    expiredSnapshotID,
                    transaction: transaction
                )
                try transaction.clear(
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
            try transaction.setValue(
                StorageFrameCodec.encode(pending),
                for: self.pendingKey(snapshotID)
            )
            try transaction.setValue(
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
            try transaction.setValue(
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

    private func withControlAccess<Result: Sendable>(
        transaction: (any TransactionAccess)?,
        _ operation: @Sendable @escaping (
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        if let transaction {
            return try await operation(transaction)
        }
        if ActiveDatabaseTransactionContext.binding != nil {
            // A Base may live in a different storage domain from control
            // metadata. Transaction runners intentionally reject nesting even
            // across domains, so the independently owned control transaction
            // runs without inheriting the request's TaskLocal transaction.
            return try await Task.detached {
                try await self.container.withControlMetadataTransaction {
                    transaction in
                    try await operation(transaction.storageAccess)
                }
            }.value
        }
        return try await container.withControlMetadataTransaction {
            transaction in
            try await operation(transaction.storageAccess)
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
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
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
                throw DatabaseQueryExecutionError.querySnapshotCorrupted
            }
            if record.expiresAt <= now {
                return (slot, record.snapshotID, record.expiresAt)
            }
        }
        throw DatabaseQueryExecutionError.querySnapshotLimitExceeded(
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
                        .querySnapshotCorrupted
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
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
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
