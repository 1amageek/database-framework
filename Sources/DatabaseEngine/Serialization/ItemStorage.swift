import DatabaseTypes
import StorageKit

/// Request-accounted ownership for one decoded item payload.
///
/// The reservation covers the backend envelope retained by inline payloads
/// and the peak external assembly footprint. Consumers borrow the bytes only
/// while this owner remains alive.
package struct ItemStorageRetainedValue: Sendable {
    private let value: ByteString
    private let reservation: DatabaseIntermediateReservation

    fileprivate init(
        value: ByteString,
        reservation: DatabaseIntermediateReservation
    ) {
        self.value = value
        self.reservation = reservation
    }

    package var count: Int { value.count }

    package func withValue<Result, Failure: Error>(
        _ operation: (borrowing ByteString) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        try operation(value)
    }
}

/// Validated size information for one stored entity envelope. The admission
/// count includes the envelope owner and the peak external assembly/chunk
/// bytes that coexist while the canonical payload is decoded.
package struct ItemStorageEnvelopeAdmission: Sendable {
    package let materializedPayloadByteCount: Int
    package let decodePeakByteCount: Int
}

/// Canonical entity mutation capability bound to one transaction and one blob subspace.
///
/// Its transaction is intentionally not exposed. Non-item mutations remain
/// the responsibility of the caller that owns the transaction.
///
/// v1 stores identity-encoded payloads. Inline reads remain views into the
/// backend-owned envelope buffer. External writes pass constant-time payload
/// slices to the transaction and external reads allocate exactly one final
/// assembly buffer.
public struct ItemStorageWriter: Sendable {
    private let transaction: any TransactionAccess
    private let blobsSubspace: Subspace
    private let readCore: ItemStorageReadCore
    public let configuration: ItemStorageConfiguration

    public init(
        transaction: any TransactionAccess,
        blobsSubspace: Subspace,
        configuration: ItemStorageConfiguration
    ) {
        self.transaction = transaction
        self.blobsSubspace = blobsSubspace
        self.configuration = configuration
        self.readCore = ItemStorageReadCore(
            transaction: transaction,
            blobsSubspace: blobsSubspace,
            configuration: configuration
        )
    }

    /// Writes one complete entity using the canonical v1 physical format.
    ///
    /// All fallible validation and envelope construction happens before old
    /// blob mutations are cleared. The enclosing transaction remains the
    /// atomicity boundary for chunks, envelope, indexes, and relationships.
    public func write(_ data: ByteString, for key: ByteString) async throws {
        try validatePlainByteCount(data.count)

        let payload: ByteString
        switch configuration.encoding {
        case .identity:
            payload = data
        }
        try validateStoredByteCount(payload.count)

        let checksum = ItemChecksum.crc32c(data)
        let plainByteCount = UInt64(data.count)
        let storedByteCount = UInt64(payload.count)

        let envelope: ItemEnvelope
        if payload.count <= configuration.maximumInlineByteCount {
            envelope = try ItemEnvelope.inline(
                payload: payload,
                encoding: configuration.encoding,
                plainByteCount: plainByteCount,
                checksum: checksum
            )
        } else {
            let chunkCount = try externalChunkCount(
                storedByteCount: payload.count
            )
            let reference = try ItemEnvelope.ExternalRef(
                chunkCount: chunkCount,
                chunkByteCount: UInt32(configuration.chunkByteCount),
                storedByteCount: storedByteCount
            )
            envelope = try ItemEnvelope.external(
                reference: reference,
                encoding: configuration.encoding,
                plainByteCount: plainByteCount,
                storedByteCount: storedByteCount,
                checksum: checksum
            )
        }
        let envelopeBytes = envelope.serialize()

        try clearAllBlobs(for: key, transaction: transaction)
        if case .external(let reference) = envelope.content {
            try writeChunks(
                payload,
                for: key,
                chunkCount: Int(reference.chunkCount),
                transaction: transaction
            )
        }
        try transaction.setValue(envelopeBytes, for: key)
    }

    /// Reads, structurally validates, and checksum-verifies one entity.
    public func read(
        for key: ByteString,
        snapshot: Bool = false
    ) async throws -> ByteString? {
        try await readCore.read(for: key, snapshot: snapshot)
    }

    public func exists(
        for key: ByteString,
        snapshot: Bool = false
    ) async throws -> Bool {
        try await readCore.exists(for: key, snapshot: snapshot)
    }

    /// Deletes both the item envelope and every possible external chunk.
    public func delete(for key: ByteString) async throws {
        try clearAllBlobs(for: key, transaction: transaction)
        try transaction.clear(key: key)
    }

    /// Opens a lazy scan over canonical item envelopes.
    public func scan(
        begin: ByteString,
        end: ByteString,
        startingAfter: ByteString? = nil,
        snapshot: Bool = false,
        limit: Int = 0,
        reverse: Bool = false
    ) -> ItemScanSequence {
        readCore.scan(
            begin: begin,
            end: end,
            startingAfter: startingAfter,
            snapshot: snapshot,
            limit: limit,
            reverse: reverse
        )
    }

    private func validatePlainByteCount(_ count: Int) throws {
        guard count <= configuration.maximumPlainByteCount else {
            throw ItemStorageError.plainValueTooLarge(
                size: UInt64(count),
                maximum: UInt64(configuration.maximumPlainByteCount)
            )
        }
    }

    private func validateStoredByteCount(_ count: Int) throws {
        guard count <= configuration.maximumStoredByteCount else {
            throw ItemStorageError.storedValueTooLarge(
                size: UInt64(count),
                maximum: UInt64(configuration.maximumStoredByteCount)
            )
        }
    }

    private func externalChunkCount(
        storedByteCount: Int
    ) throws -> UInt32 {
        let (roundedSize, overflow) = storedByteCount.addingReportingOverflow(
            configuration.chunkByteCount - 1
        )
        guard !overflow,
              let count = UInt32(
                exactly: roundedSize / configuration.chunkByteCount
              ),
              count > 0 else {
            throw ItemStorageError.invalidChunkLayout
        }
        return count
    }

    private func blobPrefix(for key: ByteString) -> Subspace {
        blobsSubspace.subspace(Tuple([key]))
    }

    private func clearAllBlobs(
        for key: ByteString,
        transaction: any TransactionAccess
    ) throws {
        let (begin, end) = blobPrefix(for: key).range()
        try transaction.clearRange(beginKey: begin, endKey: end)
    }

    private func writeChunks(
        _ payload: ByteString,
        for key: ByteString,
        chunkCount: Int,
        transaction: any TransactionAccess
    ) throws {
        let blobBase = blobPrefix(for: key)
        var offset = 0
        for index in 0..<chunkCount {
            let end = Swift.min(
                offset + configuration.chunkByteCount,
                payload.count
            )
            guard let encodedIndex = Int32(exactly: index) else {
                throw ItemStorageError.invalidChunkLayout
            }
            let chunkKey = blobBase.pack(Tuple([encodedIndex]))
            // Each stored chunk owns only its visible range. In-memory and
            // deferred-copy backends must not retain the complete large payload
            // once the write call returns.
            try transaction.setValue(
                payload[offset..<end].detached(),
                for: chunkKey
            )
            offset = end
        }
        guard offset == payload.count else {
            throw ItemStorageError.invalidChunkLayout
        }
    }

}

/// Read-only item capability. Its static surface contains no mutation or
/// transaction-control API.
public struct ItemStorageReader: Sendable {
    private let core: ItemStorageReadCore

    public init(
        transaction: any TransactionReadAccess,
        blobsSubspace: Subspace,
        configuration: ItemStorageConfiguration
    ) {
        self.core = ItemStorageReadCore(
            transaction: transaction,
            blobsSubspace: blobsSubspace,
            configuration: configuration
        )
    }

    public func read(
        for key: ByteString,
        snapshot: Bool = false
    ) async throws -> ByteString? {
        try await core.read(for: key, snapshot: snapshot)
    }

    /// Reads one item while retaining its payload under the request budget.
    /// The validated envelope size is admitted before external chunks or the
    /// final assembly buffer are loaded.
    package func readRetained(
        for key: ByteString,
        snapshot: Bool = false,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) async throws -> ItemStorageRetainedValue? {
        try await core.readRetained(
            for: key,
            snapshot: snapshot,
            workMeter: workMeter,
            stage: stage
        )
    }

    /// Validates a range-row envelope and reports its decode footprint without
    /// loading external chunks.
    package func inspectEnvelopeForAdmission(
        _ envelopeBytes: borrowing ByteString
    ) throws -> ItemStorageEnvelopeAdmission {
        try core.inspectEnvelopeForAdmission(envelopeBytes)
    }

    /// Decodes an envelope already obtained from a caller-owned range cursor.
    /// Callers must perform admission with `inspectEnvelopeForAdmission`
    /// before invoking this method.
    package func decodeAdmittedEnvelope(
        _ envelopeBytes: ByteString,
        for key: ByteString,
        snapshot: Bool = false
    ) async throws -> ByteString {
        return try await core.decodeStoredValue(
            envelopeBytes,
            for: key,
            snapshot: snapshot
        ).detached()
    }

    public func exists(
        for key: ByteString,
        snapshot: Bool = false
    ) async throws -> Bool {
        try await core.exists(for: key, snapshot: snapshot)
    }

    public func scan(
        begin: ByteString,
        end: ByteString,
        startingAfter: ByteString? = nil,
        snapshot: Bool = false,
        limit: Int = 0,
        reverse: Bool = false
    ) -> ItemScanSequence {
        core.scan(
            begin: begin,
            end: end,
            startingAfter: startingAfter,
            snapshot: snapshot,
            limit: limit,
            reverse: reverse
        )
    }

    package func scanRetained(
        begin: ByteString,
        end: ByteString,
        startingAfter: ByteString? = nil,
        snapshot: Bool = false,
        limit: Int = 0,
        reverse: Bool = false,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) -> ItemStorageRetainedScanSequence {
        core.scanRetained(
            begin: begin,
            end: end,
            startingAfter: startingAfter,
            snapshot: snapshot,
            limit: limit,
            reverse: reverse,
            workMeter: workMeter,
            stage: stage
        )
    }
}

fileprivate struct ItemStorageReadCore: Sendable {
    let transaction: any TransactionReadAccess
    let blobsSubspace: Subspace
    let configuration: ItemStorageConfiguration

    func read(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        guard let envelopeBytes = try await transaction.getValue(
            for: key,
            snapshot: snapshot
        ) else {
            return nil
        }
        return try await decodeStoredValue(
            envelopeBytes,
            for: key,
            snapshot: snapshot
        )
    }

    func readRetained(
        for key: ByteString,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) async throws -> ItemStorageRetainedValue? {
        guard let envelopeBytes = try await transaction.getValue(
            for: key,
            snapshot: snapshot
        ) else {
            return nil
        }
        return try await decodeStoredValueRetained(
            envelopeBytes,
            for: key,
            snapshot: snapshot,
            workMeter: workMeter,
            stage: stage
        )
    }

    func exists(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> Bool {
        try await transaction.getValue(for: key, snapshot: snapshot) != nil
    }

    func scan(
        begin: ByteString,
        end: ByteString,
        startingAfter: ByteString?,
        snapshot: Bool,
        limit: Int,
        reverse: Bool
    ) -> ItemScanSequence {
        ItemScanSequence(
            core: self,
            begin: begin,
            end: end,
            startingAfter: startingAfter,
            snapshot: snapshot,
            limit: limit,
            reverse: reverse,
            validationError: limit < 0 ? .invalidScanLimit(limit) : nil
        )
    }

    func scanRetained(
        begin: ByteString,
        end: ByteString,
        startingAfter: ByteString?,
        snapshot: Bool,
        limit: Int,
        reverse: Bool,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) -> ItemStorageRetainedScanSequence {
        ItemStorageRetainedScanSequence(
            core: self,
            begin: begin,
            end: end,
            startingAfter: startingAfter,
            snapshot: snapshot,
            limit: limit,
            reverse: reverse,
            workMeter: workMeter,
            stage: stage,
            validationError: limit < 0 ? .invalidScanLimit(limit) : nil
        )
    }

    func decodeStoredValue(
        _ envelopeBytes: ByteString,
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString {
        guard ItemEnvelope.isEnvelope(envelopeBytes) else {
            throw ItemStorageError.notEnvelopeFormat
        }
        let envelope = try ItemEnvelope.deserialize(envelopeBytes)
        try validate(envelope)

        let storedPayload: ByteString
        switch envelope.content {
        case .inline(let payload):
            storedPayload = payload
        case .external(let reference):
            storedPayload = try await loadChunks(
                for: key,
                envelope: envelope,
                reference: reference,
                snapshot: snapshot
            )
        }

        let plainPayload: ByteString
        switch envelope.encoding {
        case .identity:
            plainPayload = storedPayload
        }
        let actualChecksum = ItemChecksum.crc32c(plainPayload)
        guard actualChecksum == envelope.checksum else {
            throw ItemEnvelopeError.checksumMismatch(
                expected: envelope.checksum,
                actual: actualChecksum
            )
        }
        return plainPayload
    }

    func inspectEnvelopeForAdmission(
        _ envelopeBytes: borrowing ByteString
    ) throws -> ItemStorageEnvelopeAdmission {
        guard ItemEnvelope.isEnvelope(envelopeBytes) else {
            throw ItemStorageError.notEnvelopeFormat
        }
        let envelope = try ItemEnvelope.deserialize(envelopeBytes)
        try validate(envelope)
        guard let payloadByteCount = Int(exactly: envelope.plainByteCount)
        else {
            throw ItemStorageError.invalidChunkLayout
        }
        let envelopeRetainedByteCount: Int
        if let measuredRetainedByteCount = envelopeBytes.retainedByteCount {
            envelopeRetainedByteCount = measuredRetainedByteCount
        } else {
            envelopeRetainedByteCount = envelopeBytes.count
        }

        let materializedPayloadByteCount: Int
        let externalAssemblyBytes: UInt64
        switch envelope.content {
        case .inline:
            // Admitted range decoding detaches the visible payload before it
            // escapes the backend result owner. Both owners coexist at peak.
            materializedPayloadByteCount = payloadByteCount
            externalAssemblyBytes = UInt64(payloadByteCount)
        case .external(let reference):
            materializedPayloadByteCount = payloadByteCount
            let (withChunk, overflow) = envelope.storedByteCount
                .addingReportingOverflow(
                    min(
                        envelope.storedByteCount,
                        UInt64(reference.chunkByteCount)
                    )
                )
            guard !overflow else {
                throw ItemStorageError.invalidChunkLayout
            }
            externalAssemblyBytes = withChunk
        }
        let (peak, overflow) = UInt64(envelopeRetainedByteCount)
            .addingReportingOverflow(externalAssemblyBytes)
        guard !overflow, let decodePeakByteCount = Int(exactly: peak) else {
            throw ItemStorageError.invalidChunkLayout
        }
        return ItemStorageEnvelopeAdmission(
            materializedPayloadByteCount: materializedPayloadByteCount,
            decodePeakByteCount: decodePeakByteCount
        )
    }

    func decodeStoredValueRetained(
        _ envelopeBytes: consuming ByteString,
        for key: ByteString,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) async throws -> ItemStorageRetainedValue {
        let measuredRetainedByteCount = envelopeBytes.retainedByteCount
        let envelopeRetainedByteCount = measuredRetainedByteCount
            ?? envelopeBytes.count
        let reservation = try workMeter.reserveIntermediate(
            bytes: UInt64(envelopeRetainedByteCount),
            at: stage
        )
        do {
            let retainedEnvelope: ByteString
            if measuredRetainedByteCount == nil {
                // An unmeasurable backend owner must not escape this boundary.
                // Admit the visible-size copy while the source still exists,
                // then release the source proxy only after dropping its owner.
                try reservation.reserveAdditional(
                    bytes: UInt64(envelopeBytes.count),
                    at: stage
                )
                var sourceEnvelope = consume envelopeBytes
                retainedEnvelope = sourceEnvelope.detached()
                sourceEnvelope = ByteString()
                reservation.releaseGuaranteedPartial(
                    bytes: UInt64(retainedEnvelope.count)
                )
            } else {
                retainedEnvelope = consume envelopeBytes
            }
            guard ItemEnvelope.isEnvelope(retainedEnvelope) else {
                throw ItemStorageError.notEnvelopeFormat
            }
            let envelope = try ItemEnvelope.deserialize(retainedEnvelope)
            try validate(envelope)

            let externalPeakByteCount: UInt64
            switch envelope.content {
            case .inline:
                externalPeakByteCount = 0
            case .external(let reference):
                let (peak, overflow) = envelope.storedByteCount
                    .addingReportingOverflow(
                        min(
                            envelope.storedByteCount,
                            UInt64(reference.chunkByteCount)
                        )
                    )
                guard !overflow else {
                    throw ItemStorageError.invalidChunkLayout
                }
                externalPeakByteCount = peak
            }
            try reservation.reserveAdditional(
                bytes: externalPeakByteCount,
                at: stage
            )
            let storedPayload: ByteString
            switch envelope.content {
            case .inline(let payload):
                storedPayload = payload
            case .external(let reference):
                storedPayload = try await loadChunks(
                    for: key,
                    envelope: envelope,
                    reference: reference,
                    snapshot: snapshot
                )
            }
            let plainPayload: ByteString
            switch envelope.encoding {
            case .identity:
                plainPayload = storedPayload
            }
            let actualChecksum = ItemChecksum.crc32c(plainPayload)
            guard actualChecksum == envelope.checksum else {
                throw ItemEnvelopeError.checksumMismatch(
                    expected: envelope.checksum,
                    actual: actualChecksum
                )
            }
            return ItemStorageRetainedValue(
                value: plainPayload,
                reservation: reservation
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    private func validate(_ envelope: ItemEnvelope) throws {
        guard envelope.encoding == configuration.encoding else {
            throw ItemStorageError.encodingMismatch(
                expected: configuration.encoding,
                actual: envelope.encoding
            )
        }
        guard envelope.plainByteCount
                <= UInt64(configuration.maximumPlainByteCount) else {
            throw ItemStorageError.plainValueTooLarge(
                size: envelope.plainByteCount,
                maximum: UInt64(configuration.maximumPlainByteCount)
            )
        }
        guard envelope.storedByteCount
                <= UInt64(configuration.maximumStoredByteCount) else {
            throw ItemStorageError.storedValueTooLarge(
                size: envelope.storedByteCount,
                maximum: UInt64(configuration.maximumStoredByteCount)
            )
        }

        switch envelope.content {
        case .inline:
            guard envelope.storedByteCount
                    <= UInt64(configuration.maximumInlineByteCount) else {
                throw ItemStorageError.nonCanonicalStorageKind
            }
        case .external(let reference):
            guard envelope.storedByteCount
                    > UInt64(configuration.maximumInlineByteCount),
                  reference.chunkByteCount
                    == UInt32(configuration.chunkByteCount) else {
                throw ItemStorageError.nonCanonicalStorageKind
            }
        }
    }

    private func blobPrefix(for key: ByteString) -> Subspace {
        blobsSubspace.subspace(Tuple([key]))
    }

    private func loadChunks(
        for key: ByteString,
        envelope: ItemEnvelope,
        reference: ItemEnvelope.ExternalRef,
        snapshot: Bool
    ) async throws -> ByteString {
        guard let totalSize = Int(exactly: envelope.storedByteCount),
              let chunkCount = Int(exactly: reference.chunkCount),
              let chunkSize = Int(exactly: reference.chunkByteCount) else {
            throw ItemStorageError.invalidChunkLayout
        }

        // External payloads require one final assembly allocation because
        // StorageKit point reads own independent backend buffers per chunk.
        guard totalSize > 0 else {
            throw ItemStorageError.invalidChunkLayout
        }
        let output = ExactByteAssemblyOwner(count: totalSize)
        var loadedByteCount = 0
        let blobBase = blobPrefix(for: key)
        for index in 0..<chunkCount {
            guard let encodedIndex = Int32(exactly: index) else {
                throw ItemStorageError.invalidChunkLayout
            }
            let chunkKey = blobBase.pack(Tuple([encodedIndex]))
            guard let chunk = try await transaction.getValue(
                for: chunkKey,
                snapshot: snapshot
            ) else {
                throw ItemEnvelopeError.chunkMissing(index: index)
            }
            let expectedByteCount = Swift.min(
                chunkSize,
                totalSize - loadedByteCount
            )
            guard chunk.count == expectedByteCount else {
                throw ItemEnvelopeError.chunkSizeMismatch(
                    index: index,
                    expected: expectedByteCount,
                    actual: chunk.count
                )
            }
            output.append(chunk)
            loadedByteCount += expectedByteCount
        }
        guard loadedByteCount == totalSize else {
            throw ItemEnvelopeError.payloadSizeMismatch(
                expected: envelope.storedByteCount,
                actual: UInt64(loadedByteCount)
            )
        }
        return output.finish()
    }
}

/// Lazy entity scan that preserves backend-native key/value ownership.
public struct ItemScanSequence: AsyncSequence, Sendable {
    public typealias Element = (key: ByteString, data: ByteString)

    private let core: ItemStorageReadCore
    private let begin: ByteString
    private let end: ByteString
    private let startingAfter: ByteString?
    private let snapshot: Bool
    private let limit: Int
    private let reverse: Bool
    private let validationError: ItemStorageError?

    fileprivate init(
        core: ItemStorageReadCore,
        begin: ByteString,
        end: ByteString,
        startingAfter: ByteString?,
        snapshot: Bool,
        limit: Int,
        reverse: Bool,
        validationError: ItemStorageError?
    ) {
        self.core = core
        self.begin = begin
        self.end = end
        self.startingAfter = startingAfter
        self.snapshot = snapshot
        self.limit = limit
        self.reverse = reverse
        self.validationError = validationError
    }

    public func makeAsyncIterator() -> AsyncIterator {
        guard validationError == nil else {
            return AsyncIterator(
                core: nil,
                cursor: nil,
                snapshot: snapshot,
                pendingError: validationError
            )
        }
        return AsyncIterator(
            core: core,
            cursor: core.transaction.rangeCursor(
                from: startingAfter.map(KeySelector.firstGreaterThan)
                    ?? .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: limit > 0 ? .small : .wantAll
            ),
            snapshot: snapshot,
            pendingError: nil
        )
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        public typealias Failure = any Error

        private var core: ItemStorageReadCore?
        private var cursor: KeyValueCursor?
        private let snapshot: Bool
        private var pendingError: ItemStorageError?

        fileprivate init(
            core: ItemStorageReadCore?,
            cursor: KeyValueCursor?,
            snapshot: Bool,
            pendingError: ItemStorageError?
        ) {
            self.core = core
            self.cursor = cursor
            self.snapshot = snapshot
            self.pendingError = pendingError
        }

        public mutating func next(
            isolation actor: isolated (any Actor)?
        ) async throws -> Element? {
            if let pendingError {
                finish()
                throw pendingError
            }
            guard let core, var activeCursor = cursor else {
                finish()
                return nil
            }
            cursor = nil
            let element: KeyValueCursor.Element
            do {
                guard let nextElement = try await activeCursor.next() else {
                    finish()
                    return nil
                }
                element = nextElement
            } catch {
                finish()
                throw error
            }
            let (key, envelopeBytes) = element
            do {
                let data = try await core.decodeStoredValue(
                    envelopeBytes,
                    for: key,
                    snapshot: snapshot
                )
                cursor = activeCursor
                return (key, data)
            } catch {
                let decodingError = error
                do {
                    try await activeCursor.finish()
                } catch {
                    finish()
                    throw StorageRangeCleanupError(
                        iterationError: decodingError,
                        cleanupError: error
                    )
                }
                finish()
                throw decodingError
            }
        }

        private mutating func finish() {
            core = nil
            cursor = nil
            pendingError = nil
        }
    }
}

/// Lazy metered scan whose current payload keeps its request reservation alive
/// until the consumer advances or releases the returned value.
package struct ItemStorageRetainedScanSequence: AsyncSequence, Sendable {
    package typealias Element = (
        key: ByteString,
        value: ItemStorageRetainedValue
    )

    private let core: ItemStorageReadCore
    private let begin: ByteString
    private let end: ByteString
    private let startingAfter: ByteString?
    private let snapshot: Bool
    private let limit: Int
    private let reverse: Bool
    private let workMeter: DatabaseWorkMeter
    private let stage: DatabaseWorkStage
    private let validationError: ItemStorageError?

    fileprivate init(
        core: ItemStorageReadCore,
        begin: ByteString,
        end: ByteString,
        startingAfter: ByteString?,
        snapshot: Bool,
        limit: Int,
        reverse: Bool,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        validationError: ItemStorageError?
    ) {
        self.core = core
        self.begin = begin
        self.end = end
        self.startingAfter = startingAfter
        self.snapshot = snapshot
        self.limit = limit
        self.reverse = reverse
        self.workMeter = workMeter
        self.stage = stage
        self.validationError = validationError
    }

    package func makeAsyncIterator() -> AsyncIterator {
        guard validationError == nil else {
            return AsyncIterator(
                core: nil,
                cursor: nil,
                snapshot: snapshot,
                workMeter: workMeter,
                stage: stage,
                pendingError: validationError
            )
        }
        return AsyncIterator(
            core: core,
            cursor: core.transaction.rangeCursor(
                from: startingAfter.map(KeySelector.firstGreaterThan)
                    ?? .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: limit > 0 ? .small : .wantAll
            ),
            snapshot: snapshot,
            workMeter: workMeter,
            stage: stage,
            pendingError: nil
        )
    }

    package struct AsyncIterator: AsyncIteratorProtocol {
        package typealias Failure = any Error

        private var core: ItemStorageReadCore?
        private var cursor: KeyValueCursor?
        private let snapshot: Bool
        private let workMeter: DatabaseWorkMeter
        private let stage: DatabaseWorkStage
        private var pendingError: ItemStorageError?

        fileprivate init(
            core: ItemStorageReadCore?,
            cursor: KeyValueCursor?,
            snapshot: Bool,
            workMeter: DatabaseWorkMeter,
            stage: DatabaseWorkStage,
            pendingError: ItemStorageError?
        ) {
            self.core = core
            self.cursor = cursor
            self.snapshot = snapshot
            self.workMeter = workMeter
            self.stage = stage
            self.pendingError = pendingError
        }

        package mutating func next(
            isolation actor: isolated (any Actor)?
        ) async throws -> Element? {
            if let pendingError {
                finish()
                throw pendingError
            }
            guard let core, var activeCursor = cursor else {
                finish()
                return nil
            }
            cursor = nil
            let element: KeyValueCursor.Element
            do {
                guard let nextElement = try await activeCursor.next() else {
                    finish()
                    return nil
                }
                element = nextElement
            } catch {
                finish()
                throw error
            }
            let (key, envelopeBytes) = element
            do {
                let value = try await core.decodeStoredValueRetained(
                    envelopeBytes,
                    for: key,
                    snapshot: snapshot,
                    workMeter: workMeter,
                    stage: stage
                )
                cursor = activeCursor
                return (key, value)
            } catch {
                let decodingError = error
                do {
                    try await activeCursor.finish()
                } catch {
                    finish()
                    throw StorageRangeCleanupError(
                        iterationError: decodingError,
                        cleanupError: error
                    )
                }
                finish()
                throw decodingError
            }
        }

        private mutating func finish() {
            core = nil
            cursor = nil
            pendingError = nil
        }
    }
}

public enum ItemStorageError: Error, CustomStringConvertible, Sendable, Equatable {
    case plainValueTooLarge(size: UInt64, maximum: UInt64)
    case storedValueTooLarge(size: UInt64, maximum: UInt64)
    case encodingMismatch(
        expected: ItemPayloadEncoding,
        actual: ItemPayloadEncoding
    )
    case nonCanonicalStorageKind
    case invalidChunkLayout
    case invalidScanLimit(Int)
    case notEnvelopeFormat

    public var description: String {
        switch self {
        case .plainValueTooLarge(let size, let maximum):
            return "Plain item size \(size) exceeds maximum \(maximum)"
        case .storedValueTooLarge(let size, let maximum):
            return "Stored item size \(size) exceeds maximum \(maximum)"
        case .encodingMismatch(let expected, let actual):
            return "Item encoding mismatch: expected \(expected), got \(actual)"
        case .nonCanonicalStorageKind:
            return "Item uses a non-canonical inline or external layout"
        case .invalidChunkLayout:
            return "Item has an invalid external chunk layout"
        case .invalidScanLimit(let limit):
            return "Item scan limit must be nonnegative: \(limit)"
        case .notEnvelopeFormat:
            return "Stored item is not a canonical ItemEnvelope"
        }
    }
}
