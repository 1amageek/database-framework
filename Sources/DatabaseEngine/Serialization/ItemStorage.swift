import StorageKit

/// Canonical record storage bound to one transaction and one blob subspace.
///
/// v1 stores identity-encoded payloads. Inline reads remain views into the
/// backend-owned envelope buffer. External writes pass constant-time payload
/// slices to the transaction and external reads allocate exactly one final
/// assembly buffer.
public struct ItemStorage: Sendable {
    private let transaction: any Transaction
    private let blobsSubspace: Subspace
    public let configuration: ItemStorageConfiguration

    public init(
        transaction: any Transaction,
        blobsSubspace: Subspace,
        configuration: ItemStorageConfiguration
    ) {
        self.transaction = transaction
        self.blobsSubspace = blobsSubspace
        self.configuration = configuration
    }

    /// Writes one complete record using the canonical v1 physical format.
    ///
    /// All fallible validation and envelope construction happens before old
    /// blob mutations are cleared. The enclosing transaction remains the
    /// atomicity boundary for chunks, envelope, indexes, and relationships.
    public func write(_ data: Bytes, for key: Bytes) async throws {
        try validatePlainByteCount(data.count)

        let payload: Bytes
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

        try clearAllBlobs(for: key)
        if case .external(let reference) = envelope.content {
            try writeChunks(
                payload,
                for: key,
                chunkCount: Int(reference.chunkCount)
            )
        }
        try transaction.setValue(envelopeBytes, for: key)
    }

    /// Reads, structurally validates, and checksum-verifies one record.
    public func read(
        for key: Bytes,
        snapshot: Bool = false
    ) async throws -> Bytes? {
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

    public func exists(
        for key: Bytes,
        snapshot: Bool = false
    ) async throws -> Bool {
        try await transaction.getValue(for: key, snapshot: snapshot) != nil
    }

    /// Deletes both the item envelope and every possible external chunk.
    public func delete(for key: Bytes) async throws {
        try clearAllBlobs(for: key)
        try transaction.clear(key: key)
    }

    /// Opens a lazy scan over canonical item envelopes.
    public func scan(
        begin: Bytes,
        end: Bytes,
        snapshot: Bool = false,
        limit: Int = 0,
        reverse: Bool = false
    ) -> ItemScanSequence {
        let validationError: ItemStorageError? = limit < 0
            ? .invalidScanLimit(limit)
            : nil
        return ItemScanSequence(
            storage: self,
            begin: begin,
            end: end,
            snapshot: snapshot,
            limit: limit,
            reverse: reverse,
            validationError: validationError
        )
    }

    /// Direct transaction access for non-item keys in the same atomic unit.
    public var underlying: any Transaction {
        transaction
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

    private func blobPrefix(for key: Bytes) -> Subspace {
        blobsSubspace.subspace(Tuple([key]))
    }

    private func clearAllBlobs(for key: Bytes) throws {
        let (begin, end) = blobPrefix(for: key).range()
        try transaction.clearRange(beginKey: begin, endKey: end)
    }

    private func writeChunks(
        _ payload: Bytes,
        for key: Bytes,
        chunkCount: Int
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
            // Bytes slicing is a constant-time view. The transaction owns any
            // copy required by its backend lifetime after this synchronous call.
            try transaction.setValue(payload[offset..<end], for: chunkKey)
            offset = end
        }
        guard offset == payload.count else {
            throw ItemStorageError.invalidChunkLayout
        }
    }

    fileprivate func decodeStoredValue(
        _ envelopeBytes: Bytes,
        for key: Bytes,
        snapshot: Bool
    ) async throws -> Bytes {
        guard ItemEnvelope.isEnvelope(envelopeBytes) else {
            throw ItemStorageError.notEnvelopeFormat
        }
        let envelope = try ItemEnvelope.deserialize(envelopeBytes)
        try validate(envelope)

        let storedPayload: Bytes
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

        let plainPayload: Bytes
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

    private func loadChunks(
        for key: Bytes,
        envelope: ItemEnvelope,
        reference: ItemEnvelope.ExternalRef,
        snapshot: Bool
    ) async throws -> Bytes {
        guard let totalSize = Int(exactly: envelope.storedByteCount),
              let chunkCount = Int(exactly: reference.chunkCount),
              let chunkSize = Int(exactly: reference.chunkByteCount) else {
            throw ItemStorageError.invalidChunkLayout
        }

        // External payloads require one final assembly allocation because
        // StorageKit point reads own independent backend buffers per chunk.
        var output = [UInt8](repeating: 0, count: totalSize)
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
            output.withUnsafeMutableBytes { destination in
                chunk.withUnsafeBytes { source in
                    UnsafeMutableRawBufferPointer(
                        rebasing: destination[
                            loadedByteCount..<(loadedByteCount + source.count)
                        ]
                    ).copyMemory(from: source)
                }
            }
            loadedByteCount += expectedByteCount
        }
        guard loadedByteCount == totalSize else {
            throw ItemEnvelopeError.payloadSizeMismatch(
                expected: envelope.storedByteCount,
                actual: UInt64(loadedByteCount)
            )
        }
        return Bytes(owningExact: output)
    }
}

/// Lazy record scan that preserves backend-native key/value ownership.
public struct ItemScanSequence: AsyncSequence, Sendable {
    public typealias Element = (key: Bytes, data: Bytes)

    private let storage: ItemStorage
    private let begin: Bytes
    private let end: Bytes
    private let snapshot: Bool
    private let limit: Int
    private let reverse: Bool
    private let validationError: ItemStorageError?

    init(
        storage: ItemStorage,
        begin: Bytes,
        end: Bytes,
        snapshot: Bool,
        limit: Int,
        reverse: Bool,
        validationError: ItemStorageError?
    ) {
        self.storage = storage
        self.begin = begin
        self.end = end
        self.snapshot = snapshot
        self.limit = limit
        self.reverse = reverse
        self.validationError = validationError
    }

    public func makeAsyncIterator() -> AsyncIterator {
        guard validationError == nil else {
            return AsyncIterator(
                storage: nil,
                cursor: nil,
                snapshot: snapshot,
                pendingError: validationError
            )
        }
        return AsyncIterator(
            storage: storage,
            cursor: storage.underlying.rangeCursor(
                from: .firstGreaterOrEqual(begin),
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
        private var storage: ItemStorage?
        private var cursor: KeyValueCursor?
        private let snapshot: Bool
        private var pendingError: ItemStorageError?

        fileprivate init(
            storage: ItemStorage?,
            cursor: KeyValueCursor?,
            snapshot: Bool,
            pendingError: ItemStorageError?
        ) {
            self.storage = storage
            self.cursor = cursor
            self.snapshot = snapshot
            self.pendingError = pendingError
        }

        public mutating func next() async throws -> Element? {
            if let pendingError {
                finish()
                throw pendingError
            }
            guard let storage, var activeCursor = cursor else {
                finish()
                return nil
            }
            cursor = nil
            do {
                guard let (key, envelopeBytes) = try await activeCursor.next()
                else {
                    finish()
                    return nil
                }
                cursor = activeCursor
                let data = try await storage.decodeStoredValue(
                    envelopeBytes,
                    for: key,
                    snapshot: snapshot
                )
                return (key, data)
            } catch {
                finish()
                throw error
            }
        }

        private mutating func finish() {
            storage = nil
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
