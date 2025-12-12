// ItemStorage.swift
// DatabaseEngine - Unified item storage abstraction
//
// Handles compression, large value splitting, and snapshot semantics.
// All item I/O should go through this layer.

import Foundation
import FoundationDB
import Core

// MARK: - ItemStorage

/// Unified item storage abstraction
///
/// **Key Design Principles**:
/// 1. One item = one key in items subspace (no split keys mixing in scans)
/// 2. Large values stored in separate blobs subspace
/// 3. Snapshot parameter propagated to all reads
/// 4. Compression applied before splitting decision
/// 5. Overwrite always cleans up old blobs first
///
/// **Data Layout**:
/// ```
/// [items]/[type]/[id]                     → ItemEnvelope (inline or external ref)
/// [blobs]/[Data(itemKey)]/[chunkIndex]    → Chunk data (only for external refs)
/// ```
///
/// **Usage**:
/// ```swift
/// let storage = ItemStorage(
///     transaction: tx,
///     blobsSubspace: blobsSub
/// )
///
/// // Write (auto-decides inline vs external)
/// try await storage.write(data, for: key)  // Note: async for overwrite cleanup
///
/// // Read with snapshot semantics
/// let data = try await storage.read(for: key, snapshot: true)
///
/// // Scan items (handles external refs transparently)
/// for try await (key, data) in storage.scan(range: range, snapshot: true) {
///     // data is always complete, regardless of storage type
/// }
/// ```
public struct ItemStorage: Sendable {
    // MARK: - Properties

    /// Underlying FDB transaction
    private let transaction: any TransactionProtocol

    /// Blobs subspace for large value chunks
    private let blobsSubspace: Subspace

    /// Transformer for compression
    private let transformer: TransformingSerializer

    /// Maximum inline size (before splitting)
    private let maxInlineSize: Int

    /// Chunk size for external storage
    private let chunkSize: Int

    // MARK: - Initialization

    /// Initialize ItemStorage
    ///
    /// - Parameters:
    ///   - transaction: FDB transaction to use
    ///   - blobsSubspace: Subspace for storing blob chunks
    ///   - maxInlineSize: Maximum size for inline storage (default: 90KB)
    ///   - chunkSize: Size of each chunk for external storage (default: 90KB)
    public init(
        transaction: any TransactionProtocol,
        blobsSubspace: Subspace,
        maxInlineSize: Int = ItemEnvelope.maxInlineSize,
        chunkSize: Int = ItemEnvelope.maxInlineSize
    ) {
        self.transaction = transaction
        self.blobsSubspace = blobsSubspace
        self.transformer = TransformingSerializer(configuration: .default)
        self.maxInlineSize = maxInlineSize
        self.chunkSize = chunkSize
    }

    // MARK: - Blob Key Helpers

    /// Get blob prefix for an item key
    /// Uses key bytes as single Tuple element for clearRange compatibility
    private func blobPrefix(for key: FDB.Bytes) -> Subspace {
        // Store the raw key bytes as a single Tuple element (byte string)
        blobsSubspace.subspace(Tuple([key]))
    }

    // MARK: - Write Operations

    /// Write an item with automatic compression and external storage
    ///
    /// Pipeline: cleanup old blobs → compress → inline or external → FDB
    ///
    /// - Parameters:
    ///   - data: The raw data to write
    ///   - key: The item key (in items subspace)
    public func write(_ data: FDB.Bytes, for key: FDB.Bytes) async throws {
        // Step 1: Clean up any existing blobs (handles overwrite case)
        try await clearExistingBlobs(for: key)

        // Step 2: Compress
        let compressed = try compress(data)

        // Step 3: Decide inline vs external
        if compressed.count <= maxInlineSize {
            // Inline: store directly with envelope
            let envelope = ItemEnvelope.inline(data: compressed)
            transaction.setValue(envelope.serialize(), for: key)
        } else {
            // External: store chunks in blobs subspace
            let chunkCount = (compressed.count + chunkSize - 1) / chunkSize

            guard chunkCount <= Int32.max else {
                throw ItemStorageError.valueTooLarge(size: compressed.count)
            }

            // Write chunks to blobs subspace
            // Key: [blobs]/[Data(itemKey)]/[chunkIndex]
            let blobBase = blobPrefix(for: key)

            var offset = 0
            for i in 0..<chunkCount {
                let chunkStart = offset
                let chunkEnd = min(offset + chunkSize, compressed.count)
                let chunk = Array(compressed[chunkStart..<chunkEnd])

                let chunkKey = blobBase.pack(Tuple([Int32(i)]))
                transaction.setValue(chunk, for: chunkKey)
                offset = chunkEnd
            }

            // Write envelope with external reference
            let ref = ItemEnvelope.ExternalRef(
                totalSize: Int64(compressed.count),
                chunkCount: Int32(chunkCount),
                chunkSize: Int32(chunkSize)
            )
            let envelope = ItemEnvelope.external(ref: ref)
            transaction.setValue(envelope.serialize(), for: key)
        }
    }

    /// Clear existing blobs if the item has external storage
    private func clearExistingBlobs(for key: FDB.Bytes) async throws {
        guard let envelopeBytes = try await transaction.getValue(for: key) else {
            return  // No existing data
        }

        guard ItemEnvelope.isEnvelope(envelopeBytes) else {
            return  // Not an envelope (shouldn't happen in new format)
        }

        let envelope = try ItemEnvelope.deserialize(envelopeBytes)

        if case .external = envelope.content {
            // Clear all blobs with range clear
            let blobBase = blobPrefix(for: key)
            let (begin, end) = blobBase.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Read Operations

    /// Read an item with snapshot semantics
    ///
    /// Pipeline: FDB → join if external → decompress → raw data
    ///
    /// - Parameters:
    ///   - key: The item key to read
    ///   - snapshot: If true, perform snapshot read (no conflict tracking)
    /// - Returns: The decompressed data, or nil if not found
    public func read(for key: FDB.Bytes, snapshot: Bool = false) async throws -> FDB.Bytes? {
        // Read envelope
        guard let envelopeBytes = try await transaction.getValue(for: key, snapshot: snapshot) else {
            return nil
        }

        // All data must be in envelope format
        guard ItemEnvelope.isEnvelope(envelopeBytes) else {
            throw ItemStorageError.notEnvelopeFormat
        }

        // Parse envelope
        let envelope = try ItemEnvelope.deserialize(envelopeBytes)

        // Get compressed data
        let compressed: FDB.Bytes
        switch envelope.content {
        case .inline(let data):
            compressed = data

        case .external(let ref):
            compressed = try await loadChunks(for: key, ref: ref, snapshot: snapshot)
        }

        // Decompress
        return try decompress(compressed)
    }

    /// Check if an item exists (without loading full data)
    ///
    /// - Parameters:
    ///   - key: The item key
    ///   - snapshot: If true, perform snapshot read
    /// - Returns: True if item exists
    public func exists(for key: FDB.Bytes, snapshot: Bool = false) async throws -> Bool {
        return try await transaction.getValue(for: key, snapshot: snapshot) != nil
    }

    // MARK: - Delete Operations

    /// Delete an item (handles external chunks with clearRange)
    ///
    /// - Parameter key: The item key to delete
    public func delete(for key: FDB.Bytes) async throws {
        // Check if external to clear blobs
        if let envelopeBytes = try await transaction.getValue(for: key),
           ItemEnvelope.isEnvelope(envelopeBytes) {
            let envelope = try ItemEnvelope.deserialize(envelopeBytes)

            if case .external = envelope.content {
                // Clear all blobs with range clear (efficient, no iteration)
                let blobBase = blobPrefix(for: key)
                let (begin, end) = blobBase.range()
                transaction.clearRange(beginKey: begin, endKey: end)
            }
        }

        // Clear the item key
        transaction.clear(key: key)
    }

    // MARK: - Scan Operations

    /// Scan items in a range with snapshot semantics
    ///
    /// Automatically handles external references, returning complete data for each item.
    ///
    /// - Parameters:
    ///   - begin: Start key (inclusive)
    ///   - end: End key (exclusive)
    ///   - snapshot: If true, perform snapshot reads
    ///   - limit: Maximum number of items (0 = unlimited)
    ///   - reverse: If true, scan in reverse order
    /// - Returns: AsyncSequence of (key, decompressed data) pairs
    public func scan(
        begin: FDB.Bytes,
        end: FDB.Bytes,
        snapshot: Bool = false,
        limit: Int = 0,
        reverse: Bool = false
    ) -> ItemScanSequence {
        ItemScanSequence(
            storage: self,
            begin: begin,
            end: end,
            snapshot: snapshot,
            limit: limit,
            reverse: reverse
        )
    }

    // MARK: - Internal: Chunk Operations

    /// Load chunks for an external reference
    func loadChunks(
        for key: FDB.Bytes,
        ref: ItemEnvelope.ExternalRef,
        snapshot: Bool
    ) async throws -> FDB.Bytes {
        let blobBase = blobPrefix(for: key)

        var result: [UInt8] = []
        result.reserveCapacity(Int(ref.totalSize))

        for i in 0..<ref.chunkCount {
            let chunkKey = blobBase.pack(Tuple([Int32(i)]))
            guard let chunk = try await transaction.getValue(for: chunkKey, snapshot: snapshot) else {
                throw ItemEnvelopeError.chunkMissing(index: Int(i))
            }
            result.append(contentsOf: chunk)
        }

        guard result.count == Int(ref.totalSize) else {
            throw ItemEnvelopeError.sizeMismatch(
                expected: Int(ref.totalSize),
                actual: result.count
            )
        }

        return result
    }

    // MARK: - Internal: Compression

    private func compress(_ value: FDB.Bytes) throws -> FDB.Bytes {
        let data = Data(value)
        let compressed = try transformer.serializeSync(data)
        return Array(compressed)
    }

    func decompress(_ value: FDB.Bytes) throws -> FDB.Bytes {
        guard !value.isEmpty else { return value }
        let data = Data(value)
        let decompressed = try transformer.deserializeSync(data)
        return Array(decompressed)
    }

    // MARK: - Direct Transaction Access

    /// Access the underlying transaction for non-item operations
    public var underlying: any TransactionProtocol {
        transaction
    }
}

// MARK: - ItemScanSequence

/// AsyncSequence for scanning items
public struct ItemScanSequence: AsyncSequence, Sendable {
    public typealias Element = (key: FDB.Bytes, data: FDB.Bytes)

    private let storage: ItemStorage
    private let begin: FDB.Bytes
    private let end: FDB.Bytes
    private let snapshot: Bool
    private let limit: Int
    private let reverse: Bool

    init(
        storage: ItemStorage,
        begin: FDB.Bytes,
        end: FDB.Bytes,
        snapshot: Bool,
        limit: Int,
        reverse: Bool
    ) {
        self.storage = storage
        self.begin = begin
        self.end = end
        self.snapshot = snapshot
        self.limit = limit
        self.reverse = reverse
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(
            storage: storage,
            begin: begin,
            end: end,
            snapshot: snapshot,
            limit: limit,
            reverse: reverse
        )
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        private let storage: ItemStorage
        private let snapshot: Bool
        private let begin: FDB.Bytes
        private let end: FDB.Bytes
        private let reverse: Bool
        private var count: Int = 0
        private let limit: Int
        private var results: [(FDB.Bytes, FDB.Bytes)] = []
        private var resultIndex: Int = 0
        private var loaded = false

        init(
            storage: ItemStorage,
            begin: FDB.Bytes,
            end: FDB.Bytes,
            snapshot: Bool,
            limit: Int,
            reverse: Bool
        ) {
            self.storage = storage
            self.snapshot = snapshot
            self.limit = limit
            self.begin = begin
            self.end = end
            self.reverse = reverse
        }

        public mutating func next() async throws -> Element? {
            // Check limit
            if limit > 0 && count >= limit {
                return nil
            }

            // Load all results on first access (getRange streams efficiently anyway)
            if !loaded {
                results = []
                let sequence = storage.underlying.getRange(
                    from: FDB.KeySelector.firstGreaterOrEqual(begin),
                    to: FDB.KeySelector.firstGreaterOrEqual(end),
                    limit: limit,
                    reverse: reverse,
                    snapshot: snapshot,
                    streamingMode: limit > 0 ? .small : .wantAll
                )
                for try await (key, value) in sequence {
                    results.append((key, value))
                }
                loaded = true
            }

            // Return next result
            guard resultIndex < results.count else {
                return nil
            }

            let (key, envelopeBytes) = results[resultIndex]
            resultIndex += 1
            count += 1

            // All data must be in envelope format
            guard ItemEnvelope.isEnvelope(envelopeBytes) else {
                throw ItemStorageError.notEnvelopeFormat
            }

            // Parse envelope and load data
            let envelope = try ItemEnvelope.deserialize(envelopeBytes)

            let compressed: FDB.Bytes
            switch envelope.content {
            case .inline(let data):
                compressed = data

            case .external(let ref):
                compressed = try await storage.loadChunks(for: key, ref: ref, snapshot: snapshot)
            }

            let data = try storage.decompress(compressed)
            return (key, data)
        }
    }
}

// MARK: - ItemStorageError

/// Errors from ItemStorage operations
public enum ItemStorageError: Error, CustomStringConvertible, Sendable {
    case valueTooLarge(size: Int)
    case notEnvelopeFormat

    public var description: String {
        switch self {
        case .valueTooLarge(let size):
            return "Value too large for storage: \(size) bytes"
        case .notEnvelopeFormat:
            return "Data is not in ItemEnvelope format - all items must use ItemStorage.write()"
        }
    }
}
