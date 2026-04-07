// ItemStorage.swift
// DatabaseEngine - Unified item storage abstraction
//
// Handles compression, large value splitting, and snapshot semantics.
// All item I/O should go through this layer.

import Foundation
import StorageKit
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
/// [blobs]/[Tuple([itemKeyBytes])]/[chunkIndex] → Chunk data (only for external refs)
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
    private let transaction: any Transaction

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
        transaction: any Transaction,
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
    private func blobPrefix(for key: Bytes) -> Subspace {
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
    ///   - isNewRecord: When true, skips clearing old blobs (optimization for known new inserts)
    public func write(_ data: Bytes, for key: Bytes, isNewRecord: Bool = false) async throws {
        // Step 1: Clear existing blobs only when overwriting (skip for known new records)
        if !isNewRecord {
            clearAllBlobs(for: key)
        }

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

    /// Clear all blob chunks for a key (efficient clearRange, no iteration).
    ///
    /// This is used for:
    /// - Overwrite (external → inline, external → external, etc.)
    /// - Delete
    /// - Cleanup even if the existing item value is corrupted/non-envelope
    private func clearAllBlobs(for key: Bytes) {
        let blobBase = blobPrefix(for: key)
        let (begin, end) = blobBase.range()
        transaction.clearRange(beginKey: begin, endKey: end)
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
    public func read(for key: Bytes, snapshot: Bool = false) async throws -> Bytes? {
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
        let compressed: Bytes
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
    public func exists(for key: Bytes, snapshot: Bool = false) async throws -> Bool {
        return try await transaction.getValue(for: key, snapshot: snapshot) != nil
    }

    // MARK: - Delete Operations

    /// Delete an item (handles external chunks with clearRange)
    ///
    /// - Parameter key: The item key to delete
    public func delete(for key: Bytes) async throws {
        // Always clear blob range (handles external storage and corrupted/non-envelope data)
        clearAllBlobs(for: key)

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
        begin: Bytes,
        end: Bytes,
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
        for key: Bytes,
        ref: ItemEnvelope.ExternalRef,
        snapshot: Bool
    ) async throws -> Bytes {
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

    private func compress(_ value: Bytes) throws -> Bytes {
        let data = Data(value)
        let compressed = try transformer.serializeSync(data)
        return Array(compressed)
    }

    func decompress(_ value: Bytes) throws -> Bytes {
        guard !value.isEmpty else { return value }
        let data = Data(value)
        let decompressed = try transformer.deserializeSync(data)
        return Array(decompressed)
    }

    // MARK: - Direct Transaction Access

    /// Access the underlying transaction for non-item operations
    public var underlying: any Transaction {
        transaction
    }
}

// MARK: - ItemScanSequence

/// AsyncSequence for scanning items
public struct ItemScanSequence: AsyncSequence, Sendable {
    public typealias Element = (key: Bytes, data: Bytes)

    private let storage: ItemStorage
    private let begin: Bytes
    private let end: Bytes
    private let snapshot: Bool
    private let limit: Int
    private let reverse: Bool

    init(
        storage: ItemStorage,
        begin: Bytes,
        end: Bytes,
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
        private let begin: Bytes
        private let end: Bytes
        private let reverse: Bool
        private let limit: Int
        private var count: Int = 0
        private var collected: [(Bytes, Bytes)]?
        private var index: Int = 0

        init(
            storage: ItemStorage,
            begin: Bytes,
            end: Bytes,
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
            // Lazily collect all KV pairs on first access
            if collected == nil {
                collected = try await storage.underlying.collectRange(
                    from: KeySelector.firstGreaterOrEqual(begin),
                    to: KeySelector.firstGreaterOrEqual(end),
                    limit: limit,
                    reverse: reverse,
                    snapshot: snapshot,
                    streamingMode: limit > 0 ? .small : .wantAll
                )
            }

            guard let items = collected, index < items.count else {
                return nil
            }

            let (key, envelopeBytes) = items[index]
            index += 1

            // All data must be in envelope format
            guard ItemEnvelope.isEnvelope(envelopeBytes) else {
                throw ItemStorageError.notEnvelopeFormat
            }

            // Parse envelope and load data
            let envelope = try ItemEnvelope.deserialize(envelopeBytes)

            let compressed: Bytes
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
